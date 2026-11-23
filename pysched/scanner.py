"""Scheduler background loops: due-run creation, dispatch and stale-run reaping.

Multiple scheduler processes run identical loops against the same SQLite
database. Safety against double dispatch comes from two layers:

* ``runs(task_id, scheduled_time)`` UNIQUE constraint when creating due runs
  (``BEGIN IMMEDIATE`` serialises the writers);
* the ``pending -> running`` claim is a conditional UPDATE, so two schedulers
  can never dispatch the same pending row.
"""

import itertools
import json
import logging
import threading
from datetime import timedelta

import httpx

from .cron import CronSchedule
from .db import ConflictError, Database
from .timeutil import iso_now, parse_iso, utcnow

logger = logging.getLogger("pysched.scheduler")

# Safety valve: never backfill more than this many missed points per scan.
MAX_BACKFILL = 10
# A run stuck in 'running' longer than timeout + grace is reaped as timed out.
RUN_GRACE_SECONDS = 15.0


class SchedulerLoop:
    def __init__(
        self,
        db: Database,
        scan_interval: float = 1.0,
        heartbeat_timeout: float = 15.0,
        http_client: httpx.Client | None = None,
    ):
        self.db = db
        self.scan_interval = scan_interval
        self.heartbeat_timeout = heartbeat_timeout
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._rr = itertools.count()
        self._http = http_client or httpx.Client(timeout=8.0)
        self._owns_http = http_client is None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run, name="scheduler-loop", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        if self._owns_http:
            self._http.close()

    def _run(self) -> None:
        while not self._stop.wait(self.scan_interval):
            try:
                self.scan_once()
            except Exception:  # noqa: BLE001 - loop must never die
                logger.exception("scheduler scan failed")

    # ------------------------------------------------------------------- scan
    def scan_once(self) -> dict[str, int]:
        created = self._create_due_runs()
        dispatched = self._dispatch_pending_runs()
        reaped = self._reap_stale_runs()
        stale_workers = self.db.mark_offline_workers(self.heartbeat_timeout)
        if stale_workers:
            logger.info("%d worker(s) currently past heartbeat timeout", stale_workers)
        return {"created": created, "dispatched": dispatched, "reaped": reaped}

    # ------------------------------------------------------------- due runs
    def _create_due_runs(self) -> int:
        created = 0
        now = utcnow()
        for task in self.db.list_enabled_tasks():
            try:
                due_points = self._due_points(task, now)
            except Exception:  # noqa: BLE001
                logger.exception("failed to compute schedule for task %s", task["id"])
                continue
            for point in due_points:
                scheduled = point.isoformat()
                try:
                    self.db.insert_run(task, scheduled, trigger="scheduled")
                    created += 1
                    logger.info(
                        "scheduled task %s (%s) at %s",
                        task["name"],
                        task["id"],
                        scheduled,
                    )
                except ConflictError:
                    # Another scheduler instance already created this point.
                    pass
            if due_points:
                self.db.set_task_last_scheduled(
                    task["id"], due_points[-1].replace(microsecond=0).isoformat()
                )
        return created

    def _due_points(self, task: dict, now) -> list:
        if task["trigger_type"] == "interval":
            return self._interval_due_points(task, now)
        return self._cron_due_points(task, now)

    @staticmethod
    def _interval_due_points(task: dict, now) -> list:
        interval = int(task["interval_seconds"])
        last_text = task.get("last_scheduled_at")
        if last_text:
            anchor = parse_iso(last_text)
        else:
            # First activation: fire one interval after creation, on a whole
            # second boundary.
            anchor = parse_iso(task["created_at"]).replace(microsecond=0)
        points = []
        candidate = anchor + timedelta(seconds=interval)
        guard = 0
        while candidate <= now and guard < MAX_BACKFILL:
            points.append(candidate)
            candidate += timedelta(seconds=interval)
            guard += 1
        return points

    @staticmethod
    def _cron_due_points(task: dict, now) -> list:
        schedule = CronSchedule(task["cron_expr"])
        last_text = task.get("last_scheduled_at")
        if last_text:
            cursor = parse_iso(last_text)
        else:
            cursor = parse_iso(task["created_at"]).replace(second=0, microsecond=0)
        points = []
        guard = 0
        while guard < MAX_BACKFILL:
            nxt = schedule.next_after(cursor)
            if nxt > now:
                break
            points.append(nxt)
            cursor = nxt
            guard += 1
        return points

    # -------------------------------------------------------------- dispatch
    def _dispatch_pending_runs(self) -> int:
        dispatched = 0
        workers = self.db.list_online_workers(self.heartbeat_timeout)
        for run in self.db.list_pending_runs():
            if not workers:
                logger.debug("run %s pending but no online worker", run["id"])
                continue
            claimed = self.db.claim_pending_run(run["id"])
            if claimed is None:
                continue  # another scheduler instance claimed it
            task = self.db.get_task(run["task_id"])
            if task is None:
                # Task was deleted while queued: retire the run silently.
                self.db.mark_run_terminal(run["id"], "failed", "task deleted")
                continue

            ordered = self._rotate(workers)
            delivered = False
            last_error = "no worker reachable"
            for worker in ordered:
                if self._send_to_worker(worker, task, claimed):
                    self.db.assign_worker(run["id"], worker["name"])
                    delivered = True
                    dispatched += 1
                    logger.info(
                        "dispatched run %s (task %s) to worker %s",
                        run["id"],
                        task["name"],
                        worker["name"],
                    )
                    break
                last_error = f"worker {worker['name']} unreachable"
                logger.warning("worker %s unreachable, trying next", worker["name"])
            if not delivered:
                # Keep the run pending for the next scan; next round will pick
                # another (or the same recovered) worker.
                self.db.release_pending_run(run["id"])
                logger.error("run %s not dispatched: %s", run["id"], last_error)
        return dispatched

    def _rotate(self, workers: list[dict]) -> list[dict]:
        """Round-robin start position so workers share the load evenly."""
        if not workers:
            return []
        start = next(self._rr) % len(workers)
        return workers[start:] + workers[:start]

    def _send_to_worker(self, worker: dict, task: dict, run: dict) -> bool:
        payload = {
            "run_id": run["id"],
            "task_name": task["name"],
            "handler": task["handler"],
            "params": run["params"],
            "timeout_seconds": task["timeout_seconds"],
            "attempt": run["attempt"] + 1,
        }
        try:
            resp = self._http.post(f"{worker['base_url']}/execute", json=payload)
            resp.raise_for_status()
            return True
        except (httpx.HTTPError, httpx.InvalidURL) as exc:
            logger.warning("dispatch to %s failed: %s", worker["name"], exc)
            return False

    # ------------------------------------------------------------------ reaper
    def _reap_stale_runs(self) -> int:
        reaped = 0
        now = utcnow()
        for run in self.db.list_running_runs():
            task = self.db.get_task(run["task_id"])
            timeout = task["timeout_seconds"] if task else 60
            updated = parse_iso(run["updated_at"])
            deadline = updated + timedelta(seconds=timeout + RUN_GRACE_SECONDS)
            if now <= deadline:
                continue
            # The worker is presumed dead/lost. If retries remain, requeue;
            # otherwise close the run out as timeout.
            if run["attempt"] < run["max_retries"]:
                self.db.release_pending_run(run["id"])
                logger.warning("run %s requeued after worker silence", run["id"])
            else:
                self.db.mark_run_terminal(
                    run["id"], "timeout", f"TIMEOUT: no callback within {timeout}s"
                )
                logger.warning("run %s reaped as timeout", run["id"])
            reaped += 1
        return reaped
