"""SQLite persistence for tasks, worker nodes and run records.

Concurrency model
-----------------
Multiple scheduler processes may share one database file. WAL mode is enabled
for concurrent readers, and every write runs in an explicit ``BEGIN IMMEDIATE``
transaction so writes are serialised. The ``runs`` table carries
``UNIQUE(task_id, scheduled_time)`` which is the hard guarantee that a given
task at a given planned time is only ever dispatched once, regardless of how
many scheduler instances scan simultaneously.
"""

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any

from .timeutil import iso_now

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    name              TEXT NOT NULL UNIQUE,
    handler           TEXT NOT NULL,
    trigger_type      TEXT NOT NULL CHECK (trigger_type IN ('cron', 'interval')),
    cron_expr         TEXT,
    interval_seconds  INTEGER,
    params            TEXT NOT NULL DEFAULT '{}',
    timeout_seconds   INTEGER NOT NULL DEFAULT 60,
    max_retries       INTEGER NOT NULL DEFAULT 0,
    enabled           INTEGER NOT NULL DEFAULT 1,
    last_scheduled_at TEXT,
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workers (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    name           TEXT NOT NULL UNIQUE,
    base_url       TEXT NOT NULL,
    last_heartbeat TEXT NOT NULL,
    created_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id        INTEGER NOT NULL,
    task_name      TEXT NOT NULL,
    scheduled_time TEXT NOT NULL,
    trigger        TEXT NOT NULL CHECK (trigger IN ('scheduled', 'manual')),
    status         TEXT NOT NULL DEFAULT 'pending',
    attempt        INTEGER NOT NULL DEFAULT 0,
    max_retries    INTEGER NOT NULL DEFAULT 0,
    worker_name    TEXT,
    params         TEXT NOT NULL DEFAULT '{}',
    result         TEXT,
    error          TEXT,
    attempts_log   TEXT NOT NULL DEFAULT '[]',
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    UNIQUE (task_id, scheduled_time)
);

CREATE INDEX IF NOT EXISTS idx_runs_task ON runs (task_id);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs (status);
CREATE INDEX IF NOT EXISTS idx_workers_heartbeat ON workers (last_heartbeat);
"""


class ConflictError(Exception):
    """Raised when a UNIQUE constraint is hit (e.g. duplicate scheduled run)."""


class Database:
    def __init__(self, path: str | Path):
        self.path = str(path)
        self._lock = threading.RLock()
        self.init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def init_schema(self) -> None:
        with self._lock, self._connect() as conn:
            conn.executescript(SCHEMA)

    # ------------------------------------------------------------------ tasks
    def create_task(self, data: dict[str, Any]) -> dict[str, Any]:
        now = iso_now(seconds_only=False)
        with self._lock, self._connect() as conn:
            try:
                cur = conn.execute(
                    """
                    INSERT INTO tasks (name, handler, trigger_type, cron_expr,
                                       interval_seconds, params, timeout_seconds,
                                       max_retries, enabled, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                    """,
                    (
                        data["name"],
                        data["handler"],
                        data["trigger_type"],
                        data.get("cron_expr"),
                        data.get("interval_seconds"),
                        json.dumps(data.get("params") or {}),
                        data["timeout_seconds"],
                        data["max_retries"],
                        now,
                        now,
                    ),
                )
            except sqlite3.IntegrityError as exc:  # duplicate name
                raise ConflictError(f"task name {data['name']!r} already exists") from exc
            return self.get_task(cur.lastrowid, conn=conn)

    def list_tasks(self) -> list[dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute("SELECT * FROM tasks ORDER BY id").fetchall()
            return [_task_row(r) for r in rows]

    def get_task(
        self, task_id: int, conn: sqlite3.Connection | None = None
    ) -> dict[str, Any] | None:
        own = conn is None
        conn = conn or self._connect()
        try:
            row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            return _task_row(row) if row else None
        finally:
            if own:
                conn.close()

    def get_task_by_name(self, name: str) -> dict[str, Any] | None:
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT * FROM tasks WHERE name = ?", (name,)).fetchone()
            return _task_row(row) if row else None

    def update_task(self, task_id: int, changes: dict[str, Any]) -> dict[str, Any] | None:
        if not changes:
            return self.get_task(task_id)
        columns = set(
            "name handler trigger_type cron_expr interval_seconds params "
            "timeout_seconds max_retries enabled".split()
        )
        changes = {k: v for k, v in changes.items() if k in columns}
        if not changes:
            return self.get_task(task_id)
        assignments = ", ".join(f"{key} = ?" for key in changes)
        values: list[Any] = [
            json.dumps(v) if key == "params" else v for key, v in changes.items()
        ]
        values.append(iso_now(seconds_only=False))
        values.append(task_id)
        with self._lock, self._connect() as conn:
            try:
                cur = conn.execute(
                    f"UPDATE tasks SET {assignments}, updated_at = ? WHERE id = ?",
                    values,
                )
            except sqlite3.IntegrityError as exc:
                raise ConflictError("task name already in use") from exc
            if cur.rowcount == 0:
                return None
            return self.get_task(task_id, conn=conn)

    def delete_task(self, task_id: int) -> bool:
        with self._lock, self._connect() as conn:
            cur = conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
            return cur.rowcount > 0

    def list_enabled_tasks(self) -> list[dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM tasks WHERE enabled = 1 ORDER BY id"
            ).fetchall()
            return [_task_row(r) for r in rows]

    # ---------------------------------------------------------------- workers
    def upsert_worker(self, name: str, base_url: str) -> dict[str, Any]:
        now = iso_now(seconds_only=False)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO workers (name, base_url, last_heartbeat, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    base_url = excluded.base_url,
                    last_heartbeat = excluded.last_heartbeat
                """,
                (name, base_url, now, now),
            )
            row = conn.execute("SELECT * FROM workers WHERE name = ?", (name,)).fetchone()
            return _worker_row(row)

    def list_workers(self) -> list[dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute("SELECT * FROM workers ORDER BY name").fetchall()
            return [_worker_row(r) for r in rows]

    def list_online_workers(self, heartbeat_timeout: float) -> list[dict[str, Any]]:
        cutoff = _iso_past(heartbeat_timeout)
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM workers WHERE last_heartbeat > ? ORDER BY name",
                (cutoff,),
            ).fetchall()
            return [_worker_row(r) for r in rows]

    def mark_offline_workers(self, heartbeat_timeout: float) -> int:
        """Workers don't have a status column; callers derive online/offline
        from heartbeat freshness, so nothing is persisted here. Kept as an
        explicit hook returning the number of currently stale workers."""
        cutoff = _iso_past(heartbeat_timeout)
        with self._lock, self._connect() as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM workers WHERE last_heartbeat <= ?", (cutoff,)
            ).fetchone()[0]

    # ------------------------------------------------------------------- runs
    def insert_run(
        self,
        task: dict[str, Any],
        scheduled_time: str,
        trigger: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = iso_now(seconds_only=False)
        with self._lock, self._connect() as conn:
            try:
                cur = conn.execute(
                    """
                    INSERT INTO runs (task_id, task_name, scheduled_time, trigger,
                                      status, max_retries, params, created_at, updated_at)
                    VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?)
                    """,
                    (
                        task["id"],
                        task["name"],
                        scheduled_time,
                        trigger,
                        task["max_retries"],
                        json.dumps(params if params is not None else task["params"]),
                        now,
                        now,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise ConflictError(
                    f"run for task {task['id']} at {scheduled_time} already exists"
                ) from exc
            return self.get_run(cur.lastrowid, conn=conn)

    def get_run(
        self, run_id: int, conn: sqlite3.Connection | None = None
    ) -> dict[str, Any] | None:
        own = conn is None
        conn = conn or self._connect()
        try:
            row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
            return _run_row(row) if row else None
        finally:
            if own:
                conn.close()

    def list_runs(
        self, task_id: int | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        limit = min(max(limit, 1), 1000)
        with self._lock, self._connect() as conn:
            if task_id is None:
                rows = conn.execute(
                    "SELECT * FROM runs ORDER BY id DESC LIMIT ?", (limit,)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM runs WHERE task_id = ? ORDER BY id DESC LIMIT ?",
                    (task_id, limit),
                ).fetchall()
            return [_run_row(r) for r in rows]

    def list_pending_runs(self) -> list[dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM runs WHERE status = 'pending' ORDER BY id"
            ).fetchall()
            return [_run_row(r) for r in rows]

    def list_running_runs(self) -> list[dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM runs WHERE status = 'running' ORDER BY id"
            ).fetchall()
            return [_run_row(r) for r in rows]

    def claim_pending_run(self, run_id: int) -> dict[str, Any] | None:
        """Atomically flip pending -> running. Returns None if another
        scheduler (or the reaper) got there first."""
        now = iso_now(seconds_only=False)
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "UPDATE runs SET status = 'running', updated_at = ? "
                "WHERE id = ? AND status = 'pending'",
                (now, run_id),
            )
            if cur.rowcount == 0:
                return None
            return self.get_run(run_id, conn=conn)

    def release_pending_run(self, run_id: int) -> None:
        """Roll a claim back to pending when no worker could be reached."""
        now = iso_now(seconds_only=False)
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE runs SET status = 'pending', updated_at = ? "
                "WHERE id = ? AND status = 'running'",
                (now, run_id),
            )

    def assign_worker(self, run_id: int, worker_name: str) -> None:
        now = iso_now(seconds_only=False)
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE runs SET worker_name = ?, updated_at = ? WHERE id = ?",
                (worker_name, now, run_id),
            )

    def apply_callback(
        self,
        run_id: int,
        success: bool,
        result: str | None,
        error: str | None,
        worker_name: str,
    ) -> dict[str, Any] | None:
        """Apply a worker completion callback.

        Retry semantics: ``max_retries`` is the number of *retries* after the
        first attempt. A failed attempt reschedules the same run row back to
        ``pending`` until attempts are exhausted, then marks it ``failed``.
        The UNIQUE(task_id, scheduled_time) constraint stays intact because
        retries never create extra rows.
        """
        now = iso_now(seconds_only=False)
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
            if row is None:
                return None
            current = _run_row(row)
            if current["status"] in ("succeeded", "failed", "timeout"):
                # Duplicate / late callback for a terminal run: ignore.
                return current

            attempt = current["attempt"] + 1
            log = list(current["attempts_log"])
            log.append(
                {
                    "attempt": attempt,
                    "worker": worker_name,
                    "success": bool(success),
                    "error": error,
                    "finished_at": now,
                }
            )

            if success:
                conn.execute(
                    """
                    UPDATE runs SET status = 'succeeded', attempt = ?, worker_name = ?,
                                    result = ?, error = NULL, attempts_log = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (attempt, worker_name, result, json.dumps(log), now, run_id),
                )
            else:
                will_retry = attempt <= current["max_retries"]
                new_status = "pending" if will_retry else (
                    "timeout" if error and error.startswith("TIMEOUT:") else "failed"
                )
                conn.execute(
                    """
                    UPDATE runs SET status = ?, attempt = ?, worker_name = ?,
                                    result = ?, error = ?, attempts_log = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        new_status,
                        attempt,
                        worker_name,
                        result,
                        error,
                        json.dumps(log),
                        now,
                        run_id,
                    ),
                )
            return self.get_run(run_id, conn=conn)

    def mark_run_terminal(
        self, run_id: int, status: str, error: str
    ) -> dict[str, Any] | None:
        """Reaper hook: force a stuck run to a terminal state (timeout)."""
        assert status in ("timeout", "failed")
        now = iso_now(seconds_only=False)
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
            if row is None:
                return None
            current = _run_row(row)
            if current["status"] in ("succeeded", "failed", "timeout"):
                return current
            log = list(current["attempts_log"])
            log.append(
                {
                    "attempt": current["attempt"] + 1,
                    "worker": current["worker_name"],
                    "success": False,
                    "error": error,
                    "finished_at": now,
                }
            )
            conn.execute(
                "UPDATE runs SET status = ?, attempt = attempt + 1, error = ?, "
                "attempts_log = ?, updated_at = ? WHERE id = ?",
                (status, error, json.dumps(log), now, run_id),
            )
            return self.get_run(run_id, conn=conn)

    def set_task_last_scheduled(self, task_id: int, scheduled_at: str) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE tasks SET last_scheduled_at = ?, updated_at = updated_at WHERE id = ?",
                (scheduled_at, task_id),
            )


# ------------------------------------------------------------- row conversion
def _task_row(row: sqlite3.Row) -> dict[str, Any]:
    data = dict(row)
    data["params"] = json.loads(data.get("params") or "{}")
    data["enabled"] = bool(data["enabled"])
    return data


def _worker_row(row: sqlite3.Row) -> dict[str, Any]:
    return dict(row)


def _run_row(row: sqlite3.Row) -> dict[str, Any]:
    data = dict(row)
    data["params"] = json.loads(data.get("params") or "{}")
    data["attempts_log"] = json.loads(data.get("attempts_log") or "[]")
    return data


def _iso_past(seconds: float) -> str:
    from datetime import timedelta

    from .timeutil import utcnow

    return (utcnow() - timedelta(seconds=seconds)).isoformat()
