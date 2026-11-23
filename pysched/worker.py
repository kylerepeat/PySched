"""Executor (worker) service.

Lifecycle
---------
1. On startup the worker registers itself with every configured scheduler and
   then starts a heartbeat thread that re-registers periodically.
2. The scheduler dispatches work by calling ``POST /execute``. Requests are
   accepted onto a bounded thread pool and the endpoint returns immediately
   with 202 Accepted.
3. Each execution runs in its own supervisor thread so the task's timeout can
   be enforced even if the handler blocks. On completion (or timeout) the
   worker calls back the scheduler's ``POST /runs/{id}/callback``.
"""

import logging
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .handlers import get_handler
from .timeutil import iso_now

logger = logging.getLogger("pysched.worker")


class ExecuteRequest(BaseModel):
    run_id: int
    task_name: str
    handler: str
    params: dict = Field(default_factory=dict)
    timeout_seconds: int = 60
    attempt: int = 1


class Worker:
    def __init__(
        self,
        name: str,
        base_url: str,
        scheduler_urls: list[str],
        heartbeat_interval: float = 5.0,
        max_workers: int = 4,
    ):
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.scheduler_urls = [url.rstrip("/") for url in scheduler_urls]
        self.heartbeat_interval = heartbeat_interval
        self.pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="task")
        self._stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        self._http = httpx.Client(timeout=10.0)
        # Run ids currently being handled, for idempotent duplicate dispatch.
        self._inflight: set[int] = set()
        self._inflight_lock = threading.Lock()

    # ------------------------------------------------------------ registration
    def register(self) -> None:
        payload = {"name": self.name, "base_url": self.base_url}
        for url in self.scheduler_urls:
            try:
                resp = self._http.post(f"{url}/workers/register", json=payload)
                resp.raise_for_status()
                logger.info("registered with scheduler %s", url)
            except httpx.HTTPError as exc:
                logger.warning("register with %s failed: %s", url, exc)

    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(self.heartbeat_interval):
            self.register()

    def start(self) -> None:
        # Make sure the scheduler knows us before we accept any work.
        self.register()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, name="heartbeat", daemon=True
        )
        self._heartbeat_thread.start()

    def stop(self) -> None:
        self._stop.set()
        self.pool.shutdown(wait=False, cancel_futures=True)
        self._http.close()

    # --------------------------------------------------------------- execution
    def submit(self, request: ExecuteRequest) -> bool:
        """Return True if accepted, False if this run is already in flight."""
        with self._inflight_lock:
            if request.run_id in self._inflight:
                return False
            self._inflight.add(request.run_id)
        self.pool.submit(self._supervise, request)
        return True

    def _supervise(self, request: ExecuteRequest) -> None:
        result_holder: dict[str, str] = {}
        error_holder: dict[str, str] = {}

        def runner() -> None:
            try:
                handler = get_handler(request.handler)
                if handler is None:
                    raise ValueError(f"unknown handler {request.handler!r}")
                context = {
                    "run_id": request.run_id,
                    "attempt": request.attempt,
                    "task_name": request.task_name,
                    "worker_name": self.name,
                }
                result_holder["result"] = handler(request.params, context)
            except Exception as exc:  # noqa: BLE001 - report every failure
                error_holder["error"] = f"{type(exc).__name__}: {exc}"
                logger.error(
                    "task %s run %s failed:\n%s",
                    request.task_name,
                    request.run_id,
                    traceback.format_exc(),
                )

        thread = threading.Thread(
            target=runner, name=f"run-{request.run_id}", daemon=True
        )
        thread.start()
        thread.join(timeout=request.timeout_seconds)

        timed_out = thread.is_alive()
        success = not timed_out and not error_holder
        if timed_out:
            # The handler thread is a daemon and keeps running until the
            # process exits or the blocking call returns; the run itself is
            # treated as failed by timeout regardless.
            error = f"TIMEOUT: exceeded {request.timeout_seconds}s"
            result = None
            logger.warning(
                "task %s run %s timed out after %ss",
                request.task_name,
                request.run_id,
                request.timeout_seconds,
            )
        else:
            error = error_holder.get("error")
            result = result_holder.get("result")

        self._callback(request, success=success, result=result, error=error)
        with self._inflight_lock:
            self._inflight.discard(request.run_id)

    def _callback(
        self,
        request: ExecuteRequest,
        *,
        success: bool,
        result: str | None,
        error: str | None,
    ) -> None:
        payload = {
            "run_id": request.run_id,
            "success": success,
            "result": result,
            "error": error,
            "worker_name": self.name,
            "finished_at": iso_now(seconds_only=False),
        }
        # Try every scheduler; they share the same SQLite database so the
        # first one that accepts the callback wins, the rest see a terminal run.
        for url in self.scheduler_urls:
            try:
                resp = self._http.post(
                    f"{url}/runs/{request.run_id}/callback", json=payload
                )
                resp.raise_for_status()
                logger.info(
                    "callback for run %s delivered to %s (success=%s)",
                    request.run_id,
                    url,
                    success,
                )
                return
            except httpx.HTTPError as exc:
                logger.warning("callback to %s failed: %s", url, exc)
        logger.error("run %s callback could not reach any scheduler", request.run_id)


def create_app(worker: Worker) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        worker.start()
        yield
        worker.stop()

    app = FastAPI(title=f"PySched worker {worker.name}", lifespan=lifespan)
    app.state.worker = worker

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "worker": worker.name, "time": iso_now(False)}

    @app.post("/execute", status_code=202)
    def execute(request: ExecuteRequest) -> dict:
        if get_handler(request.handler) is None:
            raise HTTPException(
                status_code=404, detail=f"unknown handler {request.handler!r}"
            )
        accepted = worker.submit(request)
        if not accepted:
            # Duplicate delivery of the same run: idempotent acceptance.
            return {"accepted": False, "run_id": request.run_id, "worker": worker.name}
        return {"accepted": True, "run_id": request.run_id, "worker": worker.name}

    return app
