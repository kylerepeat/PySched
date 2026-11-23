"""Scheduler-center HTTP API and FastAPI application factory."""

import logging
from contextlib import asynccontextmanager
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .cron import CronError, CronSchedule
from .db import ConflictError, Database
from .handlers import REGISTRY
from .scanner import SchedulerLoop
from .timeutil import iso_now

logger = logging.getLogger("pysched.scheduler")


# --------------------------------------------------------------------- models
class TaskCreate(BaseModel):
    name: str = Field(min_length=1, max_length=128)
    handler: str
    trigger_type: Literal["cron", "interval"]
    cron_expr: str | None = None
    interval_seconds: int | None = Field(default=None, ge=1, le=365 * 24 * 3600)
    params: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: int = Field(default=60, ge=1, le=24 * 3600)
    max_retries: int = Field(default=0, ge=0, le=100)

    @model_validator(mode="after")
    def _validate_trigger(self) -> "TaskCreate":
        if self.trigger_type == "cron":
            if not self.cron_expr:
                raise ValueError("cron_expr is required when trigger_type=cron")
            try:
                CronSchedule(self.cron_expr)
            except CronError as exc:
                raise ValueError(f"invalid cron_expr: {exc}") from exc
        elif not self.interval_seconds:
            raise ValueError("interval_seconds is required when trigger_type=interval")
        if self.handler not in REGISTRY:
            raise ValueError(
                f"unknown handler {self.handler!r}; available: {sorted(REGISTRY)}"
            )
        return self


class TaskUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, min_length=1, max_length=128)
    handler: str | None = None
    trigger_type: Literal["cron", "interval"] | None = None
    cron_expr: str | None = None
    interval_seconds: int | None = Field(default=None, ge=1, le=365 * 24 * 3600)
    params: dict[str, Any] | None = None
    timeout_seconds: int | None = Field(default=None, ge=1, le=24 * 3600)
    max_retries: int | None = Field(default=None, ge=0, le=100)
    enabled: bool | None = None

    def validated_changes(self, existing: dict) -> dict[str, Any]:
        changes = self.model_dump(exclude_unset=True)
        merged = {**existing, **changes}
        trigger = merged["trigger_type"]
        if trigger == "cron":
            if not merged.get("cron_expr"):
                raise ValueError("cron_expr is required when trigger_type=cron")
            try:
                CronSchedule(merged["cron_expr"])
            except CronError as exc:
                raise ValueError(f"invalid cron_expr: {exc}") from exc
        elif not merged.get("interval_seconds"):
            raise ValueError(
                "interval_seconds is required when trigger_type=interval"
            )
        if merged["handler"] not in REGISTRY:
            raise ValueError(
                f"unknown handler {merged['handler']!r}; available: {sorted(REGISTRY)}"
            )
        return changes


class WorkerRegister(BaseModel):
    name: str = Field(min_length=1, max_length=128)
    base_url: str = Field(min_length=1)

    @field_validator("base_url")
    @classmethod
    def _check_url(cls, value: str) -> str:
        if not value.startswith(("http://", "https://")):
            raise ValueError("base_url must start with http:// or https://")
        return value.rstrip("/")


class CallbackRequest(BaseModel):
    run_id: int
    success: bool
    result: str | None = None
    error: str | None = None
    worker_name: str
    finished_at: str | None = None


class ManualTrigger(BaseModel):
    params: dict[str, Any] | None = None


# ----------------------------------------------------------------- factory
def create_app(
    db: Database,
    scan_interval: float = 1.0,
    heartbeat_timeout: float = 15.0,
    start_loop: bool = True,
) -> FastAPI:
    loop = SchedulerLoop(
        db,
        scan_interval=scan_interval,
        heartbeat_timeout=heartbeat_timeout,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if start_loop:
            loop.start()
        yield
        loop.stop()

    app = FastAPI(title="PySched scheduler center", lifespan=lifespan)
    app.state.db = db
    app.state.loop = loop

    def get_task_or_404(task_id: int) -> dict:
        task = db.get_task(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail=f"task {task_id} not found")
        return task

    # ------------------------------------------------------------ task CRUD
    @app.post("/tasks", status_code=201)
    def create_task(body: TaskCreate) -> dict:
        try:
            return db.create_task(body.model_dump())
        except ConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.get("/tasks")
    def list_tasks() -> list[dict]:
        return db.list_tasks()

    @app.get("/tasks/{task_id}")
    def get_task(task_id: int) -> dict:
        return get_task_or_404(task_id)

    @app.put("/tasks/{task_id}")
    def update_task(task_id: int, body: TaskUpdate) -> dict:
        existing = get_task_or_404(task_id)
        try:
            changes = body.validated_changes(existing)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        # Changing schedule definition starts a fresh anchor so old missed
        # points are not backfilled after the edit.
        if "trigger_type" in changes or "cron_expr" in changes or "interval_seconds" in changes:
            changes.setdefault("last_scheduled_at", iso_now(seconds_only=True))
        try:
            updated = db.update_task(task_id, changes)
        except ConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return updated

    @app.delete("/tasks/{task_id}", status_code=204)
    def delete_task(task_id: int) -> None:
        if not db.delete_task(task_id):
            raise HTTPException(status_code=404, detail=f"task {task_id} not found")

    @app.post("/tasks/{task_id}/pause")
    def pause_task(task_id: int) -> dict:
        get_task_or_404(task_id)
        return db.update_task(task_id, {"enabled": False})

    @app.post("/tasks/{task_id}/resume")
    def resume_task(task_id: int) -> dict:
        get_task_or_404(task_id)
        # Reset the anchor on resume so the task waits one fresh interval
        # instead of immediately backfilling everything missed while paused.
        return db.update_task(
            task_id,
            {"enabled": True, "last_scheduled_at": iso_now(seconds_only=True)},
        )

    @app.post("/tasks/{task_id}/trigger", status_code=201)
    def manual_trigger(task_id: int, body: ManualTrigger | None = None) -> dict:
        task = get_task_or_404(task_id)
        # Microsecond precision makes manual points unique even if a user
        # double-clicks; the UNIQUE constraint remains the hard backstop.
        scheduled = iso_now(seconds_only=False)
        override = body.params if body and body.params is not None else None
        try:
            return db.insert_run(task, scheduled, trigger="manual", params=override)
        except ConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    # ------------------------------------------------------------- workers
    @app.post("/workers/register")
    def register_worker(body: WorkerRegister) -> dict:
        return db.upsert_worker(body.name, body.base_url)

    @app.get("/workers")
    def list_workers() -> dict:
        all_workers = db.list_workers()
        online_names = {w["name"] for w in db.list_online_workers(heartbeat_timeout)}
        for worker in all_workers:
            worker["status"] = "online" if worker["name"] in online_names else "offline"
        return {
            "heartbeat_timeout_seconds": heartbeat_timeout,
            "workers": all_workers,
        }

    # ---------------------------------------------------------------- runs
    @app.get("/runs")
    def list_runs(
        task_id: int | None = Query(default=None),
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> list[dict]:
        return db.list_runs(task_id=task_id, limit=limit)

    @app.get("/runs/{run_id}")
    def get_run(run_id: int) -> dict:
        run = db.get_run(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail=f"run {run_id} not found")
        return run

    @app.post("/runs/{run_id}/callback")
    def run_callback(run_id: int, body: CallbackRequest) -> dict:
        if body.run_id != run_id:
            raise HTTPException(status_code=400, detail="run_id mismatch")
        run = db.apply_callback(
            run_id,
            success=body.success,
            result=body.result,
            error=body.error,
            worker_name=body.worker_name,
        )
        if run is None:
            raise HTTPException(status_code=404, detail=f"run {run_id} not found")
        return run

    # ------------------------------------------------------------- operations
    @app.post("/scan")
    def force_scan() -> dict:
        """Trigger one scan immediately (handy for tests and debugging)."""
        return loop.scan_once()

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "service": "scheduler"}

    return app
