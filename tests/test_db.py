"""Database-level guarantees: unique scheduled points and callback retry state machine."""

import pytest

from pysched.db import ConflictError, Database


def _make_task(db: Database, name: str = "t1", max_retries: int = 2) -> dict:
    return db.create_task(
        {
            "name": name,
            "handler": "echo",
            "trigger_type": "interval",
            "interval_seconds": 5,
            "params": {},
            "timeout_seconds": 30,
            "max_retries": max_retries,
        }
    )


def test_unique_constraint_blocks_duplicate_scheduled_point(db: Database):
    task = _make_task(db)
    run = db.insert_run(task, "2026-11-24T10:00:00+00:00", trigger="scheduled")
    assert run["status"] == "pending"
    with pytest.raises(ConflictError):
        db.insert_run(task, "2026-11-24T10:00:00+00:00", trigger="scheduled")


def test_claim_is_conditional_and_single_winner(db: Database):
    task = _make_task(db)
    run = db.insert_run(task, "2026-11-24T10:00:00+00:00", trigger="scheduled")
    first = db.claim_pending_run(run["id"])
    second = db.claim_pending_run(run["id"])
    assert first is not None and first["status"] == "running"
    assert second is None  # another scheduler instance loses the race


def test_release_makes_run_claimable_again(db: Database):
    task = _make_task(db)
    run = db.insert_run(task, "2026-11-24T10:00:00+00:00", trigger="scheduled")
    db.claim_pending_run(run["id"])
    db.release_pending_run(run["id"])
    reclaimed = db.claim_pending_run(run["id"])
    assert reclaimed is not None and reclaimed["status"] == "running"


def test_callback_success_terminal(db: Database):
    task = _make_task(db)
    run = db.insert_run(task, "2026-11-24T10:00:00+00:00", trigger="scheduled")
    updated = db.apply_callback(run["id"], True, "done", None, "worker-1")
    assert updated["status"] == "succeeded"
    assert updated["attempt"] == 1
    assert updated["worker_name"] == "worker-1"
    assert updated["result"] == "done"

    # A late duplicate callback must not change the terminal state.
    again = db.apply_callback(run["id"], False, None, "late", "worker-2")
    assert again["status"] == "succeeded"
    assert again["worker_name"] == "worker-1"


def test_callback_failure_retries_then_fails_on_same_row(db: Database):
    task = _make_task(db, max_retries=2)
    run = db.insert_run(task, "2026-11-24T10:00:00+00:00", trigger="scheduled")
    run_id = run["id"]

    first = db.apply_callback(run_id, False, None, "boom-1", "worker-1")
    assert first["status"] == "pending" and first["attempt"] == 1
    assert len(first["attempts_log"]) == 1

    second = db.apply_callback(run_id, False, None, "boom-2", "worker-2")
    assert second["status"] == "pending" and second["attempt"] == 2

    third = db.apply_callback(run_id, False, None, "boom-3", "worker-1")
    assert third["status"] == "failed" and third["attempt"] == 3
    assert third["error"] == "boom-3"
    # Still exactly one run row for this scheduled point.
    assert len(db.list_runs(task_id=task["id"])) == 1


def test_failure_then_success_within_retry_budget(db: Database):
    task = _make_task(db, max_retries=1)
    run = db.insert_run(task, "2026-11-24T10:00:00+00:00", trigger="scheduled")
    db.apply_callback(run["id"], False, None, "first fails", "worker-1")
    final = db.apply_callback(run["id"], True, "ok", None, "worker-2")
    assert final["status"] == "succeeded"
    assert final["attempt"] == 2
    assert [entry["success"] for entry in final["attempts_log"]] == [False, True]


def test_timeout_status_preserved(db: Database):
    task = _make_task(db, max_retries=0)
    run = db.insert_run(task, "2026-11-24T10:00:00+00:00", trigger="scheduled")
    final = db.apply_callback(
        run["id"], False, None, "TIMEOUT: exceeded 2s", "worker-1"
    )
    assert final["status"] == "timeout"


def test_worker_heartbeat_upsert_and_online_window(db: Database):
    db.upsert_worker("worker-1", "http://127.0.0.1:9001")
    assert len(db.list_online_workers(heartbeat_timeout=15.0)) == 1
    # A generous timeout still sees the fresh heartbeat; a zero timeout means
    # the worker is immediately stale.
    assert db.list_online_workers(heartbeat_timeout=0.0) == []
