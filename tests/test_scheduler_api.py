"""HTTP API tests for the scheduler center."""

from tests.conftest import http_get, http_post, wait_until


def _interval_payload(name: str, **overrides):
    payload = {
        "name": name,
        "handler": "echo",
        "trigger_type": "interval",
        "interval_seconds": 60,
        "params": {"message": "hi"},
        "timeout_seconds": 30,
        "max_retries": 0,
    }
    payload.update(overrides)
    return payload


def test_task_crud_lifecycle(make_scheduler):
    sched = make_scheduler()
    created = http_post(f"{sched.base_url}/tasks", json=_interval_payload("crud")).json()
    task_id = created["id"]
    assert created["enabled"] is True

    listed = http_get(f"{sched.base_url}/tasks").json()
    assert any(t["id"] == task_id for t in listed)

    updated = http_put(
        sched,
        f"/tasks/{task_id}",
        {"params": {"message": "changed"}, "max_retries": 3},
    )
    assert updated["params"] == {"message": "changed"}
    assert updated["max_retries"] == 3

    assert http_get(f"{sched.base_url}/tasks/{task_id}").status_code == 200
    assert http_post(f"{sched.base_url}/tasks/{task_id}/pause").json()["enabled"] is False
    assert http_post(f"{sched.base_url}/tasks/{task_id}/resume").json()["enabled"] is True

    response = http_post(f"{sched.base_url}/tasks", json=_interval_payload("crud"))
    assert response.status_code == 409  # duplicate name

    assert http_delete(sched, f"/tasks/{task_id}").status_code == 204
    assert http_get(f"{sched.base_url}/tasks/{task_id}").status_code == 404


def test_validation_errors_for_bad_tasks(make_scheduler):
    sched = make_scheduler()
    # cron trigger without expression
    bad_cron = http_post(
        f"{sched.base_url}/tasks",
        json={
            "name": "bad-cron",
            "handler": "echo",
            "trigger_type": "cron",
            "timeout_seconds": 5,
            "max_retries": 0,
        },
    )
    assert bad_cron.status_code == 422

    # malformed cron expression
    bad_expr = http_post(
        f"{sched.base_url}/tasks",
        json={
            "name": "bad-expr",
            "handler": "echo",
            "trigger_type": "cron",
            "cron_expr": "99 * * * *",
            "timeout_seconds": 5,
            "max_retries": 0,
        },
    )
    assert bad_expr.status_code == 422

    # unknown handler
    bad_handler = http_post(
        f"{sched.base_url}/tasks",
        json=_interval_payload("bad-handler", handler="does-not-exist"),
    )
    assert bad_handler.status_code == 422


def test_manual_trigger_creates_run(make_scheduler):
    sched = make_scheduler()
    task = http_post(
        f"{sched.base_url}/tasks", json=_interval_payload("manual-only")
    ).json()
    run = http_post(
        f"{sched.base_url}/tasks/{task['id']}/trigger",
        json={"params": {"message": "manual"}},
    ).json()
    assert run["trigger"] == "manual"
    assert run["params"] == {"message": "manual"}
    assert run["status"] == "pending"


def test_worker_registration_and_online_listing(make_scheduler, make_worker):
    sched = make_scheduler(heartbeat_timeout=6.0)
    make_worker("worker-1", [sched.base_url], heartbeat_interval=1.0)

    wait_until(
        lambda: http_get(f"{sched.base_url}/workers").json()["workers"][0]["status"]
        == "online",
        message="worker did not register online",
    )
    workers = http_get(f"{sched.base_url}/workers").json()
    assert workers["workers"][0]["name"] == "worker-1"
    assert workers["heartbeat_timeout_seconds"] == 6.0


# thin wrappers kept here so test bodies read like the rest of the suite
def http_put(sched, path, payload):
    import httpx

    return httpx.put(f"{sched.base_url}{path}", json=payload, timeout=10.0).json()


def http_delete(sched, path):
    import httpx

    return httpx.delete(f"{sched.base_url}{path}", timeout=10.0)
