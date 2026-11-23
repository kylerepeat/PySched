"""End-to-end flows that mirror the manual acceptance procedure."""

import time

from tests.conftest import http_get, http_post, wait_until


def _create_task(sched, name, **overrides):
    payload = {
        "name": name,
        "handler": "echo",
        "trigger_type": "interval",
        "interval_seconds": 2,
        "params": {"message": "hello"},
        "timeout_seconds": 30,
        "max_retries": 0,
    }
    payload.update(overrides)
    resp = http_post(f"{sched.base_url}/tasks", json=payload)
    assert resp.status_code == 201, resp.text
    return resp.json()


def _runs(sched, task_id):
    return http_get(
        f"{sched.base_url}/runs", params={"task_id": task_id, "limit": 200}
    ).json()


def test_interval_dispatch_single_record_per_point_with_worker_output(
    make_scheduler, make_worker
):
    sched = make_scheduler(scan_interval=0.2, heartbeat_timeout=6.0)
    make_worker("worker-1", [sched.base_url], heartbeat_interval=1.0)
    task = _create_task(sched, "every-2s", interval_seconds=2)

    def enough_runs():
        return len(_runs(sched, task["id"])) >= 3

    wait_until(enough_runs, timeout=20.0, message="not enough scheduled runs")
    time.sleep(1.5)  # let the latest run finish and call back

    runs = _runs(sched, task["id"])
    # One row per planned time point, no duplicates.
    scheduled_times = [r["scheduled_time"] for r in runs]
    assert len(scheduled_times) == len(set(scheduled_times))

    finished = [r for r in runs if r["status"] == "succeeded"]
    assert len(finished) >= 3
    for run in finished:
        assert run["worker_name"] == "worker-1"
        assert run["result"] == "hello"
        assert run["attempt"] == 1


def test_pause_and_resume(make_scheduler, make_worker):
    sched = make_scheduler(scan_interval=0.2)
    make_worker("worker-1", [sched.base_url])
    task = _create_task(sched, "pausable", interval_seconds=1)

    wait_until(lambda: len(_runs(sched, task["id"])) >= 2, timeout=15.0)
    http_post(f"{sched.base_url}/tasks/{task['id']}/pause")
    time.sleep(2.5)
    during_pause = len(_runs(sched, task["id"]))
    time.sleep(1.0)
    assert len(_runs(sched, task["id"])) == during_pause  # nothing new while paused

    http_post(f"{sched.base_url}/tasks/{task['id']}/resume")
    wait_until(
        lambda: len(_runs(sched, task["id"])) > during_pause,
        timeout=15.0,
        message="task did not resume scheduling",
    )


def test_failure_automatic_retry_on_same_run_row(make_scheduler, make_worker):
    sched = make_scheduler(scan_interval=0.2)
    make_worker("worker-1", [sched.base_url])
    task = _create_task(
        sched,
        "flaky-task",
        handler="flaky",
        interval_seconds=3600,  # only the manual trigger matters
        max_retries=2,
        params={"fail_times": 1},
    )
    run = http_post(f"{sched.base_url}/tasks/{task['id']}/trigger").json()

    def succeeded():
        fresh = http_get(f"{sched.base_url}/runs/{run['id']}").json()
        return fresh["status"] == "succeeded"

    wait_until(succeeded, timeout=15.0, message="flaky task never succeeded")
    final = http_get(f"{sched.base_url}/runs/{run['id']}").json()
    assert final["attempt"] == 2
    assert final["result"] == "ok after 2 attempts"
    assert [entry["success"] for entry in final["attempts_log"]] == [False, True]
    # Retries reuse the single run row for this time point.
    assert len(_runs(sched, task["id"])) == 1


def test_retry_exhaustion_marks_failed(make_scheduler, make_worker):
    sched = make_scheduler(scan_interval=0.2)
    make_worker("worker-1", [sched.base_url])
    task = _create_task(
        sched,
        "always-fails",
        handler="flaky",
        interval_seconds=3600,
        max_retries=1,
        params={"fail_times": 99},
    )
    run = http_post(f"{sched.base_url}/tasks/{task['id']}/trigger").json()

    wait_until(
        lambda: http_get(f"{sched.base_url}/runs/{run['id']}").json()["status"]
        == "failed",
        timeout=20.0,
        message="task was not marked failed after retry exhaustion",
    )
    final = http_get(f"{sched.base_url}/runs/{run['id']}").json()
    assert final["attempt"] == 2
    assert len(final["attempts_log"]) == 2
    assert len(_runs(sched, task["id"])) == 1


def test_timeout_is_reported_back(make_scheduler, make_worker):
    sched = make_scheduler(scan_interval=0.2)
    make_worker("worker-1", [sched.base_url])
    task = _create_task(
        sched,
        "slow-task",
        handler="slow",
        interval_seconds=3600,
        timeout_seconds=1,
        max_retries=0,
        params={"sleep_seconds": 30},
    )
    run = http_post(f"{sched.base_url}/tasks/{task['id']}/trigger").json()

    wait_until(
        lambda: http_get(f"{sched.base_url}/runs/{run['id']}").json()["status"]
        == "timeout",
        timeout=15.0,
        message="slow task was not marked timeout",
    )
    final = http_get(f"{sched.base_url}/runs/{run['id']}").json()
    assert final["error"].startswith("TIMEOUT:")
    assert final["worker_name"] == "worker-1"


def test_failover_to_second_worker_when_first_unreachable(make_scheduler, make_worker):
    sched = make_scheduler(scan_interval=0.2, heartbeat_timeout=6.0)
    worker1 = make_worker("worker-1", [sched.base_url], heartbeat_interval=1.0)
    make_worker("worker-2", [sched.base_url], heartbeat_interval=1.0)

    # Wait for both workers to show online.
    wait_until(
        lambda: {w["name"] for w in http_get(f"{sched.base_url}/workers").json()["workers"]}
        == {"worker-1", "worker-2"},
        message="both workers did not register",
    )

    # Kill worker-1 without deregistering. It stays "online" until heartbeat
    # timeout, but the scheduler's connection attempt fails and it must try
    # worker-2 immediately.
    worker1.stop()
    task = _create_task(
        sched, "failover", interval_seconds=3600, params={"message": "by worker-2"}
    )
    run = http_post(f"{sched.base_url}/tasks/{task['id']}/trigger").json()

    wait_until(
        lambda: http_get(f"{sched.base_url}/runs/{run['id']}").json()["status"]
        == "succeeded",
        timeout=20.0,
        message="run did not fail over to worker-2",
    )
    final = http_get(f"{sched.base_url}/runs/{run['id']}").json()
    assert final["worker_name"] == "worker-2"
    assert final["result"] == "by worker-2"


def test_offline_detection_after_heartbeat_timeout(make_scheduler, make_worker):
    sched = make_scheduler(scan_interval=0.2, heartbeat_timeout=3.0)
    worker1 = make_worker("worker-1", [sched.base_url], heartbeat_interval=1.0)
    make_worker("worker-2", [sched.base_url], heartbeat_interval=1.0)

    worker1.stop()
    wait_until(
        lambda: next(
            w["status"]
            for w in http_get(f"{sched.base_url}/workers").json()["workers"]
            if w["name"] == "worker-1"
        )
        == "offline",
        timeout=12.0,
        message="worker-1 was not marked offline after heartbeat timeout",
    )
    statuses = {
        w["name"]: w["status"]
        for w in http_get(f"{sched.base_url}/workers").json()["workers"]
    }
    assert statuses == {"worker-1": "offline", "worker-2": "online"}


def test_two_scheduler_instances_schedule_each_point_once(
    make_scheduler, make_worker
):
    # Two scheduler processes sharing one SQLite file, one worker registered
    # with both of them.
    sched_a = make_scheduler(scan_interval=0.2, heartbeat_timeout=6.0)
    sched_b = make_scheduler(scan_interval=0.2, heartbeat_timeout=6.0)
    make_worker(
        "worker-1",
        [sched_a.base_url, sched_b.base_url],
        heartbeat_interval=1.0,
    )

    # Create the task through scheduler A.
    task = _create_task(sched_a, "dual-scheduler", interval_seconds=2)

    wait_until(lambda: len(_runs(sched_b, task["id"])) >= 4, timeout=25.0)
    time.sleep(2.0)

    # Both scheduler instances see the same rows through the shared database.
    # Poll briefly because the two API reads can otherwise straddle a commit.
    def views_agree():
        return len(_runs(sched_a, task["id"])) == len(_runs(sched_b, task["id"]))

    wait_until(views_agree, timeout=10.0, message="scheduler views never converged")
    runs_a = _runs(sched_a, task["id"])
    runs_b = _runs(sched_b, task["id"])
    assert len(runs_a) == len(runs_b)

    scheduled_times = [r["scheduled_time"] for r in runs_a]
    assert len(scheduled_times) == len(set(scheduled_times)), (
        "a scheduled point was dispatched twice"
    )
    assert all(r["status"] == "succeeded" for r in runs_a)
