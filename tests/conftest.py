"""Shared pytest fixtures: real uvicorn servers on loopback ports."""

import socket
import threading
import time
from collections.abc import Callable, Iterator

import httpx
import pytest
import uvicorn

from pysched.db import Database
from pysched.scheduler import create_app as create_scheduler_app
from pysched.worker import Worker, create_app as create_worker_app


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class ServerHandle:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self._server: uvicorn.Server | None = None
        self._thread: threading.Thread | None = None

    def start(self, app) -> None:
        config = uvicorn.Config(app, host="127.0.0.1", port=free_port(), log_level="warning")
        self._server = uvicorn.Server(config)
        self.base_url = f"http://127.0.0.1:{config.port}"
        self._thread = threading.Thread(target=self._server.run, daemon=True)
        self._thread.start()
        self.wait_ready()

    def wait_ready(self, timeout: float = 10.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                resp = httpx.get(f"{self.base_url}/health", timeout=1.0)
                if resp.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.05)
        raise RuntimeError(f"server at {self.base_url} did not become ready")

    def stop(self) -> None:
        if self._server:
            self._server.should_exit = True
        if self._thread:
            self._thread.join(timeout=10)


@pytest.fixture
def db_path(tmp_path) -> Iterator[str]:
    path = tmp_path / "test_pysched.db"
    yield str(path)


@pytest.fixture
def db(db_path) -> Database:
    return Database(db_path)


@pytest.fixture
def make_scheduler(db_path) -> Iterator[Callable[..., ServerHandle]]:
    handles: list[ServerHandle] = []

    def _make(
        scan_interval: float = 0.2, heartbeat_timeout: float = 6.0
    ) -> ServerHandle:
        database = Database(db_path)
        app = create_scheduler_app(
            database,
            scan_interval=scan_interval,
            heartbeat_timeout=heartbeat_timeout,
        )
        handle = ServerHandle("")
        handle.start(app)
        handles.append(handle)
        return handle

    yield _make

    for handle in handles:
        handle.stop()


@pytest.fixture
def make_worker() -> Iterator[Callable[..., ServerHandle]]:
    handles: list[ServerHandle] = []

    def _make(
        name: str,
        scheduler_urls: list[str],
        heartbeat_interval: float = 1.0,
    ) -> ServerHandle:
        worker = Worker(
            name=name,
            base_url="http://127.0.0.1:0",  # replaced with the real port below
            scheduler_urls=scheduler_urls,
            heartbeat_interval=heartbeat_interval,
        )
        app = create_worker_app(worker)
        handle = ServerHandle("")
        handle.start(app)
        # The worker registered at startup with the placeholder URL; correct it.
        worker.base_url = handle.base_url
        worker.register()
        handles.append(handle)
        return handle

    yield _make

    for handle in handles:
        handle.stop()


def http_post(url: str, **kwargs) -> httpx.Response:
    return httpx.post(url, timeout=10.0, **kwargs)


def http_get(url: str, **kwargs) -> httpx.Response:
    return httpx.get(url, timeout=10.0, **kwargs)


def wait_until(
    predicate: Callable[[], bool],
    timeout: float = 20.0,
    interval: float = 0.2,
    message: str = "condition not met in time",
) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except Exception as exc:  # noqa: BLE001 - tolerate transient HTTP errors
            last_error = exc
        time.sleep(interval)
    raise AssertionError(f"{message} (last error: {last_error})")
