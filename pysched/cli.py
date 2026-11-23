"""Command-line entry points: ``pysched-scheduler`` and ``pysched-worker``."""

import argparse
import logging

import uvicorn

from .db import Database
from .scheduler import create_app as create_scheduler_app
from .worker import Worker, create_app as create_worker_app


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )


def scheduler_main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run a PySched scheduler center")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument(
        "--db", default="pysched.db", help="SQLite file path (shared by all scheduler instances)"
    )
    parser.add_argument("--scan-interval", type=float, default=1.0)
    parser.add_argument(
        "--heartbeat-timeout",
        type=float,
        default=15.0,
        help="seconds without heartbeat before a worker is considered offline",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    _setup_logging(args.verbose)
    db = Database(args.db)
    app = create_scheduler_app(
        db,
        scan_interval=args.scan_interval,
        heartbeat_timeout=args.heartbeat_timeout,
    )
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


def worker_main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run a PySched executor worker")
    parser.add_argument("--name", required=True, help="unique worker name, e.g. worker-1")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument(
        "--scheduler-url",
        action="append",
        required=True,
        help="scheduler base URL; repeat the flag to register with several instances",
    )
    parser.add_argument("--heartbeat-interval", type=float, default=5.0)
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    _setup_logging(args.verbose)
    base_url = f"http://{args.host}:{args.port}"
    worker = Worker(
        name=args.name,
        base_url=base_url,
        scheduler_urls=args.scheduler_url,
        heartbeat_interval=args.heartbeat_interval,
        max_workers=args.max_workers,
    )
    app = create_worker_app(worker)
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")
