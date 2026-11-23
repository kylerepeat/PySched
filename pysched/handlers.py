"""Built-in sample task handlers used for testing and demos.

A handler is any callable with the signature::

    def handler(params: dict, context: dict) -> str

``params`` comes from the task configuration (or manual trigger), ``context``
carries runtime information: ``run_id``, ``attempt`` (1-based), ``task_name``
and ``worker_name``. The returned string becomes the run record's ``result``.
Raising an exception marks the attempt failed and triggers a retry when
retries remain.
"""

import time
from typing import Callable

Handler = Callable[[dict, dict], str]

REGISTRY: dict[str, Handler] = {}


def register(name: str) -> Callable[[Handler], Handler]:
    def decorator(func: Handler) -> Handler:
        REGISTRY[name] = func
        return func

    return decorator


@register("echo")
def echo(params: dict, context: dict) -> str:
    """Always succeeds. Prints and returns the configured message."""
    message = str(params.get("message", "hello"))
    print(f"[echo] task={context['task_name']} attempt={context['attempt']} -> {message}")
    return message


@register("flaky")
def flaky(params: dict, context: dict) -> str:
    """Fails for the first ``fail_times`` attempts, then succeeds.

    Defaults to failing once so it demonstrates "first failure, then success"
    with max_retries >= 1.
    """
    fail_times = int(params.get("fail_times", 1))
    if context["attempt"] <= fail_times:
        print(
            f"[flaky] task={context['task_name']} attempt={context['attempt']} "
            f"failing on purpose ({context['attempt']}/{fail_times})"
        )
        raise RuntimeError(f"simulated failure on attempt {context['attempt']}")
    print(
        f"[flaky] task={context['task_name']} attempt={context['attempt']} succeeded"
    )
    return f"ok after {context['attempt']} attempts"


@register("slow")
def slow(params: dict, context: dict) -> str:
    """Sleeps for ``sleep_seconds`` (default 30). Pair with a short task
    timeout to exercise timeout handling and retry exhaustion."""
    sleep_seconds = float(params.get("sleep_seconds", 30))
    print(
        f"[slow] task={context['task_name']} attempt={context['attempt']} "
        f"sleeping {sleep_seconds}s"
    )
    time.sleep(sleep_seconds)
    return f"slept {sleep_seconds}s"


def get_handler(name: str) -> Handler | None:
    return REGISTRY.get(name)
