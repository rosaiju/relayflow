"""Registry of the only code RelayFlow will execute.

Workflow definitions refer to task types by name; there is deliberately no way
to submit shell commands or code. Handlers receive a TaskContext for
cooperative cancellation and timeouts, and the task's materialized input.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID


class PermanentTaskError(Exception):
    """Retrying cannot help (e.g. invalid document). Fails the task immediately."""


class TaskCancelled(Exception):
    """Raised by TaskContext.check() when the run was cancelled or ownership was lost."""


class TaskTimedOut(Exception):
    """Raised by TaskContext.check() when the task's deadline passed."""


@dataclass
class TaskContext:
    run_id: UUID
    task_key: str
    attempt_number: int
    deadline: float  # time.monotonic() value
    notify_url: str = "http://127.0.0.1:8100"
    fault_injection_enabled: bool = False
    cancel_event: threading.Event = field(default_factory=threading.Event)
    timeout_event: threading.Event = field(default_factory=threading.Event)
    lost_event: threading.Event = field(default_factory=threading.Event)

    def deadline_remaining(self) -> float:
        return self.deadline - time.monotonic()

    def cancelled(self) -> bool:
        return self.cancel_event.is_set() or self.lost_event.is_set()

    def check(self) -> None:
        """Cooperative check point: raise if the handler should stop now."""
        if self.timeout_event.is_set() or time.monotonic() >= self.deadline:
            raise TaskTimedOut(f"task '{self.task_key}' exceeded its timeout")
        if self.lost_event.is_set():
            raise TaskCancelled("lease ownership lost")
        if self.cancel_event.is_set():
            raise TaskCancelled("run cancellation requested")

    def sleep(self, seconds: float) -> None:
        """Sleep in small slices, checking for cancellation and timeout."""
        end = time.monotonic() + seconds
        while True:
            self.check()
            remaining = end - time.monotonic()
            if remaining <= 0:
                return
            self.cancel_event.wait(min(remaining, 0.05))


Handler = Callable[[TaskContext, dict[str, Any]], dict[str, Any]]

_REGISTRY: dict[str, Handler] = {}


def task_type(name: str) -> Callable[[Handler], Handler]:
    def decorator(fn: Handler) -> Handler:
        if name in _REGISTRY:
            raise ValueError(f"task type {name!r} registered twice")
        _REGISTRY[name] = fn
        return fn

    return decorator


def _load_builtin() -> None:
    # Importing the modules registers their handlers.
    from relayflow.tasks import demo, document, notify  # noqa: F401


def get_handler(name: str) -> Handler:
    _load_builtin()
    try:
        return _REGISTRY[name]
    except KeyError:
        raise PermanentTaskError(f"unsupported task type {name!r}") from None


def registered_types() -> frozenset[str]:
    _load_builtin()
    return frozenset(_REGISTRY)
