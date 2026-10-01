"""Fault injection for tests and local demos only. Disabled by default.

A fault fires only when BOTH are true:
  * the worker runs with RELAYFLOW_ENABLE_FAULT_INJECTION=1, and
  * the run's input contains `demo.fail_attempts` / `demo.crash_after_execute`,
    which the API accepts only when its own RELAYFLOW_ENABLE_FAULT_INJECTION=1.
`demo.delay_seconds` is a bounded cooperative delay, not a fault, and always applies.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from relayflow.tasks.registry import TaskContext

log = logging.getLogger("relayflow.faults")
CRASH_EXIT_CODE = 137


class InjectedFailure(RuntimeError):
    """A retryable failure injected on purpose."""


def _demo(task_input: dict[str, Any]) -> dict[str, Any]:
    demo = (task_input.get("run_input") or {}).get("demo")
    return demo if isinstance(demo, dict) else {}


def _listed(option: Any, ctx: TaskContext) -> bool:
    if not isinstance(option, dict):
        return False
    attempts = option.get(ctx.task_key)
    return isinstance(attempts, list) and ctx.attempt_number in attempts


def before_handler(ctx: TaskContext, task_input: dict[str, Any]) -> None:
    demo = _demo(task_input)
    delay = (demo.get("delay_seconds") or {}).get(ctx.task_key)
    if isinstance(delay, (int, float)) and not isinstance(delay, bool) and delay > 0:
        ctx.sleep(min(float(delay), 30.0))
    if ctx.fault_injection_enabled and _listed(demo.get("fail_attempts"), ctx):
        raise InjectedFailure(f"injected failure on attempt {ctx.attempt_number}")


def crash_after_handler(ctx: TaskContext, task_input: dict[str, Any]) -> None:
    """Simulate a crash after the task's effect happened but before RelayFlow hears about it."""
    if ctx.fault_injection_enabled and _listed(_demo(task_input).get("crash_after_execute"), ctx):
        log.warning(
            "FAULT INJECTION: exiting after %s attempt %s, before reporting",
            ctx.task_key,
            ctx.attempt_number,
        )
        logging.shutdown()
        os._exit(CRASH_EXIT_CODE)
