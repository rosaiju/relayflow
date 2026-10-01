"""Small task types used by tests, demos, and benchmarks."""

from __future__ import annotations

from typing import Any

from relayflow.tasks.registry import TaskContext, task_type

MAX_SLEEP_SECONDS = 60.0


@task_type("demo.noop")
def noop(ctx: TaskContext, task_input: dict[str, Any]) -> dict[str, Any]:
    ctx.check()
    return {"ok": True}


@task_type("demo.sleep")
def sleep(ctx: TaskContext, task_input: dict[str, Any]) -> dict[str, Any]:
    run_input = task_input.get("run_input") or {}
    params = task_input.get("params") or {}
    seconds = run_input.get("seconds", params.get("seconds", 0))
    if not isinstance(seconds, (int, float)) or isinstance(seconds, bool):
        seconds = 0
    seconds = max(0.0, min(float(seconds), MAX_SLEEP_SECONDS))
    ctx.sleep(seconds)
    return {"slept_seconds": seconds}


@task_type("demo.fail")
def fail(ctx: TaskContext, task_input: dict[str, Any]) -> dict[str, Any]:
    ctx.check()
    raise RuntimeError(f"demo.fail always fails (attempt {ctx.attempt_number})")
