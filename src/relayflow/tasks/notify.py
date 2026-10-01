"""Deliver a "report ready" event to the mock notification service.

The Idempotency-Key is derived from (run_id, task_key) and NOT from the attempt
number, so every attempt of this task sends the same key. If a worker crashes
after the service accepted the notification but before RelayFlow recorded
success, the retry is recognized by the service as a duplicate. This protection
exists only because the receiving service implements idempotency.
"""

from __future__ import annotations

from typing import Any

import httpx

from relayflow.tasks.registry import PermanentTaskError, TaskContext, task_type


def idempotency_key(run_id: object, task_key: str) -> str:
    return f"relayflow:{run_id}:{task_key}"


@task_type("notify.report_ready")
def report_ready(ctx: TaskContext, task_input: dict[str, Any]) -> dict[str, Any]:
    ctx.check()
    report = (task_input.get("deps") or {}).get("report")
    if not isinstance(report, dict):
        raise PermanentTaskError("notify requires the 'report' dependency output")
    key = idempotency_key(ctx.run_id, ctx.task_key)
    payload = {
        "event": "report_ready",
        "run_id": str(ctx.run_id),
        "title": report.get("title"),
        "summary": report.get("summary"),
        "report_digest": report.get("report_digest"),
    }
    remaining = max(1.0, ctx.deadline_remaining())
    response = httpx.post(
        f"{ctx.notify_url}/notifications",
        json=payload,
        headers={"Idempotency-Key": key},
        timeout=min(10.0, remaining),
    )
    if response.status_code == 409:
        raise PermanentTaskError(f"notification service rejected key {key}: {response.text}")
    response.raise_for_status()  # other errors are retryable
    body = response.json()
    return {
        "notification_id": body["id"],
        "idempotency_key": key,
        "duplicate": body.get("duplicate", False),
    }
