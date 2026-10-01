"""Automated crash-recovery demonstration against the Docker Compose stack.

    uv run python scripts/demo_recovery.py            # Windows PowerShell or any shell

Every step asserts on persisted state (API + direct PostgreSQL invariant checks);
the script exits non-zero if any assertion fails. It never deletes the database
volume. Fault injection is enabled for this stack only (local demo).

Steps
  1. start the stack with two workers (fault injection on, short leases)
  2. submit a document workflow: `keywords` is slowed down, and the worker that
     runs `notify` attempt 1 will crash after the notification is delivered
  3. kill the worker container executing `keywords`
  4. observe lease expiry and reassignment to the other worker
  5. verify completed tasks were preserved and the workflow finishes
  6. verify the notify crash produced exactly one logical notification
  7. restart PostgreSQL mid-run (same volume) and verify that run still completes
  8. restart every service without deleting the volume; verify history and results
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parent.parent
API = "http://127.0.0.1:8000/api"
MOCK = "http://127.0.0.1:8100"
DB_URL = "postgresql+psycopg://relayflow:relayflow@127.0.0.1:5433/relayflow"
LEASE, HEARTBEAT = 6, 2
ENV = {
    **os.environ,
    "RELAYFLOW_ENABLE_FAULT_INJECTION": "1",
    "RELAYFLOW_LEASE_SECONDS": str(LEASE),
    "RELAYFLOW_HEARTBEAT_SECONDS": str(HEARTBEAT),
    "RELAYFLOW_SCHEDULER_INTERVAL_SECONDS": "0.5",
}
DOCUMENT = {
    "title": "Recovery demo: lighthouse inspection (synthetic)",
    "text": (
        "The inspection team climbed the lighthouse at dawn. The lamp, the lens and the "
        "generator were inspected. The lens needed cleaning; the generator needed fuel. "
        "The team cleaned the lens, refuelled the generator and logged the inspection."
    ),
}
LOG: list[str] = []


def say(message: str) -> None:
    line = f"[{time.strftime('%H:%M:%S')}] {message}"
    LOG.append(line)
    print(line, flush=True)


def check(condition: bool, message: str) -> None:
    if not condition:
        say(f"FAIL: {message}")
        raise SystemExit(1)
    say(f"  ok  {message}")


def compose(*args: str, quiet: bool = True) -> None:
    subprocess.run(
        ["docker", "compose", *args], cwd=ROOT, env=ENV, check=True, capture_output=quiet
    )


def wait(predicate: Callable[[], Any], what: str, timeout: float = 60) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            value = predicate()
        except httpx.HTTPError:
            value = None
        if value:
            return value
        time.sleep(0.3)
    say(f"FAIL: timed out after {timeout}s waiting for {what}")
    raise SystemExit(1)


def get(path: str) -> Any:
    response = httpx.get(f"{API}{path}", timeout=5)
    response.raise_for_status()
    return response.json()


def task(run: dict[str, Any], key: str) -> dict[str, Any]:
    return next(t for t in run["tasks"] if t["task_key"] == key)


def invariants() -> list[str]:
    sys.path.insert(0, str(ROOT / "src"))
    from relayflow.db import make_engine
    from relayflow.testing import check_invariants

    engine = make_engine(DB_URL, pool_size=1)
    try:
        return check_invariants(engine)
    finally:
        engine.dispose()


def healthy_workers() -> list[dict[str, Any]]:
    return [w for w in get("/workers") if w["healthy"]]


def main() -> int:
    say("1. Starting the stack (2 workers, scheduler, API, mock notification service)")
    compose(
        "up",
        "-d",
        "--build",
        "--wait",
        "postgres",
        "mocknotify",
        "api",
        "scheduler",
        "worker-a",
        "worker-b",
        "dashboard",
    )
    wait(lambda: len(healthy_workers()) >= 2, "two healthy workers")
    check(get("/health")["fault_injection"] is True, "fault injection enabled for this demo stack")

    say("2. Submitting the document workflow")
    demo = {"delay_seconds": {"keywords": 8}, "crash_after_execute": {"notify": [1]}}
    response = httpx.post(
        f"{API}/runs",
        timeout=10,
        json={
            "workflow_name": "document-processing",
            "input": {**DOCUMENT, "demo": demo},
            "idempotency_key": f"demo-{int(time.time())}",
        },
    )
    response.raise_for_status()
    run_id = response.json()["run_id"]
    say(f"   run {run_id}  dashboard: http://127.0.0.1:8080/runs/{run_id}")

    say("3. Waiting for `keywords` to start, then killing the worker that runs it")
    run = wait(
        lambda: (r := get(f"/runs/{run_id}")) and task(r, "keywords")["status"] == "running" and r,
        "keywords to be running",
    )
    victim_attempt = task(run, "keywords")["attempts"][-1]
    victim = victim_attempt["worker_id"].split(":")[0]
    validate_before = task(run, "validate")
    check(validate_before["status"] == "succeeded", "validate already succeeded before the crash")
    compose("kill", victim)
    say(
        f"   killed container {victim} (attempt {victim_attempt['attempt_number']}, "
        f"worker {victim_attempt['worker_id']})"
    )

    say("4. Waiting for lease expiry and reassignment")
    run = wait(
        lambda: (r := get(f"/runs/{run_id}")) and len(task(r, "keywords")["attempts"]) >= 2 and r,
        "a second keywords attempt",
        timeout=LEASE * 4,
    )
    first, second = task(run, "keywords")["attempts"][:2]
    check(first["status"] == "lease_expired", "attempt 1 recorded as lease_expired")
    check(
        second["worker_id"].split(":")[0] != victim,
        f"attempt 2 claimed by another worker ({second['worker_id'].split(':')[0]})",
    )
    gap = (
        datetime.fromisoformat(second["started_at"]) - datetime.fromisoformat(first["heartbeat_at"])
    ).total_seconds()
    say(
        f"   last heartbeat {first['heartbeat_at']} -> reassigned {second['started_at']} "
        f"({gap:.2f}s, PostgreSQL time; lease {LEASE}s)"
    )
    compose("up", "-d", victim)  # bring the killed worker back for the rest of the demo
    say(f"   restarted {victim}")

    say("5-6. Waiting for the run to finish (the notify worker will crash once on purpose)")
    run = wait(
        lambda: (
            (r := get(f"/runs/{run_id}"))
            and r["status"] in ("succeeded", "failed", "cancelled")
            and r
        ),
        "the run to finish",
        timeout=120,
    )
    check(run["status"] == "succeeded", "run succeeded")
    validate_after = task(run, "validate")
    check(len(validate_after["attempts"]) == 1, "validate was not re-executed")
    check(
        validate_after["output"] == validate_before["output"], "validate output preserved unchanged"
    )
    notify = task(run, "notify")
    statuses = [a["status"] for a in notify["attempts"]]
    check(
        statuses == ["lease_expired", "succeeded"],
        f"notify attempts {statuses}: crash after delivery, then retry",
    )
    check(
        notify["output"]["duplicate"] is True, "retry was recognized by the receiver as a duplicate"
    )
    key = f"relayflow:{run_id}:notify"
    records = [
        n
        for n in httpx.get(f"{MOCK}/notifications", timeout=5).json()["items"]
        if n["idempotency_key"] == key
    ]
    check(len(records) == 1, "exactly one logical notification recorded by the mock service")
    check(
        records[0]["delivery_count"] == 2,
        "the service received the request twice (delivery_count = 2)",
    )
    check(
        records[0]["id"] == notify["output"]["notification_id"],
        "engine output references that same notification",
    )
    for worker in ("worker-a", "worker-b"):
        compose("up", "-d", worker)  # the crash-after-notify worker exited with code 137
    check(invariants() == [], "database invariants hold")

    say("7. Restarting PostgreSQL while a run is in flight (volume kept)")
    response = httpx.post(
        f"{API}/runs",
        timeout=10,
        json={
            "workflow_name": "document-processing",
            "input": {
                **DOCUMENT,
                "title": "Recovery demo: database restart",
                "demo": {"delay_seconds": {"word_count": 6}},
            },
        },
    )
    response.raise_for_status()
    db_run = response.json()["run_id"]
    wait(
        lambda: task(get(f"/runs/{db_run}"), "word_count")["status"] == "running",
        "word_count running",
    )
    compose("restart", "postgres")
    say("   postgres restarted")
    wait(lambda: get("/health")["database"] == "ok", "API to reconnect")
    final = wait(
        lambda: (r := get(f"/runs/{db_run}")) and r["status"] in ("succeeded", "failed") and r,
        "the in-flight run to finish",
        timeout=120,
    )
    check(final["status"] == "succeeded", "run in flight during the PostgreSQL restart succeeded")
    attempts = {t["task_key"]: [a["status"] for a in t["attempts"]] for t in final["tasks"]}
    say(f"   attempts per task: {json.dumps(attempts)}")
    check(invariants() == [], "database invariants hold after the restart")

    say("8. Restarting every service without deleting the volume")
    before = get(f"/runs/{run_id}")
    compose("stop")
    compose(
        "up",
        "-d",
        "--wait",
        "postgres",
        "mocknotify",
        "api",
        "scheduler",
        "worker-a",
        "worker-b",
        "dashboard",
    )
    wait(lambda: len(healthy_workers()) >= 2, "workers after restart")
    after = get(f"/runs/{run_id}")
    strip = lambda r: {k: v for k, v in r.items() if k != "db_now"}  # noqa: E731
    check(
        strip(after) == strip(before), "run history, attempts and outputs identical after restart"
    )
    records = [
        n
        for n in httpx.get(f"{MOCK}/notifications", timeout=5).json()["items"]
        if n["idempotency_key"] == key
    ]
    check(len(records) == 1 and records[0]["delivery_count"] == 2, "notification record persisted")
    response = httpx.post(
        f"{API}/runs",
        timeout=10,
        json={
            "workflow_name": "document-processing",
            "input": {**DOCUMENT, "title": "Recovery demo: after restart"},
        },
    )
    fresh = response.json()["run_id"]
    done = wait(
        lambda: (r := get(f"/runs/{fresh}")) and r["status"] == "succeeded" and r,
        "a new run after restart",
    )
    check(done["status"] == "succeeded", "new work runs normally after the restart")
    check(invariants() == [], "database invariants hold at the end")

    say(f"DEMO PASSED. Open http://127.0.0.1:8080/runs/{run_id} to see the recovery evidence.")
    return 0


if __name__ == "__main__":
    code = 1
    try:
        code = main()
    finally:
        out = ROOT / "demo-output"
        out.mkdir(exist_ok=True)
        (out / "demo_recovery.log").write_text("\n".join(LOG) + "\n", encoding="utf-8")
    sys.exit(code)
