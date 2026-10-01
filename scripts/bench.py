"""Bounded benchmarks against the Docker Compose stack. Do not run alongside tests.

    uv run python scripts/bench.py                 # all scenarios (~3-4 minutes)
    uv run python scripts/bench.py --quick         # smaller datasets

Measured from PostgreSQL timestamps (one clock), never from guesses:
  * throughput  = tasks completed / (last attempt finished - first attempt started)
  * dispatch latency = attempt.started_at - task.queued_at  (first attempts; ready -> claimed)
  * handler time = attempt.finished_at - attempt.started_at (includes the report transaction)
  * orchestration overhead (sleep workload) = makespan - ideal makespan, where
    ideal = ceil(tasks / total slots) * task duration
  * crash recovery = next attempt started_at - crashed attempt's last heartbeat_at
Results are written to bench-results/<timestamp>.json and summarized on stdout.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import statistics
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from sqlalchemy import Engine, text  # noqa: E402

from relayflow.catalog import BUILTIN_WORKFLOWS  # noqa: E402
from relayflow.db import make_engine  # noqa: E402
from relayflow.engine import register_workflow, submit_run  # noqa: E402

DB_URL = "postgresql+psycopg://relayflow:relayflow@127.0.0.1:5433/relayflow"
API = "http://127.0.0.1:8000/api"
SLEEP_WF = {
    "name": "bench-sleep",
    "version": 1,
    "description": "one demo.sleep task; duration from run input (benchmarks)",
    "tasks": [{"key": "s", "type": "demo.sleep", "params": {"seconds": 0.25}}],
}


def compose(env: dict[str, str], *args: str) -> None:
    subprocess.run(
        ["docker", "compose", *args],
        cwd=ROOT,
        env={**os.environ, **env},
        check=True,
        capture_output=True,
    )


def start_stack(env: dict[str, str], workers: list[str]) -> None:
    services = ["postgres", "mocknotify", "api", "scheduler", *workers]
    compose(env, "up", "-d", "--wait", "--force-recreate", *services)
    others = [w for w in ("worker-a", "worker-b") if w not in workers]
    if others:
        compose(env, "stop", *others)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        healthy = [w for w in httpx.get(f"{API}/workers", timeout=5).json() if w["healthy"]]
        if len(healthy) >= len(workers):
            return
        time.sleep(0.5)
    raise RuntimeError("workers did not become healthy")


def wait_runs_done(engine: Engine, run_ids: list[Any], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with engine.connect() as conn:
            left = conn.execute(
                text(
                    "SELECT count(*) FROM runs WHERE id = ANY(:ids) AND status IN "
                    "('running','cancelling')"
                ),
                {"ids": run_ids},
            ).scalar_one()
        if left == 0:
            return
        time.sleep(0.5)
    raise RuntimeError("benchmark runs did not finish in time")


def pct(values: list[float], p: float) -> float:
    ordered = sorted(values)
    k = (len(ordered) - 1) * p / 100
    lo, hi = math.floor(k), math.ceil(k)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo)


def summarize(values: list[float]) -> dict[str, float]:
    return {
        "mean_ms": round(statistics.fmean(values) * 1000, 1),
        "p50_ms": round(pct(values, 50) * 1000, 1),
        "p95_ms": round(pct(values, 95) * 1000, 1),
        "max_ms": round(max(values) * 1000, 1),
    }


def submit_batch(
    engine: Engine,
    env: dict[str, str],
    workers: list[str],
    workflow: str,
    n: int,
    run_input: dict[str, Any],
    *,
    paced_interval: float | None,
) -> list[Any]:
    """prequeued (paced_interval=None): stop workers, queue all n runs, start workers, so the
    measurement is not limited by how fast this script can submit. paced: submit one run every
    `paced_interval` seconds with workers running (light load, for pickup latency)."""
    if paced_interval is None:
        compose(env, "stop", *workers)
        run_ids = [
            submit_run(engine, workflow_name=workflow, input=run_input).run_id for _ in range(n)
        ]
        compose(env, "start", *workers)
        return run_ids
    run_ids = []
    for _ in range(n):
        run_ids.append(submit_run(engine, workflow_name=workflow, input=run_input).run_id)
        time.sleep(paced_interval)
    return run_ids


def batch(
    engine: Engine,
    env: dict[str, str],
    workers: list[str],
    workflow: str,
    n: int,
    run_input: dict[str, Any],
    timeout: float,
    *,
    paced_interval: float | None = None,
) -> dict[str, Any]:
    run_ids = submit_batch(
        engine, env, workers, workflow, n, run_input, paced_interval=paced_interval
    )
    wait_runs_done(engine, run_ids, timeout)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT EXTRACT(EPOCH FROM a.started_at - t.queued_at) AS dispatch, "
                "EXTRACT(EPOCH FROM a.finished_at - a.started_at) AS handler, "
                "a.started_at, a.finished_at, a.worker_id, a.status "
                "FROM attempts a JOIN tasks t ON t.id = a.task_id "
                "WHERE a.run_id = ANY(:ids) AND a.attempt_number = 1"
            ),
            {"ids": run_ids},
        ).all()
        bad = conn.execute(
            text("SELECT count(*) FROM runs WHERE id = ANY(:ids) AND status <> 'succeeded'"),
            {"ids": run_ids},
        ).scalar_one()
    makespan = (max(r.finished_at for r in rows) - min(r.started_at for r in rows)).total_seconds()
    result: dict[str, Any] = {
        "mode": "prequeued" if paced_interval is None else f"paced every {paced_interval}s",
        "runs": n,
        "tasks": len(rows),
        "failed_runs": bad,
        "handler_time": summarize([float(r.handler) for r in rows]),
        "tasks_per_worker": _per_worker(rows),
    }
    if paced_interval is None:
        result["makespan_s"] = round(makespan, 3)
        result["throughput_tasks_per_s"] = round(len(rows) / makespan, 1)
    else:
        result["dispatch_latency"] = summarize([float(r.dispatch) for r in rows])
    return result


def _per_worker(rows: list[Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for r in rows:
        name = r.worker_id.split(":")[0]
        counts[name] = counts.get(name, 0) + 1
    return counts


def crash_trials(engine: Engine, env: dict[str, str], trials: int) -> list[dict[str, Any]]:
    results = []
    for i in range(trials):
        run_id = submit_run(engine, workflow_name="test-sleep", input={"seconds": 4}).run_id
        owner = None
        deadline = time.monotonic() + 30
        while owner is None and time.monotonic() < deadline:
            with engine.connect() as conn:
                owner = conn.execute(
                    text("SELECT worker_id FROM attempts WHERE run_id = :r AND status = 'running'"),
                    {"r": run_id},
                ).scalar()
            time.sleep(0.1)
        if owner is None:
            raise RuntimeError("sleep task never started")
        service = owner.split(":")[0]
        compose(env, "kill", service)
        wait_runs_done(engine, [run_id], timeout=90)
        with engine.connect() as conn:
            a1, a2 = conn.execute(
                text(
                    "SELECT status, heartbeat_at, lease_expires_at, finished_at, started_at, worker_id "
                    "FROM attempts WHERE run_id = :r ORDER BY attempt_number"
                ),
                {"r": run_id},
            ).all()[:2]
        results.append(
            {
                "trial": i + 1,
                "killed": service,
                "first_attempt_status": a1.status,
                "lease_expiry_after_last_heartbeat_s": round(
                    (a1.lease_expires_at - a1.heartbeat_at).total_seconds(), 3
                ),
                "detected_after_lease_expiry_s": round(
                    (a1.finished_at - a1.lease_expires_at).total_seconds(), 3
                ),
                "reassigned_after_last_heartbeat_s": round(
                    (a2.started_at - a1.heartbeat_at).total_seconds(), 3
                ),
                "reassigned_to": a2.worker_id.split(":")[0],
            }
        )
        compose(env, "up", "-d", "--wait", service)
        time.sleep(2)
    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true")
    args = parser.parse_args()
    noop_n, sleep_n, latency_n, trials = (100, 48, 20, 1) if args.quick else (500, 160, 60, 3)

    env = {
        "RELAYFLOW_ENABLE_FAULT_INJECTION": "0",
        "RELAYFLOW_LEASE_SECONDS": "10",
        "RELAYFLOW_HEARTBEAT_SECONDS": "3",
        "RELAYFLOW_POLL_INTERVAL_SECONDS": "0.5",
        "RELAYFLOW_SCHEDULER_INTERVAL_SECONDS": "1",
        "RELAYFLOW_WORKER_CONCURRENCY": "4",
    }
    engine = make_engine(DB_URL, pool_size=4)
    for spec in [*BUILTIN_WORKFLOWS, SLEEP_WF]:
        register_workflow(engine, spec)

    report: dict[str, Any] = {
        "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
        "host": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "python_client": platform.python_version(),
        },
        "settings": env,
        "scenarios": {},
    }
    one, two = ["worker-a"], ["worker-a", "worker-b"]
    print("throughput: noop, 1 worker x 4 slots (prequeued) ...", flush=True)
    start_stack(env, one)
    report["scenarios"]["throughput_noop_1x4"] = batch(
        engine, env, one, "bench-noop", noop_n, {}, 300
    )
    print("throughput: noop, 2 workers x 4 slots (prequeued) ...", flush=True)
    start_stack(env, two)
    report["scenarios"]["throughput_noop_2x4"] = batch(
        engine, env, two, "bench-noop", noop_n, {}, 300
    )
    print("throughput: 0.25 s sleep, 2 workers x 4 slots (prequeued) ...", flush=True)
    sleep = batch(engine, env, two, "bench-sleep", sleep_n, {"seconds": 0.25}, 300)
    ideal = math.ceil(sleep_n / 8) * 0.25
    sleep["ideal_makespan_s"] = ideal
    sleep["orchestration_overhead_s"] = round(sleep["makespan_s"] - ideal, 3)
    sleep["overhead_per_task_slot_ms"] = round(
        1000 * sleep["orchestration_overhead_s"] / math.ceil(sleep_n / 8), 1
    )
    report["scenarios"]["throughput_sleep0.25_2x4"] = sleep
    print("dispatch latency: noop paced 5/s, 2 workers, poll 0.5 s ...", flush=True)
    report["scenarios"]["latency_noop_poll0.5"] = batch(
        engine, env, two, "bench-noop", latency_n, {}, 120, paced_interval=0.2
    )
    print("dispatch latency: noop paced 5/s, 2 workers, poll 0.1 s ...", flush=True)
    fast = {**env, "RELAYFLOW_POLL_INTERVAL_SECONDS": "0.1"}
    start_stack(fast, two)
    report["scenarios"]["latency_noop_poll0.1"] = batch(
        engine, fast, two, "bench-noop", latency_n, {}, 120, paced_interval=0.2
    )
    start_stack(env, two)
    print(f"worker crash recovery ({trials} trials, lease 10 s, heartbeat 3 s) ...", flush=True)
    report["crash_recovery"] = crash_trials(engine, env, trials)
    start_stack(env, ["worker-a", "worker-b"])
    engine.dispose()

    out = ROOT / "bench-results"
    out.mkdir(exist_ok=True)
    path = out / f"{report['timestamp'].replace(':', '').replace('+0000', 'Z')}.json"
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"\nwritten to {path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
