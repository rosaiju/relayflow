"""In-process Worker + Scheduler against real PostgreSQL (subprocess crash tests live in
tests/correctness and the demo scripts)."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from uuid import UUID

from sqlalchemy import Engine

from relayflow.config import Settings
from relayflow.engine import request_cancel, submit_run
from relayflow.scheduler.main import Scheduler
from relayflow.testing import attempts_for, check_invariants, run_status, task_states, wait_for
from relayflow.worker.main import Worker


@contextmanager
def running(settings: Settings, engine: Engine, workers: int = 2) -> Iterator[list[Worker]]:
    pool = [
        Worker(settings.model_copy(update={"worker_name": f"w{i}"}), engine) for i in range(workers)
    ]
    scheduler = Scheduler(settings, engine)
    threads = [threading.Thread(target=w.run, daemon=True) for w in pool]
    threads.append(threading.Thread(target=scheduler.run, daemon=True))
    for t in threads:
        t.start()
    try:
        yield pool
    finally:
        for w in pool:
            w.request_stop()
        scheduler.request_stop()
        for t in threads:
            t.join(timeout=15)


def finished(engine: Engine, run_id: UUID) -> bool:
    return run_status(engine, run_id) in ("succeeded", "failed", "cancelled")


def test_diamond_runs_to_completion_across_workers(engine: Engine, settings: Settings) -> None:
    runs = [submit_run(engine, workflow_name="test-diamond", input={}).run_id for _ in range(5)]
    with running(settings, engine):
        for run_id in runs:
            wait_for(lambda r=run_id: finished(engine, r), timeout=30)
    assert all(run_status(engine, r) == "succeeded" for r in runs)
    assert check_invariants(engine) == []


def test_failure_blocks_descendants_but_independent_branch_finishes(
    engine: Engine, settings: Settings
) -> None:
    run_id = submit_run(engine, workflow_name="test-fail", input={}).run_id
    with running(settings, engine):
        wait_for(lambda: finished(engine, run_id), timeout=30)
    assert run_status(engine, run_id) == "failed"
    assert task_states(engine, run_id) == {
        "ok": "succeeded",
        "after_ok": "succeeded",
        "bad": "failed",
        "after_bad": "blocked",
    }
    assert [a["status"] for a in attempts_for(engine, run_id, "bad")] == ["failed", "failed"]
    assert check_invariants(engine) == []


def test_worker_watchdog_times_out_and_retries(engine: Engine, settings: Settings) -> None:
    run_id = submit_run(engine, workflow_name="test-timeout", input={"seconds": 5}).run_id
    with running(settings, engine, workers=1):
        wait_for(lambda: finished(engine, run_id), timeout=30)
    assert run_status(engine, run_id) == "failed"
    statuses = [a["status"] for a in attempts_for(engine, run_id, "s")]
    assert statuses == ["timed_out", "timed_out"]
    assert check_invariants(engine) == []


def test_cooperative_cancellation_of_running_task(engine: Engine, settings: Settings) -> None:
    run_id = submit_run(engine, workflow_name="test-sleep", input={"seconds": 20}).run_id
    with running(settings, engine, workers=1):
        wait_for(lambda: task_states(engine, run_id)["s"] == "running", timeout=10)
        assert request_cancel(engine, run_id) == "cancelling"
        wait_for(lambda: finished(engine, run_id), timeout=10)  # well before 20 s
    assert run_status(engine, run_id) == "cancelled"
    assert [a["status"] for a in attempts_for(engine, run_id, "s")] == ["cancelled"]
    assert check_invariants(engine) == []


def test_graceful_shutdown_releases_in_flight_attempt(engine: Engine, settings: Settings) -> None:
    fast_stop = settings.model_copy(update={"shutdown_grace_seconds": 0.5})
    run_id = submit_run(engine, workflow_name="test-sleep", input={"seconds": 30}).run_id
    worker = Worker(fast_stop, engine)
    thread = threading.Thread(target=worker.run, daemon=True)
    thread.start()
    wait_for(lambda: task_states(engine, run_id)["s"] == "running", timeout=10)
    worker.request_stop()
    thread.join(timeout=10)
    assert not thread.is_alive()
    assert [a["status"] for a in attempts_for(engine, run_id, "s")] == ["released"]
    assert task_states(engine, run_id)["s"] == "queued"  # handed back immediately, no lease wait
    assert check_invariants(engine) == []
