"""Competing claims (spec 6.3, 8): many claimers, each task claimed by exactly one,
at most one running attempt per task, contiguous attempt numbers."""

from __future__ import annotations

import threading
from collections import Counter
from typing import Any

from sqlalchemy import Engine, text

from relayflow.engine import ClaimedTask, fail_attempt
from relayflow.testing import attempts_for, force_available_now

from ._helpers import (
    assert_invariants,
    claim,
    complete,
    drive,
    one_task_workflow,
    release_and_collect,
    run_python,
    run_threads,
    scalar,
    submit,
)

THREADS = 12


def test_threads_claim_each_task_exactly_once(engine: Engine) -> None:
    run_ids = [submit(engine, "bench-noop") for _ in range(60)]
    barrier = threading.Barrier(THREADS)

    def worker(i: int) -> list[ClaimedTask]:
        barrier.wait()
        mine = []
        while (c := claim(engine, f"w{i}")) is not None:
            mine.append(c)
        return mine

    claimed = [c for batch in run_threads(THREADS, worker) for c in batch]
    task_ids = [c.task_id for c in claimed]
    assert len(task_ids) == len(run_ids) == len(set(task_ids))
    assert {c.run_id for c in claimed} == set(run_ids)
    assert all(c.attempt_number == 1 for c in claimed)
    assert len({c.lease_token for c in claimed}) == len(claimed)
    assert scalar(engine, "SELECT count(*) FROM attempts") == len(run_ids)
    assert scalar(engine, "SELECT count(*) FROM tasks WHERE status = 'running'") == len(run_ids)
    assert_invariants(engine)

    run_threads(len(claimed), lambda i: complete(engine, claimed[i]))
    assert scalar(engine, "SELECT count(*) FROM runs WHERE status = 'succeeded'") == len(run_ids)
    assert_invariants(engine)


def test_concurrent_claim_fail_reclaim_keeps_one_running_attempt(engine: Engine) -> None:
    """Tasks bounce through failures while many threads claim; a monitor samples the
    database for >1 running attempt per task while it happens."""
    one_task_workflow(engine, "flaky", max_attempts=3)
    run_ids = [submit(engine, "flaky") for _ in range(20)]
    stop = threading.Event()
    violations: list[Any] = []

    def monitor() -> None:
        while not stop.is_set():
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT task_id FROM attempts WHERE status = 'running' "
                        "GROUP BY task_id HAVING count(*) > 1"
                    )
                ).all()
            if rows:
                violations.append(rows)

    mon = threading.Thread(target=monitor, daemon=True)
    mon.start()
    barrier = threading.Barrier(8)

    def worker(i: int) -> int:
        barrier.wait()
        done = 0
        idle = 0
        while idle < 30:
            c = claim(engine, f"w{i}")
            if c is None:
                idle += 1
                with engine.connect() as conn:
                    ids = (
                        conn.execute(text("SELECT id FROM tasks WHERE status = 'queued'"))
                        .scalars()
                        .all()
                    )
                for task_id in ids:
                    force_available_now(engine, task_id)
                continue
            idle = 0
            if c.attempt_number < 3:
                assert fail_attempt(
                    engine, attempt_id=c.attempt_id, lease_token=c.lease_token, error="boom"
                )
            else:
                assert complete(engine, c)
                done += 1
        return done

    try:
        totals = run_threads(8, worker)
    finally:
        stop.set()
        mon.join(10)
    assert violations == []
    assert sum(totals) == len(run_ids)
    for run_id in run_ids:
        attempts = attempts_for(engine, run_id, "t")
        assert [a["attempt_number"] for a in attempts] == [1, 2, 3]
        assert [a["status"] for a in attempts] == ["failed", "failed", "succeeded"]
    assert scalar(engine, "SELECT count(*) FROM runs WHERE status = 'succeeded'") == len(run_ids)
    assert_invariants(engine)


CLAIM_CHILD = r"""
import json, sys
from relayflow.db import make_engine
from relayflow.engine import claim_task, complete_attempt
engine = make_engine(sys.argv[1] if len(sys.argv) > 1 else {url!r}, pool_size=2)
sys.stdin.readline()  # wait for "go" so all children start together
claimed = []
while True:
    c = claim_task(engine, worker_id={name!r}, lease_seconds=60)
    if c is None:
        break
    claimed.append([str(c.task_id), c.attempt_number])
    assert complete_attempt(engine, attempt_id=c.attempt_id, lease_token=c.lease_token,
                            output={{"by": {name!r}}})
engine.dispose()
print(json.dumps(claimed))
"""


def test_processes_claim_each_task_exactly_once(engine: Engine, db_url: str) -> None:
    run_ids = [submit(engine, "test-diamond") for _ in range(15)]
    children = [run_python(CLAIM_CHILD.format(url=db_url, name=f"proc-{i}")) for i in range(4)]
    results = release_and_collect(children, timeout=120)
    claims = [tuple(x) for r in results for x in r]
    counts = Counter(task for task, _ in claims)
    assert all(n == 1 for n in counts.values()), counts.most_common(3)
    assert all(attempt == 1 for _, attempt in claims)
    # A child stops when it momentarily sees nothing queued; a final local sweep runs
    # whatever is left. Every task must still have run exactly once.
    drive(engine)
    total_attempts = scalar(engine, "SELECT count(*) FROM attempts")
    assert total_attempts == 4 * len(run_ids)
    assert scalar(engine, "SELECT count(*) FROM runs WHERE status = 'succeeded'") == len(run_ids)
    assert_invariants(engine)
