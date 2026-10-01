"""Idempotent submission (spec 6.1): one key -> one run, enforced by the unique
constraint even under concurrent threads and processes."""

from __future__ import annotations

import threading

import pytest
from sqlalchemy import Engine

from relayflow.engine import submit_run
from relayflow.engine.errors import IdempotencyConflict

from ._helpers import release_and_collect, run_python, run_threads, scalar

DOC = {"title": "t", "text": "hello world"}


def _count_runs(engine: Engine, key: str) -> int:
    return int(scalar(engine, "SELECT count(*) FROM runs WHERE idempotency_key = :k", k=key))


def test_concurrent_threads_same_key_create_one_run(engine: Engine) -> None:
    n = 16
    barrier = threading.Barrier(n)

    def go(i: int) -> tuple[object, bool]:
        barrier.wait()
        r = submit_run(
            engine, workflow_name="test-diamond", input={"x": 1}, idempotency_key="same-key"
        )
        return r.run_id, r.created

    results = run_threads(n, go)
    assert len({run_id for run_id, _ in results}) == 1
    assert sum(created for _, created in results) == 1
    assert _count_runs(engine, "same-key") == 1
    run_id = results[0][0]
    assert scalar(engine, "SELECT count(*) FROM tasks WHERE run_id = :r", r=run_id) == 4
    assert scalar(engine, "SELECT count(*) FROM tasks") == 4


def test_concurrent_threads_mixed_payloads(engine: Engine) -> None:
    n = 12
    barrier = threading.Barrier(n)

    def go(i: int) -> tuple[str, object]:
        barrier.wait()
        try:
            r = submit_run(
                engine,
                workflow_name="test-chain",
                input={"variant": i % 2},
                idempotency_key="mixed",
            )
            return "ok", (r.run_id, r.created, i % 2)
        except IdempotencyConflict:
            return "conflict", i % 2

    results = run_threads(n, go)
    oks = [v for kind, v in results if kind == "ok"]
    conflicts = [v for kind, v in results if kind == "conflict"]
    assert len({run_id for run_id, _, _ in oks}) == 1
    assert sum(created for _, created, _ in oks) == 1
    winner_variant = {variant for _, _, variant in oks}
    assert len(winner_variant) == 1  # every success carries the winning payload
    assert all(v != next(iter(winner_variant)) for v in conflicts)
    assert len(oks) + len(conflicts) == n and len(conflicts) == n // 2
    assert _count_runs(engine, "mixed") == 1


def test_conflicting_payload_raises_and_does_not_create(engine: Engine) -> None:
    first = submit_run(engine, workflow_name="test-chain", input={"a": 1}, idempotency_key="k1")
    assert first.created
    with pytest.raises(IdempotencyConflict):
        submit_run(engine, workflow_name="test-chain", input={"a": 2}, idempotency_key="k1")
    with pytest.raises(IdempotencyConflict):  # same input, different workflow
        submit_run(engine, workflow_name="test-diamond", input={"a": 1}, idempotency_key="k1")
    again = submit_run(engine, workflow_name="test-chain", input={"a": 1}, idempotency_key="k1")
    assert again.run_id == first.run_id and again.created is False
    assert _count_runs(engine, "k1") == 1
    assert scalar(engine, "SELECT count(*) FROM runs") == 1


def test_key_order_insensitive_and_version_resolution(engine: Engine) -> None:
    """request_hash is over canonical JSON and the *resolved* version (6.1)."""
    a = submit_run(engine, workflow_name="test-chain", input={"x": 1, "y": 2}, idempotency_key="k2")
    b = submit_run(
        engine,
        workflow_name="test-chain",
        input={"y": 2, "x": 1},
        workflow_version=1,
        idempotency_key="k2",
    )
    assert b.run_id == a.run_id and b.created is False
    assert _count_runs(engine, "k2") == 1


def test_no_key_means_distinct_runs(engine: Engine) -> None:
    ids = {submit_run(engine, workflow_name="test-chain", input={}).run_id for _ in range(3)}
    assert len(ids) == 3


SUBMIT_CHILD = r"""
import json, sys
from relayflow.db import make_engine
from relayflow.engine import submit_run
engine = make_engine({url!r}, pool_size=1)
sys.stdin.readline()
r = submit_run(engine, workflow_name="document-processing", input={doc!r},
               idempotency_key="proc-key")
engine.dispose()
print(json.dumps([str(r.run_id), r.created]))
"""


def test_concurrent_processes_same_key_create_one_run(engine: Engine, db_url: str) -> None:
    children = [run_python(SUBMIT_CHILD.format(url=db_url, doc=DOC)) for _ in range(5)]
    results = release_and_collect(children, timeout=120)
    assert len({run_id for run_id, _ in results}) == 1
    assert sum(created for _, created in results) == 1
    assert _count_runs(engine, "proc-key") == 1
    assert scalar(engine, "SELECT count(*) FROM tasks") == 5
