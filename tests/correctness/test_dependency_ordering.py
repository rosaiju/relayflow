"""Dependency ordering (spec T3, 6.2, 8 "dispatched only after all dependencies succeeded")
and the lost-promotion race (spec 2.1)."""

from __future__ import annotations

import threading

import pytest
from sqlalchemy import Engine, text

from relayflow.engine import ClaimedTask
from relayflow.testing import events_for, task_states

from ._helpers import (
    assert_invariants,
    claim,
    claim_one,
    complete,
    register,
    run_threads,
    scalar,
    submit,
    task_row,
)


def test_chain_is_strictly_sequential(engine: Engine) -> None:
    run_id = submit(engine, "test-chain")
    assert task_states(engine, run_id) == {"a": "queued", "b": "pending", "c": "pending"}
    a = claim_one(engine, "a")
    assert claim(engine) is None  # b is not claimable while a runs
    assert complete(engine, a, {"from": "a"})
    assert task_states(engine, run_id) == {"a": "succeeded", "b": "queued", "c": "pending"}
    b = claim_one(engine, "b")
    assert b.input["deps"] == {"a": {"from": "a"}}  # 6.2 materialized input
    assert claim(engine) is None
    assert complete(engine, b, {"from": "b"})
    c = claim_one(engine, "c")
    assert c.input["deps"] == {"b": {"from": "b"}}
    assert complete(engine, c)
    assert scalar(engine, "SELECT status FROM runs WHERE id = :r", r=run_id) == "succeeded"
    assert_invariants(engine)


@pytest.mark.parametrize("first", ["b", "c"])
def test_diamond_join_waits_for_both_parents(engine: Engine, first: str) -> None:
    run_id = submit(engine, "test-diamond")
    assert complete(engine, claim_one(engine, "a"), {"v": "a"})
    parents = {c.task_key: c for c in (claim_one(engine), claim_one(engine))}
    assert set(parents) == {"b", "c"}
    assert claim(engine) is None
    second = "c" if first == "b" else "b"
    assert complete(engine, parents[first], {"v": first})
    assert task_row(engine, run_id, "d")["status"] == "pending"
    assert claim(engine) is None  # d must not be claimable with one parent outstanding
    assert complete(engine, parents[second], {"v": second})
    d = claim_one(engine, "d")
    assert d.input["deps"] == {"b": {"v": "b"}, "c": {"v": "c"}}
    assert complete(engine, d)
    assert_invariants(engine)


def _fan_in(engine: Engine, width: int) -> None:
    tasks = [{"key": f"p{i}", "type": "demo.noop"} for i in range(width)]
    tasks.append(
        {"key": "child", "type": "demo.noop", "depends_on": [f"p{i}" for i in range(width)]}
    )
    register(engine, f"fan-in-{width}", tasks)


@pytest.mark.parametrize("iteration", range(10))
def test_concurrent_sibling_completions_promote_shared_child(
    engine: Engine, iteration: int
) -> None:
    """Lost-promotion race: both parents of d complete at the same instant."""
    run_id = submit(engine, "test-diamond")
    assert complete(engine, claim_one(engine, "a"))
    b, c = claim_one(engine), claim_one(engine)
    barrier = threading.Barrier(2)

    def finish(i: int) -> bool:
        barrier.wait()
        return complete(engine, (b, c)[i])

    assert run_threads(2, finish) == [True, True]
    assert task_row(engine, run_id, "d")["status"] == "queued"
    assert claim_one(engine, "d")
    assert_invariants(engine)


def test_wide_fan_in_promotes_child_exactly_once(engine: Engine) -> None:
    width = 8
    _fan_in(engine, width)
    run_id = submit(engine, f"fan-in-{width}")
    parents = [claim_one(engine) for _ in range(width)]
    assert claim(engine) is None
    barrier = threading.Barrier(width)

    def finish(i: int) -> bool:
        barrier.wait()
        return complete(engine, parents[i], {"i": i})

    assert all(run_threads(width, finish))
    child = task_row(engine, run_id, "child")
    assert child["status"] == "queued"
    assert len(child["input"]["deps"]) == width
    ready_events = [
        e
        for e in events_for(engine, run_id)
        if e["kind"] == "tasks_ready" and "child" in (e["data"] or {}).get("tasks", [])
    ]
    assert len(ready_events) == 1
    assert_invariants(engine)


def test_concurrent_workers_never_claim_before_dependencies(engine: Engine) -> None:
    """Many diamonds driven by many threads: at the moment of every claim, all of the
    claimed task's dependencies are already succeeded in the database."""
    run_ids = [submit(engine, "test-diamond") for _ in range(12)]
    violations: list[str] = []
    barrier = threading.Barrier(6)

    def worker(i: int) -> int:
        barrier.wait()
        idle, done = 0, 0
        while idle < 40:
            c: ClaimedTask | None = claim(engine, f"w{i}")
            if c is None:
                idle += 1
                continue
            idle = 0
            with engine.connect() as conn:
                not_ok = (
                    conn.execute(
                        text(
                            "SELECT p.task_key FROM tasks t, unnest(t.depends_on) d(key) "
                            "JOIN tasks p ON p.task_key = d.key "
                            "WHERE t.id = :t AND p.run_id = t.run_id AND p.status <> 'succeeded'"
                        ),
                        {"t": c.task_id},
                    )
                    .scalars()
                    .all()
                )
            if not_ok:
                violations.append(f"{c.task_key} claimed with unfinished deps {not_ok}")
            assert complete(engine, c)
            done += 1
        return done

    assert sum(run_threads(6, worker)) == 4 * len(run_ids)
    assert violations == []
    assert scalar(engine, "SELECT count(*) FROM runs WHERE status = 'succeeded'") == len(run_ids)
    assert_invariants(engine)
