"""Retry limits, backoff bounds, non-retryable errors, failure propagation and run
settlement (spec T6, T7, T8, T9, R2, R3, 6.7, 6.8)."""

from __future__ import annotations

import random

import pytest
from sqlalchemy import Engine

from relayflow.engine import backoff_delay, fail_attempt, recover_expired, release_attempt
from relayflow.testing import attempts_for, force_available_now, force_lease_expiry, task_states

from ._helpers import (
    assert_invariants,
    attempt_row,
    claim,
    claim_one,
    complete,
    one_task_workflow,
    register,
    run_row,
    scalar,
    submit,
    task_row,
)


def _fail(engine: Engine, c, **kw) -> bool:  # type: ignore[no-untyped-def]
    return fail_attempt(
        engine,
        attempt_id=c.attempt_id,
        lease_token=c.lease_token,
        error=kw.pop("error", "boom"),
        **kw,
    )


def test_max_attempts_counts_failed_timed_out_and_lease_expired(engine: Engine) -> None:
    one_task_workflow(engine, "three", max_attempts=3)
    run_id = submit(engine, "three")
    c1 = claim_one(engine)
    assert _fail(engine, c1)  # failed
    force_available_now(engine, c1.task_id)
    c2 = claim_one(engine)
    assert _fail(engine, c2, kind="timed_out")  # timed_out
    assert task_row(engine, run_id, "t")["status"] == "queued"
    force_available_now(engine, c2.task_id)
    c3 = claim_one(engine)
    force_lease_expiry(engine, c3.attempt_id)
    assert recover_expired(engine).expired == 1  # lease_expired -> third counted failure
    task = task_row(engine, run_id, "t")
    assert task["status"] == "failed" and task["failure_count"] == 3
    assert [a["status"] for a in attempts_for(engine, run_id, "t")] == [
        "failed",
        "timed_out",
        "lease_expired",
    ]
    assert run_row(engine, run_id)["status"] == "failed"
    assert_invariants(engine)


def test_released_attempts_do_not_count_against_max_attempts(engine: Engine) -> None:
    one_task_workflow(engine, "two", max_attempts=2)
    run_id = submit(engine, "two")
    for _ in range(5):
        c = claim_one(engine)  # T8: immediately claimable again, no force needed
        assert release_attempt(engine, attempt_id=c.attempt_id, lease_token=c.lease_token)
        task = task_row(engine, run_id, "t")
        assert task["status"] == "queued" and task["failure_count"] == 0
        assert task["available_at"] <= scalar(engine, "SELECT now()")
    c = claim_one(engine)
    assert c.attempt_number == 6
    assert _fail(engine, c)
    assert task_row(engine, run_id, "t")["status"] == "queued"  # 1 of 2 failures used
    force_available_now(engine, c.task_id)
    c = claim_one(engine)
    assert _fail(engine, c)
    task = task_row(engine, run_id, "t")
    assert task["status"] == "failed" and task["failure_count"] == 2
    statuses = [a["status"] for a in attempts_for(engine, run_id, "t")]
    assert statuses == ["released"] * 5 + ["failed", "failed"]
    assert_invariants(engine)


def test_backoff_delay_function_bounds() -> None:
    rng = random.Random(1234)
    for failures in range(1, 12):
        for base, cap in ((0.1, 0.2), (1.0, 30.0), (0.5, 3.0), (2.0, 300.0)):
            raw = min(cap, base * 2 ** (failures - 1))
            for _ in range(50):
                d = backoff_delay(failures, base, cap, rng)
                assert raw / 2 - 1e-9 <= d <= raw + 1e-9


class _Extreme(random.Random):
    def __init__(self, high: bool) -> None:
        super().__init__(0)
        self.high = high

    def uniform(self, a: float, b: float) -> float:
        return b if self.high else a


@pytest.mark.parametrize("high", [False, True])
def test_requeue_available_at_matches_backoff_formula(engine: Engine, high: bool) -> None:
    """available_at - finished_at (both now() of the same transaction) is the delay."""
    base, cap = 0.5, 3.0
    one_task_workflow(
        engine, "bo", max_attempts=10, backoff_base_seconds=base, backoff_max_seconds=cap
    )
    run_id = submit(engine, "bo")
    for failures in range(1, 10):
        c = claim_one(engine)
        assert _fail(engine, c, rng=_Extreme(high))
        task = task_row(engine, run_id, "t")
        finished = attempt_row(engine, c.attempt_id)["finished_at"]
        delay = (task["available_at"] - finished).total_seconds()
        raw = min(cap, base * 2 ** (failures - 1))
        assert delay == pytest.approx(raw if high else raw / 2, abs=1e-3)
        assert task["failure_count"] == failures
        assert claim(engine) is None  # backoff respected: not claimable before available_at
        force_available_now(engine, c.task_id)
    assert_invariants(engine)


def test_requeue_delay_with_default_rng_is_within_bounds(engine: Engine) -> None:
    base, cap = 0.25, 1.0
    one_task_workflow(
        engine, "bo2", max_attempts=10, backoff_base_seconds=base, backoff_max_seconds=cap
    )
    run_id = submit(engine, "bo2")
    for failures in range(1, 10):
        c = claim_one(engine)
        assert _fail(engine, c)
        delay = (
            task_row(engine, run_id, "t")["available_at"]
            - attempt_row(engine, c.attempt_id)["finished_at"]
        ).total_seconds()
        raw = min(cap, base * 2 ** (failures - 1))
        assert raw / 2 - 1e-3 <= delay <= raw + 1e-3
        force_available_now(engine, c.task_id)


def test_non_retryable_error_fails_immediately_and_blocks_descendants(engine: Engine) -> None:
    run_id = submit(engine, "test-chain")  # max_attempts 3 by default
    a = claim_one(engine, "a")
    assert _fail(engine, a, retryable=False, error="invalid document")
    assert task_states(engine, run_id) == {"a": "failed", "b": "blocked", "c": "blocked"}
    assert task_row(engine, run_id, "a")["attempt_count"] == 1
    run = run_row(engine, run_id)
    assert run["status"] == "failed" and "a" in run["error"]
    assert claim(engine) is None
    assert_invariants(engine)


def test_terminal_failure_blocks_descendants_while_independent_branch_finishes(
    engine: Engine,
) -> None:
    run_id = submit(engine, "test-fail")
    first = {c.task_key: c for c in (claim_one(engine), claim_one(engine))}
    assert set(first) == {"ok", "bad"}
    assert _fail(engine, first["bad"])
    force_available_now(engine, first["bad"].task_id)
    bad2 = claim_one(engine, "bad")
    assert _fail(engine, bad2)  # max_attempts 2 -> terminal
    assert task_states(engine, run_id) == {
        "ok": "running",
        "bad": "failed",
        "after_bad": "blocked",
        "after_ok": "pending",
    }
    assert run_row(engine, run_id)["status"] == "running"  # R3 guard: ok still running
    assert complete(engine, first["ok"])
    after_ok = claim_one(engine, "after_ok")
    assert run_row(engine, run_id)["status"] == "running"
    assert complete(engine, after_ok)
    run = run_row(engine, run_id)
    assert run["status"] == "failed" and run["finished_at"] is not None
    assert "bad" in run["error"]
    assert task_states(engine, run_id) == {
        "ok": "succeeded",
        "bad": "failed",
        "after_bad": "blocked",
        "after_ok": "succeeded",
    }
    assert_invariants(engine)


def test_run_fails_at_the_moment_the_last_branch_fails(engine: Engine) -> None:
    run_id = submit(engine, "test-fail")
    first = {c.task_key: c for c in (claim_one(engine), claim_one(engine))}
    assert complete(engine, first["ok"])
    assert complete(engine, claim_one(engine, "after_ok"))
    assert run_row(engine, run_id)["status"] == "running"
    assert _fail(engine, first["bad"], retryable=False)
    assert run_row(engine, run_id)["status"] == "failed"
    assert_invariants(engine)


def test_run_succeeds_only_when_every_task_succeeded(engine: Engine) -> None:
    run_id = submit(engine, "test-diamond")
    for _ in range(4):
        assert run_row(engine, run_id)["status"] == "running"
        assert run_row(engine, run_id)["finished_at"] is None
        assert complete(engine, claim_one(engine))
    run = run_row(engine, run_id)
    assert run["status"] == "succeeded" and run["finished_at"] is not None
    assert run["error"] is None
    assert_invariants(engine)


def test_transitive_descendants_blocked_in_diamond(engine: Engine) -> None:
    register(
        engine,
        "deep",
        [
            {"key": "a", "type": "demo.noop"},
            {"key": "b", "type": "demo.noop", "depends_on": ["a"]},
            {"key": "x", "type": "demo.noop"},
            {"key": "c", "type": "demo.noop", "depends_on": ["b", "x"]},
            {"key": "d", "type": "demo.noop", "depends_on": ["c"]},
        ],
    )
    run_id = submit(engine, "deep")
    a = claim_one(engine, "a")
    x = claim_one(engine, "x")
    assert _fail(engine, a, retryable=False)
    assert task_states(engine, run_id) == {
        "a": "failed",
        "b": "blocked",
        "x": "running",
        "c": "blocked",
        "d": "blocked",
    }
    assert complete(engine, x)
    assert task_states(engine, run_id)["c"] == "blocked"  # x succeeding must not unblock c
    assert run_row(engine, run_id)["status"] == "failed"
    assert_invariants(engine)


def test_oversized_output_is_rejected_without_changing_state(engine: Engine) -> None:
    from relayflow.engine.errors import ValidationFailed

    run_id = submit(engine, "test-sleep")
    c = claim_one(engine)
    with pytest.raises(ValidationFailed):
        complete(engine, c, {"blob": "x" * (64 * 1024 + 10)})
    assert task_row(engine, run_id, "s")["status"] == "running"
    assert complete(engine, c, {"small": True})
    assert_invariants(engine)
