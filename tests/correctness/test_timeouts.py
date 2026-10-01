"""Timeouts (spec 5.3, 6.6 step 2, 7.2): the scheduler backstop marks a still-heartbeating
attempt timed_out after timeout_seconds + grace; timeouts are retryable and counted."""

from __future__ import annotations

from sqlalchemy import Engine

from relayflow.engine import fail_attempt, heartbeat, recover_expired
from relayflow.testing import attempts_for, force_available_now, wait_for

from ._helpers import (
    LEASE,
    assert_invariants,
    attempt_row,
    claim,
    claim_one,
    complete,
    run_row,
    scalar,
    snapshot,
    submit,
    task_row,
)

GRACE = 0.3


def _past_deadline(engine: Engine, attempt_id: object, grace: float) -> bool:
    return bool(
        scalar(
            engine,
            "SELECT now() > started_at + make_interval(secs => t.timeout_seconds + :g) "
            "FROM attempts a JOIN tasks t ON t.id = a.task_id WHERE a.id = :a",
            a=attempt_id,
            g=grace,
        )
    )


def test_scheduler_backstop_times_out_heartbeating_attempt(engine: Engine) -> None:
    run_id = submit(engine, "test-timeout")  # timeout 1 s, max_attempts 2
    c = claim_one(engine, "s")
    # Before the deadline (+grace) nothing happens, however often the scheduler runs.
    assert recover_expired(engine, timeout_grace_seconds=GRACE).timed_out == 0

    def heartbeating_until_deadline() -> bool:
        hb = heartbeat(engine, [(c.attempt_id, c.lease_token)], lease_seconds=LEASE)
        assert hb[c.attempt_id].owned  # the lease stays valid: only the timeout applies
        return _past_deadline(engine, c.attempt_id, GRACE)

    wait_for(heartbeating_until_deadline, timeout=10, interval=0.1)
    result = recover_expired(engine, timeout_grace_seconds=GRACE)
    assert (result.expired, result.timed_out) == (0, 1)
    assert attempt_row(engine, c.attempt_id)["status"] == "timed_out"
    task = task_row(engine, run_id, "s")
    assert task["status"] == "queued" and task["failure_count"] == 1  # retryable (6.7)
    # The timed-out worker's token is now stale.
    before = snapshot(engine, run_id)
    assert complete(engine, c) is False
    assert (
        heartbeat(engine, [(c.attempt_id, c.lease_token)], lease_seconds=LEASE)[c.attempt_id].owned
        is False
    )
    assert snapshot(engine, run_id) == before

    force_available_now(engine, c.task_id)
    c2 = claim_one(engine, "s")
    assert c2.attempt_number == 2
    wait_for(lambda: _past_deadline(engine, c2.attempt_id, GRACE), timeout=10)
    assert recover_expired(engine, timeout_grace_seconds=GRACE).timed_out == 1
    assert task_row(engine, run_id, "s")["status"] == "failed"  # 2 of 2 counted
    assert [a["status"] for a in attempts_for(engine, run_id, "s")] == ["timed_out"] * 2
    assert run_row(engine, run_id)["status"] == "failed"
    assert claim(engine) is None
    assert_invariants(engine)


def test_large_grace_defers_backstop(engine: Engine) -> None:
    submit(engine, "test-timeout")
    c = claim_one(engine)
    wait_for(lambda: _past_deadline(engine, c.attempt_id, 0.0), timeout=10)
    assert recover_expired(engine, timeout_grace_seconds=60).timed_out == 0
    assert attempt_row(engine, c.attempt_id)["status"] == "running"
    assert complete(engine, c)  # the worker may still finish within the grace


def test_worker_reported_timeout_is_counted(engine: Engine) -> None:
    run_id = submit(engine, "test-timeout")
    c = claim_one(engine)
    assert fail_attempt(
        engine,
        attempt_id=c.attempt_id,
        lease_token=c.lease_token,
        error="deadline",
        kind="timed_out",
    )
    assert attempt_row(engine, c.attempt_id)["status"] == "timed_out"
    task = task_row(engine, run_id, "s")
    assert task["status"] == "queued" and task["failure_count"] == 1
    assert_invariants(engine)
