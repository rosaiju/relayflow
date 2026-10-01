"""Leases and stale ownership (spec 5.3, 6.4, 6.5, 6.6, 8): an expired or superseded
token can neither heartbeat nor change any state; a new attempt's token works."""

from __future__ import annotations

import threading
import uuid

from sqlalchemy import Engine

from relayflow.engine import (
    cancel_attempt,
    complete_attempt,
    fail_attempt,
    heartbeat,
    recover_expired,
    release_attempt,
    request_cancel,
)
from relayflow.testing import attempts_for, force_available_now, force_lease_expiry

from ._helpers import (
    LEASE,
    assert_invariants,
    attempt_row,
    claim,
    claim_one,
    complete,
    run_row,
    run_threads,
    scalar,
    snapshot,
    submit,
    task_row,
)


def _all_reports_rejected(engine: Engine, attempt_id: uuid.UUID, token: uuid.UUID) -> None:
    hb = heartbeat(engine, [(attempt_id, token)], lease_seconds=LEASE)
    assert hb[attempt_id].owned is False
    assert (
        complete_attempt(engine, attempt_id=attempt_id, lease_token=token, output={"x": 1}) is False
    )
    assert fail_attempt(engine, attempt_id=attempt_id, lease_token=token, error="late") is False
    assert (
        fail_attempt(
            engine, attempt_id=attempt_id, lease_token=token, error="late", retryable=False
        )
        is False
    )
    assert (
        fail_attempt(
            engine, attempt_id=attempt_id, lease_token=token, error="late", kind="timed_out"
        )
        is False
    )
    assert cancel_attempt(engine, attempt_id=attempt_id, lease_token=token) is False
    assert release_attempt(engine, attempt_id=attempt_id, lease_token=token) is False


def test_expired_token_is_rejected_everywhere_and_changes_nothing(engine: Engine) -> None:
    run_id = submit(engine, "test-sleep")
    c = claim_one(engine, "s", worker="worker-a")
    force_lease_expiry(engine, c.attempt_id)
    assert recover_expired(engine).expired == 1
    att = attempt_row(engine, c.attempt_id)
    assert att["status"] == "lease_expired"
    task = task_row(engine, run_id, "s")
    assert task["status"] == "queued" and task["failure_count"] == 1  # T6: retryable
    before = snapshot(engine, run_id)
    _all_reports_rejected(engine, c.attempt_id, c.lease_token)
    assert snapshot(engine, run_id) == before
    assert_invariants(engine)


def test_heartbeat_cannot_revive_lapsed_lease_before_scheduler_runs(engine: Engine) -> None:
    run_id = submit(engine, "test-sleep")
    c = claim_one(engine, "s")
    force_lease_expiry(engine, c.attempt_id)
    lapsed_at = attempt_row(engine, c.attempt_id)["lease_expires_at"]
    before = snapshot(engine, run_id)
    # Scheduler has NOT run yet: attempt is still 'running' in the table but lapsed.
    _all_reports_rejected(engine, c.attempt_id, c.lease_token)
    assert snapshot(engine, run_id) == before
    assert attempt_row(engine, c.attempt_id)["lease_expires_at"] == lapsed_at
    assert recover_expired(engine).expired == 1
    assert attempt_row(engine, c.attempt_id)["status"] == "lease_expired"
    assert_invariants(engine)


def test_new_attempt_token_works_and_old_token_cannot_touch_it(engine: Engine) -> None:
    run_id = submit(engine, "test-sleep")
    old = claim_one(engine, "s", worker="worker-a")
    force_lease_expiry(engine, old.attempt_id)
    recover_expired(engine)
    force_available_now(engine, old.task_id)
    new = claim_one(engine, "s", worker="worker-b")
    assert new.attempt_number == 2 and new.attempt_id != old.attempt_id
    before = snapshot(engine, run_id)
    # The old token against the old attempt, and against the new attempt id.
    _all_reports_rejected(engine, old.attempt_id, old.lease_token)
    assert (
        complete_attempt(
            engine, attempt_id=new.attempt_id, lease_token=old.lease_token, output={"forged": True}
        )
        is False
    )
    assert (
        heartbeat(engine, [(new.attempt_id, old.lease_token)], lease_seconds=LEASE)[
            new.attempt_id
        ].owned
        is False
    )
    assert snapshot(engine, run_id) == before
    # The current owner is unaffected.
    assert (
        heartbeat(engine, [(new.attempt_id, new.lease_token)], lease_seconds=LEASE)[
            new.attempt_id
        ].owned
        is True
    )
    assert complete(engine, new, {"winner": "b"})
    assert task_row(engine, run_id, "s")["output"] == {"winner": "b"}
    attempts = attempts_for(engine, run_id, "s")
    assert [(a["worker_id"], a["status"]) for a in attempts] == [
        ("worker-a", "lease_expired"),
        ("worker-b", "succeeded"),
    ]
    assert run_row(engine, run_id)["status"] == "succeeded"
    assert_invariants(engine)


def test_random_token_is_rejected(engine: Engine) -> None:
    run_id = submit(engine, "test-sleep")
    c = claim_one(engine, "s")
    before = snapshot(engine, run_id)
    _all_reports_rejected(engine, c.attempt_id, uuid.uuid4())
    _all_reports_rejected(engine, uuid.uuid4(), c.lease_token)
    assert snapshot(engine, run_id) == before
    assert complete(engine, c)


def test_heartbeat_renews_live_lease_so_recovery_skips_it(engine: Engine) -> None:
    submit(engine, "test-sleep")
    c = claim(engine, lease=5)
    assert c is not None
    first = attempt_row(engine, c.attempt_id)["lease_expires_at"]
    hb = heartbeat(engine, [(c.attempt_id, c.lease_token)], lease_seconds=30)
    assert hb[c.attempt_id].owned is True and hb[c.attempt_id].cancel_requested is False
    assert attempt_row(engine, c.attempt_id)["lease_expires_at"] > first
    assert recover_expired(engine).expired == 0
    assert attempt_row(engine, c.attempt_id)["status"] == "running"


def test_lease_expiry_exhausts_max_attempts_then_fails_run(engine: Engine) -> None:
    run_id = submit(engine, "test-sleep")  # max_attempts defaults to 3
    for n in (1, 2, 3):
        c = claim_one(engine, "s", worker=f"worker-{n}")
        assert c.attempt_number == n
        force_lease_expiry(engine, c.attempt_id)
        assert recover_expired(engine).expired == 1
        force_available_now(engine, c.task_id)
    assert claim(engine) is None
    task = task_row(engine, run_id, "s")
    assert task["status"] == "failed" and task["failure_count"] == 3
    assert [a["status"] for a in attempts_for(engine, run_id, "s")] == ["lease_expired"] * 3
    run = run_row(engine, run_id)
    assert run["status"] == "failed" and run["finished_at"] is not None
    assert "s" in run["error"]
    assert_invariants(engine)


def test_concurrent_schedulers_expire_each_attempt_exactly_once(engine: Engine) -> None:
    run_ids = [submit(engine, "test-sleep") for _ in range(20)]
    claims = [claim_one(engine) for _ in run_ids]
    for c in claims:
        force_lease_expiry(engine, c.attempt_id)
    barrier = threading.Barrier(5)

    def scheduler(i: int) -> int:
        barrier.wait()
        return recover_expired(engine).expired

    assert sum(run_threads(5, scheduler)) == len(claims)
    assert scalar(engine, "SELECT count(*) FROM attempts WHERE status = 'lease_expired'") == 20
    assert (
        scalar(engine, "SELECT count(*) FROM tasks WHERE failure_count = 1 AND status = 'queued'")
        == 20
    )
    assert_invariants(engine)


def test_completion_racing_recovery_has_exactly_one_winner(engine: Engine) -> None:
    """A report and the scheduler race on a just-lapsed lease: exactly one transition."""
    for _ in range(10):
        run_id = submit(engine, "test-sleep")
        c = claim_one(engine)
        force_lease_expiry(engine, c.attempt_id)
        barrier = threading.Barrier(2)

        def actor(i: int, c=c, barrier=barrier) -> object:
            barrier.wait()
            if i == 0:
                return complete(engine, c)
            return recover_expired(engine).expired

        completed, expired = run_threads(2, actor)
        # The lease has lapsed in PostgreSQL time, so the report must lose (5.3).
        assert completed is False and expired == 1
        assert task_row(engine, run_id, "s")["status"] == "queued"
    assert_invariants(engine)


def test_lease_expiry_while_cancelling_cancels_task_without_retry(engine: Engine) -> None:
    run_id = submit(engine, "test-sleep")
    c = claim_one(engine)
    assert request_cancel(engine, run_id) == "cancelling"
    force_lease_expiry(engine, c.attempt_id)
    assert recover_expired(engine).expired == 1
    task = task_row(engine, run_id, "s")
    assert task["status"] == "cancelled" and task["attempt_count"] == 1  # T12, no retry
    assert run_row(engine, run_id)["status"] == "cancelled"
    assert_invariants(engine)
