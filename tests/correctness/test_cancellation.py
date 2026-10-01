"""Cooperative cancellation (spec R4, R5, T10, T11, T12, 6.9)."""

from __future__ import annotations

import threading

import pytest
from sqlalchemy import Engine, text

from relayflow.engine import (
    cancel_attempt,
    fail_attempt,
    heartbeat,
    release_attempt,
    request_cancel,
)
from relayflow.engine.errors import InvalidTransition
from relayflow.testing import events_for, task_states

from ._helpers import (
    LEASE,
    assert_invariants,
    claim,
    claim_one,
    complete,
    drive,
    run_row,
    run_threads,
    submit,
    task_row,
)


def test_cancel_with_nothing_running_cancels_immediately(engine: Engine) -> None:
    run_id = submit(engine, "test-chain")
    assert request_cancel(engine, run_id) == "cancelled"  # R4 + R5 in one transaction
    assert task_states(engine, run_id) == {"a": "cancelled", "b": "cancelled", "c": "cancelled"}
    run = run_row(engine, run_id)
    assert run["cancel_requested_at"] is not None and run["finished_at"] is not None
    assert claim(engine) is None
    assert_invariants(engine)


def test_cancel_with_running_task_waits_for_cooperative_ack(engine: Engine) -> None:
    run_id = submit(engine, "test-diamond")
    a = claim_one(engine, "a")
    hb = heartbeat(engine, [(a.attempt_id, a.lease_token)], lease_seconds=LEASE)
    assert hb[a.attempt_id].cancel_requested is False
    assert request_cancel(engine, run_id) == "cancelling"
    assert task_states(engine, run_id) == {
        "a": "running",
        "b": "cancelled",
        "c": "cancelled",
        "d": "cancelled",
    }
    hb = heartbeat(engine, [(a.attempt_id, a.lease_token)], lease_seconds=LEASE)
    assert hb[a.attempt_id].owned is True and hb[a.attempt_id].cancel_requested is True
    assert run_row(engine, run_id)["status"] == "cancelling"
    assert cancel_attempt(engine, attempt_id=a.attempt_id, lease_token=a.lease_token)  # T11
    assert task_row(engine, run_id, "a")["status"] == "cancelled"
    run = run_row(engine, run_id)
    assert run["status"] == "cancelled" and run["finished_at"] is not None
    assert_invariants(engine)


def test_completion_before_cancel(engine: Engine) -> None:
    run_id = submit(engine, "test-diamond")
    a = claim_one(engine, "a")
    assert complete(engine, a, {"a": 1})
    assert task_states(engine, run_id)["b"] == "queued"
    assert request_cancel(engine, run_id) == "cancelled"
    assert task_states(engine, run_id) == {
        "a": "succeeded",
        "b": "cancelled",
        "c": "cancelled",
        "d": "cancelled",
    }
    assert task_row(engine, run_id, "a")["output"] == {"a": 1}
    assert_invariants(engine)


def test_cancel_before_completion_still_records_success(engine: Engine) -> None:
    run_id = submit(engine, "test-diamond")
    a = claim_one(engine, "a")
    assert request_cancel(engine, run_id) == "cancelling"
    assert complete(engine, a, {"effect": "happened"}) is True  # still a valid owner
    a_row = task_row(engine, run_id, "a")
    assert a_row["status"] == "succeeded" and a_row["output"] == {"effect": "happened"}
    # Children are not promoted (they were already cancelled).
    assert task_states(engine, run_id) == {
        "a": "succeeded",
        "b": "cancelled",
        "c": "cancelled",
        "d": "cancelled",
    }
    kinds = [e["kind"] for e in events_for(engine, run_id)]
    assert "tasks_ready" not in kinds[kinds.index("cancel_requested") :]
    assert run_row(engine, run_id)["status"] == "cancelled"
    assert claim(engine) is None
    assert_invariants(engine)


@pytest.mark.parametrize("iteration", range(8))
def test_concurrent_cancel_and_completion(engine: Engine, iteration: int) -> None:
    run_id = submit(engine, "test-diamond")
    a = claim_one(engine, "a")
    barrier = threading.Barrier(2)

    def actor(i: int) -> object:
        barrier.wait()
        return complete(engine, a, {"a": "done"}) if i == 0 else request_cancel(engine, run_id)

    completed, cancel_status = run_threads(2, actor)
    assert completed is True
    assert cancel_status in ("cancelling", "cancelled")
    # Either order converges on the same end state (6.9).
    assert task_states(engine, run_id) == {
        "a": "succeeded",
        "b": "cancelled",
        "c": "cancelled",
        "d": "cancelled",
    }
    assert task_row(engine, run_id, "a")["output"] == {"a": "done"}
    assert run_row(engine, run_id)["status"] == "cancelled"
    assert claim(engine) is None
    assert_invariants(engine)


@pytest.mark.parametrize("outcome", ["succeeded", "failed", "cancelled"])
def test_cancel_on_terminal_run_is_rejected(engine: Engine, outcome: str) -> None:
    if outcome == "failed":
        run_id = submit(engine, "test-sleep")
        c = claim_one(engine)
        fail_attempt(
            engine, attempt_id=c.attempt_id, lease_token=c.lease_token, error="x", retryable=False
        )
    else:
        run_id = submit(engine, "test-chain")
        if outcome == "succeeded":
            drive(engine)
        else:
            request_cancel(engine, run_id)
    assert run_row(engine, run_id)["status"] == outcome
    before = run_row(engine, run_id)
    with pytest.raises(InvalidTransition):
        request_cancel(engine, run_id)
    assert run_row(engine, run_id) == before


def test_cancel_is_idempotent_while_cancelling(engine: Engine) -> None:
    run_id = submit(engine, "test-sleep")
    c = claim_one(engine)
    assert request_cancel(engine, run_id) == "cancelling"
    first = run_row(engine, run_id)
    assert request_cancel(engine, run_id) == "cancelling"
    assert request_cancel(engine, run_id) == "cancelling"
    second = run_row(engine, run_id)
    assert second["cancel_requested_at"] == first["cancel_requested_at"]
    assert second["status"] == "cancelling"
    assert [e["kind"] for e in events_for(engine, run_id)].count("cancel_requested") == 1
    assert cancel_attempt(engine, attempt_id=c.attempt_id, lease_token=c.lease_token)
    assert_invariants(engine)


def test_claim_skips_runs_with_cancel_requested(engine: Engine, admin_engine: Engine) -> None:
    """White-box guard check: a queued task in a run with cancel_requested_at set is not
    claimable (6.3 WHERE r.cancel_requested_at IS NULL). The state is constructed by hand
    because the engine never leaves queued tasks in a cancel-requested run."""
    victim = submit(engine, "test-sleep")
    other = submit(engine, "test-sleep")
    with admin_engine.begin() as conn:
        conn.execute(
            text("UPDATE runs SET cancel_requested_at = now() WHERE id = :r"), {"r": victim}
        )
    c = claim_one(engine)
    assert c.run_id == other
    assert claim(engine) is None
    with admin_engine.begin() as conn:  # restore a consistent state for the invariant check
        conn.execute(
            text("UPDATE runs SET cancel_requested_at = NULL WHERE id = :r"), {"r": victim}
        )


@pytest.mark.parametrize("how", ["fail", "fail_permanent", "release"])
def test_failure_or_release_while_cancelling_cancels_task(engine: Engine, how: str) -> None:
    run_id = submit(engine, "test-sleep")
    c = claim_one(engine)
    request_cancel(engine, run_id)
    if how == "release":
        assert release_attempt(engine, attempt_id=c.attempt_id, lease_token=c.lease_token)
    else:
        assert fail_attempt(
            engine,
            attempt_id=c.attempt_id,
            lease_token=c.lease_token,
            error="boom",
            retryable=how == "fail",
        )
    task = task_row(engine, run_id, "s")
    assert task["status"] == "cancelled"  # T12: no retry scheduled
    assert task["attempt_count"] == 1
    assert run_row(engine, run_id)["status"] == "cancelled"
    assert claim(engine) is None
    assert_invariants(engine)


def test_concurrent_claims_and_cancels(engine: Engine) -> None:
    """Claim vs cancel (6.9): every run ends cancelled-or-cancelling consistently, a claim
    either committed before the cancel (task running, run cancelling) or not at all, and
    no operation errors out (e.g. with a deadlock)."""
    run_ids = [submit(engine, "test-chain") for _ in range(40)]
    barrier = threading.Barrier(6)

    def actor(i: int) -> list[object]:
        barrier.wait()
        if i < 3:
            got = []
            for _ in range(30):
                c = claim(engine, f"w{i}")
                if c is not None:
                    got.append(c)
            return got
        return [request_cancel(engine, r) for r in run_ids[i - 3 :: 3]]

    results = run_threads(6, actor)
    claims = [c for r in results[:3] for c in r]
    for c in claims:
        status = run_row(engine, c.run_id)["status"]
        assert status == "cancelling", status
        assert cancel_attempt(engine, attempt_id=c.attempt_id, lease_token=c.lease_token)
    for run_id in run_ids:
        assert run_row(engine, run_id)["status"] == "cancelled"
    assert_invariants(engine)
