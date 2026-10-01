"""Manual retry (spec R6, T13, T14, 6.10, 8 "succeeded output never overwritten")."""

from __future__ import annotations

import threading

import pytest
from sqlalchemy import Engine

from relayflow.engine import fail_attempt, request_cancel, retry_run
from relayflow.engine.errors import InvalidTransition
from relayflow.testing import attempts_for, events_for, force_available_now, task_states

from ._helpers import (
    assert_invariants,
    claim,
    claim_one,
    complete,
    drive,
    run_row,
    run_threads,
    snapshot,
    submit,
    task_row,
)


def _fail(engine: Engine, c, retryable: bool = True) -> None:  # type: ignore[no-untyped-def]
    assert fail_attempt(
        engine,
        attempt_id=c.attempt_id,
        lease_token=c.lease_token,
        error="boom",
        retryable=retryable,
    )


def _fail_test_fail_run(engine: Engine) -> tuple[object, str]:
    """Drive test-fail to `failed`: ok/after_ok succeed, bad fails twice, after_bad blocked."""
    run_id = submit(engine, "test-fail")
    first = {c.task_key: c for c in (claim_one(engine), claim_one(engine))}
    assert complete(engine, first["ok"], {"ok": "first-run", "n": [1, 2, 3]})
    assert complete(engine, claim_one(engine, "after_ok"), {"after_ok": True})
    _fail(engine, first["bad"])
    force_available_now(engine, first["bad"].task_id)
    _fail(engine, claim_one(engine, "bad"))
    assert run_row(engine, run_id)["status"] == "failed"
    return run_id, task_row(engine, run_id, "ok")["output_text"]


@pytest.mark.parametrize("state", ["running", "cancelling", "succeeded", "cancelled"])
def test_retry_rejected_unless_failed(engine: Engine, state: str) -> None:
    run_id = submit(engine, "test-sleep")
    if state in ("running", "cancelling"):
        claim_one(engine)
        if state == "cancelling":
            request_cancel(engine, run_id)
    elif state == "succeeded":
        drive(engine)
    else:
        request_cancel(engine, run_id)
    assert run_row(engine, run_id)["status"] == state
    before = snapshot(engine, run_id)
    with pytest.raises(InvalidTransition):
        retry_run(engine, run_id)
    assert snapshot(engine, run_id) == before
    if state in ("running", "cancelling"):  # leave the database tidy for the invariant check
        from relayflow.engine import recover_expired
        from relayflow.testing import force_lease_expiry

        for a in attempts_for(engine, run_id, "s"):
            force_lease_expiry(engine, a["id"])
        recover_expired(engine)


def test_retry_preserves_succeeded_outputs_and_resets_failures(engine: Engine) -> None:
    run_id, ok_output = _fail_test_fail_run(engine)
    after_ok_output = task_row(engine, run_id, "after_ok")["output_text"]
    ok_attempts = attempts_for(engine, run_id, "ok")
    assert task_row(engine, run_id, "bad")["failure_count"] == 2

    assert retry_run(engine, run_id) == "running"
    run = run_row(engine, run_id)
    assert run["status"] == "running" and run["manual_retry_count"] == 1
    assert run["finished_at"] is None and run["error"] is None
    assert task_states(engine, run_id) == {
        "ok": "succeeded",
        "bad": "queued",
        "after_bad": "pending",
        "after_ok": "succeeded",
    }
    bad = task_row(engine, run_id, "bad")
    assert bad["failure_count"] == 0  # T13: fresh budget
    assert bad["attempt_count"] == 2  # history is kept
    assert "run_retried" in [e["kind"] for e in events_for(engine, run_id)]

    # Fresh budget: bad may fail once more without becoming terminal (max_attempts 2).
    c = claim_one(engine, "bad")
    assert c.attempt_number == 3
    _fail(engine, c)
    assert task_row(engine, run_id, "bad")["status"] == "queued"
    force_available_now(engine, c.task_id)
    c = claim_one(engine, "bad")
    assert c.attempt_number == 4
    assert complete(engine, c, {"bad": "fixed"})
    assert complete(engine, claim_one(engine, "after_bad"))
    assert claim(engine) is None  # ok and after_ok were never re-executed
    assert run_row(engine, run_id)["status"] == "succeeded"

    assert task_row(engine, run_id, "ok")["output_text"] == ok_output  # byte-for-byte
    assert task_row(engine, run_id, "after_ok")["output_text"] == after_ok_output
    assert attempts_for(engine, run_id, "ok") == ok_attempts
    assert [a["attempt_number"] for a in attempts_for(engine, run_id, "bad")] == [1, 2, 3, 4]
    assert [a["status"] for a in attempts_for(engine, run_id, "bad")] == [
        "failed",
        "failed",
        "failed",
        "succeeded",
    ]
    assert_invariants(engine)


def test_retry_twice_keeps_increasing_attempt_numbers(engine: Engine) -> None:
    run_id, ok_output = _fail_test_fail_run(engine)
    for round_no in (1, 2):
        assert retry_run(engine, run_id) == "running"
        for _ in range(2):
            c = claim_one(engine, "bad")
            _fail(engine, c)
            force_available_now(engine, c.task_id)
        run = run_row(engine, run_id)
        assert run["status"] == "failed" and run["manual_retry_count"] == round_no
        assert task_states(engine, run_id)["after_bad"] == "blocked"
    numbers = [a["attempt_number"] for a in attempts_for(engine, run_id, "bad")]
    assert numbers == [1, 2, 3, 4, 5, 6]
    assert task_row(engine, run_id, "ok")["output_text"] == ok_output
    assert_invariants(engine)


def test_retry_unblocks_chain_and_promotes_only_ready_tasks(engine: Engine) -> None:
    run_id = submit(engine, "test-chain")
    _fail(engine, claim_one(engine, "a"), retryable=False)
    assert task_states(engine, run_id) == {"a": "failed", "b": "blocked", "c": "blocked"}
    retry_run(engine, run_id)
    assert task_states(engine, run_id) == {"a": "queued", "b": "pending", "c": "pending"}
    assert drive(engine) == 3
    assert run_row(engine, run_id)["status"] == "succeeded"
    assert_invariants(engine)


def test_retry_in_diamond_keeps_sibling_output(engine: Engine) -> None:
    run_id = submit(engine, "test-diamond")
    assert complete(engine, claim_one(engine, "a"), {"a": 1})
    b, c = sorted((claim_one(engine), claim_one(engine)), key=lambda x: x.task_key)
    assert complete(engine, c, {"c": 1})
    _fail(engine, b, retryable=False)
    assert task_states(engine, run_id) == {
        "a": "succeeded",
        "b": "failed",
        "c": "succeeded",
        "d": "blocked",
    }
    c_output = task_row(engine, run_id, "c")["output_text"]
    retry_run(engine, run_id)
    assert task_states(engine, run_id) == {
        "a": "succeeded",
        "b": "queued",
        "c": "succeeded",
        "d": "pending",
    }
    b2 = claim_one(engine, "b")
    assert b2.input["deps"] == {"a": {"a": 1}}
    assert complete(engine, b2, {"b": 2})
    d = claim_one(engine, "d")
    assert d.input["deps"] == {"b": {"b": 2}, "c": {"c": 1}}
    assert complete(engine, d)
    assert task_row(engine, run_id, "c")["output_text"] == c_output
    assert run_row(engine, run_id)["status"] == "succeeded"
    assert_invariants(engine)


def test_concurrent_retries_apply_once(engine: Engine) -> None:
    run_id, _ = _fail_test_fail_run(engine)
    barrier = threading.Barrier(4)

    def retry(i: int) -> str:
        barrier.wait()
        try:
            return retry_run(engine, run_id)
        except InvalidTransition:
            return "rejected"

    outcomes = run_threads(4, retry)
    assert sorted(outcomes) == ["rejected", "rejected", "rejected", "running"]
    assert run_row(engine, run_id)["manual_retry_count"] == 1
    assert_invariants(engine)


def test_retry_unknown_run_raises(engine: Engine) -> None:
    import uuid

    from relayflow.engine.errors import RunNotFound

    with pytest.raises(RunNotFound):
        retry_run(engine, uuid.uuid4())
