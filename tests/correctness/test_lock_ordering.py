"""Lock ordering (spec 2.1, 6.3): "The claim transaction locks only a task row (with
SKIP LOCKED) and inserts an attempt; it never waits on a run lock", and the
run -> task -> attempt order is supposed to rule out deadlocks.

The interleavings are created deterministically: an admin connection holds a row lock
on a *pending* task, which pauses a real request_cancel right after it has taken the run
lock (it then waits for the task locks). While it is paused we run a real claim_task.
"""

from __future__ import annotations

import threading
import time
from typing import Any

from sqlalchemy import Engine, text

from relayflow.engine import cancel_attempt, claim_task, request_cancel
from relayflow.testing import task_states, wait_for

from ._helpers import LEASE, assert_invariants, run_row, submit, task_row


def _waiting_on_lock(admin_engine: Engine) -> bool:
    with admin_engine.connect() as conn:
        return bool(
            conn.execute(
                text(
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'relayflow' "
                    "AND wait_event_type = 'Lock' AND query LIKE '%FROM tasks%FOR%UPDATE%'"
                )
            ).scalar()
        )


def _cancel_paused_then_claim(engine: Engine, admin_engine: Engine) -> dict[str, Any]:
    """test-fail has queued tasks ok/bad and pending tasks after_ok/after_bad.

    1. admin locks the pending tasks (claims never touch pending tasks);
    2. request_cancel(run) locks the run row, then blocks on the pending task locks;
    3. claim_task runs; it may take a queued task of the same run;
    4. admin releases; both operations finish.
    Returns what happened: whether the claim blocked while cancel held the run lock and
    any exception raised by either side.
    """
    run_id = submit(engine, "test-fail")
    out: dict[str, Any] = {"run_id": run_id}

    def do_cancel() -> None:
        try:
            out["cancel"] = request_cancel(engine, run_id)
        except BaseException as exc:
            out["cancel_error"] = exc

    def do_claim() -> None:
        try:
            out["claim"] = claim_task(engine, worker_id="w", lease_seconds=LEASE)
        except BaseException as exc:
            out["claim_error"] = exc

    admin = admin_engine.connect()
    tx = admin.begin()
    canceller = claimer = None
    try:
        admin.execute(
            text("SELECT id FROM tasks WHERE run_id = :r AND status = 'pending' FOR UPDATE"),
            {"r": run_id},
        )
        canceller = threading.Thread(target=do_cancel, daemon=True)
        canceller.start()
        wait_for(lambda: _waiting_on_lock(admin_engine), timeout=10, interval=0.05)
        claimer = threading.Thread(target=do_claim, daemon=True)
        started = time.monotonic()
        claimer.start()
        claimer.join(3.0)
        out["claim_blocked"] = claimer.is_alive()
        out["claim_wait"] = time.monotonic() - started
    finally:
        tx.rollback()
        admin.close()
        for t in (canceller, claimer):
            if t is not None:
                t.join(20)
    return out


def test_claim_does_not_wait_on_run_lock(engine: Engine, admin_engine: Engine) -> None:
    """While request_cancel holds the run lock, claiming a queued task must not block."""
    out = _cancel_paused_then_claim(engine, admin_engine)
    assert not out["claim_blocked"], (
        "claim_task blocked while request_cancel held the run row lock (spec 2.1/6.3: the "
        "claim never waits on a run lock). Cause: the attempt/event INSERTs take FOR KEY "
        "SHARE on runs through the run_id foreign key, which conflicts with SELECT ... FOR "
        f"UPDATE on runs. Outcome after release: {out}"
    )


def test_cancel_and_claim_do_not_deadlock(engine: Engine, admin_engine: Engine) -> None:
    """Same interleaving: neither operation may fail (e.g. DeadlockDetected), and the
    result must be one of the two legal orders of 6.9 (claim-then-cancel or cancel-first)."""
    out = _cancel_paused_then_claim(engine, admin_engine)
    errors = {k: v for k, v in out.items() if k.endswith("_error")}
    assert not errors, f"claim/cancel failed: {errors}"
    run_id = out["run_id"]
    claimed = out.get("claim")
    if claimed is not None:  # claim committed first: running task is cancelled cooperatively
        assert out["cancel"] == "cancelling"
        assert task_states(engine, run_id)[claimed.task_key] == "running"
        assert cancel_attempt(
            engine, attempt_id=claimed.attempt_id, lease_token=claimed.lease_token
        )
    assert run_row(engine, run_id)["status"] == "cancelled"
    assert_invariants(engine)


def test_claim_lock_pattern_does_not_deadlock_with_cancel(
    engine: Engine, admin_engine: Engine
) -> None:
    """Reproduces the interleaving: (1) claim has locked the task row; (2) request_cancel
    locks the run and waits for the task row; (3) claim inserts its attempt/event rows,
    whose run_id FK needs a KEY SHARE lock on the run. The admin connection plays the
    claim (steps 1 and 3) with the claim's own lock pattern; request_cancel is real."""
    run_id = submit(engine, "test-sleep")
    task_id = task_row(engine, run_id, "s")["id"]
    errors: dict[str, BaseException] = {}
    cancel_result: dict[str, str] = {}

    def cancel() -> None:
        try:
            cancel_result["status"] = request_cancel(engine, run_id)
        except BaseException as exc:
            errors["cancel"] = exc

    with admin_engine.connect() as admin:
        tx = admin.begin()
        # Step 1: the claim's SELECT ... FOR UPDATE OF t SKIP LOCKED locked the task.
        admin.execute(
            text("SELECT id FROM tasks WHERE id = :t FOR UPDATE SKIP LOCKED"), {"t": task_id}
        )
        t = threading.Thread(target=cancel, daemon=True)
        t.start()
        time.sleep(0.5)  # let request_cancel take the run lock and queue on the task row
        try:
            # Step 3: the claim writes its event row (FK -> runs), like claim_task does.
            admin.execute(
                text(
                    "INSERT INTO events (run_id, kind, message) "
                    "VALUES (:r, 'probe', 'claim-like insert')"
                ),
                {"r": run_id},
            )
            tx.rollback()
        except BaseException as exc:
            errors["claim"] = exc
            tx.rollback()
    t.join(20)
    assert not t.is_alive()
    assert not errors, f"deadlock between claim and cancel: {errors}"
    assert cancel_result["status"] == "cancelled"
    assert_invariants(engine)
