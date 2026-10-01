"""Invariants enforced by PostgreSQL itself (spec 3): constraints, partial unique index,
immutability triggers. Each statement runs in its own transaction and is rolled back."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import Engine, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from relayflow.engine import request_cancel

from ._helpers import claim_one, complete, submit, task_row


def _must_fail(
    engine: Engine,
    sql: str,
    exc: type[Exception] = DBAPIError,
    match: str | None = None,
    **params: object,
) -> None:
    with pytest.raises(exc, match=match), engine.begin() as conn:
        conn.execute(text(sql), params)


def test_second_running_attempt_for_a_task_is_rejected(engine: Engine) -> None:
    submit(engine, "test-sleep")
    c = claim_one(engine)
    _must_fail(
        engine,
        "INSERT INTO attempts (id, task_id, run_id, attempt_number, worker_id, lease_token, "
        "status, lease_expires_at) VALUES (:id, :t, :r, 2, 'intruder', :tok, 'running', "
        "now() + interval '1 minute')",
        IntegrityError,
        "uq_one_running_attempt_per_task",
        id=uuid.uuid4(),
        t=c.task_id,
        r=c.run_id,
        tok=uuid.uuid4(),
    )


def test_duplicate_attempt_number_and_lease_token_rejected(engine: Engine) -> None:
    submit(engine, "test-sleep")
    c = claim_one(engine)
    base = (
        "INSERT INTO attempts (id, task_id, run_id, attempt_number, worker_id, lease_token, "
        "status, lease_expires_at, finished_at) VALUES (:id, :t, :r, :n, 'x', :tok, "
        "'failed', now(), now())"
    )
    _must_fail(
        engine,
        base,
        IntegrityError,
        "uq_attempt_number",
        id=uuid.uuid4(),
        t=c.task_id,
        r=c.run_id,
        n=1,
        tok=uuid.uuid4(),
    )
    _must_fail(
        engine,
        base,
        IntegrityError,
        "lease_token",
        id=uuid.uuid4(),
        t=c.task_id,
        r=c.run_id,
        n=7,
        tok=c.lease_token,
    )


def test_run_snapshot_and_identity_are_immutable(engine: Engine) -> None:
    run_id = submit(engine, "test-chain", {"x": 1}, idempotency_key="imm")
    for sql in (
        "UPDATE runs SET definition_snapshot = '{}'::jsonb WHERE id = :r",
        "UPDATE runs SET input = '{\"x\": 2}'::jsonb WHERE id = :r",
        "UPDATE runs SET idempotency_key = 'other' WHERE id = :r",
        "UPDATE runs SET request_hash = 'forged' WHERE id = :r",
        "UPDATE runs SET workflow_version = 2 WHERE id = :r",
    ):
        _must_fail(engine, sql, DBAPIError, "immutable", r=run_id)


def test_workflow_definitions_are_immutable(engine: Engine) -> None:
    _must_fail(
        engine,
        "UPDATE workflow_definitions SET spec = '{}'::jsonb WHERE name = 'test-chain'",
        DBAPIError,
        "immutable",
    )


def test_succeeded_task_output_and_status_are_final(engine: Engine) -> None:
    run_id = submit(engine, "test-sleep")
    assert complete(engine, claim_one(engine), {"v": 1})
    task_id = task_row(engine, run_id, "s")["id"]
    _must_fail(
        engine,
        "UPDATE tasks SET output = '{\"v\": 2}'::jsonb WHERE id = :t",
        DBAPIError,
        "final",
        t=task_id,
    )
    _must_fail(
        engine, "UPDATE tasks SET status = 'queued' WHERE id = :t", DBAPIError, "final", t=task_id
    )
    _must_fail(engine, "UPDATE tasks SET output = NULL WHERE id = :t", DBAPIError, t=task_id)
    assert task_row(engine, run_id, "s")["output"] == {"v": 1}


def test_final_attempt_cannot_change_and_token_is_immutable(engine: Engine) -> None:
    submit(engine, "test-sleep")
    c = claim_one(engine)
    _must_fail(
        engine,
        "UPDATE attempts SET lease_token = :tok WHERE id = :a",
        DBAPIError,
        "immutable",
        tok=uuid.uuid4(),
        a=c.attempt_id,
    )
    assert complete(engine, c)
    _must_fail(
        engine,
        "UPDATE attempts SET status = 'running', finished_at = NULL WHERE id = :a",
        DBAPIError,
        "final",
        a=c.attempt_id,
    )
    _must_fail(
        engine,
        "UPDATE attempts SET status = 'failed' WHERE id = :a",
        DBAPIError,
        "final",
        a=c.attempt_id,
    )


def test_check_constraints(engine: Engine) -> None:
    run_id = submit(engine, "test-chain")
    a = task_row(engine, run_id, "a")["id"]
    b = task_row(engine, run_id, "b")["id"]
    _must_fail(
        engine,
        "UPDATE tasks SET status = 'running' WHERE id = :t",
        IntegrityError,
        "ck_task_running_has_attempt",
        t=a,
    )
    _must_fail(
        engine,
        "UPDATE tasks SET status = 'succeeded' WHERE id = :t",
        IntegrityError,
        "ck_task_output_when_succeeded",
        t=a,
    )
    _must_fail(
        engine,
        "UPDATE tasks SET status = 'queued' WHERE id = :t",
        IntegrityError,
        "ck_task_input_when_dispatchable",
        t=b,
    )
    _must_fail(
        engine,
        "UPDATE tasks SET failure_count = max_attempts + 1 WHERE id = :t",
        IntegrityError,
        "ck_task_failures_within_budget",
        t=a,
    )
    _must_fail(engine, "UPDATE tasks SET status = 'bogus' WHERE id = :t", IntegrityError, t=a)
    _must_fail(
        engine,
        "UPDATE runs SET status = 'succeeded' WHERE id = :r",
        IntegrityError,
        "ck_run_finished_iff_terminal",
        r=run_id,
    )
    _must_fail(
        engine,
        "UPDATE runs SET finished_at = now() WHERE id = :r",
        IntegrityError,
        "ck_run_finished_iff_terminal",
        r=run_id,
    )
    _must_fail(
        engine,
        "UPDATE runs SET status = 'cancelling' WHERE id = :r",
        IntegrityError,
        "ck_run_cancelling_has_request",
        r=run_id,
    )
    request_cancel(engine, run_id)  # leave the run terminal and consistent


def test_duplicate_idempotency_key_rejected_by_database(engine: Engine) -> None:
    run_id = submit(engine, "test-chain", idempotency_key="dup")
    _must_fail(
        engine,
        "INSERT INTO runs (id, workflow_name, workflow_version, definition_id, "
        "definition_snapshot, input, status, idempotency_key, request_hash) "
        "SELECT :id, workflow_name, workflow_version, definition_id, definition_snapshot, "
        "input, 'running', 'dup', request_hash FROM runs WHERE id = :r",
        IntegrityError,
        "idempotency_key",
        id=uuid.uuid4(),
        r=run_id,
    )
