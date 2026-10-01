"""State-changing engine operations.

Each public function runs exactly one short transaction (`with engine.begin()`)
on its own pooled connection, and never runs task code. Transitions follow the
state machines in docs/architecture.md section 5; the numbers in comments (T5,
R3, ...) refer to that document.

Locking rule (section 2.1): run -> task -> attempt. Every function that changes
run-level state first locks the run row. The claim only locks a task row with
SKIP LOCKED.
"""

from __future__ import annotations

import hashlib
import json
import random
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy import Connection, Engine, RowMapping, text

from relayflow.definitions import canonical_json, descendants, validate_definition
from relayflow.engine.errors import (
    DefinitionConflict,
    IdempotencyConflict,
    InvalidTransition,
    RunNotFound,
    ValidationFailed,
    WorkflowNotFound,
)
from relayflow.states import (
    AttemptStatus,
    RunStatus,
    TaskStatus,
    assert_run_transition,
    assert_task_transition,
)

Row = Mapping[str, Any] | RowMapping

MAX_RUN_INPUT_BYTES = 64 * 1024
MAX_OUTPUT_BYTES = 64 * 1024
MAX_ERROR_CHARS = 2000
MAX_DEMO_DELAY_SECONDS = 30


# --------------------------------------------------------------------------- results


@dataclass(frozen=True)
class RegisterResult:
    definition_id: int
    created: bool


@dataclass(frozen=True)
class SubmitResult:
    run_id: UUID
    created: bool


@dataclass(frozen=True)
class ClaimedTask:
    attempt_id: UUID
    lease_token: UUID
    task_id: UUID
    run_id: UUID
    task_key: str
    task_type: str
    input: dict[str, Any]
    attempt_number: int
    timeout_seconds: float


@dataclass(frozen=True)
class HeartbeatResult:
    owned: bool
    cancel_requested: bool


@dataclass(frozen=True)
class RecoveryResult:
    expired: int
    timed_out: int


# --------------------------------------------------------------------------- helpers


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"))


def _truncate(message: str) -> str:
    return message if len(message) <= MAX_ERROR_CHARS else message[: MAX_ERROR_CHARS - 3] + "..."


def backoff_delay(
    failure_count: int, base: float, cap: float, rng: random.Random | None = None
) -> float:
    """Exponential backoff with "equal jitter": a delay in [raw/2, raw] (spec 6.7)."""
    raw = min(cap, base * (2 ** max(0, failure_count - 1)))
    r = rng if rng is not None else random
    return float(raw / 2 + r.uniform(0, raw / 2))


def _event(
    conn: Connection,
    run_id: UUID,
    kind: str,
    message: str,
    *,
    task_key: str | None = None,
    attempt_id: UUID | None = None,
    worker_id: str | None = None,
    data: Mapping[str, Any] | None = None,
) -> None:
    conn.execute(
        text(
            "INSERT INTO events (run_id, task_key, attempt_id, worker_id, kind, message, data) "
            "VALUES (:run_id, :task_key, :attempt_id, :worker_id, :kind, :message, "
            "CAST(:data AS jsonb))"
        ),
        {
            "run_id": run_id,
            "task_key": task_key,
            "attempt_id": attempt_id,
            "worker_id": worker_id,
            "kind": kind,
            "message": message,
            "data": _json(dict(data or {})),
        },
    )


def _lock_run(conn: Connection, run_id: UUID) -> RowMapping:
    row = (
        conn.execute(text("SELECT * FROM runs WHERE id = :id FOR NO KEY UPDATE"), {"id": run_id})
        .mappings()
        .first()
    )
    if row is None:
        raise RunNotFound(f"run {run_id} not found")
    return row


def _lock_task(conn: Connection, task_id: UUID) -> RowMapping:
    row = (
        conn.execute(text("SELECT * FROM tasks WHERE id = :id FOR NO KEY UPDATE"), {"id": task_id})
        .mappings()
        .one()
    )
    return row


def _set_task_status(
    conn: Connection, task: RowMapping, new: TaskStatus, extra_sql: str = "", **params: Any
) -> None:
    """Guarded task update: only applies if the task is still in its observed state."""
    old = TaskStatus(task["status"])
    assert_task_transition(old, new)
    sets = "status = :new, updated_at = now()" + (", " + extra_sql if extra_sql else "")
    result = conn.execute(
        text(f"UPDATE tasks SET {sets} WHERE id = :id AND status = :old"),
        {"new": new.value, "old": old.value, "id": task["id"], **params},
    )
    if result.rowcount != 1:  # cannot happen while we hold the run lock; fail loudly if it does
        raise RuntimeError(f"task {task['id']} changed state concurrently (expected {old})")


def _set_run_status(conn: Connection, run: Row, new: RunStatus, extra_sql: str = "") -> None:
    old = RunStatus(run["status"])
    assert_run_transition(old, new)
    terminal = new in (RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED)
    sets = "status = :new, updated_at = now()"
    sets += ", finished_at = now()" if terminal else ", finished_at = NULL"
    if extra_sql:
        sets += ", " + extra_sql
    conn.execute(
        text(f"UPDATE runs SET {sets} WHERE id = :id AND status = :old"),
        {"new": new.value, "old": old.value, "id": run["id"]},
    )


def _task_rows(conn: Connection, run_id: UUID, *, lock: bool = False) -> list[RowMapping]:
    sql = "SELECT * FROM tasks WHERE run_id = :run_id ORDER BY created_at, task_key"
    if lock:
        sql += " FOR NO KEY UPDATE"
    return list(conn.execute(text(sql), {"run_id": run_id}).mappings())


def _materialized_input(run: Row, task: Row, outputs: Mapping[str, Any]) -> str:
    """Spec 6.2: everything a worker needs, persisted on the task row."""
    spec_task = next(t for t in run["definition_snapshot"]["tasks"] if t["key"] == task["task_key"])
    return _json(
        {
            "run_input": run["input"],
            "params": spec_task.get("params", {}),
            "deps": {dep: outputs[dep] for dep in task["depends_on"]},
        }
    )


def _promote_ready(conn: Connection, run: Row) -> list[str]:
    """T3: pending tasks whose dependencies all succeeded become queued (run must be running)."""
    if run["status"] != RunStatus.RUNNING:
        return []
    tasks = _task_rows(conn, run["id"])
    outputs = {t["task_key"]: t["output"] for t in tasks if t["status"] == TaskStatus.SUCCEEDED}
    promoted = []
    for task in tasks:
        if task["status"] == TaskStatus.PENDING and all(d in outputs for d in task["depends_on"]):
            _set_task_status(
                conn,
                task,
                TaskStatus.QUEUED,
                "input = CAST(:input AS jsonb), available_at = now(), queued_at = now()",
                input=_materialized_input(run, task, outputs),
            )
            promoted.append(task["task_key"])
    if promoted:
        _event(
            conn,
            run["id"],
            "tasks_ready",
            f"dependencies satisfied: {', '.join(promoted)}",
            data={"tasks": promoted},
        )
    return promoted


def _block_descendants(conn: Connection, run: Row, failed_key: str) -> list[str]:
    """T9: every pending transitive dependent of a terminally failed task becomes blocked."""
    targets = descendants(run["definition_snapshot"], failed_key)
    blocked = []
    for task in _task_rows(conn, run["id"]):
        if task["task_key"] in targets and task["status"] == TaskStatus.PENDING:
            _set_task_status(
                conn,
                task,
                TaskStatus.BLOCKED,
                "last_error = :err",
                err=f"blocked: dependency '{failed_key}' failed",
            )
            blocked.append(task["task_key"])
    if blocked:
        _event(
            conn,
            run["id"],
            "tasks_blocked",
            f"blocked by failure of '{failed_key}': {', '.join(sorted(blocked))}",
            task_key=failed_key,
            data={"tasks": sorted(blocked)},
        )
    return blocked


def _finalize_run(conn: Connection, run_id: UUID) -> str:
    """R2/R3/R5: settle the run's status once no task can make progress."""
    run = conn.execute(text("SELECT * FROM runs WHERE id = :id"), {"id": run_id}).mappings().one()
    counts: dict[str, int] = {
        row.status: row.n
        for row in conn.execute(
            text("SELECT status, count(*) AS n FROM tasks WHERE run_id = :id GROUP BY status"),
            {"id": run_id},
        )
    }
    status = RunStatus(run["status"])
    active = sum(counts.get(s, 0) for s in ("pending", "queued", "running"))
    if status == RunStatus.CANCELLING and counts.get("running", 0) == 0:
        _set_run_status(conn, run, RunStatus.CANCELLED)
        _event(conn, run_id, "run_cancelled", "run cancelled")
        return RunStatus.CANCELLED
    if status == RunStatus.RUNNING and active == 0:
        total = sum(counts.values())
        if counts.get("succeeded", 0) == total:
            _set_run_status(conn, run, RunStatus.SUCCEEDED)
            _event(conn, run_id, "run_succeeded", "all tasks succeeded")
            return RunStatus.SUCCEEDED
        _set_run_status(conn, run, RunStatus.FAILED)
        _event(conn, run_id, "run_failed", f"run failed: {run['error'] or 'task failure'}")
        return RunStatus.FAILED
    return status


# --------------------------------------------------------------------------- definitions


def register_workflow(engine: Engine, spec: Any) -> RegisterResult:
    workflow = validate_definition(spec)
    normalized = workflow.to_dict()
    with engine.begin() as conn:
        new_id = conn.execute(
            text(
                "INSERT INTO workflow_definitions (name, version, spec) "
                "VALUES (:name, :version, CAST(:spec AS jsonb)) "
                "ON CONFLICT (name, version) DO NOTHING RETURNING id"
            ),
            {"name": workflow.name, "version": workflow.version, "spec": _json(normalized)},
        ).scalar()
        if new_id is not None:
            return RegisterResult(definition_id=int(new_id), created=True)
        existing = conn.execute(
            text("SELECT id, spec FROM workflow_definitions WHERE name = :n AND version = :v"),
            {"n": workflow.name, "v": workflow.version},
        ).one()
        if canonical_json(existing.spec) != canonical_json(normalized):
            raise DefinitionConflict(
                f"{workflow.name} v{workflow.version} already exists with a different definition; "
                "register a new version instead"
            )
        return RegisterResult(definition_id=int(existing.id), created=False)


# --------------------------------------------------------------------------- submission


def validate_run_input(
    run_input: Any, snapshot: Mapping[str, Any], allow_fault_injection: bool
) -> None:
    errors: list[str] = []
    if not isinstance(run_input, dict):
        raise ValidationFailed(["input must be a JSON object"])
    if len(_json(run_input).encode()) > MAX_RUN_INPUT_BYTES:
        raise ValidationFailed([f"input exceeds {MAX_RUN_INPUT_BYTES} bytes"])
    demo = run_input.get("demo")
    if demo is None:
        return
    if not isinstance(demo, dict):
        raise ValidationFailed(["input.demo must be an object"])
    keys = {t["key"] for t in snapshot["tasks"]}
    for option, value in demo.items():
        if option not in ("delay_seconds", "fail_attempts", "crash_after_execute"):
            errors.append(f"unknown demo option {option!r}")
            continue
        if option != "delay_seconds" and not allow_fault_injection:
            errors.append(
                f"demo.{option} is fault injection and is disabled "
                "(set RELAYFLOW_ENABLE_FAULT_INJECTION=1 for local demos/tests)"
            )
            continue
        if not isinstance(value, dict):
            errors.append(f"demo.{option} must map task keys to values")
            continue
        for task_key, setting in value.items():
            if task_key not in keys:
                errors.append(f"demo.{option}: unknown task {task_key!r}")
            elif option == "delay_seconds":
                if (
                    not isinstance(setting, (int, float))
                    or isinstance(setting, bool)
                    or not (0 <= setting <= MAX_DEMO_DELAY_SECONDS)
                ):
                    errors.append(
                        f"demo.delay_seconds.{task_key} must be 0..{MAX_DEMO_DELAY_SECONDS}"
                    )
            elif not (
                isinstance(setting, list)
                and all(
                    isinstance(n, int) and not isinstance(n, bool) and 1 <= n <= 20 for n in setting
                )
            ):
                errors.append(f"demo.{option}.{task_key} must be a list of attempt numbers 1..20")
    if errors:
        raise ValidationFailed(errors)


def request_hash(workflow_name: str, workflow_version: int, run_input: Any) -> str:
    payload = canonical_json(
        {"workflow": workflow_name, "version": workflow_version, "input": run_input}
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def submit_run(
    engine: Engine,
    *,
    workflow_name: str,
    input: Any,
    workflow_version: int | None = None,
    idempotency_key: str | None = None,
    allow_fault_injection: bool = False,
) -> SubmitResult:
    if idempotency_key is not None and not 1 <= len(idempotency_key) <= 200:
        raise ValidationFailed(["idempotency_key must be 1..200 characters"])
    with engine.begin() as conn:
        if workflow_version is None:
            definition = (
                conn.execute(
                    text(
                        "SELECT * FROM workflow_definitions WHERE name = :n ORDER BY version DESC LIMIT 1"
                    ),
                    {"n": workflow_name},
                )
                .mappings()
                .first()
            )
        else:
            definition = (
                conn.execute(
                    text("SELECT * FROM workflow_definitions WHERE name = :n AND version = :v"),
                    {"n": workflow_name, "v": workflow_version},
                )
                .mappings()
                .first()
            )
        if definition is None:
            version_text = "latest" if workflow_version is None else f"v{workflow_version}"
            raise WorkflowNotFound(f"workflow {workflow_name} {version_text} not found")
        snapshot = definition["spec"]
        validate_run_input(input, snapshot, allow_fault_injection)
        digest = request_hash(definition["name"], definition["version"], input)

        run_id = uuid.uuid4()
        inserted = conn.execute(
            text(
                "INSERT INTO runs (id, workflow_name, workflow_version, definition_id, "
                "definition_snapshot, input, status, idempotency_key, request_hash) "
                "VALUES (:id, :name, :version, :def_id, CAST(:snapshot AS jsonb), "
                "CAST(:input AS jsonb), 'running', :key, :hash) "
                "ON CONFLICT (idempotency_key) DO NOTHING RETURNING id"
            ),
            {
                "id": run_id,
                "name": definition["name"],
                "version": definition["version"],
                "def_id": definition["id"],
                "snapshot": _json(snapshot),
                "input": _json(input),
                "key": idempotency_key,
                "hash": digest,
            },
        ).scalar()
        if inserted is None:
            # The key exists (possibly committed a moment ago by a concurrent submission:
            # ON CONFLICT waited for it). Same request -> same logical run.
            existing = conn.execute(
                text("SELECT id, request_hash FROM runs WHERE idempotency_key = :k"),
                {"k": idempotency_key},
            ).one()
            if existing.request_hash != digest:
                raise IdempotencyConflict(
                    f"idempotency key {idempotency_key!r} was already used with a different request"
                )
            return SubmitResult(run_id=existing.id, created=False)

        assert_run_transition(None, RunStatus.RUNNING)  # R1
        run = {"id": run_id, "input": input, "definition_snapshot": snapshot}
        for spec_task in snapshot["tasks"]:
            ready = not spec_task["depends_on"]
            status = TaskStatus.QUEUED if ready else TaskStatus.PENDING  # T2 / T1
            assert_task_transition(None, status)
            task = {"task_key": spec_task["key"], "depends_on": spec_task["depends_on"]}
            conn.execute(
                text(
                    "INSERT INTO tasks (id, run_id, task_key, task_type, depends_on, status, input, "
                    "max_attempts, timeout_seconds, backoff_base_seconds, backoff_max_seconds, "
                    "queued_at) VALUES (:id, :run_id, :key, :type, :deps, :status, "
                    "CAST(:input AS jsonb), :max_attempts, :timeout, :base, :cap, "
                    "CASE WHEN :ready THEN now() END)"
                ),
                {
                    "id": uuid.uuid4(),
                    "run_id": run_id,
                    "key": spec_task["key"],
                    "type": spec_task["type"],
                    "deps": list(spec_task["depends_on"]),
                    "status": status.value,
                    "input": _materialized_input(run, task, {}) if ready else None,
                    "max_attempts": spec_task["max_attempts"],
                    "timeout": spec_task["timeout_seconds"],
                    "base": spec_task["backoff_base_seconds"],
                    "cap": spec_task["backoff_max_seconds"],
                    "ready": ready,
                },
            )
        _event(
            conn,
            run_id,
            "run_submitted",
            f"submitted {definition['name']} v{definition['version']}",
            data={"idempotency_key": idempotency_key},
        )
        return SubmitResult(run_id=run_id, created=True)


# --------------------------------------------------------------------------- claim / heartbeat


def claim_task(engine: Engine, *, worker_id: str, lease_seconds: float) -> ClaimedTask | None:
    """T4: atomically claim one ready task (SKIP LOCKED) and create its attempt."""
    with engine.begin() as conn:
        task = (
            conn.execute(
                text(
                    "SELECT t.* FROM tasks t JOIN runs r ON r.id = t.run_id "
                    "WHERE t.status = 'queued' AND t.available_at <= now() "
                    "AND r.status = 'running' AND r.cancel_requested_at IS NULL "
                    "ORDER BY t.available_at, t.created_at "
                    "LIMIT 1 FOR NO KEY UPDATE OF t SKIP LOCKED"
                )
            )
            .mappings()
            .first()
        )
        if task is None:
            return None
        attempt_id, token = uuid.uuid4(), uuid.uuid4()
        attempt_number = task["attempt_count"] + 1
        conn.execute(
            text(
                "INSERT INTO attempts (id, task_id, run_id, attempt_number, worker_id, lease_token, "
                "status, lease_expires_at) VALUES (:id, :task_id, :run_id, :n, :worker, :token, "
                "'running', now() + make_interval(secs => CAST(:lease AS double precision)))"
            ),
            {
                "id": attempt_id,
                "task_id": task["id"],
                "run_id": task["run_id"],
                "n": attempt_number,
                "worker": worker_id,
                "token": token,
                "lease": lease_seconds,
            },
        )
        _set_task_status(
            conn,
            task,
            TaskStatus.RUNNING,
            "current_attempt_id = :aid, attempt_count = attempt_count + 1",
            aid=attempt_id,
        )
        previous = conn.execute(
            text(
                "SELECT status, worker_id FROM attempts WHERE task_id = :t AND attempt_number = :n"
            ),
            {"t": task["id"], "n": attempt_number - 1},
        ).first()
        message = f"attempt {attempt_number} claimed by {worker_id}"
        data: dict[str, Any] = {"attempt_number": attempt_number}
        if previous is not None:
            message += f" (previous attempt {previous.status} on {previous.worker_id})"
            data |= {"previous_status": previous.status, "previous_worker": previous.worker_id}
        _event(
            conn,
            task["run_id"],
            "task_claimed",
            message,
            task_key=task["task_key"],
            attempt_id=attempt_id,
            worker_id=worker_id,
            data=data,
        )
        return ClaimedTask(
            attempt_id=attempt_id,
            lease_token=token,
            task_id=task["id"],
            run_id=task["run_id"],
            task_key=task["task_key"],
            task_type=task["task_type"],
            input=task["input"],
            attempt_number=attempt_number,
            timeout_seconds=float(task["timeout_seconds"]),
        )


def heartbeat(
    engine: Engine, leases: Sequence[tuple[UUID, UUID]], *, lease_seconds: float
) -> dict[UUID, HeartbeatResult]:
    """Renew leases still owned (token matches, running, not yet expired). Never revives a lapsed lease."""
    if not leases:
        return {}
    with engine.begin() as conn:
        renewed = {
            row.id: row.run_id
            for row in conn.execute(
                text(
                    "UPDATE attempts a SET heartbeat_at = now(), "
                    "lease_expires_at = now() + make_interval(secs => CAST(:lease AS double precision)) "
                    "FROM (SELECT unnest(CAST(:ids AS uuid[])) AS id, "
                    "             unnest(CAST(:tokens AS uuid[])) AS token) l "
                    "WHERE a.id = l.id AND a.lease_token = l.token AND a.status = 'running' "
                    "AND a.lease_expires_at > now() RETURNING a.id, a.run_id"
                ),
                {
                    "ids": [a for a, _ in leases],
                    "tokens": [t for _, t in leases],
                    "lease": lease_seconds,
                },
            )
        }
        cancelling: set[UUID] = set()
        if renewed:
            cancelling = {
                row.id
                for row in conn.execute(
                    text(
                        "SELECT id FROM runs WHERE id = ANY(CAST(:ids AS uuid[])) "
                        "AND cancel_requested_at IS NOT NULL"
                    ),
                    {"ids": list(set(renewed.values()))},
                )
            }
    return {
        attempt_id: HeartbeatResult(
            owned=attempt_id in renewed,
            cancel_requested=renewed.get(attempt_id) in cancelling,
        )
        for attempt_id, _ in leases
    }


# --------------------------------------------------------------------------- reporting


def _owned_attempt(
    conn: Connection, attempt_id: UUID, lease_token: UUID
) -> tuple[RowMapping, RowMapping, RowMapping] | None:
    """Lock run -> task -> attempt and return them if the caller still owns the attempt.

    Ownership = token matches, attempt still running, and the lease has not lapsed in
    PostgreSQL time. Anything else is a stale report and changes nothing.
    """
    ref = conn.execute(
        text("SELECT run_id, task_id FROM attempts WHERE id = :id"), {"id": attempt_id}
    ).first()
    if ref is None:
        return None
    run = _lock_run(conn, ref.run_id)
    task = _lock_task(conn, ref.task_id)
    attempt = (
        conn.execute(
            text(
                "SELECT * FROM attempts WHERE id = :id AND lease_token = :token "
                "AND status = 'running' AND lease_expires_at > now() FOR NO KEY UPDATE"
            ),
            {"id": attempt_id, "token": lease_token},
        )
        .mappings()
        .first()
    )
    if attempt is None or task["current_attempt_id"] != attempt_id:
        return None
    return run, task, attempt


def _close_attempt(
    conn: Connection, attempt_id: UUID, status: AttemptStatus, error: str | None
) -> None:
    conn.execute(
        text(
            "UPDATE attempts SET status = :s, finished_at = now(), error = :e "
            "WHERE id = :id AND status = 'running'"
        ),
        {"s": status.value, "e": _truncate(error) if error else None, "id": attempt_id},
    )


def _after_attempt_failure(
    conn: Connection,
    run: RowMapping,
    task: RowMapping,
    attempt: RowMapping,
    outcome: AttemptStatus,
    error: str,
    *,
    retryable: bool,
    rng: random.Random | None,
) -> None:
    """Shared tail of fail/timeout/expiry: T12 if cancelling, else T6 (retry) or T7 (+T9)."""
    _close_attempt(conn, attempt["id"], outcome, error)
    key, run_id = task["task_key"], run["id"]
    worker = attempt["worker_id"]
    _event(
        conn,
        run_id,
        f"attempt_{outcome.value}",
        f"attempt {attempt['attempt_number']} {outcome.value} on {worker}: {_truncate(error)}",
        task_key=key,
        attempt_id=attempt["id"],
        worker_id=worker,
        data={"attempt_number": attempt["attempt_number"]},
    )
    if run["status"] == RunStatus.CANCELLING:
        _set_task_status(
            conn,
            task,
            TaskStatus.CANCELLED,
            "current_attempt_id = NULL, finished_at = now(), last_error = :e",
            e=_truncate(error),
        )
        _event(conn, run_id, "task_cancelled", "cancelled (run is cancelling)", task_key=key)
    else:
        failures = task["failure_count"] + 1
        if retryable and failures < task["max_attempts"]:
            delay = backoff_delay(
                failures, task["backoff_base_seconds"], task["backoff_max_seconds"], rng
            )
            _set_task_status(
                conn,
                task,
                TaskStatus.QUEUED,
                "current_attempt_id = NULL, failure_count = :f, last_error = :e, "
                "available_at = now() + make_interval(secs => CAST(:d AS double precision)), "
                "queued_at = now()",
                f=failures,
                e=_truncate(error),
                d=delay,
            )
            _event(
                conn,
                run_id,
                "retry_scheduled",
                f"retry {failures + 1}/{task['max_attempts']} in {delay:.2f}s",
                task_key=key,
                data={"delay_seconds": round(delay, 3), "failure_count": failures},
            )
        else:
            _set_task_status(
                conn,
                task,
                TaskStatus.FAILED,
                "current_attempt_id = NULL, failure_count = :f, last_error = :e, finished_at = now()",
                f=failures,
                e=_truncate(error),
            )
            reason = (
                "not retryable"
                if not retryable
                else f"{failures}/{task['max_attempts']} attempts failed"
            )
            _event(conn, run_id, "task_failed", f"task failed ({reason})", task_key=key)
            if run["error"] is None:
                conn.execute(
                    text("UPDATE runs SET error = :e WHERE id = :id"),
                    {"e": _truncate(f"task '{key}' failed: {error}"), "id": run_id},
                )
            _block_descendants(conn, run, key)
    _finalize_run(conn, run_id)


def complete_attempt(
    engine: Engine, *, attempt_id: UUID, lease_token: UUID, output: dict[str, Any]
) -> bool:
    """T5: store the output and mark the task succeeded atomically. False if the report is stale."""
    if not isinstance(output, dict):
        raise ValidationFailed(["task output must be a JSON object"])
    encoded = _json(output)
    if len(encoded.encode()) > MAX_OUTPUT_BYTES:
        raise ValidationFailed([f"task output exceeds {MAX_OUTPUT_BYTES} bytes"])
    with engine.begin() as conn:
        owned = _owned_attempt(conn, attempt_id, lease_token)
        if owned is None:
            return False
        run, task, attempt = owned
        _close_attempt(conn, attempt_id, AttemptStatus.SUCCEEDED, None)
        _set_task_status(
            conn,
            task,
            TaskStatus.SUCCEEDED,
            "output = CAST(:out AS jsonb), current_attempt_id = NULL, finished_at = now(), "
            "last_error = NULL",
            out=encoded,
        )
        _event(
            conn,
            run["id"],
            "task_succeeded",
            f"attempt {attempt['attempt_number']} succeeded on {attempt['worker_id']}",
            task_key=task["task_key"],
            attempt_id=attempt_id,
            worker_id=attempt["worker_id"],
        )
        _promote_ready(conn, run)  # no-op unless the run is still running (cancel race, 6.9)
        _finalize_run(conn, run["id"])
        return True


def fail_attempt(
    engine: Engine,
    *,
    attempt_id: UUID,
    lease_token: UUID,
    error: str,
    retryable: bool = True,
    kind: str = "failed",
    rng: random.Random | None = None,
) -> bool:
    """T6/T7/T12 for a handler error (kind="failed") or a worker-detected timeout ("timed_out")."""
    outcome = {"failed": AttemptStatus.FAILED, "timed_out": AttemptStatus.TIMED_OUT}.get(kind)
    if outcome is None:
        raise ValueError(f"kind must be 'failed' or 'timed_out', not {kind!r}")
    with engine.begin() as conn:
        owned = _owned_attempt(conn, attempt_id, lease_token)
        if owned is None:
            return False
        run, task, attempt = owned
        _after_attempt_failure(
            conn, run, task, attempt, outcome, error, retryable=retryable, rng=rng
        )
        return True


def cancel_attempt(engine: Engine, *, attempt_id: UUID, lease_token: UUID) -> bool:
    """T11: the handler stopped because the run is cancelling. If the run is not
    cancelling (the worker stopped for another reason) this behaves like release (T8)."""
    with engine.begin() as conn:
        owned = _owned_attempt(conn, attempt_id, lease_token)
        if owned is None:
            return False
        run, task, attempt = owned
        if run["status"] != RunStatus.CANCELLING:
            _release(conn, run, task, attempt)
            return True
        _close_attempt(conn, attempt_id, AttemptStatus.CANCELLED, "cancelled cooperatively")
        _set_task_status(
            conn, task, TaskStatus.CANCELLED, "current_attempt_id = NULL, finished_at = now()"
        )
        _event(
            conn,
            run["id"],
            "task_cancelled",
            f"attempt {attempt['attempt_number']} stopped cooperatively on {attempt['worker_id']}",
            task_key=task["task_key"],
            attempt_id=attempt_id,
            worker_id=attempt["worker_id"],
        )
        _finalize_run(conn, run["id"])
        return True


def _release(conn: Connection, run: RowMapping, task: RowMapping, attempt: RowMapping) -> None:
    _close_attempt(conn, attempt["id"], AttemptStatus.RELEASED, "released by worker shutdown")
    _event(
        conn,
        run["id"],
        "attempt_released",
        f"attempt {attempt['attempt_number']} released by {attempt['worker_id']}",
        task_key=task["task_key"],
        attempt_id=attempt["id"],
        worker_id=attempt["worker_id"],
    )
    if run["status"] == RunStatus.CANCELLING:
        _set_task_status(
            conn, task, TaskStatus.CANCELLED, "current_attempt_id = NULL, finished_at = now()"
        )
        _finalize_run(conn, run["id"])
    else:
        _set_task_status(
            conn,
            task,
            TaskStatus.QUEUED,
            "current_attempt_id = NULL, available_at = now(), queued_at = now()",
        )


def release_attempt(engine: Engine, *, attempt_id: UUID, lease_token: UUID) -> bool:
    """T8: graceful shutdown hands the task back immediately; not counted as a failure."""
    with engine.begin() as conn:
        owned = _owned_attempt(conn, attempt_id, lease_token)
        if owned is None:
            return False
        _release(conn, *owned)
        return True


# --------------------------------------------------------------------------- recovery


def _recover_one(
    conn: Connection,
    attempt_id: UUID,
    condition: str,
    outcome: AttemptStatus,
    grace: float,
    rng: random.Random | None,
) -> bool:
    ref = conn.execute(
        text("SELECT run_id, task_id FROM attempts WHERE id = :id"), {"id": attempt_id}
    ).first()
    if ref is None:
        return False
    run = _lock_run(conn, ref.run_id)
    task = _lock_task(conn, ref.task_id)
    attempt = (
        conn.execute(
            text(
                "SELECT a.* FROM attempts a JOIN tasks t ON t.id = a.task_id "
                f"WHERE a.id = :id AND a.status = 'running' AND {condition} FOR NO KEY UPDATE OF a"
            ),
            {"id": attempt_id, "grace": grace},
        )
        .mappings()
        .first()
    )
    if attempt is None:  # renewed, reported, or recovered by someone else meanwhile
        return False
    if outcome == AttemptStatus.LEASE_EXPIRED:
        error = f"lease expired (worker {attempt['worker_id']} stopped heartbeating)"
    else:
        error = f"timed out after {task['timeout_seconds']:g}s (detected by scheduler)"
    _after_attempt_failure(conn, run, task, attempt, outcome, error, retryable=True, rng=rng)
    return True


LEASE_EXPIRED_SQL = "a.lease_expires_at <= now()"
TIMED_OUT_SQL = "a.started_at + make_interval(secs => t.timeout_seconds + CAST(:grace AS double precision)) <= now()"


def recover_expired(
    engine: Engine,
    *,
    limit: int = 100,
    timeout_grace_seconds: float = 5.0,
    rng: random.Random | None = None,
) -> RecoveryResult:
    """Scheduler step (6.6): expire lapsed leases, then enforce the timeout backstop.

    Candidates are found without locks; each is re-checked under the run lock in its
    own short transaction, so concurrent schedulers and late heartbeats are safe.
    """
    counts = {}
    for outcome, condition in (
        (AttemptStatus.LEASE_EXPIRED, LEASE_EXPIRED_SQL),
        (AttemptStatus.TIMED_OUT, TIMED_OUT_SQL),
    ):
        with engine.connect() as conn:
            ids: list[UUID] = list(
                conn.execute(
                    text(
                        "SELECT a.id FROM attempts a JOIN tasks t ON t.id = a.task_id "
                        f"WHERE a.status = 'running' AND {condition} ORDER BY a.started_at LIMIT :limit"
                    ),
                    {"limit": limit, "grace": timeout_grace_seconds},
                ).scalars()
            )
            conn.rollback()
        done = 0
        for attempt_id in ids:
            with engine.begin() as conn:
                done += _recover_one(
                    conn, attempt_id, condition, outcome, timeout_grace_seconds, rng
                )
        counts[outcome] = done
    return RecoveryResult(
        expired=counts[AttemptStatus.LEASE_EXPIRED], timed_out=counts[AttemptStatus.TIMED_OUT]
    )


# --------------------------------------------------------------------------- run controls


def request_cancel(engine: Engine, run_id: UUID) -> str:
    """R4 (+T10, possibly R5). Idempotent while cancelling; 409-style error when terminal."""
    with engine.begin() as conn:
        run = _lock_run(conn, run_id)
        status = RunStatus(run["status"])
        if status == RunStatus.CANCELLING:
            return status.value
        if status != RunStatus.RUNNING:
            raise InvalidTransition(f"run is {status.value}; only running runs can be cancelled")
        _set_run_status(conn, run, RunStatus.CANCELLING, "cancel_requested_at = now()")
        cancelled = []
        for task in _task_rows(conn, run_id, lock=True):
            if task["status"] in (TaskStatus.PENDING, TaskStatus.QUEUED):
                _set_task_status(conn, task, TaskStatus.CANCELLED, "finished_at = now()")
                cancelled.append(task["task_key"])
        running = [
            t["task_key"] for t in _task_rows(conn, run_id) if t["status"] == TaskStatus.RUNNING
        ]
        _event(
            conn,
            run_id,
            "cancel_requested",
            f"cancel requested; cancelled {cancelled or 'nothing'}; "
            f"waiting for running {running or 'none'}",
            data={"cancelled": cancelled, "running": running},
        )
        return _finalize_run(conn, run_id)


def retry_run(engine: Engine, run_id: UUID) -> str:
    """R6 (+T13, T14, T3): retry a terminally failed run, keeping succeeded outputs."""
    with engine.begin() as conn:
        run = _lock_run(conn, run_id)
        if run["status"] != RunStatus.FAILED:
            raise InvalidTransition(f"run is {run['status']}; only failed runs can be retried")
        tasks = _task_rows(conn, run_id, lock=True)
        if any(
            t["status"] in ("pending", "queued", "running") for t in tasks
        ):  # impossible if R3 held
            raise InvalidTransition("run still has active tasks")
        reset_failed, reset_blocked = [], []
        _set_run_status(
            conn,
            run,
            RunStatus.RUNNING,
            "manual_retry_count = manual_retry_count + 1, error = NULL",
        )
        outputs = {t["task_key"]: t["output"] for t in tasks if t["status"] == TaskStatus.SUCCEEDED}
        for task in tasks:
            if task["status"] == TaskStatus.FAILED:
                # Dependencies of a task that ran are all succeeded, so it is ready (T13).
                _set_task_status(
                    conn,
                    task,
                    TaskStatus.QUEUED,
                    "failure_count = 0, finished_at = NULL, available_at = now(), queued_at = now(), "
                    "input = CAST(:input AS jsonb)",
                    input=_materialized_input(run, task, outputs),
                )
                reset_failed.append(task["task_key"])
            elif task["status"] == TaskStatus.BLOCKED:
                _set_task_status(
                    conn,
                    task,
                    TaskStatus.PENDING,
                    "failure_count = 0, finished_at = NULL, last_error = NULL",
                )
                reset_blocked.append(task["task_key"])
        run = _lock_run(conn, run_id)  # re-read: status is now running
        _event(
            conn,
            run_id,
            "run_retried",
            f"manual retry #{run['manual_retry_count']}: requeued {reset_failed}, "
            f"unblocked {reset_blocked}; kept {sorted(outputs)}",
            data={"requeued": reset_failed, "unblocked": reset_blocked, "kept": sorted(outputs)},
        )
        _promote_ready(conn, run)
        return RunStatus(run["status"]).value
