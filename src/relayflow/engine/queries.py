"""Read-only queries used by the API and dashboard. They return JSON-able dicts.

Every value shown in the dashboard comes from these queries, i.e. from persisted
PostgreSQL state, including derived timing computed with PostgreSQL's clock.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import Engine, text

from relayflow.definitions import topological_order, validate_definition
from relayflow.engine.errors import RunNotFound, WorkflowNotFound


def _plain(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value


def _row(mapping: Any) -> dict[str, Any]:
    return {k: _plain(v) for k, v in dict(mapping).items()}


RUN_SUMMARY_COLUMNS = (
    "r.id, r.workflow_name, r.workflow_version, r.status, r.idempotency_key, r.manual_retry_count, "
    "r.cancel_requested_at, r.error, r.created_at, r.updated_at, r.finished_at, r.input->>'title' AS title"
)


def get_run(engine: Engine, run_id: UUID) -> dict[str, Any]:
    with engine.connect() as conn:
        run = (
            conn.execute(
                text("SELECT r.*, now() AS db_now FROM runs r WHERE r.id = :id"), {"id": run_id}
            )
            .mappings()
            .first()
        )
        if run is None:
            raise RunNotFound(f"run {run_id} not found")
        tasks = list(
            conn.execute(text("SELECT * FROM tasks WHERE run_id = :id"), {"id": run_id}).mappings()
        )
        attempts = list(
            conn.execute(
                text(
                    "SELECT a.*, (a.status = 'running' AND a.lease_expires_at <= now()) AS lease_lapsed "
                    "FROM attempts a WHERE a.run_id = :id ORDER BY a.attempt_number"
                ),
                {"id": run_id},
            ).mappings()
        )
        conn.rollback()

    spec = validate_definition(run["definition_snapshot"])
    order = {key: i for i, key in enumerate(topological_order(spec))}
    by_task: dict[UUID, list[dict[str, Any]]] = {}
    for attempt in attempts:
        by_task.setdefault(attempt["task_id"], []).append(_row(attempt))
    task_list = [
        {**_row(t), "attempts": by_task.get(t["id"], [])}
        for t in sorted(tasks, key=lambda t: order.get(t["task_key"], 0))
    ]
    return {**_row(run), "tasks": task_list}


def list_runs(engine: Engine, *, status: str | None, limit: int, offset: int) -> dict[str, Any]:
    where = "WHERE r.status = :status" if status else ""
    with engine.connect() as conn:
        total = conn.execute(
            text(f"SELECT count(*) FROM runs r {where}"), {"status": status}
        ).scalar()
        rows = conn.execute(
            text(
                f"SELECT {RUN_SUMMARY_COLUMNS}, "
                "(SELECT count(*) FROM tasks t WHERE t.run_id = r.id) AS task_total, "
                "(SELECT count(*) FROM tasks t WHERE t.run_id = r.id AND t.status = 'succeeded') "
                "  AS task_succeeded, "
                "(SELECT count(*) FROM attempts a WHERE a.run_id = r.id) AS attempt_total "
                f"FROM runs r {where} ORDER BY r.created_at DESC LIMIT :limit OFFSET :offset"
            ),
            {"status": status, "limit": limit, "offset": offset},
        ).mappings()
        items = [_row(r) for r in rows]
        conn.rollback()
    return {"items": items, "total": int(total or 0)}


def list_events(engine: Engine, run_id: UUID, *, after_id: int = 0) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        if conn.execute(text("SELECT 1 FROM runs WHERE id = :id"), {"id": run_id}).first() is None:
            raise RunNotFound(f"run {run_id} not found")
        rows = conn.execute(
            text("SELECT * FROM events WHERE run_id = :id AND id > :after ORDER BY id"),
            {"id": run_id, "after": after_id},
        ).mappings()
        result = [_row(r) for r in rows]
        conn.rollback()
    return result


def list_workers(engine: Engine, *, lease_seconds: float) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT w.*, EXTRACT(EPOCH FROM (now() - w.last_heartbeat_at)) AS heartbeat_age_seconds, "
                "(w.status = 'active' AND now() - w.last_heartbeat_at "
                "   <= make_interval(secs => CAST(:lease AS double precision))) AS healthy, "
                "(SELECT count(*) FROM attempts a WHERE a.worker_id = w.id) AS attempts_total, "
                "(SELECT count(*) FROM attempts a WHERE a.worker_id = w.id AND a.status = 'running') "
                "  AS attempts_running "
                "FROM workers w ORDER BY w.status = 'stopped', w.last_heartbeat_at DESC LIMIT 100"
            ),
            {"lease": lease_seconds},
        ).mappings()
        result = [_row(r) for r in rows]
        conn.rollback()
    for worker in result:
        worker["heartbeat_age_seconds"] = float(worker["heartbeat_age_seconds"])
    return result


def overview(engine: Engine, *, lease_seconds: float) -> dict[str, Any]:
    with engine.connect() as conn:
        runs = {
            r.status: r.n
            for r in conn.execute(text("SELECT status, count(*) AS n FROM runs GROUP BY status"))
        }
        tasks = {
            r.status: r.n
            for r in conn.execute(text("SELECT status, count(*) AS n FROM tasks GROUP BY status"))
        }
        recoveries = conn.execute(
            text("SELECT count(*) FROM attempts WHERE status = 'lease_expired'")
        ).scalar()
        db_now = conn.execute(text("SELECT now()")).scalar()
        conn.rollback()
    return {
        "runs": runs,
        "tasks": tasks,
        "lease_expirations": int(recoveries or 0),
        "workers": list_workers(engine, lease_seconds=lease_seconds),
        "db_now": _plain(db_now),
    }


def list_workflows(engine: Engine) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT id, name, version, spec, created_at FROM workflow_definitions "
                "ORDER BY name, version"
            )
        ).mappings()
        result = [_row(r) for r in rows]
        conn.rollback()
    return result


def get_workflow(engine: Engine, name: str, version: int) -> dict[str, Any]:
    with engine.connect() as conn:
        row = (
            conn.execute(
                text(
                    "SELECT id, name, version, spec, created_at FROM workflow_definitions "
                    "WHERE name = :n AND version = :v"
                ),
                {"n": name, "v": version},
            )
            .mappings()
            .first()
        )
        conn.rollback()
    if row is None:
        raise WorkflowNotFound(f"workflow {name} v{version} not found")
    return _row(row)
