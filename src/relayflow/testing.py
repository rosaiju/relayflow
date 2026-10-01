"""Helpers for tests and local demos. Not used by the engine at runtime.

Includes explicit failure points (force_lease_expiry) so tests can create
lease-expiry situations deterministically instead of sleeping and hoping.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from collections.abc import Callable, Mapping
from typing import Any
from uuid import UUID

from sqlalchemy import Engine, text

from relayflow.catalog import TEST_WORKFLOWS

__all__ = [
    "TEST_WORKFLOWS",
    "attempts_for",
    "check_invariants",
    "events_for",
    "force_available_now",
    "force_lease_expiry",
    "run_status",
    "start_mocknotify_process",
    "start_scheduler_process",
    "start_worker_process",
    "task_states",
    "wait_for",
]

FAST_TIMINGS = {
    "RELAYFLOW_LEASE_SECONDS": "2",
    "RELAYFLOW_HEARTBEAT_SECONDS": "0.5",
    "RELAYFLOW_POLL_INTERVAL_SECONDS": "0.1",
    "RELAYFLOW_SCHEDULER_INTERVAL_SECONDS": "0.2",
    "RELAYFLOW_TIMEOUT_GRACE_SECONDS": "1",
    "RELAYFLOW_SHUTDOWN_GRACE_SECONDS": "3",
}


def force_lease_expiry(engine: Engine, attempt_id: UUID) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE attempts SET lease_expires_at = now() - interval '1 second' "
                "WHERE id = :id AND status = 'running'"
            ),
            {"id": attempt_id},
        )


def force_available_now(engine: Engine, task_id: UUID) -> None:
    with engine.begin() as conn:
        conn.execute(text("UPDATE tasks SET available_at = now() WHERE id = :id"), {"id": task_id})


def run_status(engine: Engine, run_id: UUID) -> str:
    with engine.connect() as conn:
        return str(
            conn.execute(
                text("SELECT status FROM runs WHERE id = :id"), {"id": run_id}
            ).scalar_one()
        )


def task_states(engine: Engine, run_id: UUID) -> dict[str, str]:
    with engine.connect() as conn:
        return {
            row.task_key: row.status
            for row in conn.execute(
                text("SELECT task_key, status FROM tasks WHERE run_id = :id"), {"id": run_id}
            )
        }


def attempts_for(engine: Engine, run_id: UUID, task_key: str) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        return [
            dict(r)
            for r in conn.execute(
                text(
                    "SELECT a.* FROM attempts a JOIN tasks t ON t.id = a.task_id "
                    "WHERE a.run_id = :id AND t.task_key = :key ORDER BY a.attempt_number"
                ),
                {"id": run_id, "key": task_key},
            ).mappings()
        ]


def events_for(engine: Engine, run_id: UUID) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        return [
            dict(r)
            for r in conn.execute(
                text("SELECT * FROM events WHERE run_id = :id ORDER BY id"), {"id": run_id}
            ).mappings()
        ]


INVARIANT_QUERIES: dict[str, str] = {
    "running task without exactly one running attempt": """
        SELECT t.id FROM tasks t WHERE t.status = 'running' AND (
          SELECT count(*) FROM attempts a WHERE a.task_id = t.id AND a.status = 'running') <> 1""",
    "non-running task with a running attempt": """
        SELECT t.id FROM tasks t JOIN attempts a ON a.task_id = t.id
        WHERE a.status = 'running' AND t.status <> 'running'""",
    "current_attempt_id does not point at the running attempt": """
        SELECT t.id FROM tasks t JOIN attempts a ON a.id = t.current_attempt_id
        WHERE a.status <> 'running' OR a.task_id <> t.id""",
    "succeeded task without output": """
        SELECT id FROM tasks WHERE status = 'succeeded' AND output IS NULL""",
    "dispatched task whose dependencies have not all succeeded": """
        SELECT t.id FROM tasks t WHERE t.status IN ('queued','running','succeeded') AND EXISTS (
          SELECT 1 FROM unnest(t.depends_on) d(key) JOIN tasks p
            ON p.run_id = t.run_id AND p.task_key = d.key WHERE p.status <> 'succeeded')""",
    "succeeded attempt count differs from 1 for a succeeded task": """
        SELECT t.id FROM tasks t WHERE t.status = 'succeeded' AND (
          SELECT count(*) FROM attempts a WHERE a.task_id = t.id AND a.status = 'succeeded') <> 1""",
    "terminal run with unfinished tasks": """
        SELECT r.id FROM runs r JOIN tasks t ON t.run_id = r.id
        WHERE r.status IN ('succeeded','failed','cancelled')
          AND t.status IN ('pending','queued','running')""",
    "succeeded run with a non-succeeded task": """
        SELECT r.id FROM runs r JOIN tasks t ON t.run_id = r.id
        WHERE r.status = 'succeeded' AND t.status <> 'succeeded'""",
    "failed run without a failed task": """
        SELECT r.id FROM runs r WHERE r.status = 'failed'
          AND NOT EXISTS (SELECT 1 FROM tasks t WHERE t.run_id = r.id AND t.status = 'failed')""",
    "blocked task with no failed ancestor dependency": """
        SELECT t.id FROM tasks t WHERE t.status = 'blocked' AND NOT EXISTS (
          SELECT 1 FROM unnest(t.depends_on) d(key) JOIN tasks p
            ON p.run_id = t.run_id AND p.task_key = d.key WHERE p.status IN ('failed','blocked'))""",
    "attempt numbers not contiguous": """
        SELECT task_id FROM attempts GROUP BY task_id
        HAVING max(attempt_number) <> count(*)""",
    "task attempt_count differs from attempts recorded": """
        SELECT t.id FROM tasks t WHERE t.attempt_count <>
          (SELECT count(*) FROM attempts a WHERE a.task_id = t.id)""",
    "cancelled or cancelling run without cancel request": """
        SELECT id FROM runs WHERE status IN ('cancelling','cancelled') AND cancel_requested_at IS NULL""",
}


def check_invariants(engine: Engine) -> list[str]:
    """Return a description of every violated invariant (empty list = all hold)."""
    problems = []
    with engine.connect() as conn:
        for description, sql in INVARIANT_QUERIES.items():
            ids = [str(r[0]) for r in conn.execute(text(sql))]
            if ids:
                problems.append(f"{description}: {ids[:5]}")
    return problems


def wait_for(predicate: Callable[[], Any], timeout: float = 20.0, interval: float = 0.1) -> Any:
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() > deadline:
            raise TimeoutError(f"condition not met within {timeout}s")
        time.sleep(interval)


def _child_env(db_url: str, extra: Mapping[str, str] | None) -> dict[str, str]:
    env = dict(os.environ)
    env.update(FAST_TIMINGS)
    env["RELAYFLOW_DATABASE_URL"] = db_url
    env["PYTHONUNBUFFERED"] = "1"
    env.update(extra or {})
    return env


def start_worker_process(
    db_url: str, *, name: str, concurrency: int = 2, env: Mapping[str, str] | None = None
) -> subprocess.Popen[bytes]:
    child_env = _child_env(db_url, env)
    child_env["RELAYFLOW_WORKER_NAME"] = name
    child_env["RELAYFLOW_WORKER_CONCURRENCY"] = str(concurrency)
    return subprocess.Popen([sys.executable, "-m", "relayflow.worker"], env=child_env)


def start_scheduler_process(
    db_url: str, *, env: Mapping[str, str] | None = None
) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, "-m", "relayflow.scheduler"], env=_child_env(db_url, env)
    )


def start_mocknotify_process(
    mock_db_url: str, *, port: int, env: Mapping[str, str] | None = None
) -> subprocess.Popen[bytes]:
    child_env = dict(os.environ)
    child_env.update(
        {
            "MOCKNOTIFY_DATABASE_URL": mock_db_url,
            "MOCKNOTIFY_HOST": "127.0.0.1",
            "MOCKNOTIFY_PORT": str(port),
            "MOCKNOTIFY_ALLOW_RESET": "1",
            "PYTHONUNBUFFERED": "1",
        }
    )
    child_env.update(env or {})
    return subprocess.Popen([sys.executable, "-m", "mocknotify"], env=child_env)
