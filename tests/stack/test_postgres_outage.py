"""End-to-end: a PostgreSQL outage longer than the worker lease while a task is running.

Scenario (spec 7.3, 6.6, 8, 9.1). The `notify` task gets a 4 s cooperative delay
before its handler, which gives a controlled window:

  t0        a worker claims `notify` (attempt 1); this test sees it running
  t0+~1s    this test kills RelayFlow's PostgreSQL with SIGKILL (a crash: no clean
            shutdown, WAL recovery on restart). The receiver has its own database.
  t0+4s     the handler delivers the notification: the external effect happens
            during the outage, which this test observes at the receiver
            the worker cannot report success (database down); once its local lease
            deadline passes it gives up without reporting
  outage    kept for at least 2 x lease
  restart   the scheduler expires attempt 1's lapsed lease; another attempt re-sends
            the same Idempotency-Key; the receiver deduplicates; the run completes

Then the stale attempt's token is used directly against the engine: every update must
be rejected and change nothing.
"""

from __future__ import annotations

import time
from typing import Any

import httpx
import pytest
from sqlalchemy import Engine, text

from relayflow.engine import complete_attempt, fail_attempt, heartbeat, release_attempt
from relayflow.testing import check_invariants

from .conftest import (
    API,
    DOCUMENT,
    LEASE_SECONDS,
    MOCK,
    api_get,
    compose,
    healthy_worker_count,
    poll,
    requires_stack,
    submit,
    task_of,
    unique_key,
)

pytestmark = [requires_stack, pytest.mark.stack, pytest.mark.timeout(300)]

NOTIFY_DELAY = 4.0
OUTAGE_SECONDS = 2 * LEASE_SECONDS + 2  # comfortably longer than the lease


def receiver_records(key: str) -> list[dict[str, Any]]:
    items = httpx.get(f"{MOCK}/notifications", timeout=5).json()["items"]
    return [n for n in items if n["idempotency_key"] == key]


def attempts(db: Engine, run_id: str, task_key: str) -> list[Any]:
    with db.connect() as conn:
        return list(
            conn.execute(
                text(
                    "SELECT a.* FROM attempts a JOIN tasks t ON t.id = a.task_id "
                    "WHERE a.run_id = :r AND t.task_key = :k ORDER BY a.attempt_number"
                ),
                {"r": run_id, "k": task_key},
            ).mappings()
        )


def task_row(db: Engine, run_id: str, task_key: str) -> Any:
    with db.connect() as conn:
        return (
            conn.execute(
                text("SELECT * FROM tasks WHERE run_id = :r AND task_key = :k"),
                {"r": run_id, "k": task_key},
            )
            .mappings()
            .one()
        )


def test_postgres_outage_longer_than_lease(relayflow_db: Engine) -> None:
    response = submit(
        {**DOCUMENT, "demo": {"delay_seconds": {"notify": NOTIFY_DELAY}}}, unique_key("outage")
    )
    assert response.status_code == 201
    run_id = response.json()["run_id"]
    effect_key = f"relayflow:{run_id}:notify"

    # 1. Wait for notify attempt 1 to be running, then take the database down.
    run = poll(
        lambda: (
            (r := api_get(f"/runs/{run_id}")) and task_of(r, "notify")["status"] == "running" and r
        ),
        "notify to be running",
        timeout=60,
    )
    seen_running = time.monotonic()
    victim = task_of(run, "notify")["attempts"][-1]
    assert victim["attempt_number"] == 1
    victim_service = victim["worker_id"].split(":")[0]
    assert receiver_records(effect_key) == []  # no effect yet

    try:
        compose("kill", "postgres")
        outage_started = time.monotonic()
        # The window is controlled: the database must be down before the delayed handler
        # fires, otherwise this run would not exercise the scenario (fail, don't pass).
        lag = outage_started - seen_running
        assert lag < NOTIFY_DELAY - 0.5, f"missed the synchronization window ({lag:.2f}s)"

        # 2. During the outage: the API reports the database as unreachable, the receiver
        #    (own database) is up, and the external effect happens.
        health = httpx.get(f"{API}/health", timeout=10)
        assert health.status_code == 503 and health.json()["database"] == "unreachable"
        assert httpx.get(f"{MOCK}/health", timeout=5).json() == {"status": "ok"}
        delivered = poll(
            lambda: receiver_records(effect_key),
            "the effect during the outage",
            timeout=NOTIFY_DELAY + 5,
        )
        assert delivered[0]["delivery_count"] == 1
        # Keep the outage going well past the lease.
        remaining = OUTAGE_SECONDS - (time.monotonic() - outage_started)
        if remaining > 0:
            time.sleep(remaining)
        assert httpx.get(f"{API}/health", timeout=10).status_code == 503  # still down
        outage_length = time.monotonic() - outage_started
    finally:
        compose("start", "postgres")
    assert outage_length > LEASE_SECONDS

    # 3. Recovery after reconnection: API, workers and scheduler reconnect on their own.
    poll(lambda: api_get("/health")["database"] == "ok", "API reconnect", timeout=60, interval=0.5)
    poll(
        lambda: healthy_worker_count() >= 2, "workers heartbeating again", timeout=60, interval=0.5
    )
    final = poll(
        lambda: (r := api_get(f"/runs/{run_id}")) and r["status"] in ("succeeded", "failed") and r,
        "the run to finish",
        timeout=120,
        interval=0.5,
    )
    assert final["status"] == "succeeded"

    # 4. Lease semantics in PostgreSQL time: attempt 1 lapsed and was expired by the scheduler;
    #    attempt 2 started only after that.
    notify_attempts = attempts(relayflow_db, run_id, "notify")
    assert [a["status"] for a in notify_attempts] == ["lease_expired", "succeeded"]
    first, second = notify_attempts
    assert first["worker_id"] == victim["worker_id"]
    assert first["finished_at"] >= first["lease_expires_at"]
    assert (first["finished_at"] - first["heartbeat_at"]).total_seconds() >= LEASE_SECONDS
    assert second["started_at"] >= first["finished_at"]

    # 5. Receiver-side deduplication: the repeated effect is one logical notification.
    records = receiver_records(effect_key)
    assert len(records) == 1
    assert records[0]["delivery_count"] == 2
    notify_task = task_row(relayflow_db, run_id, "notify")
    assert notify_task["output"]["duplicate"] is True
    assert notify_task["output"]["notification_id"] == records[0]["id"]

    # 6. Stale updates with attempt 1's token are rejected and change nothing.
    token, attempt_id = first["lease_token"], first["id"]
    before = dict(notify_task)
    assert (
        complete_attempt(
            relayflow_db, attempt_id=attempt_id, lease_token=token, output={"stale": True}
        )
        is False
    )
    assert (
        fail_attempt(relayflow_db, attempt_id=attempt_id, lease_token=token, error="stale") is False
    )
    assert release_attempt(relayflow_db, attempt_id=attempt_id, lease_token=token) is False
    assert (
        heartbeat(relayflow_db, [(attempt_id, token)], lease_seconds=LEASE_SECONDS)[
            attempt_id
        ].owned
        is False
    )
    assert dict(task_row(relayflow_db, run_id, "notify")) == before
    assert [a["status"] for a in attempts(relayflow_db, run_id, "notify")] == [
        "lease_expired",
        "succeeded",
    ]

    # 7. The stale worker itself never reported attempt 1 (it gave up at its local deadline).
    logs = compose("logs", "--no-color", "--no-log-prefix", victim_service)
    mine = [line for line in logs.splitlines() if f"{run_id}/notify" in line]
    assert any("could not report" in line or "discarding result" in line for line in mine), mine
    assert not any("attempt 1 succeeded" in line for line in mine), mine

    assert check_invariants(relayflow_db) == []
