"""End-to-end: restart only the API while workers and the scheduler keep running.

The API is stateless; durable state lives in PostgreSQL. With the API container
stopped, the run must keep progressing (observed directly in the database), and after
the API comes back the persisted state, the result, and idempotent resubmission must
all be intact.
"""

from __future__ import annotations

import httpx
import pytest
from sqlalchemy import Engine, text

from relayflow.testing import check_invariants

from .conftest import (
    API,
    DOCUMENT,
    api_get,
    compose,
    poll,
    requires_stack,
    submit,
    task_of,
    unique_key,
)

pytestmark = [requires_stack, pytest.mark.stack, pytest.mark.timeout(240)]


def db_status(db: Engine, run_id: str) -> tuple[str, dict[str, str]]:
    with db.connect() as conn:
        run = conn.execute(
            text("SELECT status FROM runs WHERE id = :r"), {"r": run_id}
        ).scalar_one()
        tasks = dict(
            conn.execute(
                text("SELECT task_key, status FROM tasks WHERE run_id = :r"), {"r": run_id}
            ).all()
        )
    return run, tasks


def api_down() -> bool:
    try:
        httpx.get(f"{API}/health", timeout=2)
    except httpx.TransportError:
        return True
    return False


def test_api_only_restart_while_workers_continue(relayflow_db: Engine) -> None:
    key = unique_key("api-restart")
    payload = {
        **DOCUMENT,
        "title": "API restart test (synthetic)",
        "demo": {"delay_seconds": {"keywords": 8}},
    }
    first = submit(payload, key)
    assert first.status_code == 201
    run_id = first.json()["run_id"]

    # 1. Wait until work is in flight, and record the persisted state the API shows.
    before = poll(
        lambda: (
            (r := api_get(f"/runs/{run_id}"))
            and task_of(r, "keywords")["status"] == "running"
            and r
        ),
        "keywords to be running",
        timeout=60,
    )
    validate_before = task_of(before, "validate")
    assert validate_before["status"] == "succeeded"

    try:
        # 2. Stop only the API. Workers and the scheduler keep running.
        compose("stop", "api")
        assert api_down(), "API should be unreachable while its container is stopped"
        status, tasks = db_status(relayflow_db, run_id)
        assert status == "running" and tasks["keywords"] == "running"

        # 3. The run finishes with no API process at all (observed directly in PostgreSQL).
        poll(
            lambda: db_status(relayflow_db, run_id)[0] == "succeeded",
            "the run to finish while the API is down",
            timeout=90,
            interval=0.5,
        )
        assert api_down(), "API must still be down when the run finishes"
    finally:
        compose("start", "api")

    # 4. API restarted: persisted state and results are served unchanged.
    poll(lambda: api_get("/health")["status"] == "ok", "API to come back", timeout=60, interval=0.5)
    after = api_get(f"/runs/{run_id}")
    assert after["status"] == "succeeded"
    assert {t["task_key"]: t["status"] for t in after["tasks"]} == db_status(relayflow_db, run_id)[
        1
    ]
    validate_after = task_of(after, "validate")
    assert validate_after["output"] == validate_before["output"]
    assert [a["id"] for a in validate_after["attempts"]] == [
        a["id"] for a in validate_before["attempts"]
    ]
    assert task_of(after, "notify")["output"]["duplicate"] is False

    # 5. Same key + same payload -> the same logical run; a different payload -> 409.
    again = submit(payload, key)
    assert again.status_code == 200
    assert again.json()["run_id"] == run_id and again.json()["created"] is False
    conflict = submit({**payload, "title": "different"}, key)
    assert conflict.status_code == 409
    with relayflow_db.connect() as conn:
        assert (
            conn.execute(
                text("SELECT count(*) FROM runs WHERE idempotency_key = :k"), {"k": key}
            ).scalar_one()
            == 1
        )

    assert check_invariants(relayflow_db) == []
