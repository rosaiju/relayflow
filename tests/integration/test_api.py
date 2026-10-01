"""HTTP API contract (docs/architecture.md section 10) against real PostgreSQL."""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient
from sqlalchemy import Engine

from relayflow.engine import claim_task, complete_attempt, fail_attempt
from relayflow.testing import force_available_now

DOC = {"title": "Field notes", "text": "Herons wade. Herons wait. The marsh is quiet."}


def submit(client: TestClient, **body: Any) -> Any:
    payload = {"workflow_name": "document-processing", "input": DOC, **body}
    return client.post("/api/runs", json=payload)


def test_health(api_client: TestClient) -> None:
    body = api_client.get("/api/health").json()
    assert body["status"] == "ok" and body["database"] == "ok"
    assert body["fault_injection"] is False


def test_submit_and_read_run(api_client: TestClient) -> None:
    response = submit(api_client)
    assert response.status_code == 201
    run = response.json()["run"]
    assert run["status"] == "running"
    assert run["definition_snapshot"]["name"] == "document-processing"
    states = {t["task_key"]: t["status"] for t in run["tasks"]}
    assert states == {
        "validate": "queued",
        "word_count": "pending",
        "keywords": "pending",
        "report": "pending",
        "notify": "pending",
    }
    assert [t["task_key"] for t in run["tasks"]][0] == "validate"
    listed = api_client.get("/api/runs", params={"status": "running"}).json()
    assert listed["total"] == 1 and listed["items"][0]["title"] == "Field notes"
    events = api_client.get(f"/api/runs/{run['id']}/events").json()
    assert events[0]["kind"] == "run_submitted"


def test_idempotent_submission_header_and_body(api_client: TestClient) -> None:
    first = submit(api_client, idempotency_key="k-1")
    again = api_client.post(
        "/api/runs",
        headers={"Idempotency-Key": "k-1"},
        json={"workflow_name": "document-processing", "input": DOC},
    )
    assert (first.status_code, again.status_code) == (201, 200)
    assert first.json()["run_id"] == again.json()["run_id"]
    assert again.json()["created"] is False
    conflict = submit(api_client, idempotency_key="k-1", input={**DOC, "title": "Other"})
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["code"] == "idempotency_conflict"
    assert api_client.get("/api/runs").json()["total"] == 1


def test_submission_validation(api_client: TestClient) -> None:
    assert submit(api_client, workflow_name="nope").status_code == 404
    assert submit(api_client, workflow_version=99).status_code == 404
    assert submit(api_client, input={"title": "t", "text": "x" * 70_000}).status_code == 422
    faulty = submit(api_client, input={**DOC, "demo": {"crash_after_execute": {"notify": [1]}}})
    assert faulty.status_code == 422
    assert "fault injection" in faulty.json()["detail"]["message"]
    delayed = submit(api_client, input={**DOC, "demo": {"delay_seconds": {"keywords": 2}}})
    assert delayed.status_code == 201
    huge = api_client.post(
        "/api/runs", content=b"x" * 200_000, headers={"Content-Type": "application/json"}
    )
    assert huge.status_code == 413
    assert api_client.get("/api/runs", params={"status": "bogus"}).status_code == 422
    assert api_client.get("/api/runs/not-a-uuid").status_code == 422
    missing = api_client.get("/api/runs/00000000-0000-0000-0000-000000000000")
    assert missing.status_code == 404


def test_register_workflow_versions(api_client: TestClient) -> None:
    spec = {
        "name": "api-wf",
        "version": 1,
        "tasks": [
            {"key": "a", "type": "demo.noop"},
            {"key": "b", "type": "demo.noop", "depends_on": ["a"]},
        ],
    }
    assert api_client.post("/api/workflows", json=spec).status_code == 201
    assert api_client.post("/api/workflows", json=spec).status_code == 200
    changed = {**spec, "tasks": [{"key": "a", "type": "demo.noop"}]}
    assert api_client.post("/api/workflows", json=changed).status_code == 409
    cyclic = {
        **spec,
        "version": 2,
        "tasks": [
            {"key": "a", "type": "demo.noop", "depends_on": ["b"]},
            {"key": "b", "type": "demo.noop", "depends_on": ["a"]},
        ],
    }
    bad = api_client.post("/api/workflows", json=cyclic)
    assert bad.status_code == 422 and any("cycle" in e for e in bad.json()["detail"]["errors"])
    shell = {**spec, "version": 3, "tasks": [{"key": "a", "type": "shell"}]}
    assert api_client.post("/api/workflows", json=shell).status_code == 422
    assert api_client.get("/api/workflows/api-wf/versions/1").json()["spec"]["tasks"][1][
        "depends_on"
    ] == ["a"]


def test_cancel_and_retry_controls(api_client: TestClient, engine: Engine) -> None:
    run_id = submit(api_client).json()["run_id"]
    assert api_client.post(f"/api/runs/{run_id}/retry").status_code == 409  # active run
    cancelled = api_client.post(f"/api/runs/{run_id}/cancel")
    assert cancelled.status_code == 202 and cancelled.json()["status"] == "cancelled"
    assert api_client.post(f"/api/runs/{run_id}/cancel").status_code == 409  # terminal
    assert api_client.post(f"/api/runs/{run_id}/retry").status_code == 409

    failing = api_client.post("/api/runs", json={"workflow_name": "test-fail", "input": {}})
    fail_id = failing.json()["run_id"]
    while True:
        claimed = claim_task(engine, worker_id="t", lease_seconds=30)
        if claimed is None:
            run = api_client.get(f"/api/runs/{fail_id}").json()
            queued = [t for t in run["tasks"] if t["status"] == "queued"]
            if not queued:
                break
            force_available_now(engine, queued[0]["id"])
            continue
        if claimed.task_type == "demo.fail":
            fail_attempt(
                engine, attempt_id=claimed.attempt_id, lease_token=claimed.lease_token, error="boom"
            )
        else:
            complete_attempt(
                engine, attempt_id=claimed.attempt_id, lease_token=claimed.lease_token, output={}
            )
    assert api_client.get(f"/api/runs/{fail_id}").json()["status"] == "failed"
    retried = api_client.post(f"/api/runs/{fail_id}/retry")
    assert retried.status_code == 200 and retried.json()["status"] == "running"


def test_overview_and_workers(api_client: TestClient) -> None:
    submit(api_client)
    body = api_client.get("/api/overview").json()
    assert body["runs"] == {"running": 1}
    assert body["tasks"]["queued"] == 1
    assert body["settings"]["lease_seconds"] == 2
    assert api_client.get("/api/workers").json() == []
    assert api_client.get("/api/nope").status_code == 404
