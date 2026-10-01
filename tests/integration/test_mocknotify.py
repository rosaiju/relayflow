"""The mock notification service deduplicates atomically by Idempotency-Key."""

from __future__ import annotations

import threading
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from mocknotify.app import create_app


@pytest.fixture
def mock_client(_migrated: str) -> Iterator[TestClient]:
    base = _migrated.rpartition("/")[0]
    admin = create_engine(f"{base}/postgres", isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        if not conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname='mocknotify_test'")
        ).first():
            conn.execute(text("CREATE DATABASE mocknotify_test"))
    admin.dispose()
    url = f"{base}/mocknotify_test"
    with TestClient(create_app(url)) as client:
        reset = create_engine(url)
        with reset.begin() as conn:
            conn.execute(text("TRUNCATE notifications"))
        reset.dispose()
        yield client


BODY = {"event": "report_ready", "run_id": "r1", "summary": "ok"}


def test_first_delivery_then_duplicate_then_conflict(mock_client: TestClient) -> None:
    first = mock_client.post("/notifications", json=BODY, headers={"Idempotency-Key": "k"})
    again = mock_client.post("/notifications", json=BODY, headers={"Idempotency-Key": "k"})
    assert (first.status_code, again.status_code) == (201, 200)
    assert first.json()["id"] == again.json()["id"]
    assert again.json()["duplicate"] is True and again.json()["delivery_count"] == 2
    other = mock_client.post(
        "/notifications", json={**BODY, "summary": "different"}, headers={"Idempotency-Key": "k"}
    )
    assert other.status_code == 409
    assert mock_client.post("/notifications", json=BODY).status_code == 400
    assert len(mock_client.get("/notifications").json()["items"]) == 1


def test_concurrent_identical_requests_record_one_notification(mock_client: TestClient) -> None:
    barrier = threading.Barrier(8)
    ids: list[str] = []
    lock = threading.Lock()

    def send() -> None:
        barrier.wait()
        response = mock_client.post(
            "/notifications", json=BODY, headers={"Idempotency-Key": "race"}
        )
        assert response.status_code in (200, 201)
        with lock:
            ids.append(response.json()["id"])

    threads = [threading.Thread(target=send) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(set(ids)) == 1
    items = mock_client.get("/notifications").json()["items"]
    assert len(items) == 1 and items[0]["delivery_count"] == 8
