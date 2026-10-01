"""Fixtures for tests that drive the real Docker Compose stack.

These tests stop and start real containers (PostgreSQL, the API), so they are opt-in:
set RELAYFLOW_STACK_TESTS=1 and have Docker available. They use the stack's main
database (never truncated) and identify their own runs by unique idempotency keys.
"""

from __future__ import annotations

import os
import subprocess
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from sqlalchemy import Engine

from relayflow.db import make_engine

ROOT = Path(__file__).resolve().parents[2]
API = "http://127.0.0.1:8000/api"
MOCK = "http://127.0.0.1:8100"
DB_URL = "postgresql+psycopg://relayflow:relayflow@127.0.0.1:5433/relayflow"
LEASE_SECONDS = 6.0
HEARTBEAT_SECONDS = 1.0
STACK_ENV = {
    "RELAYFLOW_LEASE_SECONDS": str(LEASE_SECONDS),
    "RELAYFLOW_HEARTBEAT_SECONDS": str(HEARTBEAT_SECONDS),
    "RELAYFLOW_SCHEDULER_INTERVAL_SECONDS": "0.5",
    "RELAYFLOW_POLL_INTERVAL_SECONDS": "0.2",
}
SERVICES = ["postgres", "mocknotify-db", "mocknotify", "api", "scheduler", "worker-a", "worker-b"]
DOCUMENT = {
    "title": "Stack test document (synthetic)",
    "text": "Gulls circled the pier. The pier creaked; the gulls landed and the tide turned.",
}


STACK_ENABLED = os.environ.get("RELAYFLOW_STACK_TESTS") == "1"
requires_stack = pytest.mark.skipif(
    not STACK_ENABLED, reason="stack tests stop real containers; set RELAYFLOW_STACK_TESTS=1"
)


def compose(*args: str, timeout: float = 180) -> str:
    result = subprocess.run(
        ["docker", "compose", *args],
        cwd=ROOT,
        env={**os.environ, **STACK_ENV},
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    output = result.stdout + result.stderr
    if result.returncode != 0:
        command = " ".join(args)
        raise RuntimeError(f"docker compose {command} failed ({result.returncode}):\n{output}")
    return output


def poll(predicate: Callable[[], Any], what: str, timeout: float, interval: float = 0.1) -> Any:
    """Bounded wait. Transport errors count as "not yet"."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            value = predicate()
        except (httpx.HTTPError, OSError):
            value = None
        if value:
            return value
        if time.monotonic() > deadline:
            raise TimeoutError(f"timed out after {timeout}s waiting for {what}")
        time.sleep(interval)


def api_get(path: str) -> Any:
    response = httpx.get(f"{API}{path}", timeout=5)
    response.raise_for_status()
    return response.json()


def submit(input_: dict[str, Any], key: str) -> httpx.Response:
    return httpx.post(
        f"{API}/runs",
        timeout=10,
        json={"workflow_name": "document-processing", "input": input_, "idempotency_key": key},
    )


def task_of(run: dict[str, Any], key: str) -> dict[str, Any]:
    return next(t for t in run["tasks"] if t["task_key"] == key)


def healthy_worker_count() -> int:
    return sum(1 for w in api_get("/workers") if w["healthy"])


def unique_key(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def ensure_stack_ready() -> None:
    """Start every service and wait for real readiness. `docker compose up --wait` is not
    used: right after a database outage the API's healthcheck can briefly report
    unhealthy, and --wait then fails even though the API is recovering."""
    compose("up", "-d", *SERVICES, timeout=600)
    poll(lambda: api_get("/health")["status"] == "ok", "API healthy", timeout=120, interval=0.5)
    poll(lambda: healthy_worker_count() >= 2, "two healthy workers", timeout=60, interval=0.5)


@pytest.fixture(scope="session")
def stack() -> Iterator[None]:
    compose("build", timeout=600)
    ensure_stack_ready()
    yield
    ensure_stack_ready()  # leave the stack complete even if a test failed midway


@pytest.fixture
def relayflow_db(stack: None) -> Iterator[Engine]:
    """Direct connection to the stack's database for verification (not truncated)."""
    eng = make_engine(DB_URL, pool_size=2)
    yield eng
    eng.dispose()
