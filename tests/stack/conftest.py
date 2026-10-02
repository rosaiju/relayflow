"""Fixtures for tests that drive a real Docker Compose stack.

These tests kill and restart real containers, so they are opt-in
(RELAYFLOW_STACK_TESTS=1) and run in a DISPOSABLE Compose project:

* project name `relayflow-stacktest` -> its own containers, network and volumes;
* host ports 15433 / 18000 / 18100 / 18080, so it runs beside the demo stack;
* teardown runs `docker compose -p relayflow-stacktest down -v`, which deletes only
  that project's volumes. The demo stack (project `relayflow`, volume
  `relayflow_pgdata`) is never touched.
"""

from __future__ import annotations

import json
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
PROJECT = "relayflow-stacktest"
DEMO_PROJECT = "relayflow"
PORTS = {"pg": 15433, "api": 18000, "mock": 18100, "dashboard": 18080}
API = f"http://127.0.0.1:{PORTS['api']}/api"
MOCK = f"http://127.0.0.1:{PORTS['mock']}"
DASHBOARD = f"http://127.0.0.1:{PORTS['dashboard']}"
DB_URL = f"postgresql+psycopg://relayflow:relayflow@127.0.0.1:{PORTS['pg']}/relayflow"
LEASE_SECONDS = 6.0
HEARTBEAT_SECONDS = 1.0
STACK_ENV = {
    "RELAYFLOW_LEASE_SECONDS": str(LEASE_SECONDS),
    "RELAYFLOW_HEARTBEAT_SECONDS": str(HEARTBEAT_SECONDS),
    "RELAYFLOW_SCHEDULER_INTERVAL_SECONDS": "0.5",
    "RELAYFLOW_POLL_INTERVAL_SECONDS": "0.2",
    "RELAYFLOW_ENABLE_FAULT_INJECTION": "0",
    "RELAYFLOW_PG_PORT": str(PORTS["pg"]),
    "RELAYFLOW_API_PORT": str(PORTS["api"]),
    "RELAYFLOW_MOCK_PORT": str(PORTS["mock"]),
    "RELAYFLOW_DASHBOARD_PORT": str(PORTS["dashboard"]),
}
SERVICES = [
    "postgres",
    "mocknotify-db",
    "mocknotify",
    "api",
    "scheduler",
    "worker-a",
    "worker-b",
    "dashboard",
]
DOCUMENT = {
    "title": "Stack test document (synthetic)",
    "text": "Gulls circled the pier. The pier creaked; the gulls landed and the tide turned.",
}

STACK_ENABLED = os.environ.get("RELAYFLOW_STACK_TESTS") == "1"
requires_stack = pytest.mark.skipif(
    not STACK_ENABLED, reason="stack tests kill real containers; set RELAYFLOW_STACK_TESTS=1"
)


def compose(*args: str, timeout: float = 180) -> str:
    """`docker compose` scoped to the disposable test project only."""
    assert PROJECT != DEMO_PROJECT
    result = subprocess.run(
        ["docker", "compose", "-p", PROJECT, *args],
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
        raise RuntimeError(
            f"docker compose -p {PROJECT} {command} failed ({result.returncode}):\n{output}"
        )
    return output


def container_identity(service: str) -> tuple[str, str]:
    """(container id, start time) - changes if the container is recreated or restarted."""
    container_id = compose("ps", "-q", service).strip()
    assert container_id, f"{service} is not running"
    raw = subprocess.run(
        ["docker", "inspect", container_id], capture_output=True, text=True, check=True
    ).stdout
    info = json.loads(raw)[0]
    return info["Id"], info["State"]["StartedAt"]


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
    poll(
        lambda: httpx.get(f"{DASHBOARD}/api/health", timeout=5).status_code == 200,
        "dashboard proxy",
        timeout=60,
        interval=0.5,
    )


@pytest.fixture(scope="session")
def stack() -> Iterator[None]:
    compose("down", "-v", "--remove-orphans")  # start from an empty disposable project
    compose("build", timeout=900)
    ensure_stack_ready()
    yield
    compose("down", "-v", "--remove-orphans")  # dispose of the test project's volumes only


@pytest.fixture
def relayflow_db(stack: None) -> Iterator[Engine]:
    """Direct connection to the disposable stack's database, for verification. Waits for
    the whole stack to be ready first, so one test's restarts cannot leak into the next."""
    ensure_stack_ready()
    eng = make_engine(DB_URL, pool_size=2)
    yield eng
    eng.dispose()
