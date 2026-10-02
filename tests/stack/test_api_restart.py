"""End-to-end: restart only the API while a workflow is active.

PostgreSQL, both workers, the scheduler and the dashboard keep running (their container
ids and start times must not change). With the API stopped, the run must keep
progressing (observed directly in PostgreSQL). After the API returns, results must be
served unchanged, idempotent resubmission must return the same logical run, and an
already-open dashboard page must show the outage and reconnect by itself, without a
reload and without the dashboard container being recreated.

Runs in the disposable `relayflow-stacktest` Compose project (see conftest.py).
"""

from __future__ import annotations

import queue
import shutil
import subprocess
import threading
from pathlib import Path

import httpx
import pytest
from sqlalchemy import Engine, text

from relayflow.testing import check_invariants

from .conftest import (
    API,
    DASHBOARD,
    DOCUMENT,
    ROOT,
    api_get,
    compose,
    container_identity,
    poll,
    requires_stack,
    submit,
    task_of,
    unique_key,
)

pytestmark = [requires_stack, pytest.mark.stack, pytest.mark.timeout(300)]

KEPT_RUNNING = ["postgres", "worker-a", "worker-b", "scheduler", "dashboard", "mocknotify"]


class BrowserWatcher:
    """Runs frontend/e2e/connection-watch.mjs and hands its milestone lines to the test."""

    def __init__(self, run_id: str) -> None:
        node = shutil.which("node")
        if node is None:
            pytest.fail("node is required for the dashboard reconnection check")
        frontend: Path = ROOT / "frontend"
        self.proc = subprocess.Popen(
            [node, "e2e/connection-watch.mjs", DASHBOARD, run_id],
            cwd=frontend,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.lines: queue.Queue[str] = queue.Queue()
        threading.Thread(target=self._pump, daemon=True).start()

    def _pump(self) -> None:
        assert self.proc.stdout is not None
        for line in self.proc.stdout:
            self.lines.put(line.strip())

    def expect(self, prefix: str, timeout: float) -> str:
        """Next milestone line must start with `prefix` (helper's own log lines skipped)."""
        try:
            while True:
                line = self.lines.get(timeout=timeout)
                if line.startswith(("READY", "DOWN", "UP", "SAME_PAGE", "RELOADED", "ERROR")):
                    break
        except queue.Empty:
            raise AssertionError(f"browser helper: no '{prefix}' within {timeout}s") from None
        assert line.startswith(prefix), f"browser helper said {line!r}, expected {prefix!r}"
        return line

    def close(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
        self.proc.wait(timeout=10)


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
        "demo": {"delay_seconds": {"keywords": 10}},
    }
    first = submit(payload, key)
    assert first.status_code == 201
    run_id = first.json()["run_id"]

    # 1. Work in flight; record what the API shows and which containers are running.
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
    identities = {service: container_identity(service) for service in KEPT_RUNNING}
    api_identity = container_identity("api")

    browser = BrowserWatcher(run_id)
    try:
        browser.expect("READY", timeout=60)
        try:
            # 2. Stop only the API.
            compose("stop", "api")
            assert api_down(), "API should refuse connections while its container is stopped"
            # The proxy must report the API as unreachable promptly: 502 (no route / refused)
            # or 504 (connect timeout, while nginx still holds the stopped container's IP).
            proxied = httpx.get(f"{DASHBOARD}/api/health", timeout=5)
            assert proxied.status_code in (502, 504), f"proxy answered {proxied.status_code}"
            status, tasks = db_status(relayflow_db, run_id)
            assert status == "running" and tasks["keywords"] == "running"
            browser.expect("DOWN", timeout=30)  # the open page noticed the outage

            # 3. The run finishes with no API process at all (observed in PostgreSQL).
            poll(
                lambda: db_status(relayflow_db, run_id)[0] == "succeeded",
                "the run to finish while the API is down",
                timeout=90,
                interval=0.5,
            )
            assert api_down(), "API must still be down when the run finishes"
        finally:
            compose("start", "api")

        # 4. The same open page reconnects by itself and shows the finished run.
        assert browser.expect("UP", timeout=150) == "UP succeeded"
        browser.expect("SAME_PAGE", timeout=10)
    finally:
        browser.close()

    # 5. Only the API restarted; everything else kept running in the same containers.
    assert {service: container_identity(service) for service in KEPT_RUNNING} == identities
    assert container_identity("api")[1] != api_identity[1], "api should have been restarted"
    assert httpx.get(f"{DASHBOARD}/api/health", timeout=10).status_code == 200

    # 6. Results are served unchanged after the restart.
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

    # 7. Same key + same payload -> the same logical run; a different payload -> 409.
    again = submit(payload, key)
    assert again.status_code == 200
    assert again.json()["run_id"] == run_id and again.json()["created"] is False
    conflict = submit({**payload, "title": "different"}, key)
    assert conflict.status_code == 409
    with relayflow_db.connect() as conn:
        count = conn.execute(
            text("SELECT count(*) FROM runs WHERE idempotency_key = :k"), {"k": key}
        )
        assert count.scalar_one() == 1

    assert check_invariants(relayflow_db) == []
