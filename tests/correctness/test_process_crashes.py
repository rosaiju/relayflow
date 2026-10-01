"""Real-process recovery (spec 6.6, 7.3, 7.4, 8, 9.1): kill workers, crash after an
external effect, restart database connections, shut down gracefully.

All processes are killed in fixture teardown, whatever happens."""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import sys
from typing import Any

import httpx
import pytest
from sqlalchemy import Engine, text
from sqlalchemy.exc import DBAPIError

from relayflow.engine import submit_run
from relayflow.testing import (
    FAST_TIMINGS,
    attempts_for,
    check_invariants,
    events_for,
    run_status,
    start_mocknotify_process,
    start_scheduler_process,
    start_worker_process,
    wait_for,
)

from ._helpers import assert_invariants, require_modules, scalar, submit

pytestmark = pytest.mark.slow

WORKER_MODULES = ("relayflow.worker.__main__", "relayflow.scheduler.__main__")
MOCK_PORT = 18142


def _running_attempt(engine: Engine, run_id: object, key: str) -> dict[str, Any] | None:
    for a in attempts_for(engine, run_id, key):
        if a["status"] == "running":
            return a
    return None


def _owner_name(worker_id: str) -> str:
    return worker_id.split(":", 1)[0]  # ids look like "<name>:<host>:<pid>:<uuid8>"


def _start_workers(
    db_url: str, processes: list[Any], names: list[str], env: dict[str, str] | None = None
) -> dict[str, subprocess.Popen[bytes]]:
    workers = {}
    for name in names:
        p = start_worker_process(db_url, name=name, concurrency=2, env=env)
        processes.append(p)
        workers[name] = p
    return workers


def _wait_settled(engine: Engine, run_id: object, timeout: float = 45) -> str:
    return wait_for(
        lambda: (s := run_status(engine, run_id)) in ("succeeded", "failed", "cancelled") and s,
        timeout=timeout,
        interval=0.2,
    )


def test_killed_worker_task_is_reassigned_after_lease_expiry(
    engine: Engine, db_url: str, processes: list[Any]
) -> None:
    require_modules(*WORKER_MODULES)
    workers = _start_workers(db_url, processes, ["crash-a", "crash-b"])
    processes.append(start_scheduler_process(db_url))
    run_id = submit(engine, "test-sleep", {"seconds": 4})

    first = wait_for(lambda: _running_attempt(engine, run_id, "s"), timeout=30)
    victim = _owner_name(first["worker_id"])
    survivor = next(n for n in workers if n != victim)
    workers[victim].kill()  # hard crash: no release, no further heartbeats
    workers[victim].wait(15)

    assert _wait_settled(engine, run_id) == "succeeded"
    attempts = attempts_for(engine, run_id, "s")
    assert [a["attempt_number"] for a in attempts] == [1, 2]
    assert attempts[0]["status"] == "lease_expired"
    assert _owner_name(attempts[0]["worker_id"]) == victim
    assert attempts[1]["status"] == "succeeded"
    assert _owner_name(attempts[1]["worker_id"]) == survivor
    assert attempts[1]["worker_id"] != attempts[0]["worker_id"]
    kinds = [e["kind"] for e in events_for(engine, run_id)]
    assert "attempt_lease_expired" in kinds
    assert kinds.count("task_claimed") == 2
    assert_invariants(engine)


def test_crash_after_external_effect_delivers_one_logical_notification(
    engine: Engine, db_url: str, mock_db_url: str, processes: list[Any]
) -> None:
    require_modules(*WORKER_MODULES, "mocknotify.__main__")
    mock = start_mocknotify_process(mock_db_url, port=MOCK_PORT)
    processes.append(mock)
    base = f"http://127.0.0.1:{MOCK_PORT}"

    def mock_ready() -> bool:
        try:
            return httpx.get(f"{base}/health", timeout=1).status_code == 200
        except httpx.HTTPError:
            return False

    wait_for(mock_ready, timeout=30, interval=0.2)
    assert httpx.delete(f"{base}/notifications", timeout=5).status_code == 200

    env = {"RELAYFLOW_ENABLE_FAULT_INJECTION": "1", "RELAYFLOW_NOTIFY_URL": base}
    workers = _start_workers(db_url, processes, ["fx-a", "fx-b"], env=env)
    processes.append(start_scheduler_process(db_url, env=env))
    run_id = submit_run(
        engine,
        workflow_name="document-processing",
        input={
            "title": "Crash test",
            "text": "the quick brown fox jumps over the lazy dog " * 5,
            "demo": {"crash_after_execute": {"notify": [1]}},
        },
        allow_fault_injection=True,
    ).run_id

    assert _wait_settled(engine, run_id, timeout=60) == "succeeded"
    exit_codes = {name: p.poll() for name, p in workers.items()}
    assert sorted(c for c in exit_codes.values() if c is not None) == [137], exit_codes

    notify = attempts_for(engine, run_id, "notify")
    assert [a["status"] for a in notify] == ["lease_expired", "succeeded"]
    assert notify[0]["worker_id"] != notify[1]["worker_id"]

    key = f"relayflow:{run_id}:notify"
    items = [
        i
        for i in httpx.get(f"{base}/notifications", timeout=5).json()["items"]
        if i["idempotency_key"] == key
    ]
    assert len(items) == 1, items  # one logical notification ...
    assert items[0]["delivery_count"] == 2  # ... delivered twice (at-least-once)
    output = scalar(
        engine,
        "SELECT output FROM tasks WHERE run_id = :r AND task_key = 'notify'",
        r=run_id,
    )
    assert output["notification_id"] == items[0]["id"]
    assert output["duplicate"] is True
    assert_invariants(engine)


def test_worker_reported_timeout_with_real_processes(
    engine: Engine, db_url: str, processes: list[Any]
) -> None:
    require_modules(*WORKER_MODULES)
    _start_workers(db_url, processes, ["to-a"])
    processes.append(start_scheduler_process(db_url))
    run_id = submit(engine, "test-timeout", {"seconds": 5})  # timeout 1 s, max_attempts 2
    assert _wait_settled(engine, run_id, timeout=40) == "failed"
    attempts = attempts_for(engine, run_id, "s")
    assert [a["status"] for a in attempts] == ["timed_out", "timed_out"]
    assert_invariants(engine)


def test_many_runs_across_workers_with_a_crash(
    engine: Engine, db_url: str, processes: list[Any]
) -> None:
    require_modules(*WORKER_MODULES)
    workers = _start_workers(db_url, processes, ["many-a", "many-b", "many-c"])
    processes.append(start_scheduler_process(db_url))
    run_ids = [submit(engine, "test-diamond") for _ in range(10)]
    run_ids += [submit(engine, "test-sleep", {"seconds": 1}) for _ in range(4)]
    wait_for(
        lambda: (
            scalar(
                engine,
                "SELECT count(*) FROM attempts WHERE status = 'running' "
                "AND worker_id LIKE 'many-a:%'",
            )
            > 0
        ),
        timeout=30,
        interval=0.05,
    )
    workers["many-a"].kill()
    for run_id in run_ids:
        assert _wait_settled(engine, run_id, timeout=60) == "succeeded"
    assert (
        scalar(
            engine,
            "SELECT count(*) FROM tasks t WHERE (SELECT count(*) FROM attempts a "
            "WHERE a.task_id = t.id AND a.status = 'succeeded') <> 1",
        )
        == 0
    )
    assert_invariants(engine)


def test_database_connections_terminated_mid_run(
    engine: Engine, admin_engine: Engine, db_url: str, processes: list[Any]
) -> None:
    """pg_terminate_backend kills every RelayFlow connection (workers, scheduler) while a
    task runs; the system must reconnect and finish correctly."""
    require_modules(*WORKER_MODULES)
    _start_workers(db_url, processes, ["db-a", "db-b"])
    processes.append(start_scheduler_process(db_url))
    run_ids = [submit(engine, "test-chain") for _ in range(3)]
    sleeper = submit(engine, "test-sleep", {"seconds": 3})
    wait_for(lambda: _running_attempt(engine, sleeper, "s"), timeout=30)
    dbname = db_url.rpartition("/")[2]

    def terminate_all() -> int:
        with admin_engine.begin() as conn:
            return int(
                conn.execute(
                    text(
                        "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity "
                        "WHERE datname = :d AND pid <> pg_backend_pid() "
                        "AND application_name <> 'relayflow-test-admin'"
                    ),
                    {"d": dbname},
                ).scalar()
            )

    for _ in range(2):  # kill, wait until processes reconnect, kill again
        wait_for(terminate_all, timeout=20, interval=0.2)
    for run_id in [*run_ids, sleeper]:
        assert _wait_settled(engine, run_id, timeout=60) == "succeeded"
    assert_invariants(engine)


def test_unreachable_database_raises_and_changes_nothing(engine: Engine, db_url: str) -> None:
    from relayflow.db import make_engine
    from relayflow.engine import claim_task, request_cancel

    base, _, name = db_url.rpartition("/")
    host = base.rpartition("@")[0]
    dead = make_engine(f"{host}@127.0.0.1:1/{name}", pool_size=1)
    run_id = submit(engine, "test-sleep")
    before = scalar(engine, "SELECT count(*) FROM attempts")
    try:
        with pytest.raises(DBAPIError) as info:
            claim_task(dead, worker_id="ghost", lease_seconds=5)
        assert "connect" in str(info.value).lower() or "refused" in str(info.value).lower()
        with pytest.raises(DBAPIError):
            request_cancel(dead, run_id)
        with pytest.raises(DBAPIError):
            submit_run(dead, workflow_name="test-sleep", input={})
    finally:
        dead.dispose()
    assert scalar(engine, "SELECT count(*) FROM attempts") == before
    assert run_status(engine, run_id) == "running"
    assert scalar(engine, "SELECT count(*) FROM runs") == 1


def test_terminated_report_transaction_is_rolled_back(engine: Engine, admin_engine: Engine) -> None:
    """A transaction killed mid-flight (connection terminated while it holds the run lock
    and has half-applied changes) leaves no trace; the real owner can still report."""
    from relayflow.engine import complete_attempt

    from ._helpers import claim_one, run_row, task_row

    run_id = submit(engine, "test-sleep")
    c = claim_one(engine)
    victim = admin_engine.connect()
    tx = victim.begin()
    pid = victim.execute(text("SELECT pg_backend_pid()")).scalar()
    victim.execute(text("SELECT id FROM runs WHERE id = :r FOR UPDATE"), {"r": run_id})
    victim.execute(
        text("UPDATE attempts SET status = 'failed', finished_at = now() WHERE id = :a"),
        {"a": c.attempt_id},
    )
    with admin_engine.begin() as conn:
        assert conn.execute(text("SELECT pg_terminate_backend(:p)"), {"p": pid}).scalar()
    with contextlib.suppress(Exception):  # the connection is gone; that is the point
        tx.rollback()
    victim.invalidate()
    victim.close()
    assert task_row(engine, run_id, "s")["status"] == "running"
    assert complete_attempt(
        engine, attempt_id=c.attempt_id, lease_token=c.lease_token, output={"ok": True}
    )
    assert run_row(engine, run_id)["status"] == "succeeded"
    assert check_invariants(engine) == []


@pytest.mark.skipif(
    sys.platform != "win32" and not hasattr(signal, "SIGTERM"), reason="needs a stop signal"
)
def test_graceful_shutdown_releases_in_flight_attempt(
    engine: Engine, db_url: str, processes: list[Any]
) -> None:
    """7.4: on a stop signal the worker stops claiming, waits shutdown_grace_seconds, then
    releases (T8) what it still owns so it is re-queued without waiting for lease expiry.

    Windows has no SIGTERM delivery to another process; the worker also listens for
    SIGBREAK, which we send as CTRL_BREAK_EVENT to a worker in its own process group."""
    require_modules(*WORKER_MODULES)
    env = dict(os.environ)
    env.update(FAST_TIMINGS)
    env.update(
        {
            "RELAYFLOW_DATABASE_URL": db_url,
            "RELAYFLOW_WORKER_NAME": "graceful",
            "RELAYFLOW_WORKER_CONCURRENCY": "1",
            "RELAYFLOW_SHUTDOWN_GRACE_SECONDS": "1",
            "PYTHONUNBUFFERED": "1",
        }
    )
    kwargs: dict[str, Any] = {}
    if sys.platform == "win32":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    worker = subprocess.Popen([sys.executable, "-m", "relayflow.worker"], env=env, **kwargs)
    processes.append(worker)
    run_id = submit(engine, "test-sleep", {"seconds": 30})
    wait_for(lambda: _running_attempt(engine, run_id, "s"), timeout=30)
    if sys.platform == "win32":
        worker.send_signal(signal.CTRL_BREAK_EVENT)
    else:
        worker.send_signal(signal.SIGTERM)
    assert worker.wait(timeout=20) == 0
    attempts = attempts_for(engine, run_id, "s")
    assert [a["status"] for a in attempts] == ["released"]
    task = scalar(engine, "SELECT row_to_json(t) FROM tasks t WHERE run_id = :r", r=run_id)
    assert task["status"] == "queued" and task["failure_count"] == 0
    # Not counted and immediately claimable (T8: available_at = now()).
    assert (
        scalar(engine, "SELECT available_at <= now() FROM tasks WHERE run_id = :r", r=run_id)
        is True
    )
    assert scalar(engine, "SELECT status FROM workers WHERE name = 'graceful'") == "stopped"
    from relayflow.engine import request_cancel

    assert request_cancel(engine, run_id) == "cancelled"
    assert_invariants(engine)


def test_cooperative_cancel_with_real_worker(
    engine: Engine, db_url: str, processes: list[Any]
) -> None:
    """6.9: the worker learns of the cancel at its next heartbeat, the handler stops at a
    check point, the worker calls cancel_attempt (T11) and the run becomes cancelled."""
    require_modules(*WORKER_MODULES)
    _start_workers(db_url, processes, ["cancel-a"])
    run_id = submit(engine, "test-sleep", {"seconds": 30})
    wait_for(lambda: _running_attempt(engine, run_id, "s"), timeout=30)
    from relayflow.engine import request_cancel

    assert request_cancel(engine, run_id) == "cancelling"
    assert _wait_settled(engine, run_id, timeout=15) == "cancelled"
    attempts = attempts_for(engine, run_id, "s")
    assert [a["status"] for a in attempts] == ["cancelled"]
    assert_invariants(engine)
