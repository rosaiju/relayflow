"""Small helpers shared by the correctness tests.

Expectations come from docs/architecture.md; these helpers only drive the public
engine API (relayflow.engine) and read state with plain SQL.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import threading
from collections.abc import Callable, Iterable
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import Engine, text

from relayflow.engine import (
    ClaimedTask,
    claim_task,
    complete_attempt,
    register_workflow,
    submit_run,
)
from relayflow.testing import check_invariants, force_available_now

# Long lease for pure-engine tests so nothing expires unless a test forces it.
LEASE = 60.0


def submit(engine: Engine, name: str, run_input: dict[str, Any] | None = None, **kw: Any) -> UUID:
    return submit_run(engine, workflow_name=name, input=run_input or {}, **kw).run_id


def claim(engine: Engine, worker: str = "worker-1", lease: float = LEASE) -> ClaimedTask | None:
    return claim_task(engine, worker_id=worker, lease_seconds=lease)


def claim_one(engine: Engine, key: str | None = None, worker: str = "worker-1") -> ClaimedTask:
    c = claim(engine, worker)
    assert c is not None, "expected a claimable task"
    if key is not None:
        assert c.task_key == key, f"claimed {c.task_key!r}, expected {key!r}"
    return c


def claim_all(engine: Engine, worker: str = "worker-1") -> list[ClaimedTask]:
    out = []
    while (c := claim(engine, worker)) is not None:
        out.append(c)
    return out


def complete(engine: Engine, c: ClaimedTask, output: dict[str, Any] | None = None) -> bool:
    out = output if output is not None else {"by": c.task_key, "attempt": c.attempt_number}
    return complete_attempt(engine, attempt_id=c.attempt_id, lease_token=c.lease_token, output=out)


def assert_invariants(engine: Engine) -> None:
    problems = check_invariants(engine)
    assert problems == [], problems


def register(engine: Engine, name: str, tasks: list[dict[str, Any]]) -> None:
    register_workflow(engine, {"name": name, "version": 1, "tasks": tasks})


def one_task_workflow(engine: Engine, name: str, **policy: Any) -> None:
    register(engine, name, [{"key": "t", "type": "demo.noop", **policy}])


def task_row(engine: Engine, run_id: UUID, key: str) -> dict[str, Any]:
    with engine.connect() as conn:
        row = (
            conn.execute(
                text(
                    "SELECT *, output::text AS output_text, input::text AS input_text "
                    "FROM tasks WHERE run_id = :r AND task_key = :k"
                ),
                {"r": run_id, "k": key},
            )
            .mappings()
            .one()
        )
        return dict(row)


def run_row(engine: Engine, run_id: UUID) -> dict[str, Any]:
    with engine.connect() as conn:
        return dict(
            conn.execute(text("SELECT * FROM runs WHERE id = :r"), {"r": run_id}).mappings().one()
        )


def attempt_row(engine: Engine, attempt_id: UUID) -> dict[str, Any]:
    with engine.connect() as conn:
        return dict(
            conn.execute(text("SELECT * FROM attempts WHERE id = :a"), {"a": attempt_id})
            .mappings()
            .one()
        )


def scalar(engine: Engine, sql: str, **params: Any) -> Any:
    with engine.connect() as conn:
        return conn.execute(text(sql), params).scalar()


def snapshot(engine: Engine, run_id: UUID) -> dict[str, Any]:
    """Canonical text dump of every row of a run (to prove a call changed nothing)."""
    with engine.connect() as conn:
        dump: dict[str, Any] = {}
        for table, order in (("runs", "id"), ("tasks", "task_key"), ("attempts", "id")):
            col = "id" if table == "runs" else "run_id"
            rows = conn.execute(
                text(
                    f"SELECT row_to_json(x)::text FROM {table} x WHERE {col} = :r ORDER BY {order}"
                ),
                {"r": run_id},
            ).scalars()
            dump[table] = list(rows)
        dump["events"] = conn.execute(
            text("SELECT count(*) FROM events WHERE run_id = :r"), {"r": run_id}
        ).scalar()
        return dump


def drive(engine: Engine, *, max_steps: int = 200, worker: str = "driver") -> int:
    """Single-threaded worker: claim and complete until nothing is claimable.

    Skips backoff waits with force_available_now. Returns the number of completions.
    """
    done = 0
    for _ in range(max_steps):
        c = claim(engine, worker)
        if c is None:
            with engine.connect() as conn:
                waiting = list(
                    conn.execute(text("SELECT id FROM tasks WHERE status = 'queued'")).scalars()
                )
            if not waiting:
                return done
            for task_id in waiting:
                force_available_now(engine, task_id)
            continue
        assert complete(engine, c)
        done += 1
    raise AssertionError("drive() did not settle")


def run_threads(n: int, target: Callable[[int], Any], timeout: float = 60.0) -> list[Any]:
    """Run target(i) in n threads; re-raise the first exception; return results by index."""
    results: list[Any] = [None] * n
    errors: list[BaseException] = []

    def wrapper(i: int) -> None:
        try:
            results[i] = target(i)
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=wrapper, args=(i,), daemon=True) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout)
        assert not t.is_alive(), "thread did not finish (deadlock or hang?)"
    if errors:
        raise errors[0]
    return results


def module_available(name: str) -> bool:
    try:
        return importlib.util.find_spec(name) is not None
    except ModuleNotFoundError:
        return False


def require_modules(*names: str) -> None:
    missing = [n for n in names if not module_available(n)]
    if missing:
        pytest.skip(f"module(s) not implemented yet: {', '.join(missing)}")


def run_python(code: str, *, env: dict[str, str] | None = None) -> subprocess.Popen[str]:
    """Start `python -c code` with pipes; the child waits for 'go' on stdin if it reads it."""
    return subprocess.Popen(
        [sys.executable, "-c", code],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )


def release_and_collect(procs: Iterable[subprocess.Popen[str]], timeout: float = 60.0) -> list[Any]:
    """Send 'go' to every child at once, then parse the last stdout line of each as JSON."""
    procs = list(procs)
    for p in procs:
        assert p.stdin is not None
        p.stdin.write("go\n")
        p.stdin.flush()
    results = []
    for p in procs:
        out, err = p.communicate(timeout=timeout)
        assert p.returncode == 0, f"child failed ({p.returncode}): {err[-2000:]}"
        results.append(json.loads(out.strip().splitlines()[-1]))
    return results
