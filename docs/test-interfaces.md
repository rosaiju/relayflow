# Test interfaces (contract between engine and test suites)

Tests derive expectations from `docs/architecture.md`. This file fixes the Python
names tests may import so engine and tests can be written in parallel.

## Layout and ownership

| path | owner | kind |
|---|---|---|
| `tests/unit/` | main agent | pure Python, no database |
| `tests/integration/` | main agent | real PostgreSQL, engine functions + API |
| `tests/correctness/` | test agent | real PostgreSQL; correctness, concurrency, subprocess-crash, failure-injection |
| `tests/conftest.py`, `src/relayflow/testing.py` | main agent | shared fixtures/helpers (test agent may request additions) |
| `frontend/e2e/` | main agent | Playwright browser tests |

Markers: `@pytest.mark.integration` (needs PostgreSQL), `@pytest.mark.slow`
(> 5 s, subprocesses). `pytest -m "not integration"` runs unit tests only.

## Database for tests

`RELAYFLOW_TEST_DATABASE_URL` (default
`postgresql+psycopg://relayflow:relayflow@127.0.0.1:5433/relayflow_test`). The
session fixture creates the database if needed and runs Alembic migrations to
head. Never SQLite.

## Fixtures (`tests/conftest.py`)

* `db_url` → str (session scope).
* `engine` → `sqlalchemy.Engine` with a clean schema: function-scoped; truncates
  all RelayFlow tables before each test and seeds the built-in workflow
  definitions (`document-processing` v1 and the test workflows below).
* `api_client` → `fastapi.testclient.TestClient` bound to `engine`'s database.
* `settings` → `relayflow.config.Settings` pointing at the test database with
  fast timings (`lease_seconds=2`, `heartbeat_seconds=0.5`,
  `poll_interval_seconds=0.1`, `scheduler_interval_seconds=0.2`).

## Engine API (`relayflow.engine`)

All functions take a SQLAlchemy `Engine` first and run one short transaction.

```python
from relayflow.engine import (
    register_workflow,  # (engine, spec: dict) -> RegisterResult(definition_id, created: bool)
    submit_run,         # (engine, *, workflow_name, input, workflow_version=None,
                        #  idempotency_key=None) -> SubmitResult(run_id: UUID, created: bool)
    claim_task,         # (engine, *, worker_id, lease_seconds) -> ClaimedTask | None
    heartbeat,          # (engine, leases: list[tuple[UUID, UUID]], *, lease_seconds)
                        #   -> dict[UUID, HeartbeatResult(owned: bool, cancel_requested: bool)]
                        #   keyed by attempt_id; leases are (attempt_id, lease_token)
    complete_attempt,   # (engine, *, attempt_id, lease_token, output: dict) -> bool
    fail_attempt,       # (engine, *, attempt_id, lease_token, error: str,
                        #  retryable: bool = True, kind: str = "failed" | "timed_out",
                        #  rng: random.Random | None = None) -> bool
    cancel_attempt,     # (engine, *, attempt_id, lease_token) -> bool
    release_attempt,    # (engine, *, attempt_id, lease_token) -> bool
    recover_expired,    # (engine, *, limit=100, timeout_grace_seconds=5.0) -> RecoveryResult(expired: int, timed_out: int)
    request_cancel,     # (engine, run_id) -> str  (resulting run status)
    retry_run,          # (engine, run_id) -> str  (resulting run status, "running")
    get_run,            # (engine, run_id) -> dict  (run + tasks + attempts, JSON-able)
)
from relayflow.engine.errors import (
    WorkflowNotFound, RunNotFound, IdempotencyConflict,
    InvalidTransition, DefinitionConflict, ValidationFailed,
)
```

`ClaimedTask` fields: `attempt_id, lease_token, task_id, run_id, task_key,
task_type, input, attempt_number, timeout_seconds`.

Return value `False` from report functions means *stale / rejected, nothing
changed*.

## Definitions (`relayflow.definitions`)

```python
validate_definition(spec: dict) -> WorkflowSpec     # raises ValidationFailed(errors: list[str])
topological_order(spec: WorkflowSpec) -> list[str]  # deterministic (ties by key)
```

## Test helpers (`relayflow.testing`)

```python
force_lease_expiry(engine, attempt_id)   # sets lease_expires_at = now() - 1s (explicit failure point)
force_available_now(engine, task_id)     # sets available_at = now() (skip backoff wait)
run_status(engine, run_id) -> str
task_states(engine, run_id) -> dict[str, str]          # task_key -> status
attempts_for(engine, run_id, task_key) -> list[dict]    # ordered by attempt_number
events_for(engine, run_id) -> list[dict]
check_invariants(engine) -> list[str]                   # empty list == all invariants hold
start_worker_process(db_url, *, name, concurrency=2, env=None) -> subprocess.Popen
start_scheduler_process(db_url, *, env=None) -> subprocess.Popen
wait_for(predicate, timeout=20.0, interval=0.1)        # polls; raises TimeoutError
TEST_WORKFLOWS                                          # dict name -> spec, seeded by fixture
```

`check_invariants` verifies (at least): one running attempt per running task
and none for other states; `current_attempt_id` consistency; succeeded tasks
have output; no task is queued/running/succeeded unless all its deps succeeded;
terminal runs have `finished_at` and no running tasks; run status consistent with
task statuses for terminal runs.

Subprocess helpers start `python -m relayflow.worker` / `python -m
relayflow.scheduler` with the settings environment, inherit fast timings, and
honour `env` overrides (e.g. `RELAYFLOW_ENABLE_FAULT_INJECTION=1`).

### Test workflows (seeded)
* `test-chain` v1: `a` → `b` → `c`, all `demo.sleep` with `seconds` param 0.
* `test-diamond` v1: `a` → (`b`, `c`) → `d`, `demo.sleep` 0.
* `test-sleep` v1: single task `s` of `demo.sleep`, `params.seconds` from
  `run_input.seconds` when present (default 5), `timeout_seconds` 30.
* `test-fail` v1: `ok` (`demo.noop`) and `bad` (`demo.fail`, `max_attempts` 2,
  backoff base 0.1) and `after_bad` depending on `bad`, plus `after_ok` depending
  on `ok`.
* `test-timeout` v1: single `demo.sleep` task with `timeout_seconds` 1,
  `max_attempts` 2, sleeping `run_input.seconds` (default 5).

## Mock notification service (`mocknotify`)

* `python -m mocknotify` serves on `MOCKNOTIFY_HOST`/`MOCKNOTIFY_PORT` (default
  127.0.0.1:8100) using `MOCKNOTIFY_DATABASE_URL` (default
  `postgresql+psycopg://relayflow:relayflow@127.0.0.1:5433/mocknotify`). It
  creates its own tables on startup.
* `mocknotify.app.create_app(database_url) -> FastAPI` for in-process tests.
* `POST /notifications` (header `Idempotency-Key`, JSON body) → 201 first time
  `{id, idempotency_key, duplicate: false, delivery_count: 1, payload, created_at}`;
  identical repeat → 200 same `id`, `duplicate: true`, `delivery_count`
  incremented; different body with same key → 409; missing key → 400.
* `GET /notifications` → `{"items": [...]}`; `DELETE /notifications` (test reset,
  only when `MOCKNOTIFY_ALLOW_RESET=1`).
* `relayflow.testing.start_mocknotify_process(mock_db_url, *, port) -> Popen`.

## Worker fault-injection behaviour (for subprocess tests)

Only when the worker has `RELAYFLOW_ENABLE_FAULT_INJECTION=1` **and** the run was
submitted with `allow_fault_injection=True` (API: server env flag):
* `input.demo.fail_attempts = {task_key: [n, ...]}` → attempt n of that task
  raises a retryable error before running the handler.
* `input.demo.crash_after_execute = {task_key: [n, ...]}` → after the handler
  returns successfully on attempt n, the worker process exits immediately with
  code 137 without reporting (simulates a crash after an external effect).
* `input.demo.delay_seconds = {task_key: s}` → cooperative delay before the
  handler (always allowed, 0..30 s).

## Helpers added after the spec review (v1.1)
* `start_worker_process(..., new_process_group=True)` + `stop_gracefully(proc)` —
  graceful stop via `CTRL_BREAK_EVENT` (Windows) or SIGTERM.
* `lock_waiters(engine, query_pattern) -> int` — counts RelayFlow sessions waiting on a
  row lock (pg_stat_activity), for deterministic lock interleavings.
