# RelayFlow architecture and specification

Version 1.1. This document is the agreed contract for the engine, the API, the
tests, and the dashboard. If code disagrees with this document, one of them is a
bug; fix the code or amend this document deliberately (and note it in the
changelog at the bottom).

RelayFlow is an educational, single-cluster durable workflow engine. It is
**not production software**: there is no authentication, no multi-tenant
isolation, and it has been exercised only on the environments listed in
`STATUS.md`.

## 1. Processes

```
 browser ──HTTP──▶ dashboard (nginx: static React build, proxies /api)
                         │
                         ▼
                   api (FastAPI) ──────────┐
                                           │  SQL (short transactions)
 worker-a ─┐                               ▼
 worker-b ─┼──────────────────────▶  PostgreSQL  (database "relayflow")
 scheduler ┘                               ▲
     │                                     │ (separate database "mocknotify")
     └── notify task ──HTTP──▶ mocknotify ─┘
```

| Process      | Module                         | Responsibility |
|--------------|--------------------------------|----------------|
| `api`        | `relayflow.api.app`            | Validates and records definitions and runs; reads state; requests cancel/retry. **Never executes tasks.** |
| `worker`     | `relayflow.worker.main`        | Claims ready tasks, executes registered handlers in a bounded thread pool, heartbeats leases, reports results. Any number of worker processes may run. |
| `scheduler`  | `relayflow.scheduler.main`     | Recovery loop: expires lapsed leases and enforces timeouts as a backstop. Worker staleness is derived at read time. More than one may run safely. |
| `mocknotify` | `mocknotify.app`               | Independent mock "notification service" with its own database and idempotency records. |
| `dashboard`  | `frontend/`                    | React + TypeScript UI that polls the API. |

PostgreSQL is the only coordination mechanism. There is no in-memory queue,
broker, or shared file system. Every process can be killed at any time; all
durable state is in PostgreSQL.

## 2. Database access model

* **Synchronous** SQLAlchemy 2.0 **Core** (not ORM sessions) over psycopg 3.
  Every engine operation is a function that opens one connection from the pool,
  runs exactly one short transaction (`with engine.begin() as conn:`), and
  returns plain dataclasses. Connections are never shared between threads; each
  worker thread / heartbeat thread / request handler checks out its own.
* Isolation level: READ COMMITTED (PostgreSQL default). Correctness comes from
  explicit row locks (`FOR UPDATE`, `FOR UPDATE SKIP LOCKED`), guarded
  `UPDATE ... WHERE <expected state>` statements, and database constraints.
* **No transaction is held open while a task handler runs.** A task execution
  is bracketed by two independent short transactions: *claim* and *report*.
* **All lease and timing decisions use PostgreSQL time** (`now()`, which is the
  transaction start time; transactions are short so this is accurate to
  milliseconds). Worker clocks are used only for local fail-safe deadlines
  (section 7.3), never to decide ownership in the database.

### 2.1 Lock ordering

To avoid deadlocks every transaction that locks more than one kind of row locks
in this order: **run → task → attempt**. The claim transaction locks only a
task row (with `SKIP LOCKED`) and inserts an attempt; it never waits on a run
lock. Run-level transitions (complete, fail, expire, cancel, retry) first take
`SELECT ... FROM runs WHERE id = :run_id FOR UPDATE`. That per-run lock
serializes all state transitions within one run (preventing, for example, two
sibling tasks completing concurrently and both failing to see that their shared
child became ready). Different runs proceed fully in parallel.

**Lock mode.** All row locks use `FOR NO KEY UPDATE` (never `FOR UPDATE`). Inserting a
row with a foreign key takes `FOR KEY SHARE` on the referenced row; `FOR UPDATE` would
conflict with it, so a claim inserting an attempt (FK → runs) would wait on — and could
deadlock with — a run-level transition such as cancel. `FOR NO KEY UPDATE` still
serializes run transitions against each other but does not conflict with FK checks. No
code path updates key columns. (Found by the independent test suite:
`tests/correctness/test_lock_ordering.py`.)

## 3. Data model

All ids are UUIDs except `workflow_definitions.id` (bigint) and `events.id`
(bigint). All timestamps are `timestamptz` set by PostgreSQL.

### workflow_definitions
| column | notes |
|---|---|
| id | bigserial PK |
| name, version | `UNIQUE (name, version)`; version ≥ 1 |
| spec | jsonb, validated definition (section 4) |
| created_at | |

Rows are immutable: a trigger rejects `UPDATE`.

### runs
| column | notes |
|---|---|
| id | uuid PK |
| workflow_name, workflow_version | copied from the definition |
| definition_id | FK → workflow_definitions |
| definition_snapshot | jsonb **immutable** copy of the spec at submission (trigger rejects changes) |
| input | jsonb, bounded (64 KiB serialized) |
| status | `running`, `cancelling`, `succeeded`, `failed`, `cancelled` (CHECK) |
| idempotency_key | text, nullable, `UNIQUE` |
| request_hash | text, SHA-256 of canonical JSON of `(workflow_name, resolved version, input)` |
| manual_retry_count | int ≥ 0 |
| cancel_requested_at | nullable |
| error | short summary of the first terminal task failure, nullable |
| created_at, updated_at, finished_at | `finished_at` non-null iff status is terminal (CHECK) |

### tasks
| column | notes |
|---|---|
| id | uuid PK |
| run_id | FK → runs; `UNIQUE (run_id, task_key)` |
| task_key, task_type | from the snapshot |
| depends_on | text[] of task keys |
| status | `pending`, `queued`, `running`, `succeeded`, `failed`, `blocked`, `cancelled` (CHECK) |
| input | jsonb, materialized when the task becomes `queued` (section 6.2) |
| output | jsonb; CHECK: `status <> 'succeeded' OR output IS NOT NULL` |
| max_attempts, timeout_seconds, backoff_base_seconds, backoff_max_seconds | retry policy, from the snapshot |
| attempt_count | total attempts ever started (used for `attempt_number`) |
| failure_count | failures counted against `max_attempts` since the last manual retry |
| available_at | earliest time the task may be claimed (backoff); meaningful when `queued` |
| current_attempt_id | the running attempt; CHECK: `(status = 'running') = (current_attempt_id IS NOT NULL)` |
| last_error | latest error text |
| created_at, updated_at, queued_at, finished_at | |

### attempts
| column | notes |
|---|---|
| id | uuid PK |
| task_id, run_id | FKs |
| attempt_number | `UNIQUE (task_id, attempt_number)` |
| worker_id | the worker that claimed it |
| lease_token | uuid, `UNIQUE`; the ownership token |
| status | `running`, `succeeded`, `failed`, `timed_out`, `lease_expired`, `cancelled`, `released` (CHECK) |
| started_at, heartbeat_at, lease_expires_at, finished_at | |
| error | text |

Partial unique index: `UNIQUE (task_id) WHERE status = 'running'` — **at most one
running attempt per task is enforced by PostgreSQL.** Attempt rows are never
deleted; manual retry keeps them.

### workers
`id` (text, e.g. `worker-a:host:pid:uuid8`), `hostname`, `pid`, `concurrency`,
`status` (`active`, `stopping`, `stopped`), `started_at`, `last_heartbeat_at`,
`in_flight`. Health is derived: a worker is *stale* when
`now() - last_heartbeat_at > lease_seconds`.

### events
Append-only audit log: `id`, `run_id`, `task_key` (nullable), `attempt_id`
(nullable), `worker_id` (nullable), `kind`, `message`, `data` jsonb,
`created_at`. Written **in the same transaction** as the transition it
describes. The dashboard uses it to show recovery evidence (e.g.
`lease_expired` on worker A followed by `task_claimed` by worker B).

## 4. Workflow definitions

```json
{
  "name": "document-processing",
  "version": 1,
  "description": "...",
  "tasks": [
    {"key": "validate", "type": "doc.validate", "depends_on": [],
     "max_attempts": 3, "timeout_seconds": 30,
     "backoff_base_seconds": 1.0, "backoff_max_seconds": 30.0,
     "params": {}}
  ]
}
```

Validation (rejects with HTTP 422 and a list of errors):
* `name` matches `^[a-z0-9][a-z0-9-]{0,63}$`; `version` integer 1..10000.
* 1..50 tasks; task keys unique and match `^[a-z][a-z0-9_]{0,39}$`.
* `type` must be a **registered task type** (section 9). No shell commands, no
  user-supplied code.
* every `depends_on` entry names an existing task; no self-dependency; no
  duplicate entries.
* the graph is **acyclic** (Kahn's algorithm; the error names a cycle).
* `max_attempts` 1..10, `timeout_seconds` 1..600, `0 < backoff_base ≤
  backoff_max ≤ 300`; `params` ≤ 4 KiB serialized.

Definitions are versioned and immutable. Registering an existing
`(name, version)` with an identical spec is a no-op (200); with a different spec
it is a 409.

## 5. State machines

### 5.1 Run states

```
            submit
              │
              ▼
          ┌────────┐  all tasks succeeded         ┌───────────┐
          │running │ ───────────────────────────▶ │ succeeded │ (terminal)
          └────────┘                              └───────────┘
           │   ▲  │ no task pending/queued/running
           │   │  │ and ≥1 task failed            ┌────────┐
           │   │  └─────────────────────────────▶ │ failed │
           │   └─────── manual retry ──────────── └────────┘
           │ cancel requested
           ▼
       ┌──────────┐ no task running               ┌───────────┐
       │cancelling│ ────────────────────────────▶ │ cancelled │ (terminal)
       └──────────┘                               └───────────┘
```

| # | from | to | trigger | guard |
|---|------|----|---------|-------|
| R1 | (none) | running | submit | definition exists, input valid |
| R2 | running | succeeded | task transition | every task `succeeded` |
| R3 | running | failed | task transition | no task in `pending/queued/running`, ≥1 `failed` |
| R4 | running | cancelling | cancel request | — (if no task is running, R5 happens in the same transaction) |
| R5 | cancelling | cancelled | task transition / cancel request | no task `running` |
| R6 | failed | running | manual retry | run is `failed` |

`failed` is terminal unless manually retried. `succeeded` and `cancelled` are
final. Cancel on `cancelling` is an idempotent no-op; cancel on a terminal run is
rejected (409). Retry on anything other than `failed` is rejected (409).

### 5.2 Task states

| # | from | to | trigger | guard / effect |
|---|------|----|---------|----------------|
| T1 | (none) | pending | run created | task has dependencies |
| T2 | (none) | queued | run created | task has no dependencies; input materialized |
| T3 | pending | queued | a dependency succeeded | **all** dependencies `succeeded` and run `running`; input materialized |
| T4 | queued | running | claim | `available_at <= now()`, run `running` and not cancel-requested; new attempt inserted |
| T5 | running | succeeded | completion with valid token | output stored in the same transaction |
| T6 | running | queued | attempt failed / timed out / lease expired, retryable, `failure_count < max_attempts` | `available_at = now() + backoff` |
| T7 | running | failed | attempt failed / timed out / lease expired and (not retryable or `failure_count >= max_attempts`) | descendants → `blocked` (T9) |
| T8 | running | queued | attempt released (graceful shutdown) | not counted as a failure; `available_at = now()` |
| T9 | pending | blocked | an ancestor failed terminally | — |
| T10 | pending/queued | cancelled | cancel request | — |
| T11 | running | cancelled | worker acknowledges cooperative cancel | run is `cancelling` |
| T12 | running | cancelled | attempt failed/expired/timed out/released while run is `cancelling` | no retry is scheduled |
| T13 | failed | queued | manual retry | `failure_count := 0`; dependencies are all succeeded by construction |
| T14 | blocked | pending | manual retry | then T3 if all deps already succeeded |

`succeeded`, `cancelled` are final for a task. `failed` and `blocked` change only
through manual retry. `failure_count` counts `failed`, `timed_out`, and
`lease_expired` attempts. `released` and `cancelled` attempts do not count.
After a counted failure, `failure_count` is incremented **first**; the task is re-queued
iff the error is retryable and the *new* `failure_count < max_attempts`. So
`max_attempts` is the total number of counted attempts a task may make.

### 5.3 Attempt states

`running` → one of `succeeded`, `failed`, `timed_out`, `lease_expired`,
`cancelled`, `released`. All are final (a trigger rejects changing a final attempt). Only an attempt in `running` whose
`lease_token` matches and whose lease has not expired (`lease_expires_at >
now()`) may be heartbeated or reported by its worker. The scheduler may move a
`running` attempt to `lease_expired` (lease lapsed) or `timed_out`
(`now() >= started_at + timeout_seconds + timeout_grace`). When both conditions hold,
lease expiry is checked first, so the attempt is recorded as `lease_expired`; both count
as one failure.

## 6. Execution

### 6.1 Submission and idempotency

`submit_run` validates the input, resolves the workflow version (latest when
omitted), computes `request_hash`, and in one transaction inserts the run with
`INSERT ... ON CONFLICT (idempotency_key) DO NOTHING RETURNING id`. If the key
already exists, it reads the existing run: same `request_hash` → returns that run
with `created = false` (HTTP 200); different hash → `IdempotencyConflict`
(HTTP 409). The unique constraint, not a Python check, guarantees that
concurrent submissions with one key produce one run. All tasks are inserted in
the same transaction as the run (T1/T2). Idempotency keys are global (not per
workflow) and never expire. The hash uses the *resolved* version, so re-using a key
without `workflow_version` after a newer version is registered is a conflict (409); pass
the version explicitly to make retries robust to new versions.

### 6.2 Task input materialization

When a task becomes `queued` its `input` column is written as:

```json
{"run_input": <run.input>, "params": <task params>, "deps": {"<dep key>": <dep output>}}
```

so any worker can execute it using only the database. Task input is bounded
because run input (64 KiB) and outputs (64 KiB each, enforced at completion) are
bounded.

### 6.3 Claim (T4)

One transaction:

```sql
SELECT t.id FROM tasks t JOIN runs r ON r.id = t.run_id
WHERE t.status = 'queued' AND t.available_at <= now()
  AND r.status = 'running' AND r.cancel_requested_at IS NULL
ORDER BY t.available_at, t.created_at
LIMIT 1 FOR UPDATE OF t SKIP LOCKED;
-- insert attempt (attempt_number = attempt_count + 1, new lease_token,
--   lease_expires_at = now() + lease_seconds)
-- update task: status running, current_attempt_id, attempt_count + 1
-- insert event task_claimed
```

`SKIP LOCKED` lets many workers claim concurrently without blocking or
double-claiming; the partial unique index on running attempts is the backstop.

### 6.4 Heartbeat

Every `heartbeat_seconds` the worker's heartbeat thread issues one statement for
all its in-flight attempts:

```sql
UPDATE attempts SET heartbeat_at = now(), lease_expires_at = now() + :lease
WHERE id = :id AND lease_token = :token AND status = 'running'
  AND lease_expires_at > now()
```

returning, per attempt, whether ownership was renewed and whether the run has
`cancel_requested_at` set. **A lapsed lease is never revived**, even if the
scheduler has not yet expired it. The same thread updates `workers.last_heartbeat_at`.

### 6.5 Reporting results

`complete_attempt(attempt_id, lease_token, output)`, `fail_attempt(...)`,
`cancel_attempt(...)`, `release_attempt(...)` each run one transaction:

1. lock the run row (`FOR UPDATE`);
2. guarded update of the attempt: `WHERE id = :id AND lease_token = :token AND
   status = 'running' AND lease_expires_at > now()`. Zero rows → the report is
   **stale**; the function returns `False` and changes nothing;
3. apply the task transition (T5–T8, T11, T12) and its consequences in the same
   transaction: store output (T5); promote ready children (T3); block
   descendants (T9); finalize the run (R2, R3, R5); write events.

Completion stores the output and marks the task succeeded atomically, so a
completed output is never lost and never half-written. Output must be a JSON object of
at most 64 KiB; otherwise `complete_attempt` raises `ValidationFailed` without changing
state, and the worker reports a **non-retryable** failure (T7) instead.

`cancel_attempt` on a run that is *not* cancelling (the handler stopped for another
reason) behaves exactly like `release_attempt` (T8).

### 6.6 Recovery (scheduler)

Every `scheduler_interval_seconds` the scheduler:
1. selects up to N attempts that are `running` with `lease_expires_at <= now()`
   (lease lapsed — worker crashed, hung, partitioned, or too slow to
   heartbeat), and for each, in its own transaction, locks run → task →
   attempt, re-checks the condition, marks the attempt `lease_expired`, and
   applies T6/T7/T12 exactly like a failure;
2. does the same for attempts past `started_at + timeout_seconds +
   timeout_grace_seconds` (status `timed_out`) — a backstop for a worker that
   keeps heartbeating but cannot stop a stuck handler;
3. nothing for workers: a worker is *stale* when its heartbeat is older than
   `lease_seconds`, derived at read time. A hard-killed worker's row keeps
   `status = 'active'`; the dashboard shows it as stale and later as history.

Running several schedulers is safe: each transition is guarded by the re-check
under lock.

### 6.7 Retries and backoff

After a counted failure, if the error is retryable and `failure_count <
max_attempts`, the task is re-queued with
`available_at = now() + delay`, where

```
raw   = min(backoff_max, backoff_base * 2 ** (failure_count - 1))
delay = raw/2 + uniform(0, raw/2)          # "equal jitter"
```

so delay ∈ [raw/2, raw]. Handlers raise `PermanentTaskError` for errors that
retrying cannot fix (e.g. an invalid document); those fail the task
immediately (T7). Lease expiry and timeouts are retryable.

### 6.8 Terminal failure propagation

When a task fails terminally (T7) all its transitive descendants that are
`pending` become `blocked` (T9) in the same transaction. Tasks that do not depend
on the failed task keep running to completion (the engine does not fail fast),
so as much successful work as possible is preserved for a manual retry. When no
task is `pending/queued/running`, the run becomes `failed` (R3) and `runs.error`
names the first failed task.

### 6.9 Cancellation (cooperative)

`request_cancel(run_id)`: lock the run; if `running` → set `cancelling` and
`cancel_requested_at`; tasks `pending/queued` → `cancelled` (T10); if no task is
`running` → `cancelled` (R5) immediately. Running tasks learn of the request at
their next heartbeat; the worker sets the handler's cancellation flag; the handler
stops at its next check point and the worker calls `cancel_attempt` (T11).

**Races, defined:**
* *Completion vs cancel:* both lock the run row, so one happens first.
  - Completion first: the task is `succeeded` with its output; cancel then
    cancels remaining tasks.
  - Cancel first: the run is `cancelling`; a valid completion afterwards is
    **still recorded as `succeeded`** (its effects may already have happened;
    discarding the output would hide that), but its children are not promoted
    (they are already `cancelled`). The run becomes `cancelled` once nothing is
    running.
* *Claim vs cancel:* the claim skips runs with `cancel_requested_at` set. A claim
  that committed just before the cancel produces a running task that is then
  cancelled cooperatively.
* *Failure while cancelling:* no retry is scheduled; the task becomes `cancelled` (T12).

Cancellation never reverses effects that already happened (a sent notification
stays sent). A handler that ignores the flag runs until it finishes or times out.

### 6.10 Manual retry

`retry_run(run_id)`: lock the run; reject unless `failed` (409 for running,
cancelling, succeeded, cancelled). Then, in one transaction:
* tasks `succeeded` are untouched (outputs preserved, not re-executed);
* `failed` → `queued` (T13) with `failure_count = 0` — **retry counters reset**
  so each manual retry grants a fresh `max_attempts` budget; `attempt_count`
  keeps increasing so attempt numbers stay unique and history is preserved;
* `blocked` → `pending` (T14), then any pending task whose dependencies all
  succeeded → `queued`;
* re-queued tasks get `available_at = now()`, `finished_at = NULL` and a freshly
  materialized `input` (identical, because dependency outputs never change);
  `last_error` is kept for reference until the next attempt finishes;
* run → `running` (R6), `manual_retry_count += 1`, `finished_at`/`error` cleared;
* there is no limit on the number of manual retries (educational scope);
* event `run_retried`.

## 7. Worker

### 7.1 Loop and bounded concurrency
A worker has `concurrency` slots (thread pool). The main loop claims a task only
when a slot is free, so in-flight work per worker is bounded. When nothing is
claimable it sleeps `poll_interval_seconds` (polling; no LISTEN/NOTIFY in v1).

### 7.2 Timeouts
Handlers receive a `TaskContext` with `deadline`, `cancelled()` and
`check()` (raises `TaskCancelled`/`TaskTimedOut`). Python cannot safely kill a
thread, so timeouts are **cooperative**: at the deadline the worker sets the
context's timeout flag, reports `timed_out` via `fail_attempt(kind="timed_out")`,
and stops renewing that lease. The thread's slot stays occupied until the thread
actually returns, so a stuck handler reduces capacity but never exceeds the
bound. The scheduler's timeout check (6.6) is the backstop.

### 7.3 Database outages and lost ownership
If heartbeats fail (database unreachable), the worker keeps a local fail-safe
deadline per attempt: `last successful renewal (monotonic clock) + lease_seconds
- safety_margin`. Past that deadline the worker assumes it has lost ownership:
it sets the handler's cancellation flag and discards the eventual result (a
report would be rejected anyway, since the lease is expired in PostgreSQL time).
It stops claiming new work until the database is reachable again (retrying with
capped backoff). Reports that fail because the database is down are retried
until the local deadline passes; after that they are abandoned and the
scheduler re-queues the task.

Heartbeats run on a Python thread. A handler that holds the GIL for a long time in C
code, a long GC pause, or a very slow database can delay heartbeats past the lease; the
task is then re-executed while the first execution may still be running. This is
another source of at-least-once duplicates, alongside crashes. Keep
`lease_seconds ≥ 3 × heartbeat_seconds` (defaults 10 s / 3 s); the code enforces only
`heartbeat_seconds < lease_seconds / 2`.

**Engine-state protection is not external-effect protection.** Tokens stop a
stale worker from changing RelayFlow's tables. They cannot stop a stale process
that is still running from calling an external service. That is why the notify
task sends an idempotency key (section 9) and why the guarantee is
at-least-once.

### 7.4 Graceful shutdown
On SIGTERM/SIGINT: stop claiming; keep heartbeating while in-flight tasks finish,
up to `shutdown_grace_seconds`; then set cancellation flags and release
(`release_attempt`, T8) any attempt still owned, so it is re-queued immediately
instead of waiting for lease expiry; mark the worker `stopped`. In Docker,
`docker compose stop` sends SIGTERM. On Windows another process cannot deliver
SIGTERM (`Popen.terminate()` is a hard kill); graceful stop there means
`CTRL_BREAK_EVENT` (handled as SIGBREAK) to a worker started in its own process group,
or Ctrl+C in its console.

## 8. Guarantees and non-guarantees

* **At-least-once execution.** A task handler may run more than once (crash
  after effect, before acknowledgement; lease expiry of a slow worker).
  RelayFlow does **not** provide exactly-once execution.
* At most one attempt per task is `running` in the database at any instant
  (partial unique index).
* A task is dispatched only after all its dependencies succeeded.
* A stale attempt (expired or superseded token) cannot heartbeat or change task,
  run, or output state.
* A succeeded task's output is never overwritten, including across manual
  retries.
* Duplicate external effects are prevented **only** where the receiving service
  implements idempotency (the mock notification service does).
* A `cancelled` run may contain `succeeded` tasks: work that finished before (or
  racing with) the cancel keeps its result. Cancellation never undoes effects.
* Python has no equivalent of Go's race detector. Concurrency is verified with
  multi-threaded and multi-process tests, deterministic lock interleavings, and SQL
  invariant checks after every scenario (`relayflow.testing.check_invariants`).

## 9. Task types and the demonstration workflow

Registered types (the only executable code):

| type | does |
|---|---|
| `doc.validate` | checks `title` (1..200 chars) and `text` (1..20 000 chars, printable); permanent error if invalid |
| `doc.word_count` | words, unique words, lines, characters, average word length |
| `doc.keywords` | top-N (param `top_n`, default 8) non-stopword terms by frequency, ties broken alphabetically |
| `doc.report` | combines the above into a report dict with a deterministic SHA-256 digest |
| `notify.report_ready` | POSTs to the mock notification service with `Idempotency-Key: relayflow:<run_id>:<task_key>` (stable across attempts) |
| `demo.sleep` | sleeps `run_input.seconds` if present, else `params.seconds`, else 0 (≤ 60), cooperatively; for tests/benchmarks |
| `demo.noop` | returns immediately; for dispatch benchmarks |
| `demo.fail` | always raises a retryable error; for tests |

`document-processing` v1: `validate` → (`word_count`, `keywords`) → `report` →
`notify`. Run input: `{"title": str, "text": str, "demo": {...}?}`.

`demo` options (bounded, all optional):
* `delay_seconds: {task_key: 0..30}` — cooperative extra delay; always allowed.
* `fail_attempts: {task_key: [attempt numbers]}` — the handler raises a retryable
  error on those attempts. **Fault injection**: requires
  `RELAYFLOW_ENABLE_FAULT_INJECTION=1` on both API (else 422) and worker (else
  ignored).
* `crash_after_execute: {task_key: [attempt numbers]}` — the worker process
  calls `os._exit(137)` after the handler returns, before reporting. Fault
  injection, same gating.

### 9.1 Mock notification service
Separate process and database. `POST /notifications` with header
`Idempotency-Key` and JSON body. In one transaction it does
`INSERT ... ON CONFLICT (idempotency_key) DO NOTHING`; if the key exists and the
body hash matches, it returns the **existing** record (200, `"duplicate": true`);
if the hash differs, 409. `GET /notifications` lists records with their
`delivery_count` (incremented on duplicates) for verification.

## 10. HTTP API (prefix `/api`)

| method | path | success | errors |
|---|---|---|---|
| GET | `/health` | 200 `{status, database}` | 503 if DB unreachable |
| GET | `/workflows` | 200 list | |
| POST | `/workflows` | 201 created / 200 identical exists | 409 different spec, 422 invalid |
| GET | `/workflows/{name}/versions/{version}` | 200 | 404 |
| POST | `/runs` body `{workflow_name, workflow_version?, input, idempotency_key?}` (or header `Idempotency-Key`) | 201 new / 200 existing | 404 workflow, 409 key conflict, 413/422 invalid |
| GET | `/runs?status=&limit=&offset=` | 200 `{items, total}` | 422 bad status |
| GET | `/runs/{id}` | 200 run + tasks (+ attempts) | 404 |
| GET | `/runs/{id}/events` | 200 list | 404 |
| POST | `/runs/{id}/cancel` | 202 | 404, 409 terminal |
| POST | `/runs/{id}/retry` | 200 | 404, 409 not failed |
| GET | `/workers` | 200 list with `healthy` flag | |
| GET | `/overview` | 200 counts by run status, task status, workers | |

Error body: `{"detail": {"code": "...", "message": "..."}}`.

## 11. Configuration (environment)

| variable | default | meaning |
|---|---|---|
| `RELAYFLOW_DATABASE_URL` | `postgresql+psycopg://relayflow:relayflow@127.0.0.1:5433/relayflow` | |
| `RELAYFLOW_LEASE_SECONDS` | 10 | lease length |
| `RELAYFLOW_HEARTBEAT_SECONDS` | 3 | must be < lease/2 |
| `RELAYFLOW_POLL_INTERVAL_SECONDS` | 0.5 | idle claim polling |
| `RELAYFLOW_WORKER_CONCURRENCY` | 4 | slots per worker |
| `RELAYFLOW_SCHEDULER_INTERVAL_SECONDS` | 1.0 | recovery loop |
| `RELAYFLOW_TIMEOUT_GRACE_SECONDS` | 5 | scheduler timeout backstop grace |
| `RELAYFLOW_SHUTDOWN_GRACE_SECONDS` | 10 | |
| `RELAYFLOW_NOTIFY_URL` | `http://127.0.0.1:8100` | mock service |
| `RELAYFLOW_ENABLE_FAULT_INJECTION` | `0` | test/demo only |
| `RELAYFLOW_WORKER_NAME` | `worker` | prefix for worker id |

## Changelog
* 1.0 (2026-10-01) — initial agreed specification.
* 1.1 (2026-10-01) — after the independent review (`docs/spec-review.md`): row locks
  are `FOR NO KEY UPDATE` (fixes claim/cancel deadlock D1); retry-count semantics,
  oversized output, cancel-when-not-cancelling, expiry-vs-timeout precedence, worker
  staleness, idempotency-key scope, retry column resets, GIL/heartbeat risk, timing
  guidance, Windows graceful stop, and succeeded-tasks-in-cancelled-runs made explicit.
