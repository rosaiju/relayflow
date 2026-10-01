# Learning guide: one workflow, from submission to crash recovery

This guide follows a single `document-processing` run through the real code, including
the moment its worker is killed. File references are clickable in most editors. Line
numbers are approximate; search for the function name if they drift.

Read `docs/architecture.md` sections 2 and 5 alongside it.

## 0. The pieces

| process | entry point | what it does |
|---|---|---|
| API | `src/relayflow/api/app.py` `create_app` | records submissions, reads state, accepts cancel/retry |
| worker (×2) | `src/relayflow/worker/main.py` `Worker` | claims tasks, runs handlers, heartbeats, reports |
| scheduler | `src/relayflow/scheduler/main.py` `Scheduler` | expires lapsed leases, timeout backstop |
| mock service | `src/mocknotify/app.py` | receives the "report ready" notification, deduplicates |

They share nothing except PostgreSQL. All state changes go through
`src/relayflow/engine/ops.py`, and each function there is **one short transaction**.

## 1. Submission (API → `submit_run`)

`POST /api/runs` arrives at `submit_run` in `api/app.py:157`. The handler is a plain
`def`, so FastAPI runs it on a threadpool thread. It calls
`engine/ops.py:379 submit_run`, which:

1. loads the newest `document-processing` definition. Its `spec` is the validated
   DAG from `definitions.py:validate_definition` (Kahn ordering, cycle detection in
   `find_cycle`);
2. validates the input size and the `demo` options (`validate_run_input`). The
   fault-injection options are refused unless the server enabled them;
3. hashes `(workflow, version, input)` (`request_hash`);
4. runs `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING RETURNING id`. If the key
   already exists, the same hash returns the existing run (HTTP 200) and a different
   hash raises `IdempotencyConflict` (409). The **unique constraint** settles
   concurrent submissions, not Python;
5. inserts the five task rows in the same transaction. `validate` has no dependencies,
   so it starts `queued` with its input already materialized (`_materialized_input`).
   The others start `pending`.

Question to ask yourself: why must the task rows be inserted in the same transaction
as the run row?

## 2. Claiming (worker → `claim_task`)

Each worker's main thread runs `_claim_loop` (`worker/main.py:148`). It only tries to
claim while a slot is free; that is the bounded concurrency. `claim_task`
(`engine/ops.py:494`):

```sql
SELECT t.* FROM tasks t JOIN runs r ON r.id = t.run_id
WHERE t.status = 'queued' AND t.available_at <= now()
  AND r.status = 'running' AND r.cancel_requested_at IS NULL
ORDER BY t.available_at, t.created_at
LIMIT 1 FOR NO KEY UPDATE OF t SKIP LOCKED
```

`SKIP LOCKED` means two workers never block on, or both take, the same row. The
transaction then inserts an **attempt** with a fresh `lease_token` and
`lease_expires_at = now() + lease`, marks the task `running`, and writes a
`task_claimed` event. The partial unique index `uq_one_running_attempt_per_task` is
the database's last line of defence.

Then the transaction **commits**, and only after that does the handler run, in a slot
thread (`_execute`, `worker/main.py:197`). No transaction stays open while user work
runs.

## 3. Heartbeats (worker → `heartbeat`)

A separate heartbeat thread (`_heartbeat_once`, `worker/main.py:357`) renews every
in-flight lease in one statement (`engine/ops.py:571`):

```sql
UPDATE attempts SET lease_expires_at = now() + :lease ...
WHERE id = :id AND lease_token = :token AND status = 'running' AND lease_expires_at > now()
```

The last condition matters: **a lapsed lease is never revived**, even if the scheduler
hasn't noticed yet. The reply also says whether the run is being cancelled, and if so
the worker sets the handler's cancel flag.

If the database is unreachable, the worker keeps a local deadline: last successful
renewal + lease − margin. Past that deadline it assumes it has lost ownership and
stops the handler.

## 4. Completion and fan-out (`complete_attempt`)

When `validate` returns, `_report_success` calls `complete_attempt`
(`engine/ops.py:746`). In one transaction it:

1. locks the **run** row (`_lock_run`, `FOR NO KEY UPDATE`), then the task, then the
   attempt (`_owned_attempt`). It continues only if the token matches and the lease
   is still valid in PostgreSQL time; otherwise it returns `False` and changes nothing;
2. stores the output and marks the task `succeeded` (a trigger makes that final);
3. `_promote_ready`: `word_count` and `keywords` now have all their dependencies
   succeeded, so they become `queued` with materialized inputs;
4. `_finalize_run`: if nothing is left to do, it settles the run status.

Why lock the run first? Suppose `word_count` and `keywords` finish at the same moment.
Without the run lock, each transaction could see the other as still `running`, and
neither would queue `report`. That is the "lost promotion" race, and
`tests/correctness/test_dependency_ordering.py` provokes it on purpose.

## 5. The crash

The browser demo slows `keywords` down by 8 s. While it runs on worker-a, you
`docker compose kill worker-a`. The process dies instantly: no `finally` blocks run,
nothing is reported, and the heartbeat thread is gone.

In the database nothing changes yet. The attempt is still `running` and its
`lease_expires_at` is about to pass.

## 6. Recovery (scheduler → `recover_expired`)

The scheduler loop (`scheduler/main.py:44`) calls `recover_expired`
(`engine/ops.py:914`) every second:

1. it finds candidates without locking them:
   `status = 'running' AND lease_expires_at <= now()`;
2. for each candidate, a new transaction locks run → task → attempt and **re-checks**
   the condition (`_recover_one`). Another scheduler, or a report that arrived just in
   time, might have handled it already;
3. it marks the attempt `lease_expired` and calls `_after_attempt_failure`. That
   counts the failure (1 of 3), computes `backoff_delay` (exponential with "equal
   jitter"), and sets the task back to `queued` with `available_at = now() + delay`.

Worker-b's next poll claims it, and the new attempt's `task_claimed` event records
"previous attempt lease_expired on worker-a". The dashboard's recovery panel is built
from exactly these persisted attempts.

What was **not** redone: `validate` and `word_count` already succeeded, and their
outputs are in their rows. Only `keywords` runs again.

## 7. What if worker-a was only slow, not dead?

If worker-a comes back after its lease lapsed and tries `complete_attempt`, the
guarded `UPDATE` matches zero rows (wrong status, expired lease) and the call returns
`False`. The engine's state is safe.

But any external effect worker-a caused **already happened**. That is why the guarantee
is *at-least-once*, and why the `notify` task sends
`Idempotency-Key: relayflow:<run_id>:notify`. The key comes from the task's identity,
not the attempt number, so every attempt sends the same key. The mock service
(`mocknotify/app.py`) does a single
`INSERT ... ON CONFLICT DO UPDATE ... WHERE request_hash matches`, so a repeated
delivery returns the original record and `delivery_count` goes up. The demo script
crashes a worker right after the notification is accepted and asserts that exactly one
logical notification exists.

## 8. Failure, blocking, manual retry

If `keywords` fails three times, `_after_attempt_failure` marks it `failed` and
`_block_descendants` marks `report` and `notify` as `blocked`. `word_count` still
finishes, then `_finalize_run` sets the run to `failed`. `POST /runs/{id}/retry`
(`retry_run`, `engine/ops.py:986`) re-queues `failed` tasks with a fresh retry
budget, moves `blocked` tasks back to `pending`, and never touches `succeeded` tasks.
Attempt numbers keep increasing, so the history stays complete.

## Exercises

1. Change `LEASE` in `scripts/demo_recovery.py` to 3 s and rerun. What changes in the
   recovery time, and what new risk appears? (Hint: see the GIL note in architecture §7.3.)
2. Delete the `AND lease_expires_at > now()` from `heartbeat`. Which test in
   `tests/correctness/test_stale_ownership.py` fails, and why?
3. Change `_lock_run` back to `FOR UPDATE` and run `tests/correctness/test_lock_ordering.py`.
   Explain the deadlock in your own words.
