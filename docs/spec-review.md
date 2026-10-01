# Independent review of the RelayFlow specification

Reviewer: test agent. Reviewed: `docs/architecture.md` v1.0 and `docs/test-interfaces.md`
(2026-10-01). Expectations in `tests/correctness/` come from the spec. Where the code
differs, the test follows the spec and the difference is listed under
[Discrepancies found](#discrepancies-found).

Each item gives a severity (**bug**, **gap**, **ambiguity**, **nit**) and a suggested amendment.

## Discrepancies found

### D1. Claim waits on the run lock and deadlocks with `request_cancel` (bug)

Spec 2.1 and 6.3 say the claim "locks only a task row (with `SKIP LOCKED`) ... it never
waits on a run lock", and that the run → task → attempt order rules out deadlocks.

What the code does instead: `claim_task` inserts an `attempts` row and an `events` row.
Both have `run_id REFERENCES runs(id)`, so PostgreSQL's FK check takes `FOR KEY SHARE` on
the run row. `_lock_run` uses `SELECT ... FOR UPDATE`, which conflicts with `FOR KEY SHARE`.
That has two effects:

1. A claim blocks on any run-level transition of the same run (complete, fail, expire,
   cancel, retry). This costs throughput but is otherwise harmless.
2. It can deadlock with `request_cancel`. The claim locks task T (step 1). Then
   `request_cancel` locks run R and waits for T, because it runs `_task_rows(lock=True)`.
   Then the claim's attempt INSERT needs `KEY SHARE` on R. PostgreSQL detects the cycle
   and aborts one side with `DeadlockDetected`. If the claim is aborted, the worker logs a
   claim error. If the cancel is aborted, `POST /runs/{id}/cancel` returns a 500.

No state is corrupted, because the aborted transaction rolls back. But the spec's
no-deadlock claim is false. The same lock pattern applies to any future run-level
operation that locks task rows after the run row.

How it was reproduced: the tests use real `claim_task` and `request_cancel`, with an
admin row lock on a *pending* task to pause the cancel after it has taken the run lock.
The stress test hit it as well, without any forced pause.

- `test_lock_ordering.py::test_claim_does_not_wait_on_run_lock`: the claim blocks for
  more than 3 s.
- `test_lock_ordering.py::test_cancel_and_claim_do_not_deadlock`: `DeadlockDetected`.
- `test_lock_ordering.py::test_claim_lock_pattern_does_not_deadlock_with_cancel`:
  `DeadlockDetected`.
- `test_cancellation.py::test_concurrent_claims_and_cancels`: an unforced stress test
  that also hit `DeadlockDetected`.

**Suggested fix (verified by hand):** lock runs with `SELECT ... FOR NO KEY UPDATE` in
`_lock_run`. That mode still serializes all run-level transitions against each other,
because `NO KEY UPDATE` conflicts with itself. It does not conflict with the FK's
`KEY SHARE`. With that change the claim no longer waits, and the interleaving above
completes without a deadlock. Amend spec 2.1 to say "run-level transitions take
`SELECT ... FOR NO KEY UPDATE` on the run row (not `FOR UPDATE`, which would conflict with
the `FOR KEY SHARE` locks taken by foreign-key checks on attempt/event inserts)". Also
consider making the worker and the API retry once on `DeadlockDetected` /
`SerializationFailure`.

No other engine discrepancy was found. 94 of the 98 collected test items pass, and all
4 failures are D1. The cooperative-cancel test was added after that full run and passes
on its own.

## Ambiguities and gaps in the spec

| # | Section | Issue | Suggested amendment |
|---|---|---|---|
| A1 | 5.2 T6/T7, 6.7 | `failure_count < max_attempts` does not say whether the count is taken before or after incrementing for the current failure. The code increments first, so `max_attempts` = total counted attempts. That is the natural meaning, but it should be written down. | "After a counted failure, `failure_count` is incremented; the task is re-queued iff retryable and the **new** `failure_count < max_attempts`." The same applies to the backoff formula's `failure_count` (it is the new value, so the first retry uses `raw = base`). |
| A2 | 5.2 T11, 6.5 | `cancel_attempt` on a run that is **not** cancelling is undefined. The code treats it as a release (T8, attempt `released`). | State this explicitly, or reject it (return `False`). Releasing is reasonable: the worker stops because it lost its cancel signal source. |
| A3 | 6.5 | Output over 64 KiB is "enforced at completion", but the transition is not specified. The engine raises `ValidationFailed` and leaves the attempt running. The worker currently catches this and calls `fail_attempt(retryable=False)`, but that rule exists only in code. | Write the rule into 6.5: oversized or non-object output means a permanent failure (T7). Tested only as "state unchanged" (`test_oversized_output_is_rejected_without_changing_state`). |
| A4 | 6.6 | If an attempt both lapsed its lease and passed its timeout, the outcome is not specified. The code checks lease expiry first, so it reports `lease_expired`. | Say "lease expiry is checked first". Both outcomes count as failures, so only the attempt status differs. |
| A5 | 5.3 vs 6.6 | 5.3 says `now() > started_at + timeout + grace`. The code uses `<=` for "is expired". | Trivial. Pick one boundary. |
| A6 | 1 vs 6.6 | Section 1's table says the scheduler "marks silent workers stale". 6.6 step 3 says staleness is derived at read time and nothing is written. Workers killed with `kill -9` stay `status='active'` forever in `workers`. | Reword section 1 to "derives worker staleness". Optionally let the scheduler set `status='stopped'` for workers stale for more than N × lease, so dashboards don't accumulate ghosts. |
| A7 | 6.1 | The idempotency key is global (not per workflow) and never expires. The request hash includes the *resolved* version. Re-submitting the same key without a version after a new version is registered therefore gives an `IdempotencyConflict`. | Document both facts. Consider scoping keys per workflow name. |
| A8 | 6.9 | The claim-skip guard (`cancel_requested_at IS NULL`) cannot be observed through the public API: `request_cancel` cancels all queued tasks in the same transaction. It matters only for the claim/cancel race. | Fine as defence in depth. Say so. The test constructs the state by hand (`test_claim_skips_runs_with_cancel_requested`). |
| A9 | 6.10 | Retry keeps `last_error` on re-queued tasks and does not say what happens to `tasks.input` (the code re-materializes it). It also does not say whether `available_at` is reset (the code sets it to `now()`). | List the columns retry resets. |
| A10 | 6.10 | There is no limit on manual retries and no rate limit. | Acceptable for an educational project. State it. |
| A11 | 7.3 | Heartbeats run on a Python thread. A handler that holds the GIL in C code, or a long GC pause, can delay heartbeats past the lease. The task is then re-executed while the first execution continues (at-least-once). | Mention this as a known source of duplicate execution, alongside crashes. |
| A12 | 7.4 | Graceful shutdown is specified for SIGTERM/SIGINT. On Windows another process can't deliver SIGTERM (`Popen.terminate()` is `TerminateProcess`, which is a hard kill). | The worker already handles `SIGBREAK`. Document that on Windows, graceful stop means `CTRL_BREAK_EVENT` to a worker started with `CREATE_NEW_PROCESS_GROUP`. This is tested by `test_graceful_shutdown_releases_in_flight_attempt`, which passes on Windows 11. |
| A13 | 3 | `check_invariants` runs each invariant as its own statement. Each check is consistent within itself, but there is no snapshot across checks. That is fine for the current checks, all of which are single-statement. | Keep every invariant a single SQL statement, or run them in one `REPEATABLE READ` transaction. |
| A14 | 8 | The guarantees don't mention that a `succeeded` task inside a `cancelled` run is possible (cancel-first race, 6.9), so a dashboard or user may be surprised. | Add it to section 8. |
| A15 | 1 vs test-interfaces | Section 1 names `relayflow.worker.main` / `relayflow.scheduler.main` / `mocknotify.app`. The process entry points are `python -m relayflow.worker` etc. | Nit. List both the module and the entry point. |
| A16 | 4 | There is no rule for `timeout_seconds` vs `lease_seconds`, or for `heartbeat_seconds` vs DB latency. With `lease=2 s` (test settings), one slow transaction (> 1.5 s) can expire a healthy lease. | Recommend `lease_seconds ≥ 3 × heartbeat_seconds` plus a margin. The code enforces only `heartbeat < lease/2`. |

## Things that are hard or unsafe to test

- **Power loss / fsync**: out of scope. We rely on PostgreSQL durability.
- **Exactly-once**: not provided. The tests assert at-least-once plus receiver
  idempotency (`delivery_count == 2`, one logical notification).
- **Races inside one engine call**: Python has no `-race` detector. We use threads with
  `threading.Barrier`, forced failure points, admin-held row locks that pause a real
  operation at a known point, and invariant SQL checks after every scenario.
- **Clock skew**: all lease decisions use PostgreSQL `now()`, so worker clock skew can't
  be tested meaningfully, and doesn't need to be.

## Requests to main agent

1. Fix D1 (`_lock_run`: `FOR UPDATE` → `FOR NO KEY UPDATE`) and amend spec 2.1 to match.
   Consider one retry on `DeadlockDetected` in the worker claim loop and the API.
2. Add a `relayflow.testing` helper to start a worker in its own process group
   (`creationflags=CREATE_NEW_PROCESS_GROUP` on Windows) plus a `stop_gracefully(proc)`
   helper that sends `CTRL_BREAK_EVENT` on Windows and SIGTERM elsewhere. The
   correctness suite currently builds this inline from `relayflow.testing.FAST_TIMINGS`.
3. Optional: expose `relayflow.testing.wait_for_lock_waiter(engine, pattern)` (a
   `pg_stat_activity` probe). It makes the deterministic lock-interleaving technique
   reusable.
4. Please keep `tests/correctness/__init__.py`. It makes the directory a package, so its
   module names cannot collide with same-named files in `tests/unit` or
   `tests/integration` under pytest's default `prepend` import mode.
5. `docs/correctness.md` is a table. Append rows for your own tests in the same format.
