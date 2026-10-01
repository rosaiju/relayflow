# Interview guide

## 30-second version

"RelayFlow is a workflow engine I built in Python on PostgreSQL. You define a DAG of
tasks; independent worker processes claim tasks with `SKIP LOCKED`, hold leases they
renew with heartbeats, and report results with an ownership token. If a worker dies,
its lease lapses, a scheduler re-queues the task, and another worker picks it up while
completed results stay put. Execution is at-least-once, so the side-effecting step
sends an idempotency key, and a separate mock service deduplicates it. I demo it by
killing a worker container mid-task and showing the recovery on a React dashboard. An
independent test suite found a real deadlock, and I fixed it."

## Two-minute technical walkthrough

1. **Data model.** runs → tasks → attempts. Attempts are the history: one row per try,
   with a `lease_token`, `lease_expires_at` and an outcome. A partial unique index
   allows at most one running attempt per task. Triggers make snapshots, succeeded
   outputs and finished attempts immutable.
2. **Claim.** One transaction: pick a queued task with `FOR NO KEY UPDATE SKIP
   LOCKED`, insert the attempt, mark the task running, commit. The task runs *after*
   the commit, so no transaction is open during user code.
3. **Lease.** A heartbeat thread extends `lease_expires_at` with `now()` from
   PostgreSQL, never the worker's clock, and only while the lease is still valid. A
   lapsed lease is never revived.
4. **Report.** Lock order is run → task → attempt. A guarded update requires the token
   and an unexpired lease. Stale reports change nothing. The output and the
   succeeded status are written together, and ready children are promoted in the same
   transaction.
5. **Recovery.** The scheduler finds lapsed leases, re-checks each one under lock,
   marks it `lease_expired`, and re-queues the task with exponential backoff and
   jitter. Retries are bounded; when they run out, the failure blocks descendants.
6. **External effects.** Tokens protect *my* tables, not the outside world. The notify
   task uses a key built from `run_id:task_key`, and the receiver deduplicates
   atomically with `INSERT … ON CONFLICT`. The demo crashes a worker after the
   notification was accepted and asserts that exactly one logical notification exists.

## Numbers I measured (see BENCHMARKS.md; one laptop, Docker Desktop)

- About 190–210 no-op tasks/s. Adding a second worker barely helped, so PostgreSQL
  transactions in the Docker VM are the bottleneck.
- Dispatch latency is about half the poll interval: 260 ms mean at 0.5 s polling,
  55 ms at 0.1 s.
- Crash → reassignment takes about 11 s with a 10 s lease (lease + scheduler tick +
  backoff).

## Tradeoffs I can defend

| Decision | Why | Cost |
|---|---|---|
| PostgreSQL as the queue | one durable source of truth; transactions give atomic claim + state change | throughput is bounded by DB writes; polling load |
| Leases + heartbeats (not "worker said it's alive") | crash detection without trusting the worker; DB time avoids clock skew | recovery takes ≥ 1 lease; a slow worker can lose its lease → duplicate run |
| At-least-once + receiver idempotency | honest and achievable; exactly-once across an external service needs the receiver's cooperation anyway | handlers and receivers must tolerate retries |
| Sync SQLAlchemy Core + threads | simple, readable, one connection per operation; no async coloring of handler code | GIL; threads can't be killed, so timeouts are cooperative |
| Polling instead of LISTEN/NOTIFY | stateless, robust to reconnects | latency ≈ poll/2; idle queries |
| Not fail-fast on task failure | independent branches finish, so manual retry redoes less | the run takes longer to report failure |
| Per-run row lock | removes lost-promotion and cancel races simply | serializes transitions *within* a run |

## Likely questions

- **"How do you prevent two workers from running the same task?"** In the database:
  `SKIP LOCKED` plus the partial unique index. In time: the lease. Then I explain why
  this still isn't exactly-once (the slow-worker case).
- **"What's a fencing token, and do you have one?"** The `lease_token` fences
  RelayFlow's own state. For external systems the analogue is the idempotency key. The
  limit: a receiver that ignores keys can't be protected.
- **"What happens if the database goes down?"** Workers stop claiming and keep
  retrying with backoff. Heartbeats fail, and once the local deadline passes a worker
  stops its handlers and discards their results. The API returns 503. The demo
  restarts PostgreSQL mid-run and asserts the run completes. In my run the restart
  took about 1 s, shorter than the lease, so nothing was re-executed.
- **"How did you test concurrency in Python without a race detector?"** Thread and
  process tests with barriers; deterministic interleavings (an admin connection holds
  a row lock so a real operation pauses at a known point); SQL invariant checks after
  every test. That is how the deadlock was found: `FOR UPDATE` conflicts with the
  `KEY SHARE` lock a foreign-key insert takes on the parent row.
- **"What would you change for production?"** LISTEN/NOTIFY for wakeups, auth,
  partitioning and archiving attempts and events, metrics and tracing, per-type
  concurrency limits, subprocess isolation for handlers so timeouts can be hard, and
  a real deployment story.

## Honest resume bullets

- Built RelayFlow, a durable workflow engine in Python/FastAPI on PostgreSQL. It
  executes DAG workflows across multiple worker processes using `SKIP LOCKED` claims,
  heartbeated leases with per-attempt ownership tokens, and scheduler-driven recovery.
  Lease expiry and stale-report rejection are verified by multi-process crash tests.
- Designed database-enforced invariants (partial unique index, immutability triggers,
  guarded updates) and bounded retries with jittered backoff. An independent
  concurrency test suite found and confirmed the fix of a claim/cancel deadlock
  caused by foreign-key lock conflicts.
- Implemented at-least-once side effects with stable idempotency keys and a mock
  receiver that deduplicates atomically. An automated Docker demo kills workers
  mid-task and after delivery, then asserts preserved outputs and a single logical
  notification.
- Built a React/TypeScript dashboard showing task graphs, attempt history, retry
  timing and recovery evidence from persisted state. Measured about 200 no-op
  tasks/s and about 11 s crash-to-reassignment with a 10 s lease on one laptop.
  Added GitHub Actions CI with PostgreSQL, Playwright and the crash demo.
