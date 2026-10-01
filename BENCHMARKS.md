# Benchmarks

These are real measurements from one laptop, taken once. They describe this
educational engine on this machine. They are not capacity claims and say nothing about
production scale. Raw output: `bench-results/2026-10-01T193709Z.json`.

## Environment

| | |
|---|---|
| Hardware | Dell laptop, Intel Core Ultra 7 258V (8 cores), 31.6 GB RAM, on AC power, Windows "Balanced" power plan |
| OS | Windows 11 Home 10.0.26200 |
| Containers | Docker Desktop 29.7.2 (WSL2 backend, kernel 6.18.33.2), VM limit 8 CPUs / 15.4 GiB |
| Services | PostgreSQL 17.11 (`postgres:17-alpine`), Python 3.13 slim images, all from `docker-compose.yml` |
| Benchmark client | Host Python 3.13.12; it submits runs straight to PostgreSQL through `relayflow.engine.submit_run` and reads timestamps back with SQL |
| Engine settings | lease 10 s, heartbeat 3 s, scheduler interval 1 s, worker concurrency 4 slots, poll interval 0.5 s (0.1 s where stated), fault injection off |
| Date | 2026-10-01 |

Only the benchmark ran during these measurements: no tests, no demo. The database
already held a few hundred earlier runs.

## How it is measured

Every duration is a difference between two PostgreSQL `timestamptz` values, so all of
them come from one clock.

* **Throughput (prequeued):** the workers are stopped, all N runs are queued, and the
  workers are started again. Throughput = tasks / (last attempt `finished_at` −
  first attempt `started_at`). Prequeuing keeps the script's own submission speed out of
  the result.
* **Dispatch latency (paced):** one run every 0.2 s while the workers are idle.
  Latency = `attempts.started_at − tasks.queued_at`, i.e. from the moment a task became
  ready until a worker claimed it.
* **Handler time:** `attempts.finished_at − attempts.started_at`. This includes the task
  code and the completion transaction.
* **Orchestration overhead (sleep workload):** makespan − ideal makespan, where
  ideal = ⌈tasks / 8 slots⌉ × 0.25 s.
* **Crash recovery:** `docker compose kill` stops the worker container while it runs
  a 4 s `demo.sleep`. Reassignment time = the next attempt's `started_at` − the killed
  attempt's last `heartbeat_at`.

## Results

### Throughput

| scenario | tasks | makespan | throughput | handler p50 / p95 |
|---|---|---|---|---|
| `demo.noop`, 1 worker × 4 slots | 500 | 2.60 s | **192 tasks/s** | 5.0 / 7.2 ms |
| `demo.noop`, 2 workers × 4 slots | 500 | 2.35 s | **213 tasks/s** | 8.7 / 15.3 ms |
| `demo.sleep` 0.25 s, 2 workers × 4 slots | 160 | 5.64 s (ideal 5.00 s) | 28.4 tasks/s | 254 / 262 ms |

Each task is one run with one task. That means three short transactions per task:
submit, claim, and complete. Complete also promotes successors and settles the run.
Adding a second worker raised no-op throughput by only about 10 %, and handler time
went up. The limit is PostgreSQL transaction throughput inside the Docker Desktop VM,
not the number of worker slots. I did not profile this further.

**Orchestration overhead:** 0.25 s tasks on 8 slots finished 0.64 s later than the
ideal schedule allows. That is about 32 ms per round of 8 tasks: claim, heartbeat and
report transactions, plus polling gaps between a slot freeing and the next claim.

### Dispatch latency (light load)

| poll interval | tasks | mean | p50 | p95 | max |
|---|---|---|---|---|---|
| 0.5 s (default) | 60 | 260 ms | 265 ms | 481 ms | 499 ms |
| 0.1 s | 60 | 55 ms | 48 ms | 100 ms | 102 ms |

Latency is mostly the idle polling wait: on average about half the poll interval,
bounded by the full interval. Faster polling cuts latency but sends more idle queries.
LISTEN/NOTIFY would remove most of this delay; it is not implemented in v1.

### Worker-crash recovery (lease 10 s, heartbeat 3 s, scheduler 1 s)

| trial | killed | lease expiry after last heartbeat | scheduler detection after expiry | reassigned after last heartbeat |
|---|---|---|---|---|
| 1 | worker-a | 10.0 s | 0.09 s | **11.06 s** |
| 2 | worker-a | 10.0 s | 0.10 s | **11.07 s** |
| 3 | worker-a | 10.0 s | 0.70 s | **11.67 s** |

Recovery time ≈ lease + up to one scheduler interval + the first retry's jittered
backoff (0.5–1 s) + up to one poll interval. The crash itself can happen up to one
heartbeat interval after the last heartbeat, so the time from crash to reassignment
is a little shorter than the last column. The lease is the main tuning knob. A
shorter lease recovers faster, but a slow worker or database is then more likely to
lose a lease it still needs, which means more duplicate executions (at-least-once).

## Reproduce

```powershell
docker compose up -d --build          # once
uv run python scripts/bench.py        # ~2 minutes; writes bench-results/<timestamp>.json
uv run python scripts/bench.py --quick
```
The script recreates and stops/starts the worker containers itself. Afterwards it
leaves both workers running with default settings.

## Measurement corrections (kept for honesty)

The first benchmark version submitted runs while the workers were already
claiming. Submitting 300 runs from the host took 2.65 s, which was longer than the
measured makespan, so its "throughput" and "latency" really measured the script's
submission rate and backlog wait. Those results were discarded and not published. The
script now prequeues for throughput and paces submissions for latency.
