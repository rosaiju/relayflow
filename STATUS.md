# RelayFlow status

_Last updated: 2026-10-01._ Read this first when resuming work.

## State: all five milestones complete

| # | Milestone | Commit |
|---|---|---|
| 1–2 | Spec (state machines, every transition), schema with DB-enforced invariants, engine, worker, scheduler, mock service, API, Compose stack, unit and integration tests | `c550a36` |
| 3 | Independent correctness suite (test agent); claim/cancel deadlock found and fixed; spec v1.1 | `159ba43` |
| 4 | React/TypeScript dashboard + Playwright e2e | `a4dad8c` |
| 5 | Crash-recovery demo, benchmarks, CI, documentation | `064da3b`, `40c6fff` (CI pin fix) |
| follow-up | Closed two verification gaps: PostgreSQL outage longer than the lease; API-only restart (stack tests, in CI) | see `git log` |

The repository is private: https://github.com/rosaiju/relayflow (default branch `main`).

## Verification actually performed

| Check | Environment | Result |
|---|---|---|
| `ruff format --check`, `ruff check`, `mypy --strict` (34 files) | Windows 11 (local) and GitHub Actions ubuntu-latest | pass |
| pytest unit (38) | Windows local + CI | pass |
| pytest integration + correctness (112, real PostgreSQL 17, includes subprocess crash tests) | Windows local (PostgreSQL in Docker Desktop) + CI (PostgreSQL service container) | pass. Full local run: 150 passed in 75 s |
| Frontend `tsc -b` + `vite build` | Windows local + CI | pass |
| Playwright e2e, 5 tests (Chromium) against the Docker stack with fault injection | Windows local + CI | pass |
| `scripts/demo_recovery.py` (asserting crash-recovery demo, all 8 steps) | Windows 11 + Docker Desktop (WSL2), and CI ubuntu-latest | pass on both |
| `scripts/bench.py` | Windows 11 + Docker Desktop only (see BENCHMARKS.md) | results recorded |
| Stack test: PostgreSQL outage > lease (`tests/stack/test_postgres_outage.py`) | Windows 11 + Docker Desktop: 5 consecutive passes after the fixes below (and 10 of 13 runs before them); also run in CI (e2e job) | pass |
| Stack test: API-only restart (`tests/stack/test_api_restart.py`) | Windows 11 + Docker Desktop: 6 consecutive passes; also run in CI (e2e job) | pass |
| GitHub Actions run 36916722830 on `40c6fff` | backend, frontend, e2e+demo jobs | **all success** |

The first CI run (36916622763) failed at setup because `astral-sh/setup-uv@v10` has no
floating major tag. Fixed by pinning `v10.2.0`.

Bugs found during verification, all fixed:
1. `doc.report` read a non-direct dependency's output. Found by the first live run;
   fixed and recovered with a real manual retry.
2. Claim/cancel deadlock (`FOR UPDATE` vs foreign-key `KEY SHARE`). Found by the
   independent test agent (D1 in `docs/spec-review.md`).
3. Worker heartbeat thread could overwrite the final `stopped` status. Found in code
   review; fixed by joining the thread first.
4. The first benchmark measured the client's submission speed. Discarded and
   redesigned (BENCHMARKS.md).

## Known limitations and things not verified

- At-least-once only. Cooperative timeouts and cancellation. Polling, not LISTEN/NOTIFY.
- Closed gap 1: a PostgreSQL outage longer than the lease is now tested end to end
  (G40 in `docs/correctness.md`). The outage is a SIGKILL of PostgreSQL (crash, then
  WAL recovery on restart) lasting 14 s with a 6 s lease. A graceful database shutdown
  of the same length is not separately tested. The demo script's quick (~1 s)
  restart still only shows reconnection.
- Closed gap 2: an API-only restart while workers and the scheduler run is tested (G41).
- Fixing gap 1 exposed a test-environment flaw: the mock receiver shared RelayFlow's
  PostgreSQL server, so a RelayFlow database outage also took the "external" service
  down. In Compose it now has its own server (`mocknotify-db`); no engine code changed.
- While stabilizing the outage test, it failed 3 times in 13 early runs:
  - Two were fixture setup errors, confirmed from the traceback: `docker compose up
    --wait` exited 1. Likely reason: the API container is briefly *unhealthy* right
    after an outage (its healthcheck checks the database), and `--wait` fails on that.
    Fix: poll real readiness (API health, healthy workers) instead of `--wait`.
  - One was a failure inside the test whose output I did not capture, so its cause is
    **unconfirmed**. The most likely candidate is the synchronization guard:
    `docker compose stop postgres` (clean shutdown plus CLI overhead) can exceed the
    3.5 s window, and the guard then fails rather than passes. Fix: `docker compose
    kill` for an instantaneous outage, with the measured lag now in the failure
    message. No assertion was weakened, and the test has not failed since.
- iOS/macOS were not tested; neither was Linux outside CI. Benchmarks were taken on
  one laptop only.
- Hard-killed workers stay `active` in the `workers` table. The dashboard derives
  staleness and collapses old instances.
- Playwright's `screenshots.mjs` is a helper, not a test.
- `fastapi.testclient` emits a Starlette deprecation warning about `httpx`. This is
  harmless at the pinned versions.

## Exact next steps (optional improvements)

1. LISTEN/NOTIFY wakeups to cut dispatch latency below the poll interval (keep
   polling as the fallback).
2. (Done: see G40.) Optionally add a graceful-shutdown variant of the outage test.
3. Retention/archiving for `events` and `attempts`; an index review at larger volume.
4. Optional subprocess isolation for handlers, so timeouts can be enforced hard.
5. Have the scheduler mark long-silent workers `stopped`.

## Resuming

```powershell
cd C:\Users\rohan\projects\relayflow
docker compose up -d --build
uv sync; uv run pytest
```
Follow `CLAUDE.md` for the commit/push rules.
