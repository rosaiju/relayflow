# RelayFlow status

_Last updated: 2026-10-01._ Read this first when resuming work.

## State: all five milestones complete

| # | Milestone | Commit |
|---|---|---|
| 1–2 | Spec (state machines, every transition), schema with DB-enforced invariants, engine, worker, scheduler, mock service, API, Compose stack, unit and integration tests | `c550a36` |
| 3 | Independent correctness suite (test agent); claim/cancel deadlock found and fixed; spec v1.1 | `159ba43` |
| 4 | React/TypeScript dashboard + Playwright e2e | `a4dad8c` |
| 5 | Crash-recovery demo, benchmarks, CI, documentation | `064da3b`, `40c6fff` (CI pin fix) |

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
- The PostgreSQL-restart demo step restarted PostgreSQL in about 1 s, which is shorter
  than the 6 s demo lease. So it verifies reconnection and completion; it does not
  verify lease expiry caused by a long outage. That path is covered differently, by
  `test_database_connections_terminated_mid_run` and the worker's local-deadline code,
  but no outage longer than the lease has been tested end to end.
- An API restart mid-run is covered only by the full-stack restart in demo step 8 (all
  services stopped together); there is no separate API-only restart test. That is
  low-risk, since the API is stateless.
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
2. A test or demo for a PostgreSQL outage longer than the lease: stop postgres for
   15 s with a 6 s lease, and assert the worker abandons and the scheduler re-runs.
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
