# RelayFlow status

_Last updated: 2026-10-01._ Read this first when resuming work.

## State: complete, feature-frozen

Feature development is frozen. Only bug fixes and verification changes should be made.

| # | Milestone | Commit |
|---|---|---|
| 1–2 | Spec (state machines, every transition), schema with DB-enforced invariants, engine, worker, scheduler, mock service, API, Compose stack, unit and integration tests | `c550a36` |
| 3 | Independent correctness suite (test agent); claim/cancel deadlock found and fixed; spec v1.1 | `159ba43` |
| 4 | React/TypeScript dashboard + Playwright e2e | `a4dad8c` |
| 5 | Crash-recovery demo, benchmarks, CI, documentation | `064da3b`, `40c6fff` |
| follow-ups | Stack tests for a PostgreSQL outage longer than the lease and an API-only restart; receiver gets its own database; dashboard proxy re-resolves the API; final verification in a disposable environment | `fa5803b`, `2e39c2e`, final closeout commit (see `git log`) |

Repository (private): https://github.com/rosaiju/relayflow, default branch `main`.

### Academic Advisor integration: isolated, not part of `main`

A separate session explored running Academic Advisor work through RelayFlow:
- In this repo it lives only on the **local** branch `integration/academic-advisor`
  (3 commits on top of `2e39c2e`). It has never been pushed and is not merged; `main`
  contains none of it.
- The Academic Advisor team repository (`C:\Users\rohan\advisor-ai`, remote
  `rosaiju/multimodal-academic-advisor`) has no commits since 2026-10-01, no relayflow
  branch locally or on its remote, and a clean working tree. That experiment left one
  local stash entry there, with its content kept in a separate clone that has no
  remote (`C:\Users\rohan\advisor-relayflow-lab\advisor-ai`).
- Nothing in that repository was modified or pushed by the RelayFlow closeout.

## Final verification (2026-10-01, closeout pass)

All of the following were run in this pass on Windows 11 + Docker Desktop (WSL2).
CI (GitHub Actions, ubuntu-latest) runs the same checks on every push to `main`.

| Check | Result |
|---|---|
| `ruff format --check`, `ruff check`, `mypy --strict` (34 source files) | pass |
| pytest (unit 38 + integration/correctness 112, real PostgreSQL 17; stack tests skipped unless enabled) | 150 passed, 2 skipped |
| Frontend `tsc -b` + `vite build` | pass |
| Playwright e2e, 5 tests (Chromium), against the demo stack with fault injection | 5 passed (again after the proxy fix) |
| `scripts/demo_recovery.py` (8 asserted steps: killed worker, crash after notification delivery, PostgreSQL restart mid-run, full restart keeping the volume) | passed |
| Stack test G40 `test_postgres_outage_longer_than_lease` (disposable project) | 3/3 consecutive passes, then 2/2 after the CI-found proxy fix |
| Stack test G41 `test_api_only_restart_while_workers_continue` (disposable project) | 3/3 consecutive passes after fixing the bug it found, then 2/2 after the CI-found proxy fix (below) |
| Demo database not reset | earliest run (2026-10-01 19:09:08 UTC) still present; the run count only grew (2390 → 2396, the runs added by e2e and the demo) |
| `scripts/bench.py` | **not rerun.** Nothing on the measured path changed (engine, worker, scheduler, benchmark script); BENCHMARKS.md results from 2026-10-01 stand |

**Stack tests run in a disposable environment.** `tests/stack` uses its own Compose
project (`relayflow-stacktest`, ports 15433/18000/18100/18080), created empty and
removed with its volumes afterwards. The demo stack (project `relayflow`, volumes
`relayflow_pgdata` and `relayflow_mocknotify-data`) is never touched. Enable with
`RELAYFLOW_STACK_TESTS=1`.

### Bugs found by verification (all fixed, no assertions weakened)
1. `doc.report` read a non-direct dependency's output. Found by the first live run.
2. Claim/cancel deadlock (`FOR UPDATE` vs the foreign-key `KEY SHARE` lock). Found by
   the independent test agent (D1 in `docs/spec-review.md`).
3. The worker heartbeat thread could overwrite the final `stopped` status. Found in
   code review.
4. The first benchmark measured the client's submission speed. Discarded and redesigned.
5. The mock receiver shared RelayFlow's PostgreSQL server, so an engine database
   outage also took the "external" service down. It now has its own server.
6. The dashboard proxy (nginx) resolved `api` once at startup, so recreating only the
   API container returned 502. Found while running the site.
7. **The dashboard's connection indicator stayed "Connected" during an API outage**
   behind the proxy: the proxy's 502 HTML page failed JSON parsing, and the previous
   state was kept. Found by G41 in this pass (the test failed at "no DOWN within 30s");
   fixed in `frontend/src/components/Layout.tsx`.
8. **Linux only, found by CI** (run 36944143147 failed both stack tests): with the API
   container stopped, requests through the dashboard proxy hung past 10 s, because
   nginx kept the stopped container's address and waited out its 60 s default connect
   timeout. Windows refuses the connection, so it passed locally. Fixed with
   `proxy_connect_timeout 2s`. The test now requires an answer within 5 s and accepts
   502 or 504 (both mean the proxy could not reach the API). The second CI failure in
   that run was a cascade: the next test submitted before the restarted API was ready.
   Each stack test now waits for full readiness. The rerun on `b40ebbd` passed.

The outage test's earlier flaky runs (3 of 13, before 2026-10-01's fixes) came from the
test harness: `compose up --wait` exiting during API recovery (confirmed), plus one
failure whose output was not captured (cause unconfirmed; most likely the
synchronization window with a graceful `stop`). The test now uses `kill` and readiness
polling. Since then it has passed every local run (5 earlier, 5 in this pass); its only
failure in CI was the cascade described in bug 8.

## Genuine remaining gaps

- **Process crash is not power loss.** All crash testing kills processes (worker
  containers, `os._exit`, SIGKILL of the PostgreSQL server process, backend
  termination); the OS and disk survive. Durability under power loss, torn writes or
  disk failure depends on PostgreSQL's WAL/fsync and hardware and is not tested.
- A graceful PostgreSQL shutdown lasting longer than the lease is not tested
  separately; only the SIGKILL outage is (G40).
- Execution is at-least-once by design. A duplicate external effect is prevented only
  by receivers that implement idempotency, as the mock service does.
- Timeouts and cancellation are cooperative, and dispatch uses polling.
- Verified environments: Windows 11 + Docker Desktop, and GitHub Actions ubuntu-latest.
  Benchmarks come from one laptop.
- Hard-killed workers stay `active` in the `workers` table; staleness is derived when
  the table is read.
- `fastapi.testclient` emits a Starlette deprecation warning about `httpx`; it is
  harmless at the pinned versions.

## Resuming

```powershell
cd C:\Users\rohan\projects\relayflow
docker compose up -d --build                     # dashboard http://127.0.0.1:8080
uv sync; uv run pytest                           # needs the compose postgres on 5433
$env:RELAYFLOW_STACK_TESTS="1"; uv run pytest tests/stack -v   # disposable project
```
Follow `CLAUDE.md` for the commit/push rules.
