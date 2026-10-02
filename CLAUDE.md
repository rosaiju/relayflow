# RelayFlow — instructions for agents

Educational durable workflow engine in Python (FastAPI + PostgreSQL) with a React
dashboard. Not production software. Read `STATUS.md` first, then
`docs/architecture.md` (the specification; code must match it).

## Scope
- **Feature-frozen (2026-10-01).** Only bug fixes and verification changes.
- Orchestration is implemented here: no Celery/Temporal/Hatchet/Prefect or other engine.
- PostgreSQL is the only coordination mechanism. Never AnchorDB, never SQLite in tests.
- The API never executes tasks (no FastAPI BackgroundTasks for durable work).
- Only registered task types run; no shell commands or user-supplied code.
- Guarantee is at-least-once. Never claim exactly-once.
- Fault injection stays opt-in (`RELAYFLOW_ENABLE_FAULT_INJECTION=1`), tests/demos only.
- Dev services bind to 127.0.0.1. No auth, no public hosting, no real data.
- Keep separate from AnchorDB (`../anchordb`).

## Commands (from repo root; Windows Git Bash or PowerShell)
```
docker compose up -d --build            # full stack; dashboard http://127.0.0.1:8080
uv sync                                 # Python 3.13 env from uv.lock
uv run pytest -m "not integration"      # unit tests
uv run pytest                           # all tests (needs postgres on 127.0.0.1:5433)
uv run ruff format --check . ; uv run ruff check . ; uv run mypy
cd frontend; npm ci; npm run typecheck; npm run build; npx playwright test
uv run python scripts/demo_recovery.py  # automated crash-recovery demo (Docker)
RELAYFLOW_STACK_TESTS=1 uv run pytest tests/stack -v   # disposable project "relayflow-stacktest"
uv run python scripts/bench.py          # benchmarks (do not run alongside tests)
```
Never run `docker compose down -v` on the demo project unless the user asks: it deletes the
database volume. (The stack tests' `down -v` is scoped to `-p relayflow-stacktest` only.)

## Commit and push rules
- Commit each meaningful, verified change with a descriptive message, then push.
- `main` must stay coherent (tests/lint pass). Unfinished work goes on a branch.
- Review staged files for credentials, private data, databases, `node_modules`,
  build output, and binaries before committing.
- Never rewrite history, change commit dates, create empty commits, or change
  global git config. Repo-local author: `Rohan Sainju <rosaiju@users.noreply.github.com>`.
- The repository `rosaiju/relayflow` is **public** (made public 2026-10-01 at the user's request, after a
  full-history scan for secrets and personal data). Never commit secrets, `.env` files or local data.
- Record unverified or skipped checks honestly in `STATUS.md`.
