"""Initial schema: definitions, runs, tasks, attempts, workers, events.

Invariants that must hold regardless of application bugs are enforced here with
constraints, partial unique indexes, and triggers (see docs/architecture.md s3).

Revision ID: 0001
Revises:
Create Date: 2026-10-01
"""

from __future__ import annotations

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


UPGRADE_SQL = r"""
CREATE TABLE workflow_definitions (
    id          bigserial PRIMARY KEY,
    name        text NOT NULL CHECK (name ~ '^[a-z0-9][a-z0-9-]{0,63}$'),
    version     integer NOT NULL CHECK (version BETWEEN 1 AND 10000),
    spec        jsonb NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_definition_name_version UNIQUE (name, version)
);

CREATE TABLE runs (
    id                   uuid PRIMARY KEY,
    workflow_name        text NOT NULL,
    workflow_version     integer NOT NULL,
    definition_id        bigint NOT NULL REFERENCES workflow_definitions(id),
    definition_snapshot  jsonb NOT NULL,
    input                jsonb NOT NULL,
    status               text NOT NULL CHECK (status IN
                           ('running','cancelling','succeeded','failed','cancelled')),
    idempotency_key      text UNIQUE CHECK (idempotency_key IS NULL
                           OR length(idempotency_key) BETWEEN 1 AND 200),
    request_hash         text NOT NULL,
    manual_retry_count   integer NOT NULL DEFAULT 0 CHECK (manual_retry_count >= 0),
    cancel_requested_at  timestamptz,
    error                text,
    created_at           timestamptz NOT NULL DEFAULT now(),
    updated_at           timestamptz NOT NULL DEFAULT now(),
    finished_at          timestamptz,
    CONSTRAINT ck_run_finished_iff_terminal CHECK (
        (status IN ('succeeded','failed','cancelled')) = (finished_at IS NOT NULL)),
    CONSTRAINT ck_run_cancelling_has_request CHECK (
        status <> 'cancelling' OR cancel_requested_at IS NOT NULL)
);
CREATE INDEX ix_runs_status_created ON runs (status, created_at DESC);
CREATE INDEX ix_runs_created ON runs (created_at DESC);

CREATE TABLE tasks (
    id                    uuid PRIMARY KEY,
    run_id                uuid NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    task_key              text NOT NULL,
    task_type             text NOT NULL,
    depends_on            text[] NOT NULL DEFAULT '{}',
    status                text NOT NULL CHECK (status IN
                            ('pending','queued','running','succeeded','failed','blocked',
                             'cancelled')),
    input                 jsonb,
    output                jsonb,
    max_attempts          integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 10),
    timeout_seconds       double precision NOT NULL CHECK (timeout_seconds > 0),
    backoff_base_seconds  double precision NOT NULL CHECK (backoff_base_seconds > 0),
    backoff_max_seconds   double precision NOT NULL
                            CHECK (backoff_max_seconds >= backoff_base_seconds),
    attempt_count         integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    failure_count         integer NOT NULL DEFAULT 0 CHECK (failure_count >= 0),
    available_at          timestamptz NOT NULL DEFAULT now(),
    current_attempt_id    uuid,
    last_error            text,
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now(),
    queued_at             timestamptz,
    finished_at           timestamptz,
    CONSTRAINT uq_task_run_key UNIQUE (run_id, task_key),
    CONSTRAINT ck_task_output_when_succeeded CHECK (status <> 'succeeded' OR output IS NOT NULL),
    CONSTRAINT ck_task_running_has_attempt CHECK (
        (status = 'running') = (current_attempt_id IS NOT NULL)),
    CONSTRAINT ck_task_input_when_dispatchable CHECK (
        status NOT IN ('queued','running','succeeded') OR input IS NOT NULL),
    CONSTRAINT ck_task_failures_within_budget CHECK (failure_count <= max_attempts)
);
CREATE INDEX ix_tasks_claimable ON tasks (available_at, created_at) WHERE status = 'queued';
CREATE INDEX ix_tasks_run ON tasks (run_id);

CREATE TABLE attempts (
    id                uuid PRIMARY KEY,
    task_id           uuid NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    run_id            uuid NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    attempt_number    integer NOT NULL CHECK (attempt_number >= 1),
    worker_id         text NOT NULL,
    lease_token       uuid NOT NULL UNIQUE,
    status            text NOT NULL CHECK (status IN
                        ('running','succeeded','failed','timed_out','lease_expired',
                         'cancelled','released')),
    started_at        timestamptz NOT NULL DEFAULT now(),
    heartbeat_at      timestamptz NOT NULL DEFAULT now(),
    lease_expires_at  timestamptz NOT NULL,
    finished_at       timestamptz,
    error             text,
    CONSTRAINT uq_attempt_number UNIQUE (task_id, attempt_number),
    CONSTRAINT ck_attempt_finished_iff_done CHECK (
        (status = 'running') = (finished_at IS NULL))
);
-- At most one running attempt per task, enforced by PostgreSQL.
CREATE UNIQUE INDEX uq_one_running_attempt_per_task ON attempts (task_id)
    WHERE status = 'running';
CREATE INDEX ix_attempts_running_lease ON attempts (lease_expires_at) WHERE status = 'running';
CREATE INDEX ix_attempts_run ON attempts (run_id);

ALTER TABLE tasks ADD CONSTRAINT fk_task_current_attempt
    FOREIGN KEY (current_attempt_id) REFERENCES attempts(id) DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE workers (
    id                 text PRIMARY KEY,
    name               text NOT NULL,
    hostname           text NOT NULL,
    pid                integer NOT NULL,
    concurrency        integer NOT NULL,
    status             text NOT NULL CHECK (status IN ('active','stopping','stopped')),
    in_flight          integer NOT NULL DEFAULT 0,
    started_at         timestamptz NOT NULL DEFAULT now(),
    last_heartbeat_at  timestamptz NOT NULL DEFAULT now(),
    stopped_at         timestamptz
);

CREATE TABLE events (
    id          bigserial PRIMARY KEY,
    run_id      uuid NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    task_key    text,
    attempt_id  uuid,
    worker_id   text,
    kind        text NOT NULL,
    message     text NOT NULL,
    data        jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_events_run ON events (run_id, id);

-- Immutability: definitions never change; a run's snapshot never changes.
CREATE FUNCTION relayflow_reject_definition_update() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'workflow_definitions rows are immutable';
END $$ LANGUAGE plpgsql;
CREATE TRIGGER trg_definitions_immutable BEFORE UPDATE ON workflow_definitions
    FOR EACH ROW EXECUTE FUNCTION relayflow_reject_definition_update();

CREATE FUNCTION relayflow_protect_run_snapshot() RETURNS trigger AS $$
BEGIN
    IF NEW.definition_snapshot IS DISTINCT FROM OLD.definition_snapshot
       OR NEW.input IS DISTINCT FROM OLD.input
       OR NEW.workflow_name IS DISTINCT FROM OLD.workflow_name
       OR NEW.workflow_version IS DISTINCT FROM OLD.workflow_version
       OR NEW.request_hash IS DISTINCT FROM OLD.request_hash
       OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key THEN
        RAISE EXCEPTION 'run definition snapshot, input and identity are immutable';
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;
CREATE TRIGGER trg_runs_snapshot_immutable BEFORE UPDATE ON runs
    FOR EACH ROW EXECUTE FUNCTION relayflow_protect_run_snapshot();

-- A succeeded task's output is never overwritten and a succeeded task never changes state.
CREATE FUNCTION relayflow_protect_succeeded_task() RETURNS trigger AS $$
BEGIN
    IF OLD.status = 'succeeded' AND (NEW.status <> 'succeeded'
                                     OR NEW.output IS DISTINCT FROM OLD.output) THEN
        RAISE EXCEPTION 'succeeded task % is final', OLD.id;
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;
CREATE TRIGGER trg_tasks_succeeded_final BEFORE UPDATE ON tasks
    FOR EACH ROW EXECUTE FUNCTION relayflow_protect_succeeded_task();

-- Attempt outcomes are final.
CREATE FUNCTION relayflow_protect_attempt() RETURNS trigger AS $$
BEGIN
    IF OLD.status <> 'running' AND NEW.status IS DISTINCT FROM OLD.status THEN
        RAISE EXCEPTION 'attempt % is already final (%)', OLD.id, OLD.status;
    END IF;
    IF NEW.lease_token IS DISTINCT FROM OLD.lease_token THEN
        RAISE EXCEPTION 'attempt lease_token is immutable';
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;
CREATE TRIGGER trg_attempts_final BEFORE UPDATE ON attempts
    FOR EACH ROW EXECUTE FUNCTION relayflow_protect_attempt();
"""

DOWNGRADE_SQL = r"""
DROP TABLE IF EXISTS events, workers CASCADE;
ALTER TABLE IF EXISTS tasks DROP CONSTRAINT IF EXISTS fk_task_current_attempt;
DROP TABLE IF EXISTS attempts, tasks, runs, workflow_definitions CASCADE;
DROP FUNCTION IF EXISTS relayflow_reject_definition_update, relayflow_protect_run_snapshot,
    relayflow_protect_succeeded_task, relayflow_protect_attempt;
"""


def upgrade() -> None:
    op.execute(UPGRADE_SQL)


def downgrade() -> None:
    op.execute(DOWNGRADE_SQL)
