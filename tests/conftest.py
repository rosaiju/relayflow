"""Shared fixtures. Integration tests use a real PostgreSQL database (never SQLite)."""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest
from sqlalchemy import Engine, create_engine, text

from relayflow.config import Settings
from relayflow.db import make_engine
from relayflow.migrate import seed, upgrade

DEFAULT_TEST_DB = "postgresql+psycopg://relayflow:relayflow@127.0.0.1:5433/relayflow_test"
TABLES = "events, attempts, tasks, runs, workers, workflow_definitions"


@pytest.fixture(scope="session")
def db_url() -> str:
    return os.environ.get("RELAYFLOW_TEST_DATABASE_URL", DEFAULT_TEST_DB)


@pytest.fixture(scope="session")
def _migrated(db_url: str) -> str:
    base, _, name = db_url.rpartition("/")
    admin = create_engine(f"{base}/postgres", isolation_level="AUTOCOMMIT")
    try:
        with admin.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :n"), {"n": name}
            ).first()
            if not exists:
                conn.execute(text(f'CREATE DATABASE "{name}"'))
    except Exception as exc:  # pragma: no cover - environment problem, not a test failure
        pytest.skip(f"PostgreSQL not reachable at {base}: {exc}")
    finally:
        admin.dispose()
    upgrade(db_url)
    return db_url


@pytest.fixture
def engine(_migrated: str) -> Iterator[Engine]:
    eng = make_engine(_migrated, pool_size=10)
    with eng.begin() as conn:
        conn.execute(text(f"TRUNCATE {TABLES} RESTART IDENTITY CASCADE"))
    seed(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def settings(db_url: str) -> Settings:
    return Settings(
        database_url=db_url,
        lease_seconds=2,
        heartbeat_seconds=0.5,
        poll_interval_seconds=0.1,
        scheduler_interval_seconds=0.2,
        timeout_grace_seconds=1,
        shutdown_grace_seconds=3,
    )


@pytest.fixture
def api_client(engine: Engine, settings: Settings):  # type: ignore[no-untyped-def]
    from fastapi.testclient import TestClient

    from relayflow.api.app import create_app

    app = create_app(settings=settings, engine=engine)
    with TestClient(app) as client:
        yield client


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Anything that touches the database is an integration test."""
    for item in items:
        fixtures = getattr(item, "fixturenames", ())
        if {"engine", "api_client", "db_url", "_migrated"} & set(fixtures):
            item.add_marker(pytest.mark.integration)
