"""Local fixtures for the correctness suite. Shared fixtures come from tests/conftest.py
and are not overridden here."""

from __future__ import annotations

import contextlib
import subprocess
from collections.abc import Iterator

import pytest
from sqlalchemy import Engine, create_engine, text

from relayflow.testing import check_invariants


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[None]):  # type: ignore[no-untyped-def]
    report = yield
    if report.when == "call" and report.failed:
        item.stash[_CALL_FAILED] = True
    return report


_CALL_FAILED = pytest.StashKey[bool]()


@pytest.fixture(autouse=True)
def _invariants_hold_after_test(request: pytest.FixtureRequest) -> Iterator[None]:
    """Safety net: every test that touched the database must leave all invariants intact."""
    if "engine" not in request.fixturenames:
        yield
        return
    engine: Engine = request.getfixturevalue("engine")  # set up first => torn down after us
    yield
    if request.node.stash.get(_CALL_FAILED, False):  # the test already reported its failure
        return
    problems = check_invariants(engine)
    assert problems == [], f"invariants violated after test: {problems}"


@pytest.fixture
def admin_engine(db_url: str, engine: Engine) -> Iterator[Engine]:
    """A separate engine (own application_name) for raw SQL / pg_terminate_backend."""
    eng = create_engine(
        db_url, pool_size=2, connect_args={"application_name": "relayflow-test-admin"}
    )
    yield eng
    eng.dispose()


@pytest.fixture
def processes() -> Iterator[list[subprocess.Popen[bytes]]]:
    """Collects child processes and always kills them at teardown."""
    procs: list[subprocess.Popen[bytes]] = []
    yield procs
    for p in procs:
        if p.poll() is None:
            p.kill()
    for p in procs:
        with contextlib.suppress(subprocess.TimeoutExpired):
            p.wait(timeout=15)


@pytest.fixture
def mock_db_url(db_url: str) -> str:
    """A dedicated mocknotify database for this suite (created if missing)."""
    base, _, _ = db_url.rpartition("/")
    name = "mocknotify_test_agent2"
    admin = create_engine(f"{base}/postgres", isolation_level="AUTOCOMMIT")
    try:
        with admin.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :n"), {"n": name}
            ).first()
            if not exists:
                conn.execute(text(f'CREATE DATABASE "{name}"'))
    finally:
        admin.dispose()
    return f"{base}/{name}"
