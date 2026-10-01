"""Apply migrations and seed built-in workflow definitions.

Usage: python -m relayflow.migrate   (uses RELAYFLOW_DATABASE_URL)
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import OperationalError

from relayflow.catalog import BUILTIN_WORKFLOWS
from relayflow.config import get_settings
from relayflow.db import make_engine
from relayflow.engine import register_workflow

log = logging.getLogger("relayflow.migrate")
MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def alembic_config(database_url: str) -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", str(MIGRATIONS_DIR))
    cfg.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
    return cfg


def wait_for_database(database_url: str, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    probe = create_engine(database_url, pool_pre_ping=True)
    try:
        while True:
            try:
                with probe.connect() as conn:
                    conn.execute(text("SELECT 1"))
                return
            except OperationalError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(1)
    finally:
        probe.dispose()


def upgrade(database_url: str) -> None:
    command.upgrade(alembic_config(database_url), "head")


def seed(engine: Engine) -> None:
    for spec in BUILTIN_WORKFLOWS:
        result = register_workflow(engine, spec)
        if result.created:
            log.info("registered %s v%s", spec["name"], spec["version"])


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    settings = get_settings()
    wait_for_database(settings.database_url)
    upgrade(settings.database_url)
    engine = make_engine(settings.database_url, pool_size=2)
    seed(engine)
    engine.dispose()
    log.info("database ready")
    return 0


if __name__ == "__main__":
    sys.exit(main())
