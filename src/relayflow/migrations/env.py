"""Alembic environment. The database URL comes from the caller (relayflow.migrate)."""

from __future__ import annotations

from alembic import context
from sqlalchemy import create_engine

url = context.config.get_main_option("sqlalchemy.url")
if not url:
    raise RuntimeError("sqlalchemy.url must be set by relayflow.migrate")

connectable = create_engine(url)
with connectable.connect() as connection:
    context.configure(connection=connection, target_metadata=None, transaction_per_migration=True)
    with context.begin_transaction():
        context.run_migrations()
connectable.dispose()
