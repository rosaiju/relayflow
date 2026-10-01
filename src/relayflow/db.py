"""Database engine construction.

RelayFlow uses synchronous SQLAlchemy Core over psycopg 3. Every engine operation
checks a connection out of the pool for exactly one short transaction, so no
connection or transaction is ever shared between threads.
"""

from __future__ import annotations

from sqlalchemy import Engine, create_engine


def make_engine(database_url: str, *, pool_size: int = 10) -> Engine:
    return create_engine(
        database_url,
        pool_size=pool_size,
        max_overflow=pool_size,
        pool_pre_ping=True,  # survive PostgreSQL restarts: stale pooled connections are replaced
        pool_recycle=300,
        connect_args={"connect_timeout": 5, "application_name": "relayflow"},
    )
