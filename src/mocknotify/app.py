"""Mock notification service: an independent receiver that implements idempotency.

It owns its own database and deduplicates with one atomic statement:

    INSERT ... ON CONFLICT (idempotency_key)
    DO UPDATE SET delivery_count = delivery_count + 1 WHERE request_hash matches

* new key              -> row inserted, delivery_count = 1        -> 201
* same key, same body  -> counter bumped, existing record returned -> 200 duplicate
* same key, other body -> the WHERE fails, no row returned         -> 409

The unique constraint makes this safe under concurrent requests. RelayFlow's
at-least-once delivery only avoids duplicate *logical* notifications because this
receiver implements idempotency; a receiver without it would record duplicates.
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import Body, FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy import Engine, create_engine, text

DEFAULT_DATABASE_URL = "postgresql+psycopg://relayflow:relayflow@127.0.0.1:5433/mocknotify"
MAX_BODY_BYTES = 16 * 1024

SCHEMA = """
CREATE TABLE IF NOT EXISTS notifications (
    id                uuid PRIMARY KEY,
    idempotency_key   text NOT NULL UNIQUE CHECK (length(idempotency_key) BETWEEN 1 AND 200),
    request_hash      text NOT NULL,
    payload           jsonb NOT NULL,
    delivery_count    integer NOT NULL DEFAULT 1 CHECK (delivery_count >= 1),
    created_at        timestamptz NOT NULL DEFAULT now(),
    last_delivered_at timestamptz NOT NULL DEFAULT now()
)
"""

UPSERT = """
INSERT INTO notifications (id, idempotency_key, request_hash, payload)
VALUES (:id, :key, :hash, CAST(:payload AS jsonb))
ON CONFLICT (idempotency_key) DO UPDATE
    SET delivery_count = notifications.delivery_count + 1, last_delivered_at = now()
    WHERE notifications.request_hash = EXCLUDED.request_hash
RETURNING id, idempotency_key, payload, delivery_count, created_at, last_delivered_at
"""


def _record(row: Any) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "idempotency_key": row.idempotency_key,
        "payload": row.payload,
        "delivery_count": row.delivery_count,
        "created_at": row.created_at.isoformat(),
        "last_delivered_at": row.last_delivered_at.isoformat(),
    }


def create_app(database_url: str | None = None) -> FastAPI:
    url = database_url or os.environ.get("MOCKNOTIFY_DATABASE_URL", DEFAULT_DATABASE_URL)
    engine: Engine = create_engine(url, pool_pre_ping=True, pool_size=5)
    allow_reset = os.environ.get("MOCKNOTIFY_ALLOW_RESET") == "1"

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        with engine.begin() as conn:
            conn.execute(text("SELECT pg_advisory_xact_lock(8100)"))  # concurrent startups
            conn.execute(text(SCHEMA))
        yield
        engine.dispose()

    app = FastAPI(title="RelayFlow mock notification service", lifespan=lifespan)

    @app.get("/health")
    def health() -> dict[str, str]:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ok"}

    @app.post("/notifications")
    def notify(
        payload: Annotated[dict[str, Any], Body()],
        idempotency_key: Annotated[str | None, Header()] = None,
    ) -> JSONResponse:
        if not idempotency_key or len(idempotency_key) > 200:
            raise HTTPException(400, "Idempotency-Key header (1..200 chars) is required")
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        if len(encoded.encode()) > MAX_BODY_BYTES:
            raise HTTPException(413, "payload too large")
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        with engine.begin() as conn:
            row = conn.execute(
                text(UPSERT),
                {"id": uuid.uuid4(), "key": idempotency_key, "hash": digest, "payload": encoded},
            ).first()
        if row is None:
            raise HTTPException(409, "Idempotency-Key was already used with a different payload")
        duplicate = row.delivery_count > 1
        return JSONResponse(
            status_code=200 if duplicate else 201, content={**_record(row), "duplicate": duplicate}
        )

    @app.get("/notifications")
    def list_notifications(limit: int = 100) -> dict[str, Any]:
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT * FROM notifications ORDER BY created_at DESC LIMIT :limit"),
                {"limit": min(max(limit, 1), 500)},
            ).all()
        return {"items": [_record(r) for r in rows]}

    @app.delete("/notifications")
    def reset() -> dict[str, str]:
        if not allow_reset:
            raise HTTPException(403, "reset disabled (set MOCKNOTIFY_ALLOW_RESET=1 for tests)")
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE notifications"))
        return {"status": "reset"}

    return app
