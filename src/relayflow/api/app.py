"""REST API. It records intent and reads state; it never executes workflow tasks.

Handlers are synchronous `def` functions: FastAPI runs them in a threadpool, and
each one checks out its own pooled connection inside an engine function.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import Engine, text
from sqlalchemy.exc import DBAPIError

from relayflow import __version__
from relayflow.config import Settings, get_settings
from relayflow.db import make_engine
from relayflow.engine import ops, queries
from relayflow.engine.errors import (
    DefinitionConflict,
    IdempotencyConflict,
    InvalidTransition,
    RelayFlowError,
    RunNotFound,
    ValidationFailed,
    WorkflowNotFound,
)
from relayflow.states import RunStatus

log = logging.getLogger("relayflow.api")

STATUS_FOR_ERROR: dict[type[RelayFlowError], int] = {
    ValidationFailed: 422,
    WorkflowNotFound: 404,
    RunNotFound: 404,
    IdempotencyConflict: 409,
    DefinitionConflict: 409,
    InvalidTransition: 409,
}
MAX_REQUEST_BYTES = 128 * 1024


class SubmitRunRequest(BaseModel):
    workflow_name: str = Field(min_length=1, max_length=64)
    workflow_version: int | None = Field(default=None, ge=1)
    input: dict[str, Any]
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=200)


def create_app(settings: Settings | None = None, engine: Engine | None = None) -> FastAPI:
    settings = settings or get_settings()
    owns_engine = engine is None
    db = (
        engine
        if engine is not None
        else make_engine(settings.database_url, pool_size=settings.db_pool_size)
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield
        if owns_engine:
            db.dispose()

    app = FastAPI(title="RelayFlow", version=__version__, lifespan=lifespan)
    # The dashboard is served from the same origin via a proxy; CORS only matters for the Vite
    # dev server, which runs on localhost.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type", "Idempotency-Key"],
    )

    @app.middleware("http")
    async def limit_body_size(request: Request, call_next: Any) -> Response:
        length = request.headers.get("content-length")
        if length is not None and length.isdigit() and int(length) > MAX_REQUEST_BYTES:
            return JSONResponse(
                status_code=413,
                content={
                    "detail": {
                        "code": "payload_too_large",
                        "message": f"request exceeds {MAX_REQUEST_BYTES} bytes",
                    }
                },
            )
        response: Response = await call_next(request)
        return response

    @app.exception_handler(RelayFlowError)
    async def domain_error(_: Request, exc: RelayFlowError) -> JSONResponse:
        status = STATUS_FOR_ERROR.get(type(exc), 400)
        detail: dict[str, Any] = {"code": exc.code, "message": str(exc)}
        if isinstance(exc, ValidationFailed):
            detail["errors"] = exc.errors
        return JSONResponse(status_code=status, content={"detail": detail})

    @app.exception_handler(DBAPIError)
    async def database_error(_: Request, exc: DBAPIError) -> JSONResponse:
        log.warning("database error: %s", exc)
        return JSONResponse(
            status_code=503,
            content={
                "detail": {
                    "code": "database_unavailable",
                    "message": "database unavailable; try again",
                }
            },
        )

    api = APIRouter(prefix="/api")

    @api.get("/health")
    def health() -> JSONResponse:
        try:
            with db.connect() as conn:
                conn.execute(text("SELECT 1"))
        except DBAPIError:
            return JSONResponse(
                status_code=503, content={"status": "degraded", "database": "unreachable"}
            )
        return JSONResponse(
            {
                "status": "ok",
                "database": "ok",
                "version": __version__,
                "fault_injection": settings.enable_fault_injection,
            }
        )

    @api.get("/workflows")
    def list_workflows() -> list[dict[str, Any]]:
        return queries.list_workflows(db)

    @api.post("/workflows")
    def register_workflow(spec: dict[str, Any], response: Response) -> dict[str, Any]:
        result = ops.register_workflow(db, spec)
        response.status_code = 201 if result.created else 200
        return {
            **queries.get_workflow(db, spec["name"], spec["version"]),
            "created": result.created,
        }

    @api.get("/workflows/{name}/versions/{version}")
    def get_workflow(name: str, version: int) -> dict[str, Any]:
        return queries.get_workflow(db, name, version)

    @api.post("/runs")
    def submit_run(
        body: SubmitRunRequest,
        response: Response,
        idempotency_key: Annotated[str | None, Header(max_length=200)] = None,
    ) -> dict[str, Any]:
        key = body.idempotency_key or idempotency_key
        if body.idempotency_key and idempotency_key and body.idempotency_key != idempotency_key:
            raise ValidationFailed(["idempotency key in header and body differ"])
        result = ops.submit_run(
            db,
            workflow_name=body.workflow_name,
            workflow_version=body.workflow_version,
            input=body.input,
            idempotency_key=key,
            allow_fault_injection=settings.enable_fault_injection,
        )
        response.status_code = 201 if result.created else 200
        return {
            "run_id": str(result.run_id),
            "created": result.created,
            "run": queries.get_run(db, result.run_id),
        }

    @api.get("/runs")
    def list_runs(
        status: Annotated[RunStatus | None, Query()] = None,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> dict[str, Any]:
        return queries.list_runs(
            db, status=status.value if status else None, limit=limit, offset=offset
        )

    @api.get("/runs/{run_id}")
    def get_run(run_id: UUID) -> dict[str, Any]:
        return queries.get_run(db, run_id)

    @api.get("/runs/{run_id}/events")
    def run_events(run_id: UUID, after: Annotated[int, Query(ge=0)] = 0) -> list[dict[str, Any]]:
        return queries.list_events(db, run_id, after_id=after)

    @api.post("/runs/{run_id}/cancel", status_code=202)
    def cancel_run(run_id: UUID) -> dict[str, Any]:
        status = ops.request_cancel(db, run_id)
        return {"run_id": str(run_id), "status": status}

    @api.post("/runs/{run_id}/retry")
    def retry_run(run_id: UUID) -> dict[str, Any]:
        status = ops.retry_run(db, run_id)
        return {"run_id": str(run_id), "status": status}

    @api.get("/workers")
    def list_workers() -> list[dict[str, Any]]:
        return queries.list_workers(db, lease_seconds=settings.lease_seconds)

    @api.get("/overview")
    def overview() -> dict[str, Any]:
        return {
            **queries.overview(db, lease_seconds=settings.lease_seconds),
            "settings": {
                "lease_seconds": settings.lease_seconds,
                "heartbeat_seconds": settings.heartbeat_seconds,
                "poll_interval_seconds": settings.poll_interval_seconds,
                "fault_injection": settings.enable_fault_injection,
            },
        }

    @api.get("/{rest:path}", include_in_schema=False)
    def not_found(rest: str) -> None:
        raise HTTPException(404, {"code": "not_found", "message": f"/api/{rest} not found"})

    app.include_router(api)
    return app
