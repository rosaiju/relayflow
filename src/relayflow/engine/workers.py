"""Worker registry rows (for dashboard health only; ownership lives in attempts)."""

from __future__ import annotations

from sqlalchemy import Engine, text


def register_worker(
    engine: Engine, *, worker_id: str, name: str, hostname: str, pid: int, concurrency: int
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO workers (id, name, hostname, pid, concurrency, status) "
                "VALUES (:id, :name, :host, :pid, :c, 'active') "
                "ON CONFLICT (id) DO UPDATE SET status = 'active', last_heartbeat_at = now()"
            ),
            {"id": worker_id, "name": name, "host": hostname, "pid": pid, "c": concurrency},
        )


def worker_heartbeat(engine: Engine, *, worker_id: str, in_flight: int, status: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE workers SET last_heartbeat_at = now(), in_flight = :n, status = :s, "
                "stopped_at = CASE WHEN :s = 'stopped' THEN now() END WHERE id = :id"
            ),
            {"id": worker_id, "n": in_flight, "s": status},
        )
