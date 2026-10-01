"""Worker process: claim, execute, heartbeat, report. See docs/architecture.md section 7.

Threads in one worker process:
  * main thread  - claim loop; claims only while a slot is free (bounded concurrency)
  * slot threads - one per in-flight task (ThreadPoolExecutor, `concurrency` threads)
  * heartbeat    - renews leases for all in-flight attempts, relays cancellation,
                   enforces timeouts, and detects lost ownership
Each database call checks out its own pooled connection for one short transaction.
No transaction is open while a handler runs.
"""

from __future__ import annotations

import logging
import os
import signal
import socket
import threading
import time
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import Engine
from sqlalchemy.exc import DBAPIError

from relayflow import faults
from relayflow.config import Settings
from relayflow.engine import (
    ClaimedTask,
    cancel_attempt,
    claim_task,
    complete_attempt,
    fail_attempt,
    heartbeat,
    release_attempt,
)
from relayflow.engine.errors import ValidationFailed
from relayflow.engine.workers import register_worker, worker_heartbeat
from relayflow.tasks.registry import (
    PermanentTaskError,
    TaskCancelled,
    TaskContext,
    TaskTimedOut,
    get_handler,
)

log = logging.getLogger("relayflow.worker")


@dataclass
class InFlight:
    claimed: ClaimedTask
    ctx: TaskContext
    # Monotonic time at which the last successful lease renewal *started*. Because PostgreSQL
    # computed the lease from its own now() *after* this instant, this is a conservative base.
    renewed_at: float
    reported: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock)

    def take_report(self) -> bool:
        """Exactly one party (handler thread, timeout watchdog, shutdown) may report."""
        with self.lock:
            if self.reported:
                return False
            self.reported = True
            return True


class Worker:
    def __init__(self, settings: Settings, engine: Engine) -> None:
        self.settings = settings
        self.engine = engine
        self.name = settings.worker_name
        self.id = f"{self.name}:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self.stop_event = threading.Event()
        self.releasing = threading.Event()
        self._inflight: dict[uuid.UUID, InFlight] = {}
        self._lock = threading.Lock()
        self._pool = ThreadPoolExecutor(
            max_workers=settings.worker_concurrency, thread_name_prefix="slot"
        )
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, name="heartbeat", daemon=True
        )
        # Local fail-safe margin: give up ownership a little before the database would.
        self._margin = min(settings.heartbeat_seconds, settings.lease_seconds * 0.25)

    # ------------------------------------------------------------------ lifecycle

    def in_flight(self) -> int:
        with self._lock:
            return len(self._inflight)

    def run(self) -> None:
        self._retry_db(
            "register worker",
            lambda: register_worker(
                self.engine,
                worker_id=self.id,
                name=self.name,
                hostname=socket.gethostname(),
                pid=os.getpid(),
                concurrency=self.settings.worker_concurrency,
            ),
        )
        log.info(
            "worker %s started (concurrency=%d lease=%.1fs heartbeat=%.1fs)",
            self.id,
            self.settings.worker_concurrency,
            self.settings.lease_seconds,
            self.settings.heartbeat_seconds,
        )
        self._heartbeat_thread.start()
        try:
            self._claim_loop()
        finally:
            self._shutdown()

    def request_stop(self, *_: Any) -> None:
        if not self.stop_event.is_set():
            log.info(
                "stop requested; finishing in-flight work (grace %.1fs)",
                self.settings.shutdown_grace_seconds,
            )
        self.stop_event.set()

    def _retry_db(self, what: str, fn: Callable[[], Any]) -> Any:
        delay = 0.5
        while not self.stop_event.is_set():
            try:
                return fn()
            except DBAPIError as exc:
                log.warning(
                    "%s failed (database unavailable?): %s; retrying in %.1fs",
                    what,
                    type(exc.orig).__name__ if exc.orig else exc,
                    delay,
                )
                self.stop_event.wait(delay)
                delay = min(delay * 2, 5.0)
        return None

    # ------------------------------------------------------------------ claiming

    def _claim_loop(self) -> None:
        backoff = self.settings.poll_interval_seconds
        while not self.stop_event.is_set():
            if self.in_flight() >= self.settings.worker_concurrency:
                self.stop_event.wait(0.02)  # all slots busy: bounded in-flight work
                continue
            started = time.monotonic()
            try:
                claimed = claim_task(
                    self.engine, worker_id=self.id, lease_seconds=self.settings.lease_seconds
                )
            except DBAPIError as exc:
                log.warning(
                    "claim failed (database unavailable?): %s; backing off %.1fs",
                    type(exc.orig).__name__ if exc.orig else exc,
                    backoff,
                )
                self.stop_event.wait(backoff)
                backoff = min(backoff * 2, 5.0)
                continue
            backoff = self.settings.poll_interval_seconds
            if claimed is None:
                self.stop_event.wait(self.settings.poll_interval_seconds)
                continue
            self._start(claimed, started)

    def _start(self, claimed: ClaimedTask, renewed_at: float) -> None:
        ctx = TaskContext(
            run_id=claimed.run_id,
            task_key=claimed.task_key,
            attempt_number=claimed.attempt_number,
            deadline=renewed_at + claimed.timeout_seconds,
            notify_url=self.settings.notify_url,
            fault_injection_enabled=self.settings.enable_fault_injection,
        )
        entry = InFlight(claimed=claimed, ctx=ctx, renewed_at=renewed_at)
        with self._lock:
            self._inflight[claimed.attempt_id] = entry
        log.info(
            "claimed %s/%s attempt %d (%s)",
            claimed.run_id,
            claimed.task_key,
            claimed.attempt_number,
            claimed.task_type,
        )
        self._pool.submit(self._execute, entry)

    # ------------------------------------------------------------------ execution

    def _execute(self, entry: InFlight) -> None:
        claimed, ctx = entry.claimed, entry.ctx
        try:
            try:
                handler = get_handler(claimed.task_type)
                faults.before_handler(ctx, claimed.input)
                output = handler(ctx, claimed.input)
            except TaskTimedOut as exc:
                self._fail(entry, "timed out", str(exc), kind="timed_out")
            except TaskCancelled as exc:
                self._report_stopped(entry, str(exc))
            except PermanentTaskError as exc:
                self._fail(
                    entry, "failed permanently", f"PermanentTaskError: {exc}", retryable=False
                )
            except Exception as exc:
                log.debug("handler error", exc_info=True)
                self._fail(entry, "failed", f"{type(exc).__name__}: {exc}")
            else:
                faults.crash_after_handler(ctx, claimed.input)  # no-op unless injected
                self._report_success(entry, output)
        finally:
            with self._lock:
                self._inflight.pop(claimed.attempt_id, None)

    def _fail(
        self,
        entry: InFlight,
        outcome: str,
        error: str,
        *,
        retryable: bool = True,
        kind: str = "failed",
    ) -> None:
        claimed = entry.claimed
        self._report(
            entry,
            outcome,
            lambda: fail_attempt(
                self.engine,
                attempt_id=claimed.attempt_id,
                lease_token=claimed.lease_token,
                error=error,
                retryable=retryable,
                kind=kind,
            ),
        )

    def _report_success(self, entry: InFlight, output: Any) -> None:
        claimed = entry.claimed
        if entry.ctx.lost_event.is_set():
            # Spec 7.3: once ownership is presumed lost, discard the result. The scheduler
            # re-queues the task; at-least-once means it may run again.
            log.warning(
                "discarding result of %s/%s attempt %d: lease ownership lost",
                claimed.run_id,
                claimed.task_key,
                claimed.attempt_number,
            )
            entry.take_report()
            return
        if not isinstance(output, dict):
            output = {"value": output}
        try:
            self._report(
                entry,
                "succeeded",
                lambda: complete_attempt(
                    self.engine,
                    attempt_id=claimed.attempt_id,
                    lease_token=claimed.lease_token,
                    output=output,
                ),
            )
        except ValidationFailed as exc:  # e.g. oversized output: the handler's fault
            entry.reported = False
            self._fail(entry, "failed (invalid output)", str(exc), retryable=False)

    def _release(self, entry: InFlight, outcome: str) -> None:
        claimed = entry.claimed
        self._report(
            entry,
            outcome,
            lambda: release_attempt(
                self.engine, attempt_id=claimed.attempt_id, lease_token=claimed.lease_token
            ),
        )

    def _report_stopped(self, entry: InFlight, reason: str) -> None:
        claimed, ctx = entry.claimed, entry.ctx
        if ctx.lost_event.is_set():
            log.warning(
                "abandoning %s/%s attempt %d: lease ownership lost",
                claimed.run_id,
                claimed.task_key,
                claimed.attempt_number,
            )
            entry.take_report()
            return
        if self.releasing.is_set():
            self._release(entry, "released")
        else:
            self._report(
                entry,
                f"cancelled ({reason})",
                lambda: cancel_attempt(
                    self.engine, attempt_id=claimed.attempt_id, lease_token=claimed.lease_token
                ),
            )

    def _report(self, entry: InFlight, outcome: str, call: Callable[[], bool]) -> None:
        """Send one report. Retries through database outages until the local lease deadline;
        after that the database will consider the lease expired anyway, so give up."""
        claimed = entry.claimed
        if not entry.take_report():
            return
        delay = 0.2
        while True:
            try:
                accepted = call()
                break
            except DBAPIError as exc:
                if time.monotonic() > entry.renewed_at + self.settings.lease_seconds - self._margin:
                    log.warning(
                        "could not report %s/%s (%s) before the lease lapsed; the "
                        "scheduler will recover it",
                        claimed.run_id,
                        claimed.task_key,
                        exc,
                    )
                    return
                time.sleep(delay)
                delay = min(delay * 2, 2.0)
        if accepted:
            log.info(
                "%s/%s attempt %d %s",
                claimed.run_id,
                claimed.task_key,
                claimed.attempt_number,
                outcome,
            )
        else:
            log.warning(
                "stale report rejected for %s/%s attempt %d (%s): ownership expired or changed",
                claimed.run_id,
                claimed.task_key,
                claimed.attempt_number,
                outcome,
            )

    # ------------------------------------------------------------------ heartbeat

    def _heartbeat_loop(self) -> None:
        interval = self.settings.heartbeat_seconds
        while True:
            self._heartbeat_once()
            if self.stop_event.is_set() and self.in_flight() == 0:
                return
            time.sleep(interval)

    def _heartbeat_once(self) -> None:
        now = time.monotonic()
        with self._lock:
            entries = [e for e in self._inflight.values() if not e.reported]
        # Cooperative timeouts: past the deadline, flag the handler and report timed_out.
        for entry in entries:
            if now >= entry.ctx.deadline and not entry.ctx.timeout_event.is_set():
                entry.ctx.timeout_event.set()
                claimed = entry.claimed
                timeout_error = f"timed out after {claimed.timeout_seconds:g}s (worker watchdog)"
                threading.Thread(
                    target=self._fail,
                    daemon=True,
                    args=(entry, "timed out (watchdog)", timeout_error),
                    kwargs={"kind": "timed_out"},
                ).start()
        live = [
            e for e in entries if not e.ctx.timeout_event.is_set() and not e.ctx.lost_event.is_set()
        ]
        try:
            results = heartbeat(
                self.engine,
                [(e.claimed.attempt_id, e.claimed.lease_token) for e in live],
                lease_seconds=self.settings.lease_seconds,
            )
            status = "stopping" if self.stop_event.is_set() else "active"
            worker_heartbeat(
                self.engine, worker_id=self.id, in_flight=self.in_flight(), status=status
            )
        except DBAPIError as exc:
            log.warning(
                "heartbeat failed (database unavailable?): %s",
                type(exc.orig).__name__ if exc.orig else exc,
            )
            for entry in live:
                if time.monotonic() > entry.renewed_at + self.settings.lease_seconds - self._margin:
                    # Conservative: assume the database already expired our lease.
                    log.warning(
                        "lease for %s/%s presumed lost; stopping the handler",
                        entry.claimed.run_id,
                        entry.claimed.task_key,
                    )
                    entry.ctx.lost_event.set()
            return
        for entry in live:
            result = results.get(entry.claimed.attempt_id)
            if result is None:
                continue
            if not result.owned:
                log.warning(
                    "lease for %s/%s attempt %d no longer owned; stopping the handler",
                    entry.claimed.run_id,
                    entry.claimed.task_key,
                    entry.claimed.attempt_number,
                )
                entry.ctx.lost_event.set()
                continue
            entry.renewed_at = now
            if result.cancel_requested and not entry.ctx.cancel_event.is_set():
                log.info(
                    "cancellation requested for %s/%s", entry.claimed.run_id, entry.claimed.task_key
                )
                entry.ctx.cancel_event.set()

    # ------------------------------------------------------------------ shutdown

    def _shutdown(self) -> None:
        deadline = time.monotonic() + self.settings.shutdown_grace_seconds
        while self.in_flight() and time.monotonic() < deadline:
            time.sleep(0.1)  # heartbeats continue while in-flight work finishes
        if self.in_flight():
            log.info("grace period over; releasing %d in-flight attempt(s)", self.in_flight())
            self.releasing.set()
            with self._lock:
                entries = list(self._inflight.values())
            for entry in entries:
                entry.ctx.cancel_event.set()
            release_deadline = time.monotonic() + 2.0
            while self.in_flight() and time.monotonic() < release_deadline:
                time.sleep(0.05)
            with self._lock:
                stuck = [e for e in self._inflight.values() if not e.reported]
            for entry in stuck:  # handler ignored cancellation: release on its behalf
                self._release(entry, "released (handler did not stop)")
        self._pool.shutdown(wait=False, cancel_futures=True)
        try:
            worker_heartbeat(self.engine, worker_id=self.id, in_flight=0, status="stopped")
        except DBAPIError:
            log.warning("could not mark worker stopped (database unavailable)")
        log.info("worker %s stopped", self.id)


def install_signal_handlers(worker: Worker) -> None:
    signal.signal(signal.SIGINT, worker.request_stop)
    signal.signal(signal.SIGTERM, worker.request_stop)
    if hasattr(signal, "SIGBREAK"):  # Windows: CTRL_BREAK_EVENT
        signal.signal(signal.SIGBREAK, worker.request_stop)
