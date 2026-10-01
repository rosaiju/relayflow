"""Scheduler / recovery process. See docs/architecture.md section 6.6.

It never executes tasks. Each pass expires lapsed leases and enforces the timeout
backstop; every recovery is its own short transaction that re-checks the
condition under the run lock, so several schedulers can run at once.
"""

from __future__ import annotations

import logging
import signal
import threading
from typing import Any

from sqlalchemy import Engine
from sqlalchemy.exc import DBAPIError

from relayflow.config import Settings
from relayflow.engine import recover_expired

log = logging.getLogger("relayflow.scheduler")


class Scheduler:
    def __init__(self, settings: Settings, engine: Engine) -> None:
        self.settings = settings
        self.engine = engine
        self.stop_event = threading.Event()

    def request_stop(self, *_: Any) -> None:
        self.stop_event.set()

    def run_once(self) -> None:
        result = recover_expired(
            self.engine, timeout_grace_seconds=self.settings.timeout_grace_seconds
        )
        if result.expired or result.timed_out:
            log.info(
                "recovered %d expired lease(s), %d timed-out attempt(s)",
                result.expired,
                result.timed_out,
            )

    def run(self) -> None:
        log.info("scheduler started (interval %.2fs)", self.settings.scheduler_interval_seconds)
        backoff = self.settings.scheduler_interval_seconds
        while not self.stop_event.is_set():
            try:
                self.run_once()
                backoff = self.settings.scheduler_interval_seconds
            except DBAPIError as exc:
                log.warning(
                    "recovery pass failed (database unavailable?): %s; retrying in %.1fs",
                    type(exc.orig).__name__ if exc.orig else exc,
                    backoff,
                )
                backoff = min(backoff * 2, 5.0)
            self.stop_event.wait(backoff)
        log.info("scheduler stopped")


def install_signal_handlers(scheduler: Scheduler) -> None:
    signal.signal(signal.SIGINT, scheduler.request_stop)
    signal.signal(signal.SIGTERM, scheduler.request_stop)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, scheduler.request_stop)
