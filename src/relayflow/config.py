"""Runtime configuration, read from RELAYFLOW_* environment variables.

See docs/architecture.md section 11 for the meaning of each setting.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_DATABASE_URL = "postgresql+psycopg://relayflow:relayflow@127.0.0.1:5433/relayflow"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RELAYFLOW_", extra="ignore")

    database_url: str = DEFAULT_DATABASE_URL
    lease_seconds: float = 10.0
    heartbeat_seconds: float = 3.0
    poll_interval_seconds: float = 0.5
    worker_concurrency: int = 4
    worker_name: str = "worker"
    scheduler_interval_seconds: float = 1.0
    timeout_grace_seconds: float = 5.0
    shutdown_grace_seconds: float = 10.0
    notify_url: str = "http://127.0.0.1:8100"
    enable_fault_injection: bool = False
    db_pool_size: int = 10

    @model_validator(mode="after")
    def _check_timings(self) -> Settings:
        if self.lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        if self.heartbeat_seconds * 2 >= self.lease_seconds:
            raise ValueError("heartbeat_seconds must be less than lease_seconds / 2")
        if not 1 <= self.worker_concurrency <= 64:
            raise ValueError("worker_concurrency must be between 1 and 64")
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
