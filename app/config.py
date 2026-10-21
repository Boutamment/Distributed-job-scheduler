"""Runtime configuration, read from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass


def _float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


@dataclass(frozen=True)
class Config:
    database_url: str
    lease_seconds: float  # how long a claimed task stays ours without a heartbeat
    heartbeat_seconds: float  # how often a worker renews its lease (keep < lease/2)
    scheduler_interval: float  # seconds between scheduler ticks
    poll_seconds: float  # worker sleep when the queue is empty
    backoff_base: float  # first retry delay; doubles each attempt
    backoff_cap: float  # maximum retry delay
    worker_concurrency: int  # tasks one worker process runs at once


def load() -> Config:
    lease = _float("LEASE_SECONDS", 10)
    return Config(
        database_url=os.environ.get(
            "DATABASE_URL", "postgresql://jobforge:jobforge@localhost:5432/jobforge"
        ),
        lease_seconds=lease,
        heartbeat_seconds=_float("HEARTBEAT_SECONDS", lease / 3),
        scheduler_interval=_float("SCHEDULER_INTERVAL", 0.25),
        poll_seconds=_float("POLL_SECONDS", 0.2),
        backoff_base=_float("BACKOFF_BASE", 1.0),
        backoff_cap=_float("BACKOFF_CAP", 30.0),
        worker_concurrency=int(os.environ.get("WORKER_CONCURRENCY", 4)),
    )
