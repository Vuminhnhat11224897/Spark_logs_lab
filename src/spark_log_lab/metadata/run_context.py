from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def create_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


@dataclass(frozen=True)
class RunContext:
    run_id: str
    job_name: str
    batch_id: str | None
    event_date: str | None
    started_at: str


def create_run_context(
    job_name: str,
    batch_id: str | None = None,
    event_date: str | None = None,
) -> RunContext:
    return RunContext(
        run_id=create_run_id(),
        job_name=job_name,
        batch_id=batch_id,
        event_date=event_date,
        started_at=utc_now_iso(),
    )
