from __future__ import annotations

import csv
from pathlib import Path

from spark_log_lab.common.paths import results_dir


AUDIT_HEADER = [
    "run_id",
    "job_name",
    "batch_id",
    "event_date",
    "input_path",
    "output_path",
    "input_count",
    "output_count",
    "rejected_count",
    "status",
    "started_at",
    "ended_at",
    "error_message",
]


def audit_results_path() -> Path:
    return results_dir() / "audit_pipeline_runs.csv"


def write_audit_record(
    run_id: str,
    job_name: str,
    batch_id: str | None,
    event_date: str | None,
    input_path: str | None,
    output_path: str | None,
    input_count: int | None,
    output_count: int | None,
    rejected_count: int | None,
    status: str,
    started_at: str,
    ended_at: str,
    error_message: str | None = None,
) -> None:
    path = audit_results_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=AUDIT_HEADER)
        if write_header:
            writer.writeheader()
        writer.writerow(
            {
                "run_id": run_id,
                "job_name": job_name,
                "batch_id": batch_id,
                "event_date": event_date,
                "input_path": input_path,
                "output_path": output_path,
                "input_count": input_count,
                "output_count": output_count,
                "rejected_count": rejected_count,
                "status": status,
                "started_at": started_at,
                "ended_at": ended_at,
                "error_message": error_message,
            }
        )
