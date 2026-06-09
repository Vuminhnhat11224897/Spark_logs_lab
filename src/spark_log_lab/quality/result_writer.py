from __future__ import annotations

import csv
from pathlib import Path

from spark_log_lab.common.paths import results_dir


QUALITY_HEADER = [
    "run_id",
    "check_name",
    "table_or_path",
    "status",
    "actual_value",
    "expected_value",
    "checked_at",
    "error_message",
]


def quality_results_path() -> Path:
    return results_dir() / "quality_results.csv"


def write_quality_result(
    run_id: str,
    check_name: str,
    table_or_path: str,
    status: str,
    actual_value: str | int | float | None,
    expected_value: str | int | float | None,
    checked_at: str,
    error_message: str | None = None,
) -> None:
    path = quality_results_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=QUALITY_HEADER)
        if write_header:
            writer.writeheader()
        writer.writerow(
            {
                "run_id": run_id,
                "check_name": check_name,
                "table_or_path": table_or_path,
                "status": status,
                "actual_value": actual_value,
                "expected_value": expected_value,
                "checked_at": checked_at,
                "error_message": error_message,
            }
        )
