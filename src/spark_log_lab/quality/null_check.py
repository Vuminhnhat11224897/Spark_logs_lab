from __future__ import annotations

from pyspark.sql import functions as F

from spark_log_lab.metadata.run_context import utc_now_iso
from spark_log_lab.quality.result_writer import write_quality_result


def check_null_rate(df, column: str, max_rate: float, table_or_path: str, run_id: str) -> bool:
    if column not in df.columns:
        raise ValueError(f"Column does not exist: {column}")

    total_count = df.count()
    null_count = df.filter(F.col(column).isNull()).count()
    null_rate = 0.0 if total_count == 0 else null_count / total_count
    passed = null_rate <= max_rate
    write_quality_result(
        run_id=run_id,
        check_name=f"null_rate_{column}",
        table_or_path=table_or_path,
        status="PASS" if passed else "FAIL",
        actual_value=round(null_rate, 6),
        expected_value=f"<= {max_rate}",
        checked_at=utc_now_iso(),
    )
    return passed
