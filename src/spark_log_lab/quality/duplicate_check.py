from __future__ import annotations

from pyspark.sql import functions as F

from spark_log_lab.common.errors import ErrorCode, raise_error
from spark_log_lab.metadata.run_context import utc_now_iso
from spark_log_lab.quality.result_writer import write_quality_result


def check_duplicate_count(df, key_columns: list[str], table_or_path: str, run_id: str) -> bool:
    missing = [column for column in key_columns if column not in df.columns]
    if missing:
        raise_error(ErrorCode.MISSING_COLUMNS, columns=missing)

    duplicate_count = (
        df.groupBy(*key_columns)
        .agg(F.count("*").alias("row_count"))
        .filter(F.col("row_count") > 1)
        .count()
    )
    passed = duplicate_count == 0
    write_quality_result(
        run_id=run_id,
        check_name="duplicate_count",
        table_or_path=table_or_path,
        status="PASS" if passed else "FAIL",
        actual_value=duplicate_count,
        expected_value=0,
        checked_at=utc_now_iso(),
    )
    return passed
