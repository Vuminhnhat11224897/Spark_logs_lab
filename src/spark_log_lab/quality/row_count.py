from __future__ import annotations

from spark_log_lab.metadata.run_context import utc_now_iso
from spark_log_lab.quality.result_writer import write_quality_result


def check_row_count_gt_zero(df, table_or_path: str, run_id: str) -> bool:
    row_count = df.count()
    passed = row_count > 0
    write_quality_result(
        run_id=run_id,
        check_name="row_count_gt_zero",
        table_or_path=table_or_path,
        status="PASS" if passed else "FAIL",
        actual_value=row_count,
        expected_value="> 0",
        checked_at=utc_now_iso(),
    )
    return passed
