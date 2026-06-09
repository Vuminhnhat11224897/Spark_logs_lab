import csv

from spark_log_lab.quality.result_writer import QUALITY_HEADER, write_quality_result


def test_quality_writer_creates_csv(monkeypatch, tmp_path):
    monkeypatch.setenv("SPARK_TRAINING_RESULTS_DIR", str(tmp_path))

    write_quality_result(
        run_id="20260609_000000",
        check_name="row_count_gt_zero",
        table_or_path="silver",
        status="PASS",
        actual_value=10,
        expected_value="> 0",
        checked_at="2026-06-09T00:00:00+00:00",
    )

    with (tmp_path / "quality_results.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.reader(handle))

    assert rows[0] == QUALITY_HEADER
    assert rows[1][1] == "row_count_gt_zero"
    assert rows[1][3] == "PASS"
