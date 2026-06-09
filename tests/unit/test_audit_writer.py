import csv

from spark_log_lab.metadata.audit import AUDIT_HEADER, write_audit_record


def test_audit_writer_creates_csv(monkeypatch, tmp_path):
    monkeypatch.setenv("SPARK_TRAINING_RESULTS_DIR", str(tmp_path))

    write_audit_record(
        run_id="20260609_000000",
        job_name="unit_test",
        batch_id=None,
        event_date=None,
        input_path="input",
        output_path="output",
        input_count=1,
        output_count=1,
        rejected_count=0,
        status="SUCCESS",
        started_at="2026-06-09T00:00:00+00:00",
        ended_at="2026-06-09T00:00:01+00:00",
    )

    with (tmp_path / "audit_pipeline_runs.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.reader(handle))

    assert rows[0] == AUDIT_HEADER
    assert rows[1][1] == "unit_test"
    assert rows[1][9] == "SUCCESS"
