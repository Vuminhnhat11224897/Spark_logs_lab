import csv

from spark_log_lab.quality.profiler import ColumnProfile, PROFILE_HEADER, data_profile_path, write_profiles


def _profile(run_id: str) -> ColumnProfile:
    return ColumnProfile(
        run_id=run_id,
        layer="raw",
        dataset="log_tracking",
        column_name="event_time",
        data_type="string",
        row_count=10,
        null_count=0,
        null_rate=0.0,
        empty_count=0,
        empty_rate=0.0,
        approx_distinct_count=10,
        mode_value="2019-11-01 00:00:00 UTC",
        mode_count=1,
        min_value="2019-11-01 00:00:00 UTC",
        max_value="2019-11-01 00:00:09 UTC",
        avg_value=None,
        stddev_value=None,
        profiled_at="2026-06-16T00:00:00+00:00",
    )


def test_data_profile_path_is_per_dataset_snapshot(monkeypatch, tmp_path):
    monkeypatch.setenv("SPARK_TRAINING_RESULTS_DIR", str(tmp_path))

    assert data_profile_path("raw", "log_tracking") == (
        tmp_path / "data_profiles" / "raw_log_tracking_profile.csv"
    )
    assert data_profile_path("bronze", "purchase behavior", version="v1") == (
        tmp_path / "data_profiles" / "bronze_purchase_behavior_profile_v1.csv"
    )


def test_write_profiles_overwrites_current_snapshot(tmp_path):
    output_path = tmp_path / "profile.csv"

    write_profiles([_profile("first")], path=output_path)
    write_profiles([_profile("second")], path=output_path)

    with output_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.reader(handle))

    assert rows[0] == PROFILE_HEADER
    assert len(rows) == 2
    assert rows[1][0] == "second"
