from pathlib import Path


def test_job_entrypoints_exist():
    root = Path(__file__).resolve().parents[2]
    expected = [
        "00_check_environment.py",
        "00_1_check_raw_files.py",
        "00_2_profile_raw.py",
        "01_build_bronze.py",
        "01_1_check_bronze.py",
        "02_build_silver.py",
        "03_build_gold.py",
        "04_run_quality_checks.py",
        "05_run_spark_benchmarks.py",
        "06_run_trino_benchmarks.py",
        "07_start_streaming_demo.py",
    ]
    assert all((root / "jobs" / name).exists() for name in expected)
