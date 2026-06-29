from pathlib import Path


def test_active_job_entrypoints_exist():
    root = Path(__file__).resolve().parents[2]
    expected = [
        "00_check_environment.py",
        "00_1_check_raw_files.py",
        "00_2_profile_raw.py",
        "01_build_bronze.py",
        "01_1_check_bronze.py",
        "01_2_profile_bronze.py",
        "02_build_silver.py",
        "02_1_check_silver.py",
    ]
    assert all((root / "jobs" / name).exists() for name in expected)


def test_active_submit_scripts_exist():
    root = Path(__file__).resolve().parents[2]
    expected = [
        "submit_raw_check.sh",
        "submit_raw_profile.sh",
        "submit_bronze_build.sh",
        "submit_bronze_check.sh",
        "submit_bronze_profile.sh",
        "submit_silver_build.sh",
        "submit_silver_check.sh",
    ]
    assert all((root / "scripts" / name).exists() for name in expected)
