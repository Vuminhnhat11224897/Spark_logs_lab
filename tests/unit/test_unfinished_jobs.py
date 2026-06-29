from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


UNFINISHED_JOBS = [
    "03_build_gold.py",
    "04_run_quality_checks.py",
    "05_run_spark_benchmarks.py",
    "06_run_trino_benchmarks.py",
    "07_start_streaming_demo.py",
]


def load_job_module(job_name: str):
    job_path = Path(__file__).resolve().parents[2] / "jobs" / job_name
    module_name = job_path.stem
    spec = importlib.util.spec_from_file_location(module_name, job_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("job_name", UNFINISHED_JOBS)
def test_unfinished_jobs_fail_with_structured_error_code(job_name, capsys):
    module = load_job_module(job_name)

    exit_code = module.main()

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "[FEATURE_NOT_IMPLEMENTED]" in captured.err
    assert job_name.removesuffix(".py") in captured.err
