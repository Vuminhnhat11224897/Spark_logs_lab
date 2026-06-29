from pathlib import Path


def test_job_entrypoints_do_not_mutate_pythonpath():
    jobs_dir = Path(__file__).resolve().parents[2] / "jobs"
    offenders = [
        path.name
        for path in sorted(jobs_dir.glob("*.py"))
        if "sys.path" in path.read_text(encoding="utf-8")
    ]

    assert offenders == []
