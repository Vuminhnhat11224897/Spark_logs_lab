from pathlib import Path


def test_active_source_tree_has_no_placeholder_modules():
    source_root = Path(__file__).resolve().parents[2] / "src" / "spark_log_lab"
    offenders = [
        path.relative_to(source_root).as_posix()
        for path in sorted(source_root.rglob("*.py"))
        if "placeholder" in path.read_text(encoding="utf-8").lower()
    ]

    assert offenders == []
