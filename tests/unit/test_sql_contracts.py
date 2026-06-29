from pathlib import Path


def test_current_batch_sql_notes_exist():
    root = Path(__file__).resolve().parents[2]
    expected = [
        root / "sql" / "warehouse" / "bronze_tables.sql",
        root / "sql" / "warehouse" / "silver_tables.sql",
    ]
    assert all(path.exists() for path in expected)


def test_future_platform_sql_is_not_tracked_as_runtime_contract():
    root = Path(__file__).resolve().parents[2]

    assert not (root / "sql" / "warehouse" / "gold_tables.sql").exists()
    assert not (root / "sql" / "trino").exists()
