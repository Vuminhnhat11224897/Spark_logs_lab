from pathlib import Path


def test_sql_placeholders_exist():
    root = Path(__file__).resolve().parents[2]
    expected = [
        root / "sql" / "warehouse" / "bronze_tables.sql",
        root / "sql" / "warehouse" / "silver_tables.sql",
        root / "sql" / "warehouse" / "gold_tables.sql",
        root / "sql" / "trino" / "01_create_schemas.sql",
        root / "sql" / "trino" / "02_gold_queries.sql",
        root / "sql" / "trino" / "03_validation_queries.sql",
    ]
    assert all(path.exists() for path in expected)
