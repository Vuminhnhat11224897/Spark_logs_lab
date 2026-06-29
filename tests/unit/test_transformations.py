import pytest


def test_pipeline_modules_import():
    pytest.importorskip("pyspark")

    from spark_log_lab.pipelines import (
        bronze_csv_to_parquet,
        silver_clean_parquet,
    )

    assert bronze_csv_to_parquet is not None
    assert silver_clean_parquet is not None
