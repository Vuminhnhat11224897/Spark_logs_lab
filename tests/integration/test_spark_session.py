def test_spark_session_factory_imports():
    import pytest

    pytest.importorskip("pyspark")
    from spark_log_lab.common.spark import create_spark_session

    assert create_spark_session is not None
