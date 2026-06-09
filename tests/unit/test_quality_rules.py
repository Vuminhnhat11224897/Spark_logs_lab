def test_quality_rule_modules_import():
    import pytest

    pytest.importorskip("pyspark")
    from spark_log_lab.quality import duplicate_check, null_check, row_count

    assert row_count.check_row_count_gt_zero is not None
    assert null_check.check_null_rate is not None
    assert duplicate_check.check_duplicate_count is not None
