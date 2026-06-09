def test_project_imports():
    import spark_log_lab
    from spark_log_lab.common.logging import get_logger
    from spark_log_lab.common.paths import project_root
    from spark_log_lab.metadata.audit import write_audit_record
    from spark_log_lab.quality.result_writer import write_quality_result

    assert spark_log_lab is not None
    assert project_root().exists()
    assert get_logger("test_project_imports").name == "test_project_imports"
    assert write_audit_record is not None
    assert write_quality_result is not None
