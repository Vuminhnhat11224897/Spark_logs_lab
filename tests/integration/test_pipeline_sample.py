from spark_log_lab.common.paths import raw_dir


def test_raw_directory_exists():
    assert raw_dir().exists()
