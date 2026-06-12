from spark_log_lab.common.paths import (
    bronze_dir,
    checkpoint_dir,
    data_dir,
    project_root,
    raw_dir,
    reports_dir,
    results_dir,
    samples_dir,
    warehouse_dir,
)


def test_project_root_exists():
    assert project_root().exists()


def test_results_dir_is_under_project():
    assert results_dir().name == "results"
    assert results_dir().parent == project_root()


def test_directory_helpers_create_directories():
    directories = [
        data_dir(),
        raw_dir(),
        samples_dir(),
        checkpoint_dir(),
        warehouse_dir(),
        bronze_dir(),
        results_dir(),
        reports_dir(),
    ]

    for path in directories:
        assert path.exists()
        assert path.is_dir()
