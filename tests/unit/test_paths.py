from spark_log_lab.common.paths import project_root, results_dir


def test_project_root_exists():
    assert project_root().exists()


def test_results_dir_is_under_project():
    assert results_dir().name == "results"
    assert results_dir().parent == project_root()
