from pathlib import Path
import tomllib


def test_runtime_dependencies_are_minimal():
    pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
    config = tomllib.loads(pyproject.read_text(encoding="utf-8"))

    assert config["project"]["dependencies"] == ["pyspark"]


def test_requirements_match_runtime_dependencies():
    requirements = Path(__file__).resolve().parents[2] / "requirements.txt"

    assert requirements.read_text(encoding="utf-8").splitlines() == ["pyspark"]
