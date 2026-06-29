import pytest

from spark_log_lab.common.errors import ErrorCode, build_error, raise_error
from spark_log_lab.common.exceptions import ConfigurationError, DataQualityError
from spark_log_lab.io.writers import _validate_partition_columns, _validate_write_mode
from spark_log_lab.quality.duplicate_check import check_duplicate_count
from spark_log_lab.quality.null_check import check_null_rate


class DummyDataFrame:
    columns = ["id"]


def test_build_error_renders_registered_template_with_context():
    error = build_error(ErrorCode.MISSING_COLUMN, column="event_id")

    assert isinstance(error, DataQualityError)
    assert error.code == "MISSING_COLUMN"
    assert error.message == "Column does not exist: event_id"
    assert error.context == {"column": "event_id"}
    assert str(error) == "[MISSING_COLUMN] Column does not exist: event_id"


def test_raise_error_raises_registered_exception_type():
    with pytest.raises(DataQualityError) as exc_info:
        raise_error(ErrorCode.INVALID_WRITE_MODE, mode="bad", valid_modes=["append"])

    assert exc_info.value.code == "INVALID_WRITE_MODE"
    assert exc_info.value.context == {"mode": "bad", "valid_modes": ["append"]}


def test_build_error_formats_sequence_context_for_message_only():
    error = build_error(ErrorCode.MISSING_COLUMNS, columns=["event_id", "session_id"])

    assert error.message == "Columns do not exist: event_id, session_id"
    assert error.context == {"columns": ["event_id", "session_id"]}


def test_build_error_reports_missing_input_paths():
    error = build_error(
        ErrorCode.MISSING_INPUT_PATHS,
        paths=["data/raw/missing.csv", "warehouse/bronze/missing"],
    )

    assert error.message == (
        "Required input paths do not exist: data/raw/missing.csv, warehouse/bronze/missing"
    )
    assert error.context == {
        "paths": ["data/raw/missing.csv", "warehouse/bronze/missing"],
    }


def test_build_error_accepts_registered_string_code():
    error = build_error("MISSING_COLUMN", column="event_id")

    assert isinstance(error, DataQualityError)
    assert error.code == "MISSING_COLUMN"
    assert error.message == "Column does not exist: event_id"


def test_build_error_reports_unknown_code_as_configuration_error():
    error = build_error("UNKNOWN_ERROR")

    assert isinstance(error, ConfigurationError)
    assert error.code == "UNREGISTERED_ERROR_CODE"
    assert error.context == {"code": "UNKNOWN_ERROR"}


def test_build_error_reports_missing_template_context_as_configuration_error():
    error = build_error(ErrorCode.INVALID_WRITE_MODE, mode="replace")

    assert isinstance(error, ConfigurationError)
    assert error.code == "ERROR_TEMPLATE_CONTEXT_MISSING"
    assert error.context == {
        "code": "INVALID_WRITE_MODE",
        "missing_keys": ["valid_modes"],
    }


def test_quality_checks_raise_data_quality_error_for_missing_columns():
    with pytest.raises(DataQualityError) as null_exc:
        check_null_rate(DummyDataFrame(), "missing", 0.1, "table", "run-id")

    assert null_exc.value.code == "MISSING_COLUMN"
    assert null_exc.value.context == {"column": "missing"}

    with pytest.raises(DataQualityError) as duplicate_exc:
        check_duplicate_count(DummyDataFrame(), ["id", "missing"], "table", "run-id")

    assert duplicate_exc.value.code == "MISSING_COLUMNS"
    assert duplicate_exc.value.context == {"columns": ["missing"]}


def test_writer_validation_raises_data_quality_error_from_registry():
    with pytest.raises(DataQualityError) as mode_exc:
        _validate_write_mode("replace")

    assert mode_exc.value.code == "INVALID_WRITE_MODE"
    assert mode_exc.value.context["mode"] == "replace"

    with pytest.raises(DataQualityError) as partition_exc:
        _validate_partition_columns(DummyDataFrame(), ["missing"])

    assert partition_exc.value.code == "INVALID_PARTITION_COLUMNS"
    assert partition_exc.value.context == {"columns": ["missing"]}
