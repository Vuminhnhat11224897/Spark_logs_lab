from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from string import Formatter
from typing import Any, NoReturn

from spark_log_lab.common.exceptions import ConfigurationError, DataQualityError, PipelineError


class ErrorCode(str, Enum):
    """Stable error codes used by pipeline modules."""

    FEATURE_NOT_IMPLEMENTED = "FEATURE_NOT_IMPLEMENTED"
    MISSING_COLUMN = "MISSING_COLUMN"
    MISSING_COLUMNS = "MISSING_COLUMNS"
    MISSING_INPUT_PATHS = "MISSING_INPUT_PATHS"
    INVALID_WRITE_MODE = "INVALID_WRITE_MODE"
    INVALID_PARTITION_COLUMNS = "INVALID_PARTITION_COLUMNS"


@dataclass(frozen=True)
class ErrorTemplate:
    """Registry entry for a managed pipeline error."""

    exception_type: type[PipelineError]
    message: str


ERROR_REGISTRY: dict[ErrorCode, ErrorTemplate] = {
    ErrorCode.FEATURE_NOT_IMPLEMENTED: ErrorTemplate(
        exception_type=ConfigurationError,
        message="{feature} is not implemented yet. Next step: {next_step}",
    ),
    ErrorCode.MISSING_COLUMN: ErrorTemplate(
        exception_type=DataQualityError,
        message="Column does not exist: {column}",
    ),
    ErrorCode.MISSING_COLUMNS: ErrorTemplate(
        exception_type=DataQualityError,
        message="Columns do not exist: {columns}",
    ),
    ErrorCode.MISSING_INPUT_PATHS: ErrorTemplate(
        exception_type=DataQualityError,
        message="Required input paths do not exist: {paths}",
    ),
    ErrorCode.INVALID_WRITE_MODE: ErrorTemplate(
        exception_type=DataQualityError,
        message="Invalid write mode: {mode}. Expected one of: {valid_modes}",
    ),
    ErrorCode.INVALID_PARTITION_COLUMNS: ErrorTemplate(
        exception_type=DataQualityError,
        message="Partition columns not found in DataFrame: {columns}",
    ),
}


def build_error(code: ErrorCode | str, **context: Any) -> PipelineError:
    """Build a structured pipeline error from a registered template."""
    error_code = _normalize_error_code(code)
    if error_code is None:
        return ConfigurationError(
            code="UNREGISTERED_ERROR_CODE",
            message=f"Error code is not registered: {code}",
            context={"code": str(code)},
        )

    template = ERROR_REGISTRY[error_code]
    missing_keys = _missing_template_keys(template.message, context)
    if missing_keys:
        return ConfigurationError(
            code="ERROR_TEMPLATE_CONTEXT_MISSING",
            message=(
                f"Error template {error_code.value} is missing context keys: "
                f"{', '.join(missing_keys)}"
            ),
            context={"code": error_code.value, "missing_keys": missing_keys},
        )

    message_context = _format_message_context(context)
    return template.exception_type(
        code=error_code.value,
        message=template.message.format(**message_context),
        context=context,
    )


def raise_error(code: ErrorCode | str, **context: Any) -> NoReturn:
    """Raise a structured pipeline error from a registered template."""
    raise build_error(code, **context)


def _normalize_error_code(code: ErrorCode | str) -> ErrorCode | None:
    if isinstance(code, ErrorCode):
        return code
    try:
        return ErrorCode(str(code))
    except ValueError:
        return None


def _missing_template_keys(message_template: str, context: Mapping[str, Any]) -> list[str]:
    field_names = {
        field_name
        for _, field_name, _, _ in Formatter().parse(message_template)
        if field_name is not None
    }
    return sorted(field_name for field_name in field_names if field_name not in context)


def _format_message_context(context: Mapping[str, Any]) -> dict[str, Any]:
    return {key: _format_message_value(value) for key, value in context.items()}


def _format_message_value(value: Any) -> Any:
    if isinstance(value, (list, tuple, set, frozenset)):
        return ", ".join(str(item) for item in value)
    return value
