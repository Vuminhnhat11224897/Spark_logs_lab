# Centralized Error Registry Design

## Goal

Add a centralized error registry so pipeline modules raise stable, structured project errors without hard-coding exception messages in each module.

## Scope

This first iteration covers existing hard-coded validation errors in:

- `src/spark_log_lab/quality/null_check.py`
- `src/spark_log_lab/quality/duplicate_check.py`
- `src/spark_log_lab/io/writers.py`

The registry will support future pipeline, quality, configuration, and business-rule errors without requiring another design change.

## Architecture

`src/spark_log_lab/common/exceptions.py` owns the exception hierarchy. `PipelineError` becomes a structured base exception with `code`, `message`, and `context` attributes. Existing subclasses remain valid: `DataQualityError` and `ConfigurationError`.

`src/spark_log_lab/common/errors.py` owns the registry and helpers. It defines:

- `ErrorCode`: stable string enum values for all managed errors.
- `ErrorTemplate`: immutable template metadata containing exception type and message template.
- `ERROR_REGISTRY`: single source of truth for error templates.
- `build_error()` and `raise_error()` helpers so callers do not render messages directly.

Call sites import `ErrorCode` and `raise_error`, then pass only structured context. For example:

```python
raise_error(ErrorCode.MISSING_COLUMN, column=column)
```

## Error Handling

Registry rendering validates that a code exists and that required template context is present. Registry misuse raises `ConfigurationError`, because it means project code configured an invalid error definition or call.

Business/data-quality validation failures raise the exception type declared by the template, usually `DataQualityError` for current quality and DataFrame writer checks.

## Testing

Add focused unit tests for:

- building a registered error with correct type, code, message, and context
- raising a registered error through `raise_error`
- converting current quality check missing-column failures from `ValueError` to `DataQualityError`
- converting writer validation failures from `ValueError` to `DataQualityError`

