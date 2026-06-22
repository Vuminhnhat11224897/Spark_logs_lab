# Centralized Error Registry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a centralized error registry/template system and migrate current hard-coded validation errors to it.

**Architecture:** Keep exception classes in `common/exceptions.py` and put the registry, error codes, templates, and raise helpers in `common/errors.py`. Call sites pass an `ErrorCode` plus context instead of formatting messages directly.

**Tech Stack:** Python 3.10+, dataclasses, enum compatibility, pytest.

---

## File Structure

- Create `src/spark_log_lab/common/errors.py`: centralized error code enum, immutable templates, registry, `build_error`, and `raise_error`.
- Modify `src/spark_log_lab/common/exceptions.py`: structured `PipelineError` with `code`, `message`, and `context`; preserve existing subclass names.
- Modify `src/spark_log_lab/quality/null_check.py`: replace missing-column `ValueError`.
- Modify `src/spark_log_lab/quality/duplicate_check.py`: replace missing-columns `ValueError`.
- Modify `src/spark_log_lab/io/writers.py`: replace invalid write mode and partition-column `ValueError`.
- Create `tests/unit/test_errors.py`: test registry behavior and migrated call sites.

### Task 1: Registry Tests

**Files:**
- Create: `tests/unit/test_errors.py`

- [x] **Step 1: Write failing tests**

```python
import pytest

from spark_log_lab.common.errors import ErrorCode, build_error, raise_error
from spark_log_lab.common.exceptions import DataQualityError
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
```

- [x] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_errors.py -q`

Expected: FAIL because `spark_log_lab.common.errors` does not exist.

### Task 2: Registry Implementation

**Files:**
- Modify: `src/spark_log_lab/common/exceptions.py`
- Create: `src/spark_log_lab/common/errors.py`

- [x] **Step 1: Implement structured exceptions and registry**

```python
from __future__ import annotations


class PipelineError(Exception):
    """Base exception for pipeline failures."""

    def __init__(self, code: str, message: str, context: dict[str, object] | None = None) -> None:
        self.code = code
        self.message = message
        self.context = context or {}
        super().__init__(self.__str__())

    def __str__(self) -> str:
        return f"[{self.code}] {self.message}"
```

Create `common/errors.py` with enum fallback, templates, registry, `build_error`, and `raise_error`.

- [x] **Step 2: Run focused tests**

Run: `.venv/bin/python -m pytest tests/unit/test_errors.py -q`

Expected: remaining FAILs from old call sites still raising `ValueError`.

### Task 3: Migrate Call Sites

**Files:**
- Modify: `src/spark_log_lab/quality/null_check.py`
- Modify: `src/spark_log_lab/quality/duplicate_check.py`
- Modify: `src/spark_log_lab/io/writers.py`

- [x] **Step 1: Replace hard-coded raises**

Use `raise_error(ErrorCode.MISSING_COLUMN, column=column)`, `raise_error(ErrorCode.MISSING_COLUMNS, columns=missing)`, `raise_error(ErrorCode.INVALID_WRITE_MODE, mode=mode, valid_modes=sorted(valid_modes))`, and `raise_error(ErrorCode.INVALID_PARTITION_COLUMNS, columns=missing)`.

- [x] **Step 2: Run focused tests**

Run: `.venv/bin/python -m pytest tests/unit/test_errors.py -q`

Expected: PASS.

### Task 4: Regression Verification

**Files:**
- No new edits unless tests expose a regression.

- [x] **Step 1: Run existing unit tests**

Run: `.venv/bin/python -m pytest tests/unit -q`

Expected: PASS.

- [x] **Step 2: Run full test suite when practical**

Run: `.venv/bin/python -m pytest -q`

Expected: PASS or clearly reported environment-specific Spark failures.
