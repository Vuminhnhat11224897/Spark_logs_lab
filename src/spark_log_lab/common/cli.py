from __future__ import annotations

import sys

from spark_log_lab.common.errors import ErrorCode, build_error


def fail_with_error(code: ErrorCode, exit_code: int = 1, **context: object) -> int:
    """Report a structured error for a command-line entrypoint."""
    print(build_error(code, **context), file=sys.stderr)
    return exit_code


def fail_not_implemented(feature: str, next_step: str) -> int:
    """Report an unfinished feature as a structured failure for job entrypoints."""
    return fail_with_error(
        ErrorCode.FEATURE_NOT_IMPLEMENTED,
        exit_code=2,
        feature=feature,
        next_step=next_step,
    )
