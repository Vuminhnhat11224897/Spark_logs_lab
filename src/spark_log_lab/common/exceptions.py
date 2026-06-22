from __future__ import annotations

from collections.abc import Mapping
from typing import Any


class PipelineError(Exception):
    """Base exception for pipeline failures."""

    def __init__(
        self,
        code: str,
        message: str | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        if message is None:
            message = code
            code = self.__class__.__name__

        self.code = code
        self.message = message
        self.context = dict(context or {})
        super().__init__(str(self))

    def __str__(self) -> str:
        return f"[{self.code}] {self.message}"


class DataQualityError(PipelineError):
    """Raised when a data quality operation cannot run."""


class ConfigurationError(PipelineError):
    """Raised when required project configuration is invalid."""
