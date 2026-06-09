class PipelineError(Exception):
    """Base exception for pipeline failures."""


class DataQualityError(PipelineError):
    """Raised when a data quality operation cannot run."""


class ConfigurationError(PipelineError):
    """Raised when required project configuration is invalid."""
