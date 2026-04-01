"""ScholarPath exception hierarchy."""


class ScholarPathError(Exception):
    """Base exception for all ScholarPath errors."""

    def __init__(self, message: str = "An unexpected error occurred") -> None:
        self.message = message
        super().__init__(self.message)


class PipelineError(ScholarPathError):
    """Raised when an evaluation pipeline step fails."""

    def __init__(self, message: str = "Pipeline processing failed") -> None:
        super().__init__(message)


class LLMError(ScholarPathError):
    """Raised when the LLM service returns an error or is unreachable."""

    def __init__(self, message: str = "LLM service error") -> None:
        super().__init__(message)


class DAGError(ScholarPathError):
    """Raised for course-dependency DAG violations (cycles, missing prereqs)."""

    def __init__(self, message: str = "DAG constraint violation") -> None:
        super().__init__(message)


class ConflictError(ScholarPathError):
    """Raised when a resource conflict is detected (duplicate, version mismatch)."""

    def __init__(self, message: str = "Resource conflict") -> None:
        super().__init__(message)


class SearchError(ScholarPathError):
    """Raised when a search or vector-lookup operation fails."""

    def __init__(self, message: str = "Search operation failed") -> None:
        super().__init__(message)


class ValidationError(ScholarPathError):
    """Raised for domain-level validation failures beyond Pydantic checks."""

    def __init__(self, message: str = "Validation failed") -> None:
        super().__init__(message)
