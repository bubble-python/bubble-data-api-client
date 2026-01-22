"""Exception types for Bubble Data API errors."""


class BubbleError(Exception):
    """Base class for all exceptions raised by the library."""


class ConfigurationError(BubbleError):
    """Raised when required configuration is missing."""

    def __init__(self, key: str) -> None:
        """Create error for missing configuration key."""
        super().__init__(f"{key} is not configured")


class BubbleHttpError(BubbleError):
    """Base class for all high level HTTP errors."""


class BubbleUnauthorizedError(BubbleHttpError):
    """Raised when the user is not authorized to access a resource."""


class InvalidBubbleUIDError(ValueError):
    """Raised when a string is not a valid Bubble UID."""

    def __init__(self, value: str) -> None:
        """Create error for invalid UID format."""
        super().__init__(f"invalid Bubble UID format: {value}")
        self.value = value


class UnknownFieldError(BubbleError):
    """Raised when an unknown field name is passed to update()."""

    def __init__(self, field_name: str) -> None:
        """Create error for unknown field name."""
        super().__init__(f"unknown field: {field_name}")
        self.field_name = field_name


class MultipleMatchesError(BubbleError):
    """Raised when create_or_update finds multiple matches with on_multiple='error'."""

    def __init__(self, typename: str, count: int, match: dict) -> None:
        """Create error for unexpected multiple matches."""
        super().__init__(f"expected 0 or 1 matches for '{typename}', found {count} with match={match}")
        self.typename = typename
        self.count = count
        self.match = match


class InvalidOnMultipleError(BubbleError):
    """Raised when an invalid on_multiple strategy is provided."""

    def __init__(self, value: str) -> None:
        """Create error for invalid on_multiple strategy value."""
        super().__init__(f"invalid on_multiple strategy: '{value}'")
        self.value = value


class PartialFailureError(BubbleError):
    """Raised when some operations in a batch succeed but others fail."""

    def __init__(
        self,
        operation: str,
        succeeded: list[str],
        failed: list[tuple[str, BaseException]],
    ) -> None:
        """Create error with lists of succeeded and failed UIDs."""
        failed_count = len(failed)
        total = len(succeeded) + failed_count
        super().__init__(f"{operation}: {failed_count}/{total} operations failed")
        self.operation = operation
        self.succeeded = succeeded
        self.failed_uids = [uid for uid, _ in failed]
        self.exceptions = [exc for _, exc in failed]
