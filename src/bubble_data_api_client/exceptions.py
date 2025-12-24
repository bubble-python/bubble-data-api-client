class BubbleError(Exception):
    """Base class for all exceptions raised by the library."""


class ConfigurationError(BubbleError):
    """Raised when required configuration is missing."""

    def __init__(self, key: str) -> None:
        super().__init__(f"{key} is not configured")


class BubbleHttpError(BubbleError):
    """Base class for all high level HTTP errors."""


class BubbleNotFoundError(BubbleHttpError):
    """Raised when a resource is not found."""


class BubbleUnauthorizedError(BubbleHttpError):
    """Raised when the user is not authorized to access a resource."""
