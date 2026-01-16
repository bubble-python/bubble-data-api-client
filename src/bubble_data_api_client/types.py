"""Bubble platform types for use with Pydantic models."""

from enum import StrEnum
from typing import Annotated, Any, TypedDict

from pydantic import AfterValidator, BeforeValidator

from bubble_data_api_client.exceptions import InvalidBubbleUIDError
from bubble_data_api_client.validation import is_bubble_uid


class BubbleField(StrEnum):
    """Built-in Bubble field names."""

    ID = "_id"
    CREATED_DATE = "Created Date"
    MODIFIED_DATE = "Modified Date"
    SLUG = "Slug"


class OnMultiple(StrEnum):
    """Strategy for handling multiple matches in create_or_update."""

    ERROR = "error"
    UPDATE_ALL = "update_all"
    UPDATE_FIRST = "update_first"
    DEDUPE_OLDEST = "dedupe_oldest"
    DEDUPE_NEWEST = "dedupe_newest"


class CreateOrUpdateResult(TypedDict):
    """Result of a create_or_update operation."""

    uids: list[str]
    created: bool


def _validate_bubble_uid(value: str) -> str:
    """Validate that a string is a valid Bubble UID."""
    if not is_bubble_uid(value):
        raise InvalidBubbleUIDError(value)
    return value


BubbleUID = Annotated[str, AfterValidator(_validate_bubble_uid)]
"""A string validated as a Bubble UID (format: digits + 'x' + digits)."""


def _coerce_optional_bubble_uid(value: Any) -> str | None:
    """Coerce to valid Bubble UID or None. Invalid values silently become None."""
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        return None
    if not is_bubble_uid(value):
        return None
    return value


OptionalBubbleUID = Annotated[str | None, BeforeValidator(_coerce_optional_bubble_uid)]
"""A Bubble UID that silently coerces invalid values (including empty string) to None."""


def _coerce_optional_bubble_uids(value: object) -> list[str] | None:
    """Coerce to list of valid Bubble UIDs or None. Empty/invalid becomes None."""
    if not isinstance(value, list):
        return None
    result = [x for x in value if isinstance(x, str) and is_bubble_uid(x)]
    return result or None


OptionalBubbleUIDs = Annotated[list[str] | None, BeforeValidator(_coerce_optional_bubble_uids)]
"""A list of Bubble UIDs that silently coerces invalid/empty to None."""
