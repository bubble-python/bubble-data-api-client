"""Bubble platform types for use with Pydantic models."""

from dataclasses import dataclass
from enum import StrEnum
from typing import Annotated, Any, Literal, TypedDict

from pydantic import AfterValidator, BeforeValidator

from bubble_data_api_client.exceptions import InvalidBubbleUIDError
from bubble_data_api_client.validation import is_bubble_uid


class BubbleField(StrEnum):
    """Built-in Bubble field names."""

    ID = "_id"
    CREATED_BY = "Created By"
    CREATED_DATE = "Created Date"
    MODIFIED_DATE = "Modified Date"
    SLUG = "Slug"


class BuiltinField(StrEnum):
    """Python ORM attribute names for Bubble's built-in fields."""

    UID = "uid"
    CREATED_BY = "created_by"
    CREATED_DATE = "created_date"
    MODIFIED_DATE = "modified_date"
    SLUG = "slug"


BUILTIN_FIELDS: set[str] = set(BuiltinField)
"""Python ORM attribute names for Bubble's built-in fields, as a set."""


class OnMultiple(StrEnum):
    """Strategy for handling multiple matches in create_or_update."""

    ERROR = "error"
    UPDATE_ALL = "update_all"
    UPDATE_FIRST = "update_first"
    DEDUPE_OLDEST_CREATED = "dedupe_oldest_created"
    DEDUPE_NEWEST_CREATED = "dedupe_newest_created"
    DEDUPE_OLDEST_MODIFIED = "dedupe_oldest_modified"
    DEDUPE_NEWEST_MODIFIED = "dedupe_newest_modified"


class CreateOrUpdateResult(TypedDict):
    """Result of a create_or_update operation."""

    uids: list[str]
    created: bool


class BulkCreateItemResult(TypedDict):
    """Result for a single item in a bulk create operation.

    On success: status="success", id=<uid>, message=None
    On error: status="error", id=None, message=<error description>
    """

    status: Literal["success", "error"]
    id: str | None
    message: str | None


@dataclass(frozen=True, slots=True)
class PageResult[T]:
    """One page of results from a Bubble find query, with envelope metadata.

    To advance pagination, use cursor + len(items), not any notion of page
    size. Bubble silently caps requested limits at 100, so a caller who
    requested more than 100 items must not assume they got what they asked
    for; len(items) is always the correct advancement step.

    Attributes:
        items: Results for this page, in Bubble's returned order.
        cursor: The cursor (offset) that produced this page, as reported
            by Bubble's response envelope.
        remaining: Number of matching records after this page.
    """

    items: list[T]
    cursor: int
    remaining: int

    @property
    def total(self) -> int:
        """Return total number of records matching the query.

        Computed as cursor + len(items) + remaining. This under-reports
        past Bubble's ~50,000 cursor cap on shared infrastructure, where
        Bubble returns an empty page with a non-zero remaining value. For
        collections larger than the cap, use keyset pagination (sort by a
        monotonic field with a greater-than constraint) rather than
        offset pagination.
        """
        return self.cursor + len(self.items) + self.remaining

    @property
    def has_more(self) -> bool:
        """Return True if there are more pages after this one."""
        return self.remaining > 0


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
