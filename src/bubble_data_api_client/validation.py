"""Validation utilities for Bubble platform data."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable

_BUBBLE_UID_PATTERN: re.Pattern[str] = re.compile(r"^[0-9]+x[0-9]+$")


def is_bubble_uid(value: Any) -> bool:
    """Check if a value is a valid Bubble UID (e.g., '1767090310181x452059685440531200')."""
    return isinstance(value, str) and _BUBBLE_UID_PATTERN.fullmatch(value) is not None


def filter_bubble_uids(values: Iterable[Any]) -> list[str]:
    """Return only valid Bubble UIDs from an iterable, filtering out invalid values."""
    return [v for v in values if is_bubble_uid(v)]
