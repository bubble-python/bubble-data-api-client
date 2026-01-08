"""Validation utilities for Bubble platform data."""

import re
from collections.abc import Iterable

_BUBBLE_UID_PATTERN: re.Pattern[str] = re.compile(r"^[0-9]+x[0-9]+$")


def is_bubble_uid(value: str) -> bool:
    """Check if a string matches the Bubble UID format (e.g., '1767090310181x452059685440531200')."""
    return _BUBBLE_UID_PATTERN.fullmatch(value) is not None


def filter_bubble_uids(values: Iterable[str]) -> list[str]:
    """Return only valid Bubble UIDs from an iterable, filtering out invalid ones."""
    return [v for v in values if is_bubble_uid(v)]
