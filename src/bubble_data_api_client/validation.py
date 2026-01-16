"""Validation utilities for Bubble platform data."""

import re
from collections.abc import Iterable
from typing import Any

_BUBBLE_UID_PATTERN: re.Pattern[str] = re.compile(r"^[0-9]+x[0-9]+$")


def is_bubble_uid(value: Any) -> bool:
    """Check if a string matches the Bubble UID format (e.g., '1767090310181x452059685440531200')."""
    if not isinstance(value, str):
        return False
    return _BUBBLE_UID_PATTERN.fullmatch(value) is not None


def filter_bubble_uids(values: Iterable[str]) -> list[str]:
    """Return only valid Bubble UIDs from an iterable, filtering out invalid ones."""
    return [v for v in values if is_bubble_uid(v)]
