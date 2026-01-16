"""Tests for bubble_data_api_client.validation module."""

import pytest

from bubble_data_api_client.validation import filter_bubble_uids, is_bubble_uid


@pytest.mark.parametrize(
    "value",
    [
        "1767090310181x452059685440531200",  # standard bubble uid
        "1x2",  # minimal valid uid
        "0x0",  # zeros
        "123456789012345678901234567890x123456789012345678901234567890",  # long
    ],
)
def test_is_bubble_uid_valid(value: str) -> None:
    """Valid Bubble UIDs should return True."""
    assert is_bubble_uid(value) is True


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        (None, "None"),
        (123, "integer"),
        (123.456, "float"),
        (["1x2"], "list"),
        ({"uid": "1x2"}, "dict"),
        (True, "boolean"),
    ],
)
def test_is_bubble_uid_non_string(value: object, reason: str) -> None:
    """Non-string values should return False without raising."""
    assert is_bubble_uid(value) is False, f"should reject: {reason}"


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        ("", "empty string"),
        ("123", "no separator"),
        ("x", "only separator"),
        ("123x", "missing second part"),
        ("x456", "missing first part"),
        ("123x456x789", "multiple separators"),
        ("abc123x456def", "letters in first part"),
        ("123xabc", "letters in second part"),
        ("prefix123x456", "prefix before valid uid"),
        ("123x456suffix", "suffix after valid uid"),
        ("prefix123x456suffix", "prefix and suffix"),
        (" 123x456", "leading whitespace"),
        ("123x456 ", "trailing whitespace"),
        ("123 x456", "space before separator"),
        ("123x 456", "space after separator"),
        ("-123x456", "negative first part"),
        ("123x-456", "negative second part"),
        ("+123x456", "plus sign first part"),
        ("123.0x456", "decimal in first part"),
        ("123x456.0", "decimal in second part"),
        ("١٢٣x٤٥٦", "arabic-indic digits"),
        ("१२३x४५६", "devanagari digits"),
    ],
)
def test_is_bubble_uid_invalid(value: str, reason: str) -> None:
    """Invalid values should return False."""
    assert is_bubble_uid(value) is False, f"should reject: {reason}"


def test_filter_bubble_uids_empty_list() -> None:
    """Empty list should return empty list."""
    assert filter_bubble_uids([]) == []


def test_filter_bubble_uids_all_valid() -> None:
    """All valid UIDs should be returned."""
    uids = ["1x2", "3x4", "5x6"]
    assert filter_bubble_uids(uids) == ["1x2", "3x4", "5x6"]


def test_filter_bubble_uids_all_invalid() -> None:
    """All invalid values should result in empty list."""
    values = ["invalid", "also-invalid", ""]
    assert filter_bubble_uids(values) == []


def test_filter_bubble_uids_mixed() -> None:
    """Only valid UIDs should be returned from mixed input."""
    values = ["1x2", "invalid", "3x4", "", "5x6"]
    assert filter_bubble_uids(values) == ["1x2", "3x4", "5x6"]


def test_filter_bubble_uids_preserves_order() -> None:
    """Order of valid UIDs should be preserved."""
    values = ["9x9", "invalid", "1x1", "5x5"]
    assert filter_bubble_uids(values) == ["9x9", "1x1", "5x5"]


def test_filter_bubble_uids_accepts_iterator() -> None:
    """Should work with any iterable, not just lists."""
    values = iter(["1x2", "invalid", "3x4"])
    assert filter_bubble_uids(values) == ["1x2", "3x4"]
