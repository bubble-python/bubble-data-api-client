"""Tests for bubble_data_api_client.types module."""

import pytest
from pydantic import BaseModel, ValidationError

from bubble_data_api_client.exceptions import InvalidBubbleUIDError
from bubble_data_api_client.types import (
    BubbleUID,
    OptionalBubbleUID,
    OptionalBubbleUIDs,
    _coerce_optional_bubble_uid,
    _coerce_optional_bubble_uids,
    _validate_bubble_uid,
)


def test_invalid_bubble_uid_error_message() -> None:
    """InvalidBubbleUIDError should have descriptive message."""
    error = InvalidBubbleUIDError("bad_value")
    assert str(error) == "invalid Bubble UID format: bad_value"
    assert error.value == "bad_value"


def test_validate_bubble_uid_returns_value_unchanged() -> None:
    """Valid UID should be returned unchanged."""
    value = "1767090310181x452059685440531200"
    result = _validate_bubble_uid(value)
    assert result == value


def test_validate_bubble_uid_raises_for_invalid() -> None:
    """Invalid UID should raise InvalidBubbleUIDError."""
    with pytest.raises(InvalidBubbleUIDError, match="invalid Bubble UID format: invalid"):
        _validate_bubble_uid("invalid")


def test_validate_bubble_uid_raises_for_empty_string() -> None:
    """Empty string should raise InvalidBubbleUIDError."""
    with pytest.raises(InvalidBubbleUIDError):
        _validate_bubble_uid("")


def test_bubble_uid_type_valid() -> None:
    """Valid UID should pass Pydantic validation."""

    class Model(BaseModel):
        uid: BubbleUID

    model = Model(uid="1767090310181x452059685440531200")
    assert model.uid == "1767090310181x452059685440531200"


def test_bubble_uid_type_invalid_raises() -> None:
    """Invalid UID should raise Pydantic ValidationError."""

    class Model(BaseModel):
        uid: BubbleUID

    with pytest.raises(ValidationError) as exc_info:
        Model(uid="invalid")

    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["type"] == "value_error"
    assert "invalid Bubble UID format" in errors[0]["msg"]


def test_bubble_uid_optional_none() -> None:
    """BubbleUID | None should accept None."""

    class Model(BaseModel):
        uid: BubbleUID | None = None

    model = Model()
    assert model.uid is None


def test_bubble_uid_optional_with_value() -> None:
    """BubbleUID | None should accept valid UID."""

    class Model(BaseModel):
        uid: BubbleUID | None = None

    model = Model(uid="123x456")
    assert model.uid == "123x456"


def test_bubble_uid_strict_list_valid() -> None:
    """list[BubbleUID] should validate all items."""

    class Model(BaseModel):
        uids: list[BubbleUID]

    model = Model(uids=["1x2", "3x4", "5x6"])
    assert model.uids == ["1x2", "3x4", "5x6"]


def test_bubble_uid_strict_list_with_invalid_raises() -> None:
    """list[BubbleUID] with invalid item should raise ValidationError."""

    class Model(BaseModel):
        uids: list[BubbleUID]

    with pytest.raises(ValidationError) as exc_info:
        Model(uids=["1x2", "invalid", "3x4"])

    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["loc"] == ("uids", 1)  # index 1 is invalid


def test_bubble_uid_strict_list_with_empty_string_raises() -> None:
    """list[BubbleUID] with empty string should raise ValidationError."""

    class Model(BaseModel):
        uids: list[BubbleUID]

    with pytest.raises(ValidationError) as exc_info:
        Model(uids=["1x2", "", "3x4"])

    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["loc"] == ("uids", 1)


def test_bubble_uid_strict_list_empty() -> None:
    """list[BubbleUID] should accept empty list."""

    class Model(BaseModel):
        uids: list[BubbleUID]

    model = Model(uids=[])
    assert model.uids == []


def test_coerce_optional_bubble_uids_valid() -> None:
    """Valid list should return filtered list."""
    assert _coerce_optional_bubble_uids(["1x2", "3x4"]) == ["1x2", "3x4"]


def test_coerce_optional_bubble_uids_mixed() -> None:
    """Mixed list should filter invalid items."""
    assert _coerce_optional_bubble_uids(["1x2", "invalid", "3x4"]) == ["1x2", "3x4"]


def test_coerce_optional_bubble_uids_all_invalid() -> None:
    """All invalid should return None."""
    assert _coerce_optional_bubble_uids(["invalid", ""]) is None


def test_coerce_optional_bubble_uids_empty_list() -> None:
    """Empty list should return None."""
    assert _coerce_optional_bubble_uids([]) is None


def test_coerce_optional_bubble_uids_none() -> None:
    """None should return None."""
    assert _coerce_optional_bubble_uids(None) is None


def test_coerce_optional_bubble_uids_non_list() -> None:
    """Non-list should return None."""
    assert _coerce_optional_bubble_uids("1x2") is None
    assert _coerce_optional_bubble_uids(123) is None


def test_optional_bubble_uids_filters_invalid() -> None:
    """OptionalBubbleUIDs should silently filter out invalid UIDs."""

    class Model(BaseModel):
        uids: OptionalBubbleUIDs

    model = Model(uids=["1x2", "invalid", "3x4", "", "5x6"])
    assert model.uids == ["1x2", "3x4", "5x6"]


def test_optional_bubble_uids_all_valid() -> None:
    """OptionalBubbleUIDs should keep all valid UIDs."""

    class Model(BaseModel):
        uids: OptionalBubbleUIDs

    model = Model(uids=["1x2", "3x4", "5x6"])
    assert model.uids == ["1x2", "3x4", "5x6"]


def test_optional_bubble_uids_all_invalid_becomes_none() -> None:
    """OptionalBubbleUIDs should return None when all invalid."""

    class Model(BaseModel):
        uids: OptionalBubbleUIDs

    model = Model(uids=["invalid", "also-invalid", ""])
    assert model.uids is None


def test_optional_bubble_uids_empty_becomes_none() -> None:
    """OptionalBubbleUIDs should return None for empty list."""

    class Model(BaseModel):
        uids: OptionalBubbleUIDs

    model = Model(uids=[])
    assert model.uids is None


def test_optional_bubble_uids_none() -> None:
    """OptionalBubbleUIDs should accept None."""

    class Model(BaseModel):
        uids: OptionalBubbleUIDs

    model = Model(uids=None)
    assert model.uids is None


def test_optional_bubble_uids_with_default() -> None:
    """OptionalBubbleUIDs with default None should work."""

    class Model(BaseModel):
        uids: OptionalBubbleUIDs = None

    model = Model()
    assert model.uids is None

    model_valid = Model(uids=["1x2", "invalid"])
    assert model_valid.uids == ["1x2"]

    model_empty = Model(uids=[])
    assert model_empty.uids is None


# OptionalBubbleUID tests


def test_coerce_optional_bubble_uid_valid() -> None:
    """Valid UID should be returned unchanged."""
    assert _coerce_optional_bubble_uid("1x2") == "1x2"
    assert _coerce_optional_bubble_uid("1767090310181x452059685440531200") == "1767090310181x452059685440531200"


def test_coerce_optional_bubble_uid_none() -> None:
    """None should return None."""
    assert _coerce_optional_bubble_uid(None) is None


def test_coerce_optional_bubble_uid_empty_string() -> None:
    """Empty string should return None."""
    assert _coerce_optional_bubble_uid("") is None


def test_coerce_optional_bubble_uid_invalid_string() -> None:
    """Invalid string should return None (not raise)."""
    assert _coerce_optional_bubble_uid("invalid") is None
    assert _coerce_optional_bubble_uid("garbage123") is None


def test_coerce_optional_bubble_uid_non_string() -> None:
    """Non-string values should return None."""
    assert _coerce_optional_bubble_uid(123) is None
    assert _coerce_optional_bubble_uid(["1x2"]) is None
    assert _coerce_optional_bubble_uid({"id": "1x2"}) is None


def test_optional_bubble_uid_valid() -> None:
    """Valid UID should pass validation."""

    class Model(BaseModel):
        uid: OptionalBubbleUID

    model = Model(uid="1x2")
    assert model.uid == "1x2"


def test_optional_bubble_uid_none() -> None:
    """None should be accepted."""

    class Model(BaseModel):
        uid: OptionalBubbleUID

    model = Model(uid=None)
    assert model.uid is None


def test_optional_bubble_uid_empty_string() -> None:
    """Empty string should become None."""

    class Model(BaseModel):
        uid: OptionalBubbleUID

    model = Model(uid="")
    assert model.uid is None


def test_optional_bubble_uid_invalid_becomes_none() -> None:
    """Invalid UID should silently become None."""

    class Model(BaseModel):
        uid: OptionalBubbleUID

    model = Model(uid="invalid")
    assert model.uid is None


def test_optional_bubble_uid_with_default() -> None:
    """OptionalBubbleUID with default None should work."""

    class Model(BaseModel):
        uid: OptionalBubbleUID = None

    model = Model()
    assert model.uid is None

    model_valid = Model(uid="1x2")
    assert model_valid.uid == "1x2"

    model_invalid = Model(uid="garbage")
    assert model_invalid.uid is None
