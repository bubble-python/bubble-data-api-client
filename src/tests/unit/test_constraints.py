from bubble_data_api_client.constraints import ConstraintType, constraint, sort_by
from bubble_data_api_client.types import BubbleField


def test_constraint_with_value():
    """Test constraint factory with a value."""
    result = constraint(BubbleField.ID, ConstraintType.IN, ["uid1", "uid2"])
    assert result == {
        "key": "_id",
        "constraint_type": "in",
        "value": ["uid1", "uid2"],
    }


def test_constraint_without_value():
    """Test constraint factory without a value."""
    result = constraint("field", ConstraintType.IS_EMPTY)
    assert result == {
        "key": "field",
        "constraint_type": "is_empty",
    }
    assert "value" not in result


def test_sort_by_default_ascending():
    """Test sort_by factory defaults to ascending."""
    assert sort_by("name") == {"sort_field": "name", "descending": False}


def test_sort_by_descending():
    """Test sort_by factory with descending=True."""
    assert sort_by("created_date", descending=True) == {
        "sort_field": "created_date",
        "descending": True,
    }
