import typing


# all constraints are of the form:
class BaseConstraint(typing.TypedDict):
    key: str
    constraint_type: str


# some constraints have a value, some do not
class Constraint(BaseConstraint, total=False):
    value: str


def constraint(
    key: str,
    constraint_type: str,
    value: typing.Any = None,
) -> Constraint:
    """Factory method to create a constraint dict."""
    result: Constraint = {"key": key, "constraint_type": constraint_type}
    if value is not None:
        result["value"] = value
    return result


# https://manual.bubble.io/core-resources/api/the-bubble-api/the-data-api/data-api-requests#constraint-types
class ConstraintTypes:
    # Use to test strict equality
    EQUALS = "equals"
    NOT_EQUAL = "not equal"

    # Use to test whether a thing's given field is empty or not
    IS_EMPTY = "is_empty"
    IS_NOT_EMPTY = "is_not_empty"

    # Use to test whether a text field contains a string.
    # Text contains will not respect partial words that are not of the same stem.
    TEXT_CONTAINS = "text contains"
    NOT_TEXT_CONTAINS = "not text contains"

    # Use to compare a thing's field value relative to a given value
    GREATER_THAN = "greater than"
    LESS_THAN = "less than"

    # Use to test whether a thing's field is in a list or not for all field types.
    IN = "in"
    NOT_IN = "not in"

    # Use to test whether a list field contains an entry or not for list fields only.
    CONTAINS = "contains"
    NOT_CONTAINS = "not contains"

    # Use to test whether a list field is empty or not for list fields only.
    EMPTY = "empty"
    NOT_EMPTY = "not empty"

    # Use to test if the current thing is within a radius from a central address.#
    # To use this, the value sent with the constraint must have an address and a range.
    GEOGRAPHIC_SEARCH = "geographic_search"
