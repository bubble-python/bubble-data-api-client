import json
import types
import typing

import httpx

from bubble_data_api_client.transport import Transport


# all constraints are of the form:
class BaseConstraint(typing.TypedDict):
    key: str
    constraint_type: str


# some constraints have a value, some do not
class Constraint(BaseConstraint, total=False):
    value: str


# https://manual.bubble.io/core-resources/api/the-bubble-api/the-data-api/data-api-requests#sorting
# in addition to 'sort_field' and 'descending', it is possible to have
# multiple additional sort fields
class AdditionalSortField(typing.TypedDict):
    sort_field: str
    descending: bool


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


class RawClient:
    """
    Raw Client layer focuses on bubble.io API endpoints.

    https://manual.bubble.io/core-resources/api/the-bubble-api/the-data-api/data-api-requests
    https://www.postman.com/bubbleapi/bubble/request/jigyk5v/
    """

    _data_api_root_url: str
    _api_key: str
    _transport: Transport

    def __init__(
        self,
        data_api_root_url: str,
        api_key: str,
    ):
        self._data_api_root_url = data_api_root_url
        self._api_key = api_key

    async def __aenter__(self) -> typing.Self:
        self._transport = Transport(
            base_url=self._data_api_root_url,
            api_key=self._api_key,
        )
        await self._transport.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        await self._transport.__aexit__(exc_type, exc_val, exc_tb)

    async def retrieve(self, typename: str, uid: str) -> httpx.Response:
        return await self._transport.get(f"/{typename}/{uid}")

    async def create(self, typename: str, data: typing.Any) -> httpx.Response:
        return await self._transport.post(url=f"/{typename}", json=data)

    async def bulk_create(self, typename: str, data: list[typing.Any]) -> httpx.Response:
        return await self._transport.post_text(
            url=f"/{typename}/bulk",
            content="\n".join(json.dumps(item) for item in data),
        )

    async def delete(self, typename: str, uid: str) -> httpx.Response:
        return await self._transport.delete(f"/{typename}/{uid}")

    async def update(self, typename: str, uid: str, data: typing.Any) -> httpx.Response:
        return await self._transport.patch(f"/{typename}/{uid}", json=data)

    async def replace(self, typename: str, uid: str, data: typing.Any) -> httpx.Response:
        return await self._transport.put(f"/{typename}/{uid}", json=data)

    # https://manual.bubble.io/core-resources/api/the-bubble-api/the-data-api/data-api-requests#get-a-list-of-things
    async def list(
        self,
        typename: str,
        *,
        constraints: list[Constraint] | None = None,
        cursor: int | None = None,
        limit: int | None = None,
        sort_field: str | None = None,
        descending: bool | None = None,
        exclude_remaining: bool | None = None,
        additional_sort_fields: list[AdditionalSortField] | None = None,
    ) -> httpx.Response:
        params: dict[str, str] = {}

        if constraints is not None:
            params["constraints"] = json.dumps(constraints)
        if cursor is not None:
            params["cursor"] = str(cursor)
        if limit is not None:
            params["limit"] = str(limit)
        if sort_field is not None:
            params["sort_field"] = str(sort_field)
        if descending is not None:
            params["descending"] = "true" if descending else "false"
        if exclude_remaining is not None:
            params["exclude_remaining"] = "true" if exclude_remaining else "false"
        if additional_sort_fields is not None:
            params["additional_sort_fields"] = json.dumps(additional_sort_fields)

        return await self._transport.get(f"/{typename}", params=params)
