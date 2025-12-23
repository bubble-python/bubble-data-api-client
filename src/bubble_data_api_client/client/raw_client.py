import json
import types
import typing

import httpx

from bubble_data_api_client.constraints import Constraint
from bubble_data_api_client.transport import Transport


# https://manual.bubble.io/core-resources/api/the-bubble-api/the-data-api/data-api-requests#sorting
# in addition to 'sort_field' and 'descending', it is possible to have
# multiple additional sort fields
class AdditionalSortField(typing.TypedDict):
    sort_field: str
    descending: bool


class RawClient:
    """
    Raw Client layer focuses on bubble.io API endpoints.

    https://manual.bubble.io/core-resources/api/the-bubble-api/the-data-api/data-api-requests
    https://www.postman.com/bubbleapi/bubble/request/jigyk5v/
    """

    _transport: Transport

    def __init__(self) -> None:
        pass

    async def __aenter__(self) -> typing.Self:
        self._transport = Transport()
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
