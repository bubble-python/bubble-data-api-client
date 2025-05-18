import types
import typing

import httpx

from bubble_data_api_client.transport import Transport


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

    async def delete(self, typename: str, uid: str) -> httpx.Response:
        return await self._transport.delete(f"/{typename}/{uid}")
