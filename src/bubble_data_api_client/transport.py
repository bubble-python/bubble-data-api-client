"""HTTP transport layer for Bubble Data API requests."""

import types
import typing

import httpx

from bubble_data_api_client.config import get_config
from bubble_data_api_client.pool import get_client


class Transport:
    """Async context manager for HTTP operations.

    Responsibilities:
    - Obtains a pooled httpx client on entry
    - Provides HTTP verb methods (get, post, patch, put, delete)
    - Raises on non-2xx responses

    HTTP client configuration (headers, retries, timeouts) is handled by
    the http_client module. Connection pooling is handled by the pool module.
    """

    _http: httpx.AsyncClient

    def __init__(self) -> None:
        pass

    async def __aenter__(self) -> typing.Self:
        self._http = get_client()
        return self

    async def __aexit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        pass

    async def request(
        self,
        method: str,
        url: str,
        *,
        content: str | None = None,
        json: typing.Any = None,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        async def do_request() -> httpx.Response:
            response: httpx.Response = await self._http.request(
                method=method,
                url=url,
                content=content,
                json=json,
                params=params,
                headers=headers,
            )
            response.raise_for_status()
            return response

        retry = get_config().get("retry")
        if retry is not None:
            return await retry(do_request)
        return await do_request()

    async def get(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
    ) -> httpx.Response:
        return await self.request(method="GET", url=url, params=params)

    async def patch(self, url: str, json: typing.Any) -> httpx.Response:
        return await self.request(method="PATCH", url=url, json=json)

    async def put(self, url: str, json: typing.Any) -> httpx.Response:
        return await self.request(method="PUT", url=url, json=json)

    async def delete(self, url: str) -> httpx.Response:
        return await self.request(method="DELETE", url=url)

    async def post(self, url: str, json: typing.Any) -> httpx.Response:
        return await self.request(method="POST", url=url, json=json)

    async def post_text(self, url: str, content: str) -> httpx.Response:
        return await self.request(
            method="POST",
            url=url,
            content=content,
            headers={"Content-Type": "text/plain"},
        )
