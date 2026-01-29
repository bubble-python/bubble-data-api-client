"""HTTP transport layer for Bubble Data API requests."""

from __future__ import annotations

import typing
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    import types

from bubble_data_api_client.config import get_config
from bubble_data_api_client.exceptions import BubbleAPIError
from bubble_data_api_client.pool import get_client


class Transport:
    """Async context manager for HTTP operations.

    Responsibilities:
    - Obtains a pooled httpx client on entry
    - Provides HTTP verb methods (get, post, patch, put, delete)
    - Raises BubbleAPIError on non-2xx responses (single point of HTTP error handling)

    All HTTP operations in this library flow through Transport.request(), which is
    the only place that checks response status and raises errors. Higher layers
    (RawClient, ORM) can assume that if a response is returned, it was successful.

    HTTP client configuration (headers, retries, timeouts) is handled by
    the http_client module. Connection pooling is handled by the pool module.
    """

    _http: httpx.AsyncClient

    def __init__(self) -> None:
        """Initialize the transport (must be used as async context manager)."""

    async def __aenter__(self) -> typing.Self:
        """Enter async context and obtain a pooled HTTP client."""
        self._http = get_client()
        return self

    async def __aexit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        """Exit async context. Client is returned to pool, not closed."""

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
        """Execute an HTTP request with optional retry logic.

        This is the single point of HTTP error handling for the library.
        Non-2xx responses are converted to BubbleAPIError before returning.

        Raises:
            BubbleAPIError: On any non-2xx HTTP response.
        """

        async def do_request() -> httpx.Response:
            response: httpx.Response = await self._http.request(
                method=method,
                url=url,
                content=content,
                json=json,
                params=params,
                headers=headers,
            )
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as e:
                raise BubbleAPIError.from_response(response) from e
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
        """Execute a GET request."""
        return await self.request(method="GET", url=url, params=params)

    async def patch(self, url: str, json: typing.Any) -> httpx.Response:
        """Execute a PATCH request with JSON body."""
        return await self.request(method="PATCH", url=url, json=json)

    async def put(self, url: str, json: typing.Any) -> httpx.Response:
        """Execute a PUT request with JSON body."""
        return await self.request(method="PUT", url=url, json=json)

    async def delete(self, url: str) -> httpx.Response:
        """Execute a DELETE request."""
        return await self.request(method="DELETE", url=url)

    async def post(self, url: str, json: typing.Any) -> httpx.Response:
        """Execute a POST request with JSON body."""
        return await self.request(method="POST", url=url, json=json)

    async def post_text(self, url: str, content: str) -> httpx.Response:
        """Execute a POST request with plain text body."""
        return await self.request(
            method="POST",
            url=url,
            content=content,
            headers={"Content-Type": "text/plain"},
        )
