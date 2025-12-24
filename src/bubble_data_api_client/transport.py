import types
import typing

import httpx

DEFAULT_USER_AGENT = "bubble-data-api-client"


def httpx_client_factory(
    base_url: str,
    api_key: str,
) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=base_url,
        headers={
            "Authorization": f"Bearer {api_key}",
            "User-Agent": DEFAULT_USER_AGENT,
        },
        transport=httpx.AsyncHTTPTransport(retries=3),
        timeout=httpx.Timeout(60.0),
    )


class Transport:
    """
    Transport layer focuses on HTTP.
    - authentication, headers, retries, timeouts: configured via httpx_client_factory
    - connection lifecycle: managed by pool module
    - HTTP verb methods: get, post, patch, put, delete
    - error handling: raise_for_status on responses
    """

    _http: httpx.AsyncClient

    def __init__(self) -> None:
        pass

    async def __aenter__(self) -> typing.Self:
        # deferred import to avoid circular dependency: pool imports transport
        from .pool import get_client

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
