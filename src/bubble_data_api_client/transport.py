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
    - manage connections
    - authentication
    - headers
    - retries, backoff
    - timeouts
    - exposes errors to the client
    """

    _base_url: str
    _api_key: str
    _http: httpx.AsyncClient

    def __init__(self, base_url: str, api_key: str):
        self._base_url = base_url
        self._api_key = api_key

    async def __aenter__(self) -> typing.Self:
        self._http = httpx_client_factory(
            base_url=self._base_url,
            api_key=self._api_key,
        )

        return self

    async def __aexit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        await self._http.aclose()

    async def request(
        self,
        method: str,
        url: str,
        *,
        content: str | None = None,
        json: typing.Any = None,
        params: dict[str, str] | None = None,
    ) -> httpx.Response:
        response: httpx.Response = await self._http.request(
            method=method,
            url=url,
            content=content,
            json=json,
            params=params,
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
        return await self.request(method="POST", url=url, content=content)
