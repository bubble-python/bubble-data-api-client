from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import httpx
import httpx2
import pytest
import tenacity

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    import respx

from bubble_data_api_client import (
    BubbleAPIError,
    BubbleCredentials,
    configure,
    http_client,
    set_credentials_provider,
    set_retry,
)
from bubble_data_api_client.pool import close_clients
from bubble_data_api_client.transport import Transport


@pytest.fixture
async def clean_client_pool() -> AsyncGenerator[None]:
    """Ensure client pool is clean before and after each test."""
    await close_clients()
    yield
    await close_clients()


def test_httpx_client_factory_custom_keepalive_expiry(test_url: str, test_api_key: str) -> None:
    client = http_client.httpx_client_factory(base_url=test_url, api_key=test_api_key, keepalive_expiry=30.0)
    assert isinstance(client._transport, httpx2.AsyncHTTPTransport)
    assert client._transport._pool._keepalive_expiry == 30.0


def test_httpx_client_factory_http2_defaults_off(test_url: str, test_api_key: str) -> None:
    client = http_client.httpx_client_factory(base_url=test_url, api_key=test_api_key)
    assert isinstance(client._transport, httpx2.AsyncHTTPTransport)
    assert client._transport._pool._http2 is False


def test_httpx_client_factory(test_url: str, test_api_key: str) -> None:
    """Test that HTTP client is instantiated with correct configuration."""
    client = http_client.httpx_client_factory(
        base_url=test_url,
        api_key=test_api_key,
    )
    assert isinstance(client, httpx2.AsyncClient)
    assert client.base_url == test_url
    assert client.headers["Authorization"] == f"Bearer {test_api_key}"
    assert client.headers["User-Agent"] == http_client.DEFAULT_USER_AGENT


@pytest.mark.parametrize("http2", [False, True])
def test_httpx_client_factory_pool_settings(test_url: str, test_api_key: str, http2: bool) -> None:
    """Test that http2 and limits reach the pool."""
    client = http_client.httpx_client_factory(base_url=test_url, api_key=test_api_key, http2=http2)
    transport = client._transport
    assert isinstance(transport, httpx2.AsyncHTTPTransport)
    pool = transport._pool
    assert pool._http1 is True
    assert pool._http2 is http2
    assert pool._max_connections == http_client.DEFAULT_MAX_CONNECTIONS
    assert pool._max_keepalive_connections == http_client.DEFAULT_MAX_KEEPALIVE_CONNECTIONS
    assert pool._keepalive_expiry == http_client.DEFAULT_KEEPALIVE_EXPIRY_SECONDS


async def test_transport_no_retry_fails_immediately(clean_client_pool: None, httpx2_mock: respx.Router) -> None:
    """Test that request fails immediately when no retry is configured."""
    configure(
        data_api_root_url="https://test.example.com",
        api_key="test-key",
        retry=None,
    )

    route = httpx2_mock.get("https://test.example.com/test").respond(500)

    async with Transport() as transport:
        with pytest.raises(BubbleAPIError) as exc_info:
            await transport.get("/test")
        assert exc_info.value.status_code == 500

    assert route.call_count == 1


async def test_transport_retry_succeeds_after_failures(clean_client_pool: None, httpx2_mock: respx.Router) -> None:
    """Test that request retries and succeeds after transient failures."""
    configure(
        data_api_root_url="https://test.example.com",
        api_key="test-key",
        retry=tenacity.AsyncRetrying(
            stop=tenacity.stop_after_attempt(3),
            wait=tenacity.wait_none(),
            retry=tenacity.retry_if_exception_type(BubbleAPIError),
        ),
    )

    route = httpx2_mock.get("https://test.example.com/test").mock(
        side_effect=[
            httpx.Response(500),
            httpx.Response(500),
            httpx.Response(200, json={"result": "ok"}),
        ]
    )

    async with Transport() as transport:
        response = await transport.get("/test")
        assert response.status_code == 200

    assert route.call_count == 3


async def test_transport_retry_exhausted(clean_client_pool: None, httpx2_mock: respx.Router) -> None:
    """Test that RetryError is raised when all retry attempts fail."""
    configure(
        data_api_root_url="https://test.example.com",
        api_key="test-key",
        retry=tenacity.AsyncRetrying(
            stop=tenacity.stop_after_attempt(3),
            wait=tenacity.wait_none(),
            retry=tenacity.retry_if_exception_type(BubbleAPIError),
        ),
    )

    route = httpx2_mock.get("https://test.example.com/test").respond(500)

    async with Transport() as transport:
        with pytest.raises(tenacity.RetryError):
            await transport.get("/test")

    assert route.call_count == 3


async def test_retry_lookup_does_not_touch_credentials(clean_client_pool: None, httpx2_mock: respx.Router) -> None:
    """Retry resolves on its own slot, so a credentials provider runs once per operation."""
    calls = 0

    def credentials_provider() -> BubbleCredentials:
        nonlocal calls
        calls += 1
        return BubbleCredentials(data_api_root_url="https://test.example.com", api_key="test-key")

    set_credentials_provider(credentials_provider)
    set_retry(
        retry=tenacity.AsyncRetrying(
            stop=tenacity.stop_after_attempt(3),
            wait=tenacity.wait_none(),
            retry=tenacity.retry_if_exception_type(BubbleAPIError),
        )
    )
    try:
        route = httpx2_mock.get("https://test.example.com/test").mock(
            side_effect=[
                httpx.Response(500),
                httpx.Response(200, json={"result": "ok"}),
                httpx.Response(200, json={"result": "ok"}),
            ]
        )

        async with Transport() as transport:
            await transport.get("/test")
            await transport.get("/test")

        assert route.call_count == 3
        # one call for the pooled client, none for the requests or the retry lookups
        assert calls == 1
    finally:
        configure(data_api_root_url="", api_key="")


class _AsyncWait(tenacity.wait.wait_base):
    """A wait strategy that really suspends, so tenacity's iter() yields mid-attempt.

    tenacity runs async strategies (see _utils.wrap_to_async_func) but types them
    as sync, hence the override suppression.
    """

    async def __call__(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, retry_state: tenacity.RetryCallState
    ) -> float:
        await asyncio.sleep(0)
        return 0.0


async def test_concurrent_requests_share_one_retry_policy_safely(
    clean_client_pool: None, httpx2_mock: respx.Router
) -> None:
    """One policy instance driving parallel requests must not lose retries.

    tenacity keeps per-attempt state on the instance, so without a per-request
    copy the gathered requests reset each other and return before retrying.
    """
    attempts: dict[str, int] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        which = request.url.params["i"]
        attempts[which] = attempts.get(which, 0) + 1
        return httpx.Response(200 if attempts[which] > 1 else 500, json={"result": which})

    httpx2_mock.get(url__startswith="https://test.example.com/test").mock(side_effect=handler)

    configure(
        data_api_root_url="https://test.example.com",
        api_key="test-key",
        retry=tenacity.AsyncRetrying(
            stop=tenacity.stop_after_attempt(2),
            wait=_AsyncWait(),
            retry=tenacity.retry_if_exception_type(BubbleAPIError),
        ),
    )

    async with Transport() as transport:
        responses = await asyncio.gather(*(transport.get("/test", params={"i": str(i)}) for i in range(8)))

    assert [r.status_code for r in responses] == [200] * 8
    assert sorted(attempts.values()) == [2] * 8
