from collections.abc import AsyncGenerator

import httpx
import pytest
import respx
import tenacity

from bubble_data_api_client import configure, http_client
from bubble_data_api_client.pool import close_clients
from bubble_data_api_client.transport import Transport


@pytest.fixture
async def clean_client_pool() -> AsyncGenerator[None]:
    """Ensure client pool is clean before and after each test."""
    await close_clients()
    yield
    await close_clients()


def test_httpx_client_factory(test_url: str, test_api_key: str) -> None:
    """Test that HTTP client is instantiated with correct configuration."""
    client = http_client.httpx_client_factory(
        base_url=test_url,
        api_key=test_api_key,
    )
    assert isinstance(client, httpx.AsyncClient)
    assert client.base_url == test_url
    assert client.headers["Authorization"] == f"Bearer {test_api_key}"
    assert client.headers["User-Agent"] == http_client.DEFAULT_USER_AGENT


@respx.mock
async def test_transport_no_retry_fails_immediately(clean_client_pool: None) -> None:
    """Test that request fails immediately when no retry is configured."""
    configure(
        data_api_root_url="https://test.example.com",
        api_key="test-key",
        retry=None,
    )

    route = respx.get("https://test.example.com/test").mock(return_value=httpx.Response(500))

    async with Transport() as transport:
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await transport.get("/test")
        assert exc_info.value.response.status_code == 500

    assert route.call_count == 1


@respx.mock
async def test_transport_retry_succeeds_after_failures(clean_client_pool: None) -> None:
    """Test that request retries and succeeds after transient failures."""
    configure(
        data_api_root_url="https://test.example.com",
        api_key="test-key",
        retry=tenacity.AsyncRetrying(
            stop=tenacity.stop_after_attempt(3),
            wait=tenacity.wait_none(),
            retry=tenacity.retry_if_exception_type(httpx.HTTPStatusError),
        ),
    )

    route = respx.get("https://test.example.com/test").mock(
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


@respx.mock
async def test_transport_retry_exhausted(clean_client_pool: None) -> None:
    """Test that RetryError is raised when all retry attempts fail."""
    configure(
        data_api_root_url="https://test.example.com",
        api_key="test-key",
        retry=tenacity.AsyncRetrying(
            stop=tenacity.stop_after_attempt(3),
            wait=tenacity.wait_none(),
            retry=tenacity.retry_if_exception_type(httpx.HTTPStatusError),
        ),
    )

    route = respx.get("https://test.example.com/test").mock(return_value=httpx.Response(500))

    async with Transport() as transport:
        with pytest.raises(tenacity.RetryError):
            await transport.get("/test")

    assert route.call_count == 3
