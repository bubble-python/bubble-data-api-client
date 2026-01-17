"""Tests for bubble_data_api_client.pool module."""

from collections.abc import AsyncGenerator

import httpx
import pytest

from bubble_data_api_client import configure
from bubble_data_api_client.exceptions import ConfigurationError
from bubble_data_api_client.pool import client_scope, close_clients, get_client


@pytest.fixture
async def clean_client_pool() -> AsyncGenerator[None]:
    """Ensure client pool is clean before and after each test."""
    await close_clients()
    yield
    await close_clients()


async def test_get_client_empty_url_raises(clean_client_pool: None) -> None:
    """get_client should raise ConfigurationError when data_api_root_url is empty."""
    configure(data_api_root_url="", api_key="valid-key")
    with pytest.raises(ConfigurationError, match="data_api_root_url"):
        get_client()


async def test_get_client_empty_api_key_raises(clean_client_pool: None) -> None:
    """get_client should raise ConfigurationError when api_key is empty."""
    configure(data_api_root_url="https://example.com", api_key="")
    with pytest.raises(ConfigurationError, match="api_key"):
        get_client()


async def test_client_scope_closes_clients(clean_client_pool: None) -> None:
    """client_scope should close clients on exit."""
    configure(data_api_root_url="https://example.com", api_key="valid-key", retry=None)

    async with client_scope():
        client = get_client()
        assert not client.is_closed

    # client should be closed after exiting scope
    assert client.is_closed


async def test_close_clients_skips_already_closed(clean_client_pool: None) -> None:
    """close_clients should skip clients that are already closed."""
    configure(data_api_root_url="https://example.com", api_key="valid-key", retry=None)

    client = get_client()
    # manually close the client first
    await client.aclose()
    assert client.is_closed

    # close_clients should not raise when client is already closed
    await close_clients()


def test_get_client_outside_async_context() -> None:
    """get_client should return uncached client when called outside async context."""
    configure(data_api_root_url="https://example.com", api_key="valid-key", retry=None)

    # called from sync context - no running event loop
    client = get_client()
    assert isinstance(client, httpx.AsyncClient)
    assert not client.is_closed
