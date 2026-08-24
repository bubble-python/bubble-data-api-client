"""Tests for bubble_data_api_client.pool module."""

from __future__ import annotations

from typing import TYPE_CHECKING

import httpx2
import pytest

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

from bubble_data_api_client import BubbleCredentials, configure, set_credentials_provider
from bubble_data_api_client.exceptions import ConfigurationError
from bubble_data_api_client.pool import _clients, client_scope, close_clients, get_client


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
    assert isinstance(client, httpx2.AsyncClient)
    assert not client.is_closed


async def test_credentials_provider_pools_per_tenant(clean_client_pool: None) -> None:
    """Each set of credentials gets its own pooled client, and reuses it."""
    tenant = "a"

    def credentials_provider() -> BubbleCredentials:
        return BubbleCredentials(data_api_root_url=f"https://{tenant}.example.com", api_key=f"key-{tenant}")

    set_credentials_provider(credentials_provider)
    try:
        a1 = get_client()
        a2 = get_client()
        tenant = "b"
        b1 = get_client()

        assert a1 is a2
        assert a1 is not b1
        assert str(a1.base_url).startswith("https://a.example.com")
        assert str(b1.base_url).startswith("https://b.example.com")
        assert {
            ("https://a.example.com", "key-a"),
            ("https://b.example.com", "key-b"),
        } <= set(_clients)

        tenant = "a"
        assert get_client() is a1
    finally:
        configure(data_api_root_url="", api_key="")


async def test_close_clients_forgets_credentials(clean_client_pool: None) -> None:
    """Pool keys hold api keys, so closing must not leave empty entries behind."""
    configure(data_api_root_url="https://example.com", api_key="secret-key")
    get_client()
    assert ("https://example.com", "secret-key") in _clients

    await close_clients()

    assert _clients == {}


async def test_failed_get_client_leaves_no_pool_entry(clean_client_pool: None) -> None:
    """A rejected config must not pin its api key in the pool."""
    configure(data_api_root_url="", api_key="secret-key")

    with pytest.raises(ConfigurationError):
        get_client()

    assert _clients == {}


async def test_close_clients_drops_entries_for_every_tenant(clean_client_pool: None) -> None:
    """close_clients must clear the entry whichever tenant's credentials made it.

    Only covers closed clients. A server that never calls close_clients() keeps
    one client per tenant by design, see get_client.
    """
    tenant = "a"

    def credentials_provider() -> BubbleCredentials:
        return BubbleCredentials(data_api_root_url=f"https://{tenant}.example.com", api_key=f"key-{tenant}")

    set_credentials_provider(credentials_provider)
    try:
        for tenant in ("a", "b", "c"):
            get_client()
            await close_clients()
            assert _clients == {}, f"{tenant} left a pool entry behind"
    finally:
        configure(data_api_root_url="", api_key="")
