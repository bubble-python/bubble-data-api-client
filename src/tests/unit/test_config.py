"""Tests for bubble_data_api_client.config module."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import tenacity

from bubble_data_api_client.config import (
    BubbleConfig,
    BubbleCredentials,
    configure,
    get_config,
    get_credentials,
    get_retry,
    set_config_provider,
    set_credentials,
    set_credentials_provider,
    set_retry,
    set_retry_provider,
)

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture(autouse=True)
def reset_config() -> Iterator[None]:
    """Config lives in module globals, so restore defaults around every test."""
    yield
    configure(data_api_root_url="", api_key="")


def test_configure_sets_static_values() -> None:
    """configure should set credentials and retry."""
    policy = tenacity.AsyncRetrying(stop=tenacity.stop_after_attempt(2))
    configure(data_api_root_url="https://static.example.com", api_key="static-key", retry=policy)

    assert get_credentials() == {"data_api_root_url": "https://static.example.com", "api_key": "static-key"}
    assert get_retry() is policy


def test_configure_omitting_retry_clears_it() -> None:
    """configure replaces all configuration, so an omitted retry means no retry."""
    configure(
        data_api_root_url="https://static.example.com",
        api_key="static-key",
        retry=tenacity.AsyncRetrying(),
    )
    configure(data_api_root_url="https://static.example.com", api_key="static-key")

    assert get_retry() is None


def test_credentials_provider_overrides_static() -> None:
    """A credentials provider should take precedence over configure values."""
    configure(data_api_root_url="https://static.example.com", api_key="static-key")
    set_credentials_provider(
        lambda: BubbleCredentials(data_api_root_url="https://dynamic.example.com", api_key="dynamic-key")
    )

    assert get_credentials()["data_api_root_url"] == "https://dynamic.example.com"
    assert get_credentials()["api_key"] == "dynamic-key"


def test_configure_clears_providers() -> None:
    """configure should drop both providers."""
    set_credentials_provider(
        lambda: BubbleCredentials(data_api_root_url="https://dynamic.example.com", api_key="dynamic-key")
    )
    set_retry_provider(lambda: tenacity.AsyncRetrying())

    configure(data_api_root_url="https://new-static.example.com", api_key="new-static-key")

    assert get_credentials()["data_api_root_url"] == "https://new-static.example.com"
    assert get_retry() is None


def test_set_credentials_leaves_retry_alone() -> None:
    """Static credentials must be reachable without discarding a retry provider."""
    policy = tenacity.AsyncRetrying()
    set_retry_provider(lambda: policy)
    set_credentials(data_api_root_url="https://static.example.com", api_key="static-key")

    assert get_credentials()["data_api_root_url"] == "https://static.example.com"
    assert get_retry() is policy


def test_set_credentials_clears_credentials_provider() -> None:
    """Static credentials should win over a previously set provider."""
    set_credentials_provider(
        lambda: BubbleCredentials(data_api_root_url="https://dynamic.example.com", api_key="dynamic-key")
    )
    set_credentials(data_api_root_url="https://static.example.com", api_key="static-key")

    assert get_credentials()["data_api_root_url"] == "https://static.example.com"


def test_set_retry_leaves_credentials_alone() -> None:
    """set_retry exists so a credentials provider can still get a retry policy."""
    set_credentials_provider(
        lambda: BubbleCredentials(data_api_root_url="https://dynamic.example.com", api_key="dynamic-key")
    )
    policy = tenacity.AsyncRetrying()
    set_retry(retry=policy)

    assert get_retry() is policy
    assert get_credentials()["data_api_root_url"] == "https://dynamic.example.com"


def test_set_retry_clears_retry_provider() -> None:
    """A static retry should win over a previously set provider."""
    set_retry_provider(lambda: tenacity.AsyncRetrying())
    set_retry(retry=None)

    assert get_retry() is None


def test_retry_provider_overrides_static() -> None:
    """A retry provider should take precedence over the static value."""
    policy = tenacity.AsyncRetrying()
    configure(data_api_root_url="https://static.example.com", api_key="static-key")
    set_retry_provider(lambda: policy)

    assert get_retry() is policy


def test_providers_resolve_independently() -> None:
    """Reading one slot must not invoke the other slot's provider."""
    credentials_calls = 0
    retry_calls = 0

    def credentials_provider() -> BubbleCredentials:
        nonlocal credentials_calls
        credentials_calls += 1
        return BubbleCredentials(data_api_root_url="https://dynamic.example.com", api_key="dynamic-key")

    def retry_provider() -> tenacity.AsyncRetrying | None:
        nonlocal retry_calls
        retry_calls += 1
        return None

    set_credentials_provider(credentials_provider)
    set_retry_provider(retry_provider)

    get_credentials()
    assert (credentials_calls, retry_calls) == (1, 0)

    get_retry()
    assert (credentials_calls, retry_calls) == (1, 1)


def test_set_config_provider_is_deprecated_but_works() -> None:
    """The legacy single provider should still drive both slots."""
    policy = tenacity.AsyncRetrying()

    def provider() -> BubbleConfig:
        return BubbleConfig(
            data_api_root_url="https://legacy.example.com",
            api_key="legacy-key",
            retry=policy,
        )

    with pytest.warns(DeprecationWarning, match="set_config_provider is deprecated"):
        set_config_provider(provider=provider)

    assert get_credentials()["data_api_root_url"] == "https://legacy.example.com"
    assert get_retry() is policy


def test_set_config_provider_without_retry() -> None:
    """A legacy provider that omits retry should resolve to no retry."""
    with pytest.warns(DeprecationWarning):
        set_config_provider(
            provider=lambda: BubbleConfig(data_api_root_url="https://legacy.example.com", api_key="legacy-key")
        )

    assert get_retry() is None


def test_get_config_is_deprecated_but_composes_both_slots() -> None:
    """The legacy getter should still return one merged dict."""
    policy = tenacity.AsyncRetrying()
    configure(data_api_root_url="https://static.example.com", api_key="static-key", retry=policy)

    with pytest.warns(DeprecationWarning, match="get_config is deprecated"):
        config = get_config()

    assert config == {
        "data_api_root_url": "https://static.example.com",
        "api_key": "static-key",
        "retry": policy,
    }
