"""Tests for bubble_data_api_client.config module."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import tenacity

from bubble_data_api_client.config import (
    BubbleCredentials,
    configure,
    get_credentials,
    get_http2,
    get_keepalive_expiry,
    get_retry,
    set_credentials,
    set_credentials_provider,
    set_retry,
    set_retry_provider,
)
from bubble_data_api_client.http_client import DEFAULT_KEEPALIVE_EXPIRY_SECONDS

if TYPE_CHECKING:
    from bubble_data_api_client.config import CredentialsProvider

STATIC = BubbleCredentials(data_api_root_url="https://static.example.com", api_key="static-key")
DYNAMIC = BubbleCredentials(data_api_root_url="https://dynamic.example.com", api_key="dynamic-key")


def dynamic_provider() -> BubbleCredentials:
    return DYNAMIC


def test_configure_sets_static_values() -> None:
    """configure should set credentials and retry."""
    policy = tenacity.AsyncRetrying(stop=tenacity.stop_after_attempt(2))
    configure(data_api_root_url="https://static.example.com", api_key="static-key", retry=policy)

    assert get_credentials() == {"data_api_root_url": "https://static.example.com", "api_key": "static-key"}
    assert get_retry() is policy


def test_configure_omitting_retry_keeps_it() -> None:
    """configure only changes what it is given."""
    policy = tenacity.AsyncRetrying()
    configure(data_api_root_url="https://static.example.com", api_key="static-key", retry=policy)
    configure(data_api_root_url="https://other.example.com", api_key="other-key")

    assert get_retry() is policy


def test_configure_http2_only_leaves_other_slots() -> None:
    policy = tenacity.AsyncRetrying()
    configure(credentials_provider=dynamic_provider, retry=policy)
    configure(http2=True)

    assert get_http2() is True
    assert get_credentials() == DYNAMIC
    assert get_retry() is policy


def test_http2_defaults_off() -> None:
    assert get_http2() is False


def test_keepalive_expiry_defaults_to_factory_default() -> None:
    assert get_keepalive_expiry() == DEFAULT_KEEPALIVE_EXPIRY_SECONDS


def test_configure_keepalive_expiry_only_leaves_other_slots() -> None:
    configure(data_api_root_url="https://static.example.com", api_key="static-key", http2=True)
    configure(keepalive_expiry=30.0)

    assert get_keepalive_expiry() == 30.0
    assert get_credentials() == STATIC
    assert get_http2() is True


def test_configure_with_no_arguments_changes_nothing() -> None:
    policy = tenacity.AsyncRetrying()
    configure(data_api_root_url="https://static.example.com", api_key="static-key", retry=policy, http2=True)
    configure()

    assert get_credentials() == STATIC
    assert get_retry() is policy
    assert get_http2() is True


def test_configure_credentials_provider() -> None:
    configure(data_api_root_url="https://static.example.com", api_key="static-key")
    configure(credentials_provider=dynamic_provider)

    assert get_credentials() == DYNAMIC


def test_configure_retry_provider_clears_static_retry() -> None:
    static_policy = tenacity.AsyncRetrying()
    dynamic_policy = tenacity.AsyncRetrying()
    configure(retry=static_policy)
    configure(retry_provider=lambda: dynamic_policy)

    assert get_retry() is dynamic_policy


def test_configure_static_retry_clears_retry_provider() -> None:
    configure(retry_provider=lambda: tenacity.AsyncRetrying())
    configure(retry=None)

    assert get_retry() is None


def test_configure_rejects_half_credentials() -> None:
    with pytest.raises(TypeError, match="must be passed together"):
        configure(data_api_root_url="https://static.example.com")
    with pytest.raises(TypeError, match="must be passed together"):
        configure(api_key="static-key")


def test_configure_rejects_static_and_provider_credentials() -> None:
    provider: CredentialsProvider = dynamic_provider
    with pytest.raises(TypeError, match="credentials_provider, not both"):
        configure(data_api_root_url="https://static.example.com", api_key="static-key", credentials_provider=provider)


def test_configure_rejects_static_and_provider_retry() -> None:
    with pytest.raises(TypeError, match="retry_provider, not both"):
        configure(retry=None, retry_provider=lambda: None)


def test_rejected_configure_changes_nothing() -> None:
    """Validation runs before any slot is written."""
    policy = tenacity.AsyncRetrying()
    configure(data_api_root_url="https://static.example.com", api_key="static-key", retry=policy)

    with pytest.raises(TypeError):
        configure(
            data_api_root_url="https://other.example.com",
            api_key="other-key",
            retry=None,
            retry_provider=lambda: None,
            http2=True,
            keepalive_expiry=30.0,
        )

    assert get_credentials() == STATIC
    assert get_retry() is policy
    assert get_http2() is False
    assert get_keepalive_expiry() == DEFAULT_KEEPALIVE_EXPIRY_SECONDS


def test_credentials_provider_overrides_static() -> None:
    """A credentials provider should take precedence over configure values."""
    configure(data_api_root_url="https://static.example.com", api_key="static-key")
    set_credentials_provider(
        lambda: BubbleCredentials(data_api_root_url="https://dynamic.example.com", api_key="dynamic-key")
    )

    assert get_credentials()["data_api_root_url"] == "https://dynamic.example.com"
    assert get_credentials()["api_key"] == "dynamic-key"


def test_configure_static_credentials_clear_only_credentials_provider() -> None:
    """Static credentials replace a credentials provider and leave a retry provider in place."""
    policy = tenacity.AsyncRetrying()
    set_credentials_provider(dynamic_provider)
    set_retry_provider(lambda: policy)

    configure(data_api_root_url="https://new-static.example.com", api_key="new-static-key")

    assert get_credentials()["data_api_root_url"] == "https://new-static.example.com"
    assert get_retry() is policy


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
