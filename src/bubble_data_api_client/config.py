"""Configuration management for Bubble Data API credentials and settings.

Two independent slots, each settable as a constant or as a provider function:

    # credentials (connection identity)
    set_credentials(data_api_root_url="https://app.bubble.io/api/1.1/obj", api_key="...")
    set_credentials_provider(lambda: credentials_for_current_tenant())

    # retry policy (request behavior)
    set_retry(tenacity.AsyncRetrying(...))
    set_retry_provider(lambda: policy_for_current_tenant())

    # or set both static values at once
    configure(data_api_root_url="...", api_key="...", retry=...)

They are kept apart because they have different lifetimes and different readers:
credentials pick the pooled client (once per operation), retry is read per request.
A provider is only called by the code path that actually needs its value.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable
from typing import TYPE_CHECKING, NotRequired, TypedDict

if TYPE_CHECKING:
    import tenacity


class BubbleCredentials(TypedDict):
    """Connection identity. Also the client pool key, see pool._make_client_key."""

    data_api_root_url: str
    api_key: str


class BubbleConfig(BubbleCredentials):
    """Credentials plus retry policy. Only used by the deprecated config provider."""

    retry: NotRequired[tenacity.AsyncRetrying | None]


type CredentialsProvider = Callable[[], BubbleCredentials]
type RetryProvider = Callable[[], tenacity.AsyncRetrying | None]
type ConfigProvider = Callable[[], BubbleConfig]

_credentials: BubbleCredentials = {"data_api_root_url": "", "api_key": ""}
_credentials_provider: CredentialsProvider | None = None
_retry: tenacity.AsyncRetrying | None = None
_retry_provider: RetryProvider | None = None


def configure(
    data_api_root_url: str,
    api_key: str,
    retry: tenacity.AsyncRetrying | None = None,
) -> None:
    """Set both slots to static values, clearing any providers.

    Convenience for the common case. Set the slots separately when only one of
    them is dynamic.
    """
    set_credentials(data_api_root_url=data_api_root_url, api_key=api_key)
    set_retry(retry=retry)


def set_credentials(data_api_root_url: str, api_key: str) -> None:
    """Set static credentials without touching retry. Clears any credentials provider."""
    global _credentials, _credentials_provider
    _credentials_provider = None
    _credentials = {"data_api_root_url": data_api_root_url, "api_key": api_key}


def set_credentials_provider(provider: CredentialsProvider) -> None:
    """Set a provider for dynamic credentials, called once per operation.

    Takes precedence over values set by configure(). Keep it cheap, a dict build
    or a ContextVar read, not a network call.
    """
    global _credentials_provider
    _credentials_provider = provider


def set_retry(retry: tenacity.AsyncRetrying | None) -> None:
    """Set the retry policy without touching credentials. Clears any retry provider.

    Transport copies the policy per request (see transport.request), so it has to
    be reconstructible from the standard AsyncRetrying parameters. A subclass
    carrying extra constructor state loses it, same as under tenacity's @retry.
    """
    global _retry, _retry_provider
    _retry_provider = None
    _retry = retry


def set_retry_provider(provider: RetryProvider) -> None:
    """Set a provider for a dynamic retry policy, called once per request."""
    global _retry_provider
    _retry_provider = provider


def get_credentials() -> BubbleCredentials:
    """Get credentials from the provider if set, otherwise a copy of the static values."""
    if _credentials_provider is not None:
        return _credentials_provider()
    # copy: handing out _credentials itself lets a caller rewrite global config
    return _credentials.copy()


def get_retry() -> tenacity.AsyncRetrying | None:
    """Get the retry policy from the provider if set, otherwise the static value."""
    if _retry_provider is not None:
        return _retry_provider()
    return _retry


def set_config_provider(provider: ConfigProvider) -> None:
    """Set one provider for both credentials and retry.

    Deprecated: use set_credentials_provider plus set_retry or set_retry_provider.
    Both slots resolve through it, so an operation of N requests calls it 1 + N
    times: once for the pooled client, then once per request for the retry policy.

    It fills both slots, so replacing only the credentials one later leaves retry
    still resolving through this provider, which then runs per request in a
    context it may no longer suit. Call set_retry too to finish migrating off it.
    """
    warnings.warn(
        "set_config_provider is deprecated, use set_credentials_provider with set_retry or set_retry_provider instead",
        DeprecationWarning,
        stacklevel=2,
    )
    set_credentials_provider(provider)
    set_retry_provider(lambda: provider().get("retry"))


def get_config() -> BubbleConfig:
    """Get credentials and retry as one dict.

    Deprecated: use get_credentials or get_retry so only the needed provider runs.
    """
    warnings.warn(
        "get_config is deprecated, use get_credentials or get_retry instead",
        DeprecationWarning,
        stacklevel=2,
    )
    credentials = get_credentials()
    return {
        "data_api_root_url": credentials["data_api_root_url"],
        "api_key": credentials["api_key"],
        "retry": get_retry(),
    }
