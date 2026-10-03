"""Configuration management for Bubble Data API credentials and settings.

configure() is the entry point. Each call changes only the options it is given:

    # credentials (connection identity), static or a provider
    configure(data_api_root_url="https://app.bubble.io/api/1.1/obj", api_key="...")
    configure(credentials_provider=lambda: credentials_for_current_tenant())

    # retry policy (request behavior), static or a provider
    configure(retry=tenacity.AsyncRetrying(...))
    configure(retry_provider=lambda: policy_for_current_tenant())

    # connection settings, apply to clients created afterwards
    configure(http2=True, keepalive_expiry=30.0)

Credentials and retry are kept apart because they have different lifetimes and
different readers: credentials pick the pooled client (once per operation), retry
is read per request. A provider is only called by the code path that needs its value.
The set_* functions set one slot at a time.
"""

from __future__ import annotations

import enum
from collections.abc import Callable
from typing import TYPE_CHECKING, TypedDict

from bubble_data_api_client.http_client import DEFAULT_KEEPALIVE_EXPIRY_SECONDS

if TYPE_CHECKING:
    import tenacity


class BubbleCredentials(TypedDict):
    """Connection identity. Part of the client pool key, see pool._make_client_key."""

    data_api_root_url: str
    api_key: str


type CredentialsProvider = Callable[[], BubbleCredentials]
type RetryProvider = Callable[[], tenacity.AsyncRetrying | None]

_credentials: BubbleCredentials = {"data_api_root_url": "", "api_key": ""}
_credentials_provider: CredentialsProvider | None = None
_retry: tenacity.AsyncRetrying | None = None
_retry_provider: RetryProvider | None = None
_http2: bool = False
_keepalive_expiry: float = DEFAULT_KEEPALIVE_EXPIRY_SECONDS


class _Unset(enum.Enum):
    """Marks a configure() argument as not passed. None can't, it means "no retry"."""

    UNSET = enum.auto()


def configure(
    data_api_root_url: str | _Unset = _Unset.UNSET,
    api_key: str | _Unset = _Unset.UNSET,
    retry: tenacity.AsyncRetrying | _Unset | None = _Unset.UNSET,
    *,
    credentials_provider: CredentialsProvider | _Unset = _Unset.UNSET,
    retry_provider: RetryProvider | _Unset = _Unset.UNSET,
    http2: bool | _Unset = _Unset.UNSET,
    keepalive_expiry: float | _Unset = _Unset.UNSET,
) -> None:
    """Set any mix of options. Omitted ones keep their current value.

    A static value clears the provider for the same slot and vice versa, as the
    set_* functions do. http2 and keepalive_expiry (seconds an idle connection is
    kept for reuse) are read when a pooled client is created, so they apply to
    clients created after the call.

    Raises:
        TypeError: Only one of data_api_root_url and api_key was passed, or a
            slot got both a static value and a provider.
    """
    global _http2, _keepalive_expiry
    url_passed = not isinstance(data_api_root_url, _Unset)
    key_passed = not isinstance(api_key, _Unset)
    if url_passed != key_passed:
        msg = "data_api_root_url and api_key must be passed together"
        raise TypeError(msg)
    if url_passed and not isinstance(credentials_provider, _Unset):
        msg = "pass static credentials or credentials_provider, not both"
        raise TypeError(msg)
    if not isinstance(retry, _Unset) and not isinstance(retry_provider, _Unset):
        msg = "pass retry or retry_provider, not both"
        raise TypeError(msg)

    # validated above, so a failed call leaves every slot untouched
    if not isinstance(data_api_root_url, _Unset) and not isinstance(api_key, _Unset):
        set_credentials(data_api_root_url=data_api_root_url, api_key=api_key)
    if not isinstance(credentials_provider, _Unset):
        set_credentials_provider(provider=credentials_provider)
    if not isinstance(retry, _Unset):
        set_retry(retry=retry)
    if not isinstance(retry_provider, _Unset):
        set_retry_provider(provider=retry_provider)
    if not isinstance(http2, _Unset):
        _http2 = http2
    if not isinstance(keepalive_expiry, _Unset):
        _keepalive_expiry = keepalive_expiry


def _reset() -> None:
    """Restore every option to its default. For tests."""
    global _http2, _keepalive_expiry
    set_credentials(data_api_root_url="", api_key="")
    set_retry(retry=None)
    _http2 = False
    _keepalive_expiry = DEFAULT_KEEPALIVE_EXPIRY_SECONDS


def set_credentials(data_api_root_url: str, api_key: str) -> None:
    """Set static credentials without touching retry. Clears any credentials provider."""
    global _credentials, _credentials_provider
    _credentials_provider = None
    _credentials = {"data_api_root_url": data_api_root_url, "api_key": api_key}


def set_credentials_provider(provider: CredentialsProvider) -> None:
    """Set a provider for dynamic credentials, called once per operation.

    Takes precedence over static credentials. Keep it cheap, a dict build
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


def get_http2() -> bool:
    """Get whether new pooled clients use HTTP/2."""
    return _http2


def get_keepalive_expiry() -> float:
    """Get how long new pooled clients keep idle connections, in seconds."""
    return _keepalive_expiry
