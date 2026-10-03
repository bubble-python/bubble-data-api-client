"""HTTP client factory for Bubble Data API."""

import httpx2

DEFAULT_USER_AGENT = "bubble-data-api-client"
DEFAULT_TIMEOUT_SECONDS = 20.0
DEFAULT_RETRY_COUNT = 3
DEFAULT_MAX_CONNECTIONS = 100
DEFAULT_MAX_KEEPALIVE_CONNECTIONS = 20
DEFAULT_KEEPALIVE_EXPIRY_SECONDS = 5.0


def httpx_client_factory(
    *,
    base_url: str,
    api_key: str,
    user_agent: str = DEFAULT_USER_AGENT,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    retries: int = DEFAULT_RETRY_COUNT,
    http2: bool = False,
    keepalive_expiry: float = DEFAULT_KEEPALIVE_EXPIRY_SECONDS,
) -> httpx2.AsyncClient:
    """Create a configured async HTTP client for the Bubble Data API."""
    return httpx2.AsyncClient(
        base_url=base_url,
        headers={
            "Authorization": f"Bearer {api_key}",
            "User-Agent": user_agent,
        },
        transport=httpx2.AsyncHTTPTransport(
            retries=retries,
            http2=http2,
            limits=httpx2.Limits(
                max_connections=DEFAULT_MAX_CONNECTIONS,
                max_keepalive_connections=DEFAULT_MAX_KEEPALIVE_CONNECTIONS,
                keepalive_expiry=keepalive_expiry,
            ),
        ),
        timeout=httpx2.Timeout(timeout),
    )
