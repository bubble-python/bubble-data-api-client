"""HTTP client factory for Bubble Data API."""

import httpx

DEFAULT_USER_AGENT = "bubble-data-api-client"
DEFAULT_TIMEOUT_SECONDS = 20.0
DEFAULT_RETRY_COUNT = 3


def httpx_client_factory(
    *,
    base_url: str,
    api_key: str,
    user_agent: str = DEFAULT_USER_AGENT,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    retries: int = DEFAULT_RETRY_COUNT,
) -> httpx.AsyncClient:
    """Create a configured async HTTP client for the Bubble Data API."""
    return httpx.AsyncClient(
        base_url=base_url,
        headers={
            "Authorization": f"Bearer {api_key}",
            "User-Agent": user_agent,
        },
        transport=httpx.AsyncHTTPTransport(retries=retries),
        timeout=httpx.Timeout(timeout),
    )
