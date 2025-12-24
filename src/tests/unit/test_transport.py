import httpx

from bubble_data_api_client import http_client


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
