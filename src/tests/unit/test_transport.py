import httpx

from bubble_data_api_client import transport


def test_httpx_client_factory(test_url: str, test_api_key: str):
    """Test that http client is instantiated."""
    httpx_client = transport.httpx_client_factory(
        base_url=test_url,
        api_key=test_api_key,
    )
    assert isinstance(httpx_client, httpx.AsyncClient)
    assert httpx_client.base_url == test_url
    assert httpx_client.headers["Authorization"] == f"Bearer {test_api_key}"
