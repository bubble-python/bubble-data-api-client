from collections.abc import AsyncGenerator

import pytest

from bubble_data_api_client import configure
from bubble_data_api_client.pool import close_clients


@pytest.fixture
def test_url() -> str:
    return "https://example.com"


@pytest.fixture
def test_api_key() -> str:
    return "123"


@pytest.fixture
async def clean_client_pool() -> AsyncGenerator[None]:
    """Ensure client pool is clean before and after each test."""
    await close_clients()
    yield
    await close_clients()


@pytest.fixture
def configured_client(clean_client_pool: None, test_url: str, test_api_key: str) -> None:
    """Configure the client for testing."""
    configure(
        data_api_root_url=test_url,
        api_key=test_api_key,
        retry=None,
    )
