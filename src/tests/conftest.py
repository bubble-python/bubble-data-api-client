# load environment variables from a .env file for local testing
from dotenv import load_dotenv

load_dotenv()

from collections.abc import AsyncGenerator  # noqa: E402

import pytest  # noqa: E402

from bubble_data_api_client import configure, settings  # noqa: E402
from bubble_data_api_client.client import raw_client  # noqa: E402


@pytest.fixture
def test_url() -> str:
    return "https://example.com"


@pytest.fixture
def test_api_key() -> str:
    return "123"


@pytest.fixture(autouse=True)
def auto_configure_client():
    """Automatically configure the client for every test run."""
    if not settings.BUBBLE_DATA_API_ROOT_URL:
        raise RuntimeError("BUBBLE_DATA_API_ROOT_URL")
    if not settings.BUBBLE_API_KEY:
        raise RuntimeError("BUBBLE_API_KEY")

    configure(
        data_api_root_url=settings.BUBBLE_DATA_API_ROOT_URL,
        api_key=settings.BUBBLE_API_KEY,
    )


@pytest.fixture
async def bubble_raw_client() -> AsyncGenerator[raw_client.RawClient]:
    """Provide a raw client for testing the low-level API."""
    if not settings.BUBBLE_DATA_API_ROOT_URL:
        raise RuntimeError("BUBBLE_DATA_API_ROOT_URL")
    if not settings.BUBBLE_API_KEY:
        raise RuntimeError("BUBBLE_API_KEY")

    async with raw_client.RawClient(
        data_api_root_url=settings.BUBBLE_DATA_API_ROOT_URL,
        api_key=settings.BUBBLE_API_KEY,
    ) as client_instance:
        yield client_instance


@pytest.fixture
def typename() -> str:
    """Return a test typename for integration tests."""
    # this typename should exist in the bubble app and should allow CRUD operations
    return "IntegrationTest"
