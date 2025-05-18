import pytest
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture
def test_url() -> str:
    return "https://example.com"


@pytest.fixture
def test_api_key() -> str:
    return "123"
