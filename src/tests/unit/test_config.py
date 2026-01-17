"""Tests for bubble_data_api_client.config module."""

from bubble_data_api_client.config import (
    NOT_SET,
    configure,
    get_config,
    set_config_provider,
)


def test_not_set_repr():
    """NOT_SET sentinel should have readable repr."""
    assert repr(NOT_SET) == "NOT_SET"


def test_set_config_provider():
    """Config provider should override static config."""
    # set static config first
    configure(data_api_root_url="https://static.example.com", api_key="static-key")
    assert get_config()["data_api_root_url"] == "https://static.example.com"

    # set provider - should override static config
    set_config_provider(lambda: {"data_api_root_url": "https://dynamic.example.com", "api_key": "dynamic-key"})
    assert get_config()["data_api_root_url"] == "https://dynamic.example.com"
    assert get_config()["api_key"] == "dynamic-key"

    # calling configure should clear the provider
    configure(data_api_root_url="https://new-static.example.com", api_key="new-static-key")
    assert get_config()["data_api_root_url"] == "https://new-static.example.com"
