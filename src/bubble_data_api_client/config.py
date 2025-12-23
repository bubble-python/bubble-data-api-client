import typing
from collections.abc import Callable

_static_config: dict[str, str | None] = {
    "data_api_root_url": None,
    "api_key": None,
}

_config_provider: Callable[[], dict[str, str | None]] | None = None


def configure(data_api_root_url: str, api_key: str) -> None:
    """Configure the Bubble Data API client with static values."""
    global _config_provider
    _config_provider = None
    _static_config["data_api_root_url"] = data_api_root_url
    _static_config["api_key"] = api_key


def set_config_provider(provider: Callable[[], dict[str, str | None]]) -> None:
    """Set a provider function for dynamic configuration."""
    global _config_provider
    _config_provider = provider


def get_config() -> typing.Mapping[str, str | None]:
    """Get current configuration from provider if set, otherwise static config."""
    if _config_provider is not None:
        return _config_provider()
    return _static_config
