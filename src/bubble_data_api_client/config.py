from collections.abc import Callable
from typing import NotRequired, TypedDict, TypeIs

import tenacity


class _NotSet:
    """Sentinel for configuration values that were not provided."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "NOT_SET"


NOT_SET = _NotSet()
type NotSetType = _NotSet


def is_set[T](value: T | NotSetType) -> TypeIs[T]:
    """Type guard for checking if value is not the NOT_SET sentinel."""
    return value is not NOT_SET


class BubbleConfig(TypedDict):
    """Configuration for Bubble Data API client."""

    data_api_root_url: str
    api_key: str
    retry: NotRequired[tenacity.AsyncRetrying | None]


type ConfigProvider = Callable[[], BubbleConfig]

_static_config: BubbleConfig = {"data_api_root_url": "", "api_key": ""}
_config_provider: ConfigProvider | None = None


def configure(
    data_api_root_url: str,
    api_key: str,
    retry: tenacity.AsyncRetrying | None | NotSetType = NOT_SET,
) -> None:
    """Configure the Bubble Data API client with static values."""
    global _config_provider, _static_config
    _config_provider = None
    _static_config = {
        "data_api_root_url": data_api_root_url,
        "api_key": api_key,
    }
    if is_set(retry):
        _static_config["retry"] = retry


def set_config_provider(provider: ConfigProvider) -> None:
    """Set a provider function for dynamic configuration."""
    global _config_provider
    _config_provider = provider


def get_config() -> BubbleConfig:
    """Get current configuration from provider if set, otherwise static config."""
    if _config_provider is not None:
        return _config_provider()
    return _static_config
