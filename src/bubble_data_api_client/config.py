import typing

_config: dict[str, typing.Any] = {
    "data_api_root_url": None,
    "api_key": None,
}


def configure(data_api_root_url: str, api_key: str) -> None:
    """Configure the Bubble Data API client."""
    _config["data_api_root_url"] = data_api_root_url
    _config["api_key"] = api_key


def get_config() -> typing.Mapping[str, str | None]:
    """Get the current configuration."""
    return _config
