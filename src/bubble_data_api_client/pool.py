"""Client pool for efficient connection reuse."""

import asyncio
import atexit
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx

from bubble_data_api_client.config import BubbleConfig, get_config
from bubble_data_api_client.exceptions import ConfigurationError
from bubble_data_api_client.http_client import httpx_client_factory

# global client pool keyed by config
_clients: dict[tuple[str, str], httpx.AsyncClient] = {}
_lock = threading.Lock()


def _make_client_key(config: BubbleConfig) -> tuple[str, str]:
    """Generate a unique key for client pooling based on config."""
    return (config["data_api_root_url"], config["api_key"])


def get_client() -> httpx.AsyncClient:
    """Get or create a client for the current config. Thread-safe."""
    config = get_config()
    key = _make_client_key(config)

    # fast path: no lock if client exists
    if key in _clients:
        return _clients[key]

    # slow path: acquire lock for creation
    with _lock:
        # double-check after acquiring lock
        if key not in _clients:
            base_url = config["data_api_root_url"]
            if not base_url:
                raise ConfigurationError("data_api_root_url")
            api_key = config["api_key"]
            if not api_key:
                raise ConfigurationError("api_key")
            _clients[key] = httpx_client_factory(base_url=base_url, api_key=api_key)
        return _clients[key]


async def close_clients() -> None:
    """Close all clients in the pool. Thread-safe. Safe to call multiple times."""
    with _lock:
        clients_to_close = list(_clients.values())
        _clients.clear()

    for client in clients_to_close:
        await client.aclose()


@asynccontextmanager
async def client_scope() -> AsyncIterator[None]:
    """Scope that ensures close_clients() is called on exit."""
    try:
        yield
    finally:
        await close_clients()


def _atexit_cleanup() -> None:
    """Best-effort cleanup at interpreter exit."""
    with _lock:
        clients_to_close = list(_clients.values())
        _clients.clear()

    if not clients_to_close:
        return

    # check if there's already a running loop
    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        running_loop = None

    try:
        if running_loop is not None:
            # loop still running at atexit, schedule cleanup tasks
            for client in clients_to_close:
                running_loop.create_task(client.aclose())
        else:
            # no running loop, create one for cleanup
            loop = asyncio.new_event_loop()
            for client in clients_to_close:
                loop.run_until_complete(client.aclose())
            loop.close()
    except Exception:
        pass


atexit.register(_atexit_cleanup)
