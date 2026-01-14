"""Client pool for efficient connection reuse."""

import asyncio
import atexit
import threading
import weakref
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx

from bubble_data_api_client.config import BubbleConfig, get_config
from bubble_data_api_client.exceptions import ConfigurationError
from bubble_data_api_client.http_client import httpx_client_factory

# type aliases
_ConfigKey = tuple[str, str]
_LoopClientMap = weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, httpx.AsyncClient]

# global client pool: config_key → { loop → client }
# WeakKeyDictionary auto-removes entries when the loop is garbage collected
_clients: dict[_ConfigKey, _LoopClientMap] = {}
_lock = threading.Lock()


def _make_client_key(config: BubbleConfig) -> _ConfigKey:
    """Generate a unique key for client pooling based on config."""
    return (config["data_api_root_url"], config["api_key"])


def _create_client_from_config(config: BubbleConfig) -> httpx.AsyncClient:
    """Create a new httpx client from config."""
    base_url = config["data_api_root_url"]
    if not base_url:
        raise ConfigurationError("data_api_root_url")
    api_key = config["api_key"]
    if not api_key:
        raise ConfigurationError("api_key")
    return httpx_client_factory(base_url=base_url, api_key=api_key)


def get_client() -> httpx.AsyncClient:
    """Get or create a client for the current config and event loop. Thread-safe.

    Each (config, event_loop) pair gets its own client. When an event loop is
    garbage collected, its associated clients are automatically removed.

    If called outside an async context (no running loop), returns a fresh
    uncached client as a fallback.
    """
    config = get_config()

    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        # no running loop - return uncached client
        # it will bind to whatever loop it's eventually used in
        return _create_client_from_config(config)

    key = _make_client_key(config)

    with _lock:
        if key not in _clients:
            _clients[key] = weakref.WeakKeyDictionary()

        loop_clients = _clients[key]

        # return cached client if it exists and is still open
        if current_loop in loop_clients and not loop_clients[current_loop].is_closed:
            return loop_clients[current_loop]

        # create new client (either missing or was closed externally)
        loop_clients[current_loop] = _create_client_from_config(config)
        return loop_clients[current_loop]


async def close_clients() -> None:
    """Close all clients for the current event loop. Thread-safe. Safe to call multiple times.

    Only closes clients bound to the calling loop. Clients for other loops are
    left alone (they should be closed by their respective loops, or will be
    garbage collected when those loops die).
    """
    current_loop = asyncio.get_running_loop()

    with _lock:
        clients_to_close: list[httpx.AsyncClient] = [
            loop_clients.pop(current_loop) for loop_clients in _clients.values() if current_loop in loop_clients
        ]

    for client in clients_to_close:
        try:
            await client.aclose()
        except Exception:
            pass  # best-effort cleanup, continue with remaining clients


@asynccontextmanager
async def client_scope() -> AsyncIterator[None]:
    """Scope that ensures close_clients() is called on exit."""
    try:
        yield
    finally:
        await close_clients()


def _atexit_cleanup() -> None:
    """Best-effort cleanup of all clients at interpreter exit."""
    with _lock:
        # collect all clients from all config/loop combinations
        clients_to_close: list[httpx.AsyncClient] = []
        for loop_clients in _clients.values():
            clients_to_close.extend(loop_clients.values())
        _clients.clear()

    if not clients_to_close:
        return

    # detect loop state to choose cleanup strategy
    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        running_loop = None

    try:
        if running_loop is not None and running_loop.is_running():
            # edge case: event loop still running, schedule cleanup with timeout
            for client in clients_to_close:
                future = asyncio.run_coroutine_threadsafe(client.aclose(), running_loop)
                try:
                    future.result(timeout=5.0)
                except Exception:
                    pass
        else:
            # no running loop, create one and close all clients
            async def _close_all() -> None:
                for client in clients_to_close:
                    try:
                        await client.aclose()
                    except Exception:
                        pass

            try:
                asyncio.run(_close_all())
            except Exception:
                pass
    except Exception:
        pass


atexit.register(_atexit_cleanup)
