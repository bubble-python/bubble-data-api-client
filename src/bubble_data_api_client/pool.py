"""Client pool for efficient connection reuse."""

from __future__ import annotations

import asyncio
import atexit
import contextlib
import threading
import weakref
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

import httpx2

from bubble_data_api_client.config import BubbleCredentials, get_credentials
from bubble_data_api_client.exceptions import ConfigurationError
from bubble_data_api_client.http_client import httpx_client_factory

# type aliases
_CredentialsKey = tuple[str, str]
_LoopClientMap = weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, httpx2.AsyncClient]

# global client pool: credentials key → { loop → client }
# WeakKeyDictionary auto-removes entries when the loop is garbage collected. The
# outer key is not weak, see _drop_empty.
_clients: dict[_CredentialsKey, _LoopClientMap] = {}
_lock = threading.Lock()


def _drop_empty() -> None:
    """Forget pool keys with no live clients. Caller must hold _lock.

    Keys hold credentials verbatim, so an entry outliving its clients would pin that
    api key for the life of the process. An entry still serving a live client is kept,
    rotated-away credentials included, until close_clients() or loop GC empties it.
    """
    for key in [key for key, loop_clients in _clients.items() if not len(loop_clients)]:
        del _clients[key]


def _make_client_key(credentials: BubbleCredentials) -> _CredentialsKey:
    """Generate a unique key for client pooling based on credentials."""
    return (credentials["data_api_root_url"], credentials["api_key"])


def _create_client_from_credentials(credentials: BubbleCredentials) -> httpx2.AsyncClient:
    """Create a new httpx2 client from credentials."""
    base_url = credentials["data_api_root_url"]
    if not base_url:
        raise ConfigurationError("data_api_root_url")
    api_key = credentials["api_key"]
    if not api_key:
        raise ConfigurationError("api_key")
    return httpx_client_factory(base_url=base_url, api_key=api_key)


def get_client() -> httpx2.AsyncClient:
    """Get or create a client for the current credentials and event loop. Thread-safe.

    Each (credentials, event_loop) pair gets its own client, cached until
    close_clients() runs on that loop or the loop is garbage collected. Nothing
    evicts by age or count, so a process cycling through many credentials holds
    one client per distinct set until then.

    If called outside an async context (no running loop), returns a fresh
    uncached client as a fallback.
    """
    credentials = get_credentials()

    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        # no running loop - return uncached client
        # it will bind to whatever loop it's eventually used in
        return _create_client_from_credentials(credentials)

    key = _make_client_key(credentials)

    with _lock:
        loop_clients = _clients.get(key)

        # return cached client if it exists and is still open
        if loop_clients is not None:
            cached = loop_clients.get(current_loop)
            if cached is not None and not cached.is_closed:
                return cached

        # create first (it can raise), so bad credentials leave no key behind
        client = _create_client_from_credentials(credentials)

        if loop_clients is None:
            _drop_empty()
            loop_clients = _clients[key] = weakref.WeakKeyDictionary()
        loop_clients[current_loop] = client
        return client


async def close_clients() -> None:
    """Close all clients for the current event loop. Thread-safe. Safe to call multiple times.

    Only closes clients bound to the calling loop. Clients for other loops are
    left alone (they should be closed by their respective loops, or will be
    garbage collected when those loops die).
    """
    current_loop = asyncio.get_running_loop()

    with _lock:
        clients_to_close: list[httpx2.AsyncClient] = [
            loop_clients.pop(current_loop) for loop_clients in _clients.values() if current_loop in loop_clients
        ]
        _drop_empty()

    for client in clients_to_close:
        if not client.is_closed:
            # best-effort cleanup, continue with remaining clients
            with contextlib.suppress(Exception):
                await client.aclose()


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
        # collect all clients from all credentials/loop combinations
        clients_to_close: list[httpx2.AsyncClient] = []
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

    with contextlib.suppress(Exception):
        if running_loop is not None and running_loop.is_running():
            # edge case: event loop still running, schedule cleanup with timeout
            for client in clients_to_close:
                if not client.is_closed:
                    with contextlib.suppress(Exception):
                        future = asyncio.run_coroutine_threadsafe(client.aclose(), running_loop)
                        future.result(timeout=5.0)
        else:
            # no running loop, create one and close all clients
            async def _close_all() -> None:
                for client in clients_to_close:
                    if not client.is_closed:
                        with contextlib.suppress(Exception):
                            await client.aclose()

            with contextlib.suppress(Exception):
                asyncio.run(_close_all())


atexit.register(_atexit_cleanup)
