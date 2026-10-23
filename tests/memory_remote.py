"""An in-memory remote for push tests.

push writes to an obstore ``MemoryStore``, and the test reads the store back.
A test then asserts what the bucket holds, not which push function ran.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import obstore
from obstore.store import MemoryStore

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterator

    import pytest


@contextmanager
def memory_remote(prefix: str = "catalog") -> Iterator[MemoryStore]:
    """Send every ``setup_store`` call of push to one MemoryStore under ``prefix``."""
    store = MemoryStore()
    with patch("portolan_cli.sync.push.setup_store", return_value=(store, prefix)):
        yield store


def stored_keys(store: MemoryStore) -> set[str]:
    """Return every key in ``store``."""
    return {str(meta["path"]) for page in obstore.list(store) for meta in page}


def stored_json(store: MemoryStore, key: str) -> Any:
    """Return the parsed JSON object at ``key``."""
    return json.loads(bytes(obstore.get(store, key).bytes()))


def record_puts(
    monkeypatch: pytest.MonkeyPatch,
    before: Callable[[str], Awaitable[None]] | None = None,
) -> list[str]:
    """Record the key of each put_async, after ``before(key)`` runs.

    ``before`` can raise to inject a fault, or sleep to hold the upload open.
    """
    keys: list[str] = []
    real_put_async = obstore.put_async

    async def put_async(store: Any, key: str, *args: Any, **kwargs: Any) -> Any:
        keys.append(key)
        if before is not None:
            await before(key)
        return await real_put_async(store, key, *args, **kwargs)

    monkeypatch.setattr(obstore, "put_async", put_async)
    return keys
