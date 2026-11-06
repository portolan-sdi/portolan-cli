"""Iceberg backend: version history as Iceberg table snapshots.

DuckDB writes one format version 3 table per collection through an Iceberg
REST catalog. ``rest`` calls the catalog for the version record, rollback, and
snapshot expiry. It integrates with portolan-cli as a built-in plugin (#947).

``hooks`` registers the backend and ``cli`` holds ``version rollback``,
``version prune``, and ``version refresh``. Neither imports DuckDB or httpx,
so the package root imports nothing heavy. ``IcebergBackend`` loads on first
access.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from portolan_cli.backends.iceberg.backend import IcebergBackend

__version__ = "0.1.0"

__all__ = ["__version__", "IcebergBackend"]


def __getattr__(name: str) -> Any:
    """Load ``IcebergBackend`` on first access."""
    if name != "IcebergBackend":
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return importlib.import_module("portolan_cli.backends.iceberg.backend").IcebergBackend
