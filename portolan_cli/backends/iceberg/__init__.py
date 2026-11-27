"""Iceberg backend: lakehouse-grade versioning for Portolan catalogs."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from portolan_cli.backends.iceberg.backend import IcebergBackend

__version__ = "0.1.0"

__all__ = ["IcebergBackend", "__version__"]


def __getattr__(name: str) -> Any:
    """Load ``IcebergBackend`` on first access."""
    if name != "IcebergBackend":
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return importlib.import_module("portolan_cli.backends.iceberg.backend").IcebergBackend
