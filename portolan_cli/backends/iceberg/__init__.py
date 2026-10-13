"""Iceberg backend: lakehouse-grade versioning for Portolan catalogs.

This subpackage provides enterprise-tier versioning for geospatial catalogs using
Apache Iceberg for vector/tabular data (GeoParquet format).

It integrates with portolan-cli as a built-in plugin (#947), providing:
- ACID transactions for concurrent writes
- Version rollback and snapshot pruning
- Automated schema evolution detection

Install with: pip install portolan-cli[iceberg]

``hooks`` registers the backend and ``cli`` holds ``version rollback`` and
``version prune``. Both load without pyiceberg, so the package root imports
nothing heavy. ``IcebergBackend`` loads on first access and needs Python 3.11+
and the extra.

Future: Icechunk support for array/raster data (COG, NetCDF, Zarr) is planned.
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
