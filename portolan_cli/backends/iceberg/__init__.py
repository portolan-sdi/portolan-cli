"""Iceberg backend: lakehouse-grade versioning for Portolan catalogs.

This subpackage provides enterprise-tier versioning for geospatial catalogs using
Apache Iceberg for vector/tabular data (GeoParquet format).

It integrates with portolan-cli as an optional built-in backend, providing:
- ACID transactions for concurrent writes
- Version rollback and snapshot pruning
- Automated schema evolution detection

Install with: pip install portolan-cli[iceberg]

Future: Icechunk support for array/raster data (COG, NetCDF, Zarr) is planned.

"""

from __future__ import annotations

import sys

if sys.version_info < (3, 11):
    raise ImportError(
        "The Iceberg backend requires Python 3.11+. "
        f"You are running Python {sys.version_info.major}.{sys.version_info.minor}."
    )

__version__ = "0.1.0"

from portolan_cli.backends.iceberg.backend import IcebergBackend

__all__ = ["__version__", "IcebergBackend"]
