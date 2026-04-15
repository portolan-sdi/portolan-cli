"""Iceberg backend: lakehouse-grade versioning for Portolan catalogs.

This subpackage provides enterprise-tier versioning for geospatial catalogs using
Apache Iceberg for vector/tabular data (GeoParquet format).

It integrates with portolan-cli as an optional built-in backend, providing:
- ACID transactions for concurrent writes
- Version rollback and snapshot pruning
- Automated schema evolution detection

Install with: pip install portolan-cli[iceberg]

Future: Icechunk support for array/raster data (COG, NetCDF, Zarr) is planned
per ADR-0015.

See: https://github.com/portolan-sdi/portolan-cli/blob/main/context/shared/adr/0015-two-tier-versioning-architecture.md
See: ADR-0041 (Iceberg as Optional Extra)
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
