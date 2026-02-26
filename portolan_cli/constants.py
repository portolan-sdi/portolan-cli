"""Shared constants for the Portolan CLI.

This module contains constants that are used across multiple modules
to avoid duplication and ensure consistency.
"""

from __future__ import annotations

# Extensions we recognize as geospatial files
GEOSPATIAL_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".geojson",
        ".parquet",
        ".shp",
        ".gpkg",
        ".fgb",
        ".csv",
        ".tif",
        ".tiff",
        ".jp2",
        ".pmtiles",
    }
)

# Cloud-native parquet extension
PARQUET_EXTENSION: str = ".parquet"

# Sidecar file patterns by primary file extension
SIDECAR_PATTERNS: dict[str, list[str]] = {
    ".shp": [".dbf", ".shx", ".prj", ".cpg", ".sbn", ".sbx", ".qix"],
    ".tif": [".tfw", ".xml", ".aux.xml", ".ovr"],
    ".tiff": [".tfw", ".xml", ".aux.xml", ".ovr"],
    ".img": [".ige", ".rrd", ".rde", ".xml"],
}

# Change detection constants (per ADR-0017)
# 2 second tolerance for NFS/CIFS compatibility where mtime resolution is coarse
MTIME_TOLERANCE_SECONDS: float = 2.0

# Maximum depth for catalog root discovery (prevent traversing to filesystem root)
MAX_CATALOG_SEARCH_DEPTH: int = 20
