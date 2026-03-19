"""Shared constants for the Portolan CLI.

This module contains constants that are used across multiple modules
to avoid duplication and ensure consistency.
"""

from __future__ import annotations

# Extensions we recognize as geospatial files
# Note: .gdb is a directory extension (FileGDB) - handled specially in detection code
GEOSPATIAL_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".geojson",
        ".parquet",
        ".shp",
        ".gpkg",
        ".fgb",
        ".gdb",  # FileGDB directory (ESRI File Geodatabase)
        ".csv",
        ".tsv",  # Tab-separated values (may or may not have geometry)
        ".tif",
        ".tiff",
        ".jp2",
        ".pmtiles",
    }
)

# Extensions for tabular data that may or may not have geometry columns.
# When these files lack geometry, they should be tracked as non-geospatial assets
# (per ADR-0028) rather than causing errors.
# Includes .parquet per Issue #177: tabular parquet files without geometry
# should be tracked as auxiliary assets when alongside a primary geo-asset.
TABULAR_EXTENSIONS: frozenset[str] = frozenset({".csv", ".tsv", ".parquet"})

# Cloud-native parquet extension
PARQUET_EXTENSION: str = ".parquet"

# Sidecar file patterns by primary file extension
# Shapefile sidecars include:
#   Required: .dbf (attributes), .shx (index)
#   Optional: .prj (projection), .cpg (code page),
#             .sbn/.sbx (ESRI spatial index), .qix (QGIS spatial index),
#             .xml (metadata), .shp.xml (ESRI shapefile metadata)
SIDECAR_PATTERNS: dict[str, list[str]] = {
    ".shp": [".dbf", ".shx", ".prj", ".cpg", ".sbn", ".sbx", ".qix", ".xml", ".shp.xml"],
    ".tif": [".tfw", ".xml", ".aux.xml", ".ovr"],
    ".tiff": [".tfw", ".xml", ".aux.xml", ".ovr"],
    ".img": [".ige", ".rrd", ".rde", ".xml"],
}

# Change detection constants (per ADR-0017)
# 2 second tolerance for NFS/CIFS compatibility where mtime resolution is coarse
MTIME_TOLERANCE_SECONDS: float = 2.0

# The .portolan directory name (Portolan internal metadata directory)
PORTOLAN_DIR: str = ".portolan"

# Maximum depth for catalog root discovery (prevent traversing to filesystem root)
MAX_CATALOG_SEARCH_DEPTH: int = 20

# Maximum depth for nested catalogs (per ADR-0032)
# Prevents excessive nesting which likely indicates misconfiguration
MAX_CATALOG_DEPTH: int = 10

# Windows reserved device names (case-insensitive)
# Files with these names (with any extension) are problematic on Windows.
# Used by scan.py and scan_fix.py for cross-platform compatibility checks.
WINDOWS_RESERVED_NAMES: frozenset[str] = frozenset(
    {
        "con",
        "prn",
        "aux",
        "nul",
        "com1",
        "com2",
        "com3",
        "com4",
        "com5",
        "com6",
        "com7",
        "com8",
        "com9",
        "lpt1",
        "lpt2",
        "lpt3",
        "lpt4",
        "lpt5",
        "lpt6",
        "lpt7",
        "lpt8",
        "lpt9",
    }
)
