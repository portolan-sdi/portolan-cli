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
TABULAR_EXTENSIONS: frozenset[str] = frozenset({".csv", ".tsv"})

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
