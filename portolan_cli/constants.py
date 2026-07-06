"""Shared constants for the Portolan CLI.

This module contains constants that are used across multiple modules
to avoid duplication and ensure consistency.
"""

from __future__ import annotations

from portolan_cli import extension_registry as _reg

# Version of the Portolan specification this CLI validates against (issue #566).
#
# SemVer, pre-1.0: breaking spec changes bump the MINOR until 1.0 (see the
# Versioning section of spec/README.md for the bump policy). The canonical
# machine-readable home is spec/schema/spec-version.json; this constant mirrors
# it so the value is available at runtime without shipping spec/ inside the
# installed package. A spec-compliance test keeps the two in sync.
#
# NOTE: This is the version of the *specification as a whole*. It is distinct
# from versions.SPEC_VERSION, which versions the versions.json manifest schema.
PORTOLAN_SPEC_VERSION: str = "0.1.0"

# The extension vocabulary below is DERIVED from portolan_cli.extension_registry
# (the single source, ADR-0055). Edit rows there, not these members.

# Extensions we recognize as geospatial files (.gdb is a FileGDB directory,
# handled specially in detection code).
GEOSPATIAL_EXTENSIONS: frozenset[str] = _reg.extensions_where(is_geospatial=True)

# Tabular data that may or may not carry geometry columns (ADR-0028). Includes
# .parquet (issue #177) and .xlsx/.xls (issue #432).
TABULAR_EXTENSIONS: frozenset[str] = _reg.extensions_where(is_tabular=True)

# Cloud-native parquet extension
PARQUET_EXTENSION: str = ".parquet"

# Sidecar file patterns by primary file extension. Matched by appending each
# pattern to the primary's stem, so compound forms (.shp.xml, .aux.xml) resolve.
SIDECAR_PATTERNS: dict[str, list[str]] = {
    primary: list(patterns) for primary, patterns in _reg.SIDECAR_OF.items()
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
