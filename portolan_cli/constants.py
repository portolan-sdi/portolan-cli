"""Shared constants for the Portolan CLI.

This module contains constants that are used across multiple modules
to avoid duplication and ensure consistency.
"""

from __future__ import annotations

from rashid.schema import bundled_schema_versions

from portolan_cli import extension_registry as _reg

# Version of the Portolan specification this CLI validates against (issue #566).
#
# DERIVED from the profile schemas rashid bundles in its wheel, so
# the CLI and the validator cannot drift: whichever spec version rashid can
# actually validate against is the one `check` reports and the one generation
# stamps. ``bundled_schema_versions()`` returns tags like ("v0.1.0",) sorted
# ascending; the newest wins and the leading "v" is dropped, because the URI
# template below and the JSON payload both want a bare SemVer triple.
#
# NOTE: This is the version of the *specification as a whole*. It is distinct
# from versions.SPEC_VERSION, which versions the versions.json manifest schema.
PORTOLAN_SPEC_VERSION: str = max(bundled_schema_versions()).removeprefix("v")

# The versioned Portolan profile schema URI every catalog and collection
# declares in ``stac_extensions`` (issue #654; rashid PTL-CNF-001/002). Its
# shape is fixed by the validator's pattern
# ``^https://schemas\.portolan-sdi\.org/portolan/v\d+\.\d+\.\d+/schema\.json$``.
PORTOLAN_SCHEMA_URI: str = (
    f"https://schemas.portolan-sdi.org/portolan/v{PORTOLAN_SPEC_VERSION}/schema.json"
)

# The collection property that once listed style asset keys in display order.
# The spec removed it (issue #739): a client filters assets on the ``style``
# role to find the styles and on ``default`` to find the one to draw first, so
# a manifest would be a second copy of the same fact. Portolan wrote it through
# 1.0.0b0, which is why both the generator (viz.style) and `check`
# (validation.legacy) still name it — one to strip it, the other to report it.
LEGACY_STYLE_MANIFEST_FIELD: str = "portolan:styles"

# The partition extension a Hive-partitioned collection declares.
# rashid's PTL-PRT-001 recognizes the incubating URI namespace, so generation
# and `check --fix` both emit this one; the older github.io URL it replaced was
# never a URI the validator accepted.
PARTITION_EXTENSION_URI: str = (
    "https://schemas.portolan-sdi.org/incubating/partition/v1.0.0/schema.json"
)

# The extension vocabulary below is DERIVED from portolan_cli.extension_registry
# (the single source). Edit rows there, not these members.

# Extensions we recognize as geospatial files (.gdb is a FileGDB directory,
# handled specially in detection code).
GEOSPATIAL_EXTENSIONS: frozenset[str] = _reg.extensions_where(is_geospatial=True)

# Tabular data that may or may not carry geometry columns. Includes
# .parquet (issue #177) and .xlsx/.xls (issue #432).
TABULAR_EXTENSIONS: frozenset[str] = _reg.extensions_where(is_tabular=True)

# Cloud-native parquet extension
PARQUET_EXTENSION: str = ".parquet"

# Sidecar file patterns by primary file extension. Matched by appending each
# pattern to the primary's stem, so compound forms (.shp.xml, .aux.xml) resolve.
SIDECAR_PATTERNS: dict[str, list[str]] = {
    primary: list(patterns) for primary, patterns in _reg.SIDECAR_OF.items()
}

# Change detection constants
# 2 second tolerance for NFS/CIFS compatibility where mtime resolution is coarse
MTIME_TOLERANCE_SECONDS: float = 2.0

# The .portolan directory name (Portolan internal metadata directory)
PORTOLAN_DIR: str = ".portolan"

# Placeholder metadata_seeding writes for a required field the harvest could not
# fill. It lives here rather than in metadata_seeding because licensing.py has to
# recognize it too: a seeded license reaches collection.license verbatim, where
# rashid reports it as PTL-LIC-001 (issue #686).
TODO_MARKER: str = "TODO: Add value"

# Maximum depth for catalog root discovery (prevent traversing to filesystem root)
MAX_CATALOG_SEARCH_DEPTH: int = 20

# Maximum depth for nested catalogs
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
