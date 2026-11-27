"""Shared constants for the Portolan CLI."""

from __future__ import annotations

from rashid_core.api import newest_profile_version

from portolan_core import extension_registry as _reg

# Derived from the profile schemas rashid bundles, so the CLI and the validator cannot drift.
# This versions the whole spec, not the versions.json schema (versions.SPEC_VERSION).
PORTOLAN_SPEC_VERSION: str = newest_profile_version().removeprefix("v")

# Lives here, not in stac.py, so the models package can read it without loading pystac.
STAC_VERSION: str = "1.1.0"

# Files Portolan writes into a collection; ``status`` and ``list`` do not report them as untracked.
MANAGED_FILES: frozenset[str] = frozenset(
    {
        "versions.json",
        "collection.json",
        "catalog.json",
        "README.md",
        "AGENTS.md",
        "metadata.yaml",
    }
)

# Its shape is fixed by the validator's pattern
# ``^https://schemas\.portolan-sdi\.org/portolan/v\d+\.\d+\.\d+/schema\.json$``.
PORTOLAN_SCHEMA_URI: str = (
    f"https://schemas.portolan-sdi.org/portolan/v{PORTOLAN_SPEC_VERSION}/schema.json"
)

# A removed collection property; viz.style strips it and `check` reports it.
LEGACY_STYLE_MANIFEST_FIELD: str = "portolan:styles"

# A removed collection property; the ``legend`` asset role replaces it.
LEGACY_LEGEND_MANIFEST_FIELD: str = "portolan:legends"

# A removed asset field; read only so `check --fix` can give the asset the ``external`` role.
LEGACY_MANAGED_FIELD: str = "portolan:managed"

# A removed asset field; push writes ``partition:glob`` alone.
LEGACY_GLOB_FIELD: str = "portolan:glob"

# ``portolan:`` fields no spec version defines; `check` reports them and `check --fix` deletes them.
REMOVED_PORTOLAN_FIELDS: tuple[str, ...] = (
    "portolan:version",
    "portolan:geospatial",
    LEGACY_STYLE_MANIFEST_FIELD,
    LEGACY_LEGEND_MANIFEST_FIELD,
    "portolan:total_size_bytes",
    "portolan:asset_count",
    "portolan:collection_count",
    "portolan:datetime_provisional",
    LEGACY_MANAGED_FIELD,
    LEGACY_GLOB_FIELD,
)

# rashid's PTL-PRT-001 accepts only the incubating URI namespace.
PARTITION_EXTENSION_URI: str = (
    "https://schemas.portolan-sdi.org/incubating/partition/v1.0.0/schema.json"
)

# The extension vocabulary below is DERIVED from portolan_core.extension_registry
# (the single source). Edit rows there, not these members.

# .gdb is a FileGDB directory, which detection code handles separately.
GEOSPATIAL_EXTENSIONS: frozenset[str] = _reg.extensions_where(is_geospatial=True)

# Tabular data that may or may not carry geometry columns.
TABULAR_EXTENSIONS: frozenset[str] = _reg.extensions_where(is_tabular=True)

# Cloud-native parquet extension
PARQUET_EXTENSION: str = ".parquet"

# Each pattern is appended to the primary's stem, so compound forms (.shp.xml) resolve.
SIDECAR_PATTERNS: dict[str, list[str]] = {
    primary: list(patterns) for primary, patterns in _reg.SIDECAR_OF.items()
}

# STAC asset roles; see portolan-spec specs/portolan/core.md, section Assets.
ROLE_VISUAL: str = "visual"
ROLE_THUMBNAIL: str = "thumbnail"
ROLE_STYLE: str = "style"
ROLE_DEFAULT: str = "default"
ROLE_COLLECTION_MIRROR: str = "collection-mirror"

# Derived artifacts the spec calls optional: an absent one is a warning, not a failed push.
# Roles `data`, `source` and `metadata` are absent because nothing can rebuild them.
OPTIONAL_DERIVATIVE_ROLES: frozenset[str] = frozenset(
    {ROLE_VISUAL, ROLE_THUMBNAIL, ROLE_STYLE, ROLE_COLLECTION_MIRROR}
)

# 2 second tolerance for NFS/CIFS compatibility where mtime resolution is coarse
MTIME_TOLERANCE_SECONDS: float = 2.0

# The .portolan directory name (Portolan internal metadata directory)
PORTOLAN_DIR: str = ".portolan"

# Lives here because licensing.py must also recognize it in a seeded license.
TODO_MARKER: str = "TODO: Add value"

# Maximum depth for catalog root discovery (prevent traversing to filesystem root)
MAX_CATALOG_SEARCH_DEPTH: int = 20

# Deeper nesting likely indicates misconfiguration.
MAX_CATALOG_DEPTH: int = 10

# Windows reserved device names (case-insensitive, with any extension).
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
