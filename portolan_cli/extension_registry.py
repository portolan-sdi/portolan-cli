"""Single source of truth for Portolan's recognized-file-extension vocabulary.

Historically the extension vocabulary was hand-maintained in four places that
drifted apart (issue #558): ``formats.py`` (cloud-native / convertible /
unsupported sets), ``constants.py`` (geospatial / tabular / sidecar tables),
``scan_classify.py`` (the ten scan categories), and ``add.py`` (media-type
and asset-role maps). Plus the human doc ``spec/extensions.md``.

This module is the one place that vocabulary lives now. Every frozenset/dict in
those modules is *derived* from :data:`EXTENSION_REGISTRY` (see the derivation
helpers below), and ``spec/extensions.md`` is tied to it by a parity test
(``tests/spec_compliance/test_extensions_doc_parity.py``). See ADR-0055.

It is deliberately a stdlib-only leaf that imports nothing from ``portolan_cli``
so it can be lifted wholesale into ``reis`` (the validator being extracted, see
issue #563) without dragging the app layers along.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

CloudNative = Literal["yes", "inspect", "no"]
"""Static cloud-native status. ``inspect`` means it depends on file content
(a ``.parquet`` may or may not carry ``geo`` metadata; a ``.tif`` may or may not
be a valid COG), so those extensions are deliberately excluded from the static
``CLOUD_NATIVE_EXTENSIONS`` set and resolved by content inspection instead."""

ConvertTarget = Literal["GeoParquet", "COG"]
Routes = Literal["vector", "raster"]
Role = Literal["data", "thumbnail", "metadata", "documentation"]


@dataclass(frozen=True)
class ExtensionSpec:
    """One row of the extension vocabulary.

    Every field feeds at least one derivation below. Defaults describe the most
    common case (an unremarkable, non-geospatial, unsupported-by-omission file),
    so each row only spells out what makes it special.
    """

    ext: str
    """Lowercase, dot-prefixed extension. Compound (``.copc.laz``) and
    directory (``.gdb``, ``.zarr``) forms are allowed and flagged below."""

    display_name: str | None = None
    """Human format label (feeds ``FORMAT_DISPLAY_NAMES``). Left ``None`` for
    extensions whose display is produced elsewhere (e.g. the ``.zarr`` /
    ``.copc.laz`` special-case branches in ``formats.py``)."""

    media_type: str | None = None
    role: Role | None = None

    cloud_native: CloudNative = "no"
    convert_target: ConvertTarget | None = None
    routes_as: Routes | None = None

    scan_category: str | None = None
    """Which ``FileCategory`` the scanner assigns by extension alone. ``None``
    means the scanner does not recognize it by extension (dirs, compound
    extensions, and unsupported formats fall here and surface as UNKNOWN)."""

    is_dir: bool = False
    is_compound: bool = False
    is_geospatial: bool = False
    is_tabular: bool = False
    is_multilayer: bool = False
    unsupported_message: str | None = None


# =============================================================================
# The registry
# =============================================================================

_NETCDF_MSG = "NetCDF is not yet supported. Support coming soon."
_HDF5_MSG = "HDF5 is not yet supported. Support coming soon."
_LAS_MSG = "LAS/LAZ point clouds require COPC format. Use pdal or other tools to convert."

EXTENSION_REGISTRY: tuple[ExtensionSpec, ...] = (
    # ---- Primary geospatial / format-detected ------------------------------
    ExtensionSpec(
        ".parquet",
        display_name="GeoParquet",
        media_type="application/vnd.apache.parquet",
        role="data",
        cloud_native="inspect",  # GeoParquet iff it carries a 'geo' metadata key
        routes_as="vector",
        scan_category="tabular_data",  # GEO_ASSET only after is_geoparquet() peek
        is_geospatial=True,
        is_tabular=True,
    ),
    ExtensionSpec(
        ".geojson",
        display_name="GeoJSON",
        media_type="application/geo+json",
        role="data",
        convert_target="GeoParquet",
        routes_as="vector",
        scan_category="geo_asset",
        is_geospatial=True,
    ),
    ExtensionSpec(
        ".json",
        display_name="JSON",
        media_type="application/json",
        role="metadata",
        # Content-inspected at runtime: GeoJSON -> convertible, STAC -> metadata,
        # otherwise unknown. Handled by a dedicated branch, so it is NOT in the
        # static convertible-vector set and carries no convert_target here.
        scan_category=None,
    ),
    ExtensionSpec(
        ".shp",
        display_name="SHP",
        media_type="application/x-shapefile",
        role="data",
        convert_target="GeoParquet",
        routes_as="vector",
        scan_category="geo_asset",
        is_geospatial=True,
    ),
    ExtensionSpec(
        ".gpkg",
        display_name="GPKG",
        media_type="application/geopackage+sqlite3",
        role="data",
        convert_target="GeoParquet",
        routes_as="vector",
        scan_category="geo_asset",
        is_geospatial=True,
        is_multilayer=True,
    ),
    ExtensionSpec(
        ".gdb",
        # FileGDB directory. Display falls back to the generic upper-cased stem
        # in formats.py; no media type (a directory, not a file we type).
        convert_target="GeoParquet",
        routes_as="vector",
        is_dir=True,
        is_geospatial=True,
        is_multilayer=True,
    ),
    ExtensionSpec(
        ".fgb",
        display_name="FlatGeobuf",
        media_type="application/vnd.flatgeobuf",
        role="data",
        cloud_native="yes",
        routes_as="vector",
        scan_category="geo_asset",
        is_geospatial=True,
    ),
    ExtensionSpec(
        # Defensive alias for FlatGeobuf in the media-type/role maps only; real
        # files use .fgb, so this participates in no format-detection set.
        ".flatgeobuf",
        media_type="application/vnd.flatgeobuf",
        role="data",
    ),
    ExtensionSpec(
        ".pmtiles",
        display_name="PMTiles",
        media_type="application/vnd.pmtiles",
        role="data",
        cloud_native="yes",
        routes_as="vector",
        scan_category="geo_asset",
        is_geospatial=True,
    ),
    ExtensionSpec(
        ".csv",
        display_name="CSV",
        media_type="text/csv",
        role="data",
        convert_target="GeoParquet",
        routes_as="vector",
        scan_category="tabular_data",  # geometry columns -> geo at a later stage
        is_geospatial=True,
        is_tabular=True,
    ),
    ExtensionSpec(
        ".tsv",
        display_name="TSV",
        # Mirrors .csv: content-inspected, dual vector/tabular (issue #558).
        convert_target="GeoParquet",
        routes_as="vector",
        scan_category="tabular_data",
        is_geospatial=True,
        is_tabular=True,
    ),
    ExtensionSpec(
        ".tif",
        display_name="COG",
        media_type="image/tiff; application=geotiff; profile=cloud-optimized",
        role="data",
        cloud_native="inspect",  # cloud-native iff a valid COG
        routes_as="raster",
        scan_category="geo_asset",
        is_geospatial=True,
    ),
    ExtensionSpec(
        ".tiff",
        display_name="COG",
        media_type="image/tiff; application=geotiff; profile=cloud-optimized",
        role="data",
        cloud_native="inspect",
        routes_as="raster",
        scan_category="geo_asset",
        is_geospatial=True,
    ),
    ExtensionSpec(
        ".jp2",
        display_name="JP2",
        convert_target="COG",
        routes_as="raster",
        scan_category="geo_asset",
        is_geospatial=True,
    ),
    # ---- Cloud-native, directory / compound (not static-set members) -------
    ExtensionSpec(
        # Zarr array store: a directory, handled by a dedicated branch, so it is
        # cloud-native but excluded from the static CLOUD_NATIVE_EXTENSIONS set.
        ".zarr",
        cloud_native="yes",
        is_dir=True,
    ),
    ExtensionSpec(
        # COPC point cloud: compound extension, handled by an endswith() branch
        # ahead of the plain-.laz unsupported check. A .copc.laz asset is typed
        # via the .laz media type (a COPC file is a LAZ file).
        ".copc.laz",
        cloud_native="yes",
        is_compound=True,
    ),
    # ---- Tabular-only ------------------------------------------------------
    ExtensionSpec(".xlsx", scan_category="tabular_data", is_tabular=True),
    ExtensionSpec(".xls", scan_category="tabular_data", is_tabular=True),
    # ---- Unsupported (rejected, but still well-typed per Matthias review) ---
    ExtensionSpec(
        ".nc",
        display_name="NetCDF",
        media_type="application/x-netcdf",
        unsupported_message=_NETCDF_MSG,
    ),
    ExtensionSpec(
        ".netcdf",
        display_name="NetCDF",
        media_type="application/x-netcdf",
        unsupported_message=_NETCDF_MSG,
    ),
    ExtensionSpec(
        ".h5", display_name="HDF5", media_type="application/x-hdf5", unsupported_message=_HDF5_MSG
    ),
    ExtensionSpec(
        ".hdf5", display_name="HDF5", media_type="application/x-hdf5", unsupported_message=_HDF5_MSG
    ),
    ExtensionSpec(
        ".las", display_name="LAS", media_type="application/vnd.las", unsupported_message=_LAS_MSG
    ),
    ExtensionSpec(
        ".laz",
        display_name="LAZ",
        media_type="application/vnd.laszip",
        unsupported_message=_LAS_MSG,
    ),
    # ---- Shapefile / raster sidecars (scanner's flat known-sidecar set) -----
    ExtensionSpec(".dbf", scan_category="known_sidecar"),
    ExtensionSpec(".shx", scan_category="known_sidecar"),
    ExtensionSpec(".prj", scan_category="known_sidecar"),
    ExtensionSpec(".cpg", scan_category="known_sidecar"),
    ExtensionSpec(".sbn", scan_category="known_sidecar"),
    ExtensionSpec(".sbx", scan_category="known_sidecar"),
    ExtensionSpec(".ovr", scan_category="known_sidecar"),
    ExtensionSpec(
        ".xml", media_type="application/xml", role="metadata", scan_category="known_sidecar"
    ),
    # ---- Documentation -----------------------------------------------------
    ExtensionSpec(
        ".md", media_type="text/markdown", role="documentation", scan_category="documentation"
    ),
    ExtensionSpec(
        ".txt", media_type="text/plain", role="documentation", scan_category="documentation"
    ),
    ExtensionSpec(".rst", scan_category="documentation"),
    ExtensionSpec(
        ".html", media_type="text/html", role="documentation", scan_category="documentation"
    ),
    ExtensionSpec(".htm", scan_category="documentation"),
    ExtensionSpec(".pdf", media_type="application/pdf", role="documentation"),
    # ---- Visualization derivatives -----------------------------------------
    ExtensionSpec(".mbtiles", scan_category="visualization"),
    # ---- Thumbnail / preview images ----------------------------------------
    ExtensionSpec(".png", media_type="image/png", role="thumbnail", scan_category="thumbnail"),
    ExtensionSpec(".jpg", media_type="image/jpeg", role="thumbnail", scan_category="thumbnail"),
    ExtensionSpec(".jpeg", media_type="image/jpeg", role="thumbnail", scan_category="thumbnail"),
    ExtensionSpec(".webp", media_type="image/webp", role="thumbnail", scan_category="thumbnail"),
    ExtensionSpec(".gif", media_type="image/gif", role="thumbnail", scan_category="thumbnail"),
    ExtensionSpec(".svg", media_type="image/svg+xml", role="thumbnail", scan_category="thumbnail"),
    # ---- Junk / ignored ----------------------------------------------------
    ExtensionSpec(".exe", scan_category="junk"),
    ExtensionSpec(".dll", scan_category="junk"),
    ExtensionSpec(".so", scan_category="junk"),
    ExtensionSpec(".dylib", scan_category="junk"),
    ExtensionSpec(".pyc", scan_category="junk"),
    ExtensionSpec(".pyo", scan_category="junk"),
    ExtensionSpec(".class", scan_category="junk"),
    ExtensionSpec(".o", scan_category="junk"),
    ExtensionSpec(".obj", scan_category="junk"),
)


# =============================================================================
# Non-extension vocabulary (single-sourced here too)
# =============================================================================

# Sidecar patterns keyed by the PRIMARY file's extension. Matched by appending
# each pattern to the primary's stem (so compound forms like ".shp.xml" and
# ".aux.xml" resolve correctly). This is a different projection than the flat
# scanner set derived from scan_category == "known_sidecar" above: it answers
# "given this primary, what sidecars might accompany it".
SIDECAR_OF: dict[str, tuple[str, ...]] = {
    ".shp": (".dbf", ".shx", ".prj", ".cpg", ".sbn", ".sbx", ".qix", ".xml", ".shp.xml"),
    ".tif": (".tfw", ".xml", ".aux.xml", ".ovr"),
    ".tiff": (".tfw", ".xml", ".aux.xml", ".ovr"),
    ".img": (".ige", ".rrd", ".rde", ".xml"),
}

JUNK_DIRS: frozenset[str] = frozenset(
    {
        "__pycache__",
        ".git",
        ".svn",
        ".hg",
        ".idea",
        ".vscode",
        "node_modules",
        ".tox",
        ".pytest_cache",
    }
)

STAC_FILENAMES: frozenset[str] = frozenset({"catalog.json", "collection.json", "versions.json"})

STYLE_FILENAMES: frozenset[str] = frozenset({"style.json"})

# Max size (bytes) for an image to be treated as a thumbnail rather than raster.
THUMBNAIL_MAX_SIZE: int = 1024 * 1024


# =============================================================================
# Derivation helpers
# =============================================================================


def extensions_where(**field_equals: object) -> frozenset[str]:
    """Return the set of ``ext`` values whose row matches every ``field=value``.

    Example: ``extensions_where(scan_category="geo_asset")``.
    """
    return frozenset(
        spec.ext
        for spec in EXTENSION_REGISTRY
        if all(getattr(spec, field) == value for field, value in field_equals.items())
    )


def field_map(field: str) -> dict[str, str]:
    """Map ``ext -> getattr(row, field)`` for rows where that field is not None.

    Used to build ``FORMAT_DISPLAY_NAMES``, ``_MEDIA_TYPE_MAP``, ``_ROLE_MAP``,
    and ``UNSUPPORTED_ERROR_MESSAGES``.
    """
    result: dict[str, str] = {}
    for spec in EXTENSION_REGISTRY:
        value = getattr(spec, field)
        if value is not None:
            result[spec.ext] = value
    return result


def cloud_native_extensions() -> frozenset[str]:
    """Statically cloud-native, single-suffix files (skip conversion).

    Excludes ``inspect`` extensions (``.parquet``/``.tif``, resolved by content)
    and the directory/compound cloud-native forms (``.zarr``/``.copc.laz``,
    resolved by dedicated branches).
    """
    return frozenset(
        spec.ext
        for spec in EXTENSION_REGISTRY
        if spec.cloud_native == "yes" and not spec.is_dir and not spec.is_compound
    )


def convertible_extensions(target: ConvertTarget) -> frozenset[str]:
    """Extensions that convert to the given cloud-native target."""
    return frozenset(spec.ext for spec in EXTENSION_REGISTRY if spec.convert_target == target)


def unsupported_extensions() -> frozenset[str]:
    """Extensions explicitly rejected with a helpful message."""
    return frozenset(
        spec.ext for spec in EXTENSION_REGISTRY if spec.unsupported_message is not None
    )


def all_known_sidecar_extensions() -> frozenset[str]:
    """Every sidecar suffix the system knows: the flat scanner set plus all
    per-primary patterns in :data:`SIDECAR_OF`."""
    from_patterns = {ext for patterns in SIDECAR_OF.values() for ext in patterns}
    return extensions_where(scan_category="known_sidecar") | frozenset(from_patterns)
