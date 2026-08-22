"""Per-item preparation and conversion routing for the add pipeline.

Extracted from ``add.py`` (issue #623). This module owns the parallelizable,
GDAL-bound phase of ``add``: format-specific conversion to cloud-native outputs
(GeoParquet / COG / plain Parquet), metadata + statistics extraction, asset
scanning, and STAC item construction. It writes item.json but performs no
versions.json or collection-link updates — those are batched in
``add.finalize_items`` to keep versioning O(n) (see Issue #281).

the CLI stays a thin wrapper; ``add.py`` orchestrates on top of the
routines here. This module deliberately imports nothing from ``add`` so the
dependency edge is one-directional (add -> preparation).
"""

from __future__ import annotations

import logging
import shutil
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pystac

from portolan_cli import extension_registry as _reg
from portolan_cli.collection_id import normalize_collection_id, validate_collection_id
from portolan_cli.config import get_setting, load_merged_metadata
from portolan_cli.conversion_config import VectorSettings, get_vector_settings
from portolan_cli.convert import apply_vector_settings, run_with_transient_convert_retry
from portolan_cli.crs import measure_wgs84_bbox, transform_bbox_to_wgs84
from portolan_cli.errors import NoGeometryError, RewriteFidelityError
from portolan_cli.formats import FormatType, detect_format, is_cloud_optimized_geotiff
from portolan_cli.metadata import (
    extract_band_statistics,
    extract_cog_metadata,
    extract_flatgeobuf_metadata,
    extract_geoparquet_metadata,
    extract_parquet_statistics,
    extract_pmtiles_metadata,
    read_extra_schema_metadata,
    read_rewrite_fidelity,
    read_spatial_layout,
)
from portolan_cli.metadata.cog import COGMetadata
from portolan_cli.metadata.flatgeobuf import FlatGeobufMetadata
from portolan_cli.metadata.geoparquet import GeoParquetMetadata, RewriteFidelity
from portolan_cli.metadata.pmtiles import PMTilesMetadata
from portolan_cli.metadata_yaml import (
    NodataMismatchError,
    apply_raster_nodata_defaults,
    apply_temporal_defaults,
    validate_metadata,
)
from portolan_cli.scan.detect import is_filegdb
from portolan_cli.stac import (
    add_projection_extension,
    add_raster_extension,
    add_vector_extension,
    create_item,
)
from portolan_cli.sync.checksums import (
    compute_checksum,
    compute_dir_checksum,
    compute_dir_size,
    file_fields_from,
)
from portolan_cli.viz.style import enrich_cog_assets

logger = logging.getLogger(__name__)


# Files to ignore when scanning item directories for assets.
# These are STAC/Portolan structural files, not user data.
# AGENTS.md is referenced via a rel="agents" link, not tracked as an asset
# ("AGENTS.md is a link, not an asset").
IGNORED_FILES: frozenset[str] = frozenset(
    {
        "catalog.json",
        "collection.json",
        "versions.json",
        "AGENTS.md",
    }
)


# Extension-to-MIME-type mapping for asset files. Derived from the extension
# registry (the single source). Edit rows there, not this map.
_MEDIA_TYPE_MAP: dict[str, str] = _reg.field_map("media_type")


# Asset keys reserved for well-known roles. _scan_item_assets prefers these
# keys over filename-derived stems so STAC consumers can find assets by role
# without inspecting file paths. Order matters for collision priority: an
# asset with role "thumbnail" prefers key "thumbnail"; if it's already taken
# (e.g. by a user-named thumbnail.png), the second asset falls back to its
# stem.
_ROLE_KEYS: dict[str, str] = {
    "thumbnail": "thumbnail",
    "metadata": "metadata",
    "documentation": "documentation",
}


# Extension-to-role mapping for asset files (data / thumbnail / metadata /
# documentation). Derived from the extension registry. Unknown
# extensions fall back to "data" in _get_asset_role().
_ROLE_MAP: dict[str, str] = _reg.field_map("role")


def _get_media_type(path: Path) -> str:
    """Determine MIME type from file extension.

    Args:
        path: Path to the file.

    Returns:
        MIME type string. Defaults to "application/octet-stream" for
        unknown extensions.
    """
    return _MEDIA_TYPE_MAP.get(path.suffix.lower(), "application/octet-stream")


def _get_asset_role(path: Path) -> str:
    """Determine STAC asset role from file extension.

    Args:
        path: Path to the file.

    Returns:
        Role string: "data", "thumbnail", "metadata", or "documentation".
        Defaults to "data" for unknown extensions.
    """
    return _ROLE_MAP.get(path.suffix.lower(), "data")


def _is_never_an_asset(file_path: Path) -> bool:
    """Report whether a directory entry can never be a tracked asset.

    Three entries qualify, and none of them depend on the item being scanned:

    - A hidden file. Editors and tools leave them beside the data.
    - A symlink. This check must precede any ``is_dir()`` branching, because
      ``is_dir()`` follows symlinks, so a symlinked ``.gdb`` directory would
      otherwise be checksummed as a container asset and escape the intended
      item boundary.
    - The scratch file of an in-place GeoParquet rewrite. The hidden test
      already covers the name the current writer uses. A killed run of an
      earlier version can leave a visible one, and tracking that as an asset
      makes every later ``add`` read a truncated file (issue #805).

    Args:
        file_path: Directory entry to test.

    Returns:
        True when the scan must skip the entry.
    """
    if file_path.name.startswith("."):
        return True
    if file_path.is_symlink():
        return True
    return is_rewrite_temp(file_path)


def _scan_item_assets(
    item_dir: Path,
    item_id: str,
    primary_file: Path,
    collection_dir: Path,
    *,
    exclude_names: frozenset[str] = frozenset(),
) -> tuple[dict[str, pystac.Asset], dict[str, tuple[Path, str, int]], list[str]]:
    """Scan an item directory for all trackable assets.

        Per issue #133, ALL files in item directories are tracked as assets.
        FileGDB directories (.gdb) are treated as single container assets (Issue #174).
        Skips: non-FileGDB directories, symlinks, hidden files, STAC structural files.

        Args:
            item_dir: Path to the item directory (where files are).
            item_id: Item identifier (for skipping item.json).
            primary_file: Path to the primary data file (gets "data" key).
            collection_dir: Path to the collection directory.
            exclude_names: Base filenames of OTHER items being added in the same
                batch (their sources and converted outputs). For a collection-level
                asset (``item_dir == collection_dir``) the flat collection directory
                also holds every sibling asset, so without this the scan re-checksums
                all siblings on every file — O(n²) (issue #465). Files here that do
                not share the primary's stem are skipped; each is tracked by its own
                ``prepare_item``. Loose companions (not in this set) are kept per
    . Ignored for item-level (subdirectory) scans.

        Returns:
            Tuple of (stac_assets, asset_files, asset_paths):
            - stac_assets: Dict mapping asset key to pystac.Asset
            - asset_files: Dict mapping filename to (path, checksum, size) tuples
            - asset_paths: List of absolute path strings
    """
    stac_assets: dict[str, pystac.Asset] = {}
    asset_files: dict[str, tuple[Path, str, int]] = {}
    asset_paths: list[str] = []

    # Resolve directory paths once, not per file (these are O(n) scans; a
    # per-file resolve() would be an O(n²) syscall storm for flat collections).
    item_dir_resolved = item_dir.resolve()
    # Issue #465: only prune cross-item siblings for collection-level (flat) scans.
    is_collection_level = bool(exclude_names) and item_dir_resolved == collection_dir.resolve()
    # Whether assets and the item.json will be co-located (affects href prefix).
    assets_colocated = item_dir_resolved == (collection_dir / item_id).resolve()
    primary_stem = primary_file.stem

    for file_path in item_dir.iterdir():
        if _is_never_an_asset(file_path):
            continue

        # Issue #465: skip siblings that belong to OTHER items in this batch.
        # Keep the primary and its own same-stem source/sidecars; keep loose
        # companions (not in exclude_names) so tracking is preserved.
        if (
            is_collection_level
            and file_path.stem != primary_stem
            and file_path.name in exclude_names
        ):
            continue
        if file_path.name in IGNORED_FILES:
            continue
        if file_path.name == f"{item_id}.json":
            continue

        if file_path.is_dir():
            # FileGDB directories are tracked as single container assets (Issue #174).
            # Other directories are skipped.
            if not is_filegdb(file_path):
                continue
            file_checksum = compute_dir_checksum(file_path)
            file_size = compute_dir_size(file_path)
            # FileGDB is always a geospatial asset
            file_media_type = "application/x-filegdb"
            file_role = "data"
        elif file_path.is_file():
            file_checksum = compute_checksum(file_path)
            file_size = file_path.stat().st_size
            file_media_type = _get_media_type(file_path)
            file_role = _get_asset_role(file_path)
        else:
            # Skip special files (sockets, devices, etc.)
            continue

        # Primary geo file gets "data" key. Other files prefer the well-known
        # role-keyed name ("thumbnail", "metadata", "documentation") so STAC
        # consumers can find them by role; on collision, fall back to stem,
        # then to filename.
        if file_path == primary_file:
            asset_key = "data"
        else:
            role_key = _ROLE_KEYS.get(file_role)
            if role_key and role_key not in stac_assets and role_key != "data":
                asset_key = role_key
            else:
                # Use stem, but disambiguate on collision (e.g. metadata.json vs metadata.xml)
                asset_key = file_path.stem
                if asset_key in stac_assets or asset_key == "data":
                    asset_key = file_path.name
        # Asset href must be relative to item JSON location.
        # PySTAC places item JSON at: {collection_dir}/{item_id}/{item_id}.json
        #
        # Case 1: Data at {collection_dir}/data.parquet (item_dir == collection_dir)
        # - Item JSON at {collection_dir}/{item_id}/{item_id}.json (subdirectory)
        # - Href needs ../{filename} to reach parent (collection) directory
        #
        # Case 2: Data at {collection_dir}/{item_id}/data.parquet
        # - item_dir == {collection_dir}/{item_id}/
        # - Item JSON at same level: {collection_dir}/{item_id}/{item_id}.json
        # - Href just needs {filename} (same directory)
        #
        # The key: if item_dir IS the collection, PySTAC creates a subdirectory
        # and we need ../ to reach the files. Otherwise, files are already in
        # the item subdirectory.
        #
        if assets_colocated:
            # Assets and item JSON are in the same directory
            asset_href = file_path.name
        else:
            # Item JSON will be in a subdirectory, need to go up one level
            asset_href = f"../{file_path.name}"

        stac_assets[asset_key] = pystac.Asset(
            href=asset_href,
            media_type=file_media_type,
            roles=[file_role],
            # NOTE: Don't set title here - it's a human-enrichable field (Issue #446).
            # Titles should come from metadata.yaml or be preserved from existing
            # metadata via merge strategy. Role-based default titles are NOT
            # auto-detected values, so they shouldn't appear with OVERWRITE.
            # file_fields_from, not file_fields: a FileGDB asset is a directory,
            # already measured above by compute_dir_checksum/compute_dir_size.
            extra_fields=file_fields_from(file_checksum, file_size),
        )
        asset_files[file_path.name] = (file_path, file_checksum, file_size)
        asset_paths.append(str(file_path))

    return stac_assets, asset_files, asset_paths


@dataclass
class PreparedItem:
    """Result of prepare_item() — metadata extracted, ready for finalization.

    This dataclass holds all the information needed to finalize an item
    (write versions.json, update collection links) without any I/O happening
    during the prepare phase.

    The prepare/finalize separation enables O(n) versioning instead of O(n²)
    by batching all version writes at the end. See Issue #281.

    Attributes:
        item_id: STAC item identifier (for item-level) or asset key (for collection-level).
        collection_id: Collection identifier (may include '/' for nested).
        format_type: Vector or raster format.
        bbox: Bounding box [min_x, min_y, max_x, max_y] in WGS84.
        asset_files: Dict mapping filename to (path, checksum, size) tuples.
        item_json_path: Path to item.json (None for collection-level vector assets).
        is_collection_level_asset: If True, asset is at collection level.
        stac_item: The PySTAC Item object (None for collection-level vector assets).
        stac_assets: Assets to add to collection.json (for collection-level assets).
        metadata: Extracted metadata (GeoParquet or COG) for table extension (Issue #304).
        partition_metadata: Partition extension fields from get_partition_metadata() (Issue #232).
    """

    item_id: str
    collection_id: str
    format_type: FormatType
    bbox: list[float]
    asset_files: dict[str, tuple[Path, str, int]]
    item_json_path: Path | None  # None for collection-level vector assets
    is_collection_level_asset: bool = False
    stac_item: pystac.Item | None = None
    stac_assets: dict[str, pystac.Asset] | None = None  # For collection-level addition
    metadata: AllMetadata | None = None
    partition_metadata: dict[str, object] | None = None


def _pre_validate_geometry(path: Path, format_type: FormatType) -> None:
    """Pre-validate that a file has valid geometry BEFORE any filesystem operations.

    Issue #163: Failed add operations should be atomic. This function checks for
    geometry/features before any conversion or copying happens, preventing partial
    artifacts from being created.

    Args:
        path: Path to the source file.
        format_type: Detected format type (VECTOR or RASTER).

    Raises:
        ValueError: If the file has no valid geometry/features.
    """
    ext = path.suffix.lower()

    # Parquet: check GeoParquet metadata
    if ext == ".parquet":
        from portolan_cli.formats import is_geoparquet

        if not is_geoparquet(path):
            raise NoGeometryError(
                path=path.stem,
                reason="The source file may have no valid geometry.",
            )
        return

    # GeoJSON: check for features with geometry
    if ext in {".geojson", ".json"}:
        import json

        try:
            # Per RFC 7946: GeoJSON MUST be encoded as UTF-8
            with open(path, encoding="utf-8") as f:
                data = json.load(f)

            # Check for features
            if data.get("type") == "FeatureCollection":
                features = data.get("features", [])
                if not features:
                    raise NoGeometryError(
                        path=path.stem,
                        reason="The source file has no features.",
                    )
                # Check that at least one feature has geometry
                has_geometry = any(f.get("geometry") is not None for f in features)
                if not has_geometry:
                    raise NoGeometryError(
                        path=path.stem,
                        reason="No features have geometry.",
                    )
            elif data.get("type") == "Feature":
                if data.get("geometry") is None:
                    raise NoGeometryError(
                        path=path.stem,
                        reason="Feature has no geometry.",
                    )
        except json.JSONDecodeError as err:
            raise ValueError(f"Invalid JSON in '{path}': {err}") from err
        return


def _cleanup_orphaned_output(output_path: Path, item_dir: Path, source_path: Path) -> None:
    """Clean up orphaned conversion output when geometry extraction fails.

    Called when conversion succeeds but produces no geometry (empty bbox).
    Removes the output file and any associated sidecars to avoid leaving
    orphaned files in the item directory.

    Args:
        output_path: Path to the converted output file.
        item_dir: Directory containing the item files.
        source_path: Original source file path (won't be deleted if same).
    """
    if not output_path.exists() or output_path == source_path:
        return

    # Resolve source_path for comparison (Issue #432: don't delete source file)
    resolved_source = source_path.resolve()

    try:
        output_path.unlink()
        logger.debug("Cleaned up orphaned conversion output: %s", output_path)
        # Also clean up any sidecars that might have been created
        for sidecar in item_dir.glob(f"{output_path.stem}.*"):
            # Don't delete the output (already deleted), JSON metadata, or the SOURCE file
            # Issue #432: source file (e.g., records.csv) matches glob (records.*)
            if (
                sidecar != output_path
                and sidecar.suffix.lower() != ".json"
                and sidecar.resolve() != resolved_source
            ):
                sidecar.unlink()
                logger.debug("Cleaned up orphaned sidecar: %s", sidecar)
    except OSError as cleanup_err:
        # Log but don't swallow the original error
        logger.warning("Failed to clean up orphaned file %s: %s", output_path, cleanup_err)


def _derive_item_id_and_asset_level(
    path: Path,
    collection_dir: Path,
    item_id: str | None,
    format_type: FormatType | None = None,
) -> tuple[str, bool]:
    """Derive item ID and detect if asset is collection-level.

    Args:
        path: Path to the asset file.
        collection_dir: Collection directory path.
        item_id: Optional explicit item ID.
        format_type: Optional format type for Hive partition handling.
            Vector formats in Hive partitions become collection-level assets.

    Returns:
        Tuple of (item_id, is_collection_level_asset).

    Raises:
        ValueError: If derived or provided item_id is invalid.

    Note:
        For nested collections (e.g., collection_id="a/b"), a file at
        catalog_root/a/file.parquet will NOT be detected as collection-level
        for collection "a/b" (since path.parent != catalog_root/a/b).
        This is intentional - the file would belong to parent collection "a".

    Note:
        Per Issue #443: Files in Hive partition directories (key=value) are
        handled specially to avoid duplicate item IDs. Vector formats become
        collection-level assets; other formats derive unique IDs from the
        partition values.
    """
    from portolan_cli.scan.detect import is_hive_partition_dir

    # If item_id is explicitly provided, treat as item-level (not collection-level)
    # This ensures --item-id creates a subdirectory structure
    if item_id is not None:
        # Validate item_id is a safe single path segment
        if not item_id or "/" in item_id or "\\" in item_id or item_id in {".", ".."}:
            raise ValueError(f"Invalid item_id '{item_id}': must be a single path segment")
        return item_id, False  # Explicit item_id = item-level structure

    # Auto-detect: collection-level if file is directly in collection directory
    is_collection_level_asset = path.parent.resolve() == collection_dir.resolve()

    # Check for Hive partition directories in path relative to collection
    # Per Issue #443: Handle Hive partitions consistently with collection_id filtering
    try:
        relative_parts = list(path.parent.resolve().relative_to(collection_dir.resolve()).parts)
    except ValueError:
        relative_parts = []

    # Separate Hive partitions from regular directories
    hive_partitions: list[tuple[str, str]] = []  # (key, value) pairs
    non_hive_parts: list[str] = []
    for part in relative_parts:
        partition = is_hive_partition_dir(part)
        if partition is not None:
            hive_partitions.append(partition)
        else:
            non_hive_parts.append(part)

    # If path contains Hive partitions, apply special handling
    if hive_partitions:
        # Issue #443: For multi-level Hive partitions (e.g., year=2023/month=01/),
        # using path.parent.name would give "month=01" for ALL year branches,
        # causing duplicate item IDs. Instead, use the full relative path as item_id.
        #
        # For single-level partitions (e.g., kdtree_cell=XXX/), path.parent.name
        # is unique, so no special handling needed - fall through to normal logic.
        if len(hive_partitions) > 1 or non_hive_parts:
            # Multi-level partitions or mixed structure: use full relative path
            # e.g., year=2023/month=01/data.parquet -> item_id = "year=2023_month=01"
            item_id = "_".join(relative_parts)
        else:
            # Single-level Hive partition (most common case, e.g., kdtree):
            # Use parent directory name as item_id (existing behavior)
            item_id = path.parent.name
    elif is_collection_level_asset:
        # Generate item ID from PARENT DIRECTORY name (Issue #163)
        # Item boundaries are directories, not filenames.
        # Example: collection/item_dir/file.parquet -> item_id = "item_dir"
        # For collection-level assets, use file stem to avoid duplicate directory name
        # Use file stem for collection-level assets to avoid collection/collection/ nesting
        item_id = path.stem
    else:
        # Use parent directory name for item-level organization
        item_id = path.parent.name

    # Validate derived item_id
    if not item_id or "/" in item_id or "\\" in item_id or item_id in {".", ".."}:
        raise ValueError(f"Invalid item_id '{item_id}': must be a single path segment")

    return item_id, is_collection_level_asset


def _validate_collection_id(collection_id: str) -> None:
    """Validate collection ID for security and STAC compliance.

    Args:
        collection_id: The collection ID to validate.

    Raises:
        ValueError: If the collection ID is invalid.
    """
    # First check: reject unsafe collection IDs (security check)
    # Forward slashes allowed for nested catalogs
    if (
        not collection_id
        or "\\" in collection_id
        or collection_id in {".", ".."}
        or any(part in {".", ".."} for part in collection_id.split("/"))
    ):
        raise ValueError(
            f"Invalid collection_id '{collection_id}': backslashes and. or .. segments not allowed"
        )

    # Second check: validate collection ID format per STAC spec
    is_valid, error_msg = validate_collection_id(collection_id)
    if not is_valid:
        suggestion = ""
        try:
            normalized = normalize_collection_id(collection_id)
            suggestion = f" Suggested: '{normalized}'"
        except ValueError:
            # Cannot normalize (e.g., all special characters)
            pass
        raise ValueError(f"Invalid collection ID '{collection_id}': {error_msg}.{suggestion}")


# Type alias for all supported metadata types
VectorMetadata = GeoParquetMetadata | PMTilesMetadata | FlatGeobufMetadata


AllMetadata = VectorMetadata | COGMetadata


def _extract_bbox_wgs84(metadata: AllMetadata, data_path: Path | None = None) -> list[float]:
    """Extract bbox from metadata, transforming to WGS84 if needed.

    PMTiles bbox is already in WGS84 (4326). Other formats may need
    CRS transformation.

    For GeoParquet in a projected CRS the extent is *measured* from the
    geometry when ``data_path`` is given. Reprojecting the stored bbox instead
    answers a different question — it gives the WGS84 envelope of a rectangle,
    which for a projected CRS is strictly larger than the envelope of the data
    inside it, so the declared extent overstates the data and `check` rejects it
    (PTL-DAT-005). Measurement falls back to the reprojected bbox whenever it
    cannot be made.

    Args:
        metadata: Metadata object with bbox attribute.
        data_path: Converted data file, when available, to measure the extent
            from instead of reprojecting the stored bbox.

    Returns:
        Bounding box as [min_x, min_y, max_x, max_y] in WGS84.
    """
    if isinstance(metadata, PMTilesMetadata):
        # PMTiles store bounds in WGS84 (4326), no transformation needed
        return list(metadata.bbox)  # type: ignore[arg-type]

    # Other formats may need CRS transformation
    crs_raw = getattr(metadata, "crs", None)
    if isinstance(crs_raw, dict):
        raise ValueError("PROJJSON CRS not supported. Convert to EPSG code or WKT string.")
    crs_str = crs_raw if isinstance(crs_raw, str) else None

    geometry_column = getattr(metadata, "geometry_column", None)
    if data_path is not None and geometry_column:
        measured = measure_wgs84_bbox(data_path, geometry_column, crs_str)
        if measured is not None:
            return list(measured)

    return list(transform_bbox_to_wgs84(metadata.bbox, crs_str))  # type: ignore[arg-type]


def _warn_if_source_newer(source_path: Path, output_path: Path) -> None:
    """Warn if source file is newer than output (suggests --reconvert)."""
    from portolan_cli.output import warn as warn_output

    if source_path.stat().st_mtime > output_path.stat().st_mtime:
        warn_output(
            f"Source file '{source_path.name}' is newer than converted output. "
            "Use --reconvert to re-convert from source."
        )


def _handle_cloud_native_vector(
    source_path: Path,
    output_path: Path,
    extract_fn: Callable[[Path], AllMetadata],
    force: bool,
    reconvert: bool,
) -> AllMetadata:
    """Handle cloud-native vector formats (PMTiles, FlatGeobuf) with force/reconvert.

    Args:
        source_path: Source file path.
        output_path: Target output path.
        extract_fn: Metadata extraction function.
        force: If True, allow overwriting existing output.
        reconvert: If True, re-copy from source.

    Returns:
        Extracted metadata.
    """
    same_file = source_path.resolve() == output_path.resolve()

    if output_path.exists() and not same_file:
        if force and not reconvert:
            # Re-extract metadata from existing, warn if source newer
            _warn_if_source_newer(source_path, output_path)
            return extract_fn(output_path)
        elif force and reconvert:
            # Re-copy from source
            shutil.copy2(source_path, output_path)
            return extract_fn(output_path)
        else:
            # No force — raise error to prevent accidental overwrite
            raise FileExistsError(
                f"File already exists: {output_path}. "
                "Rename the source file or remove the existing file."
            )

    # Output doesn't exist or same file — copy if needed
    if not same_file:
        shutil.copy2(source_path, output_path)
    return extract_fn(output_path)


def _convert_and_extract_metadata(
    path: Path,
    item_dir: Path,
    format_type: FormatType,
    *,
    catalog_root: Path | None = None,
    force: bool = False,
    reconvert: bool = False,
) -> tuple[Path, AllMetadata]:
    """Convert to cloud-native format and extract metadata.

    For cloud-native vector formats (PMTiles, FlatGeobuf), copies the file
    as-is and extracts format-specific metadata. For other vectors, converts
    to GeoParquet.

    Per Issue #386: When force=True and reconvert=False, skips conversion if
    output already exists (extracts metadata from existing output).

    Args:
        path: Source file path.
        item_dir: Item directory for output.
        format_type: Detected format type.
        catalog_root: Catalog root, for reading conversion settings.
        force: If True, bypass change detection (Issue #386).
        reconvert: If True, re-convert from source (requires force=True).

    Returns:
        Tuple of (output_path, metadata).
    """
    metadata: AllMetadata
    suffix = path.suffix.lower()

    if format_type == FormatType.VECTOR:
        # Check for cloud-native vector formats (skip conversion per issue #368)
        if suffix == ".pmtiles":
            output_path = item_dir / path.name
            metadata = _handle_cloud_native_vector(
                path, output_path, extract_pmtiles_metadata, force, reconvert
            )
        elif suffix in (".fgb", ".flatgeobuf"):
            output_path = item_dir / path.name
            metadata = _handle_cloud_native_vector(
                path, output_path, extract_flatgeobuf_metadata, force, reconvert
            )
        else:
            # Convert to GeoParquet
            output_path = item_dir / f"{path.stem}.parquet"
            if force and not reconvert and output_path.exists():
                _warn_if_source_newer(path, output_path)
                _ensure_conforming_geoparquet(output_path, catalog_root)
                metadata = extract_geoparquet_metadata(output_path)
            else:
                output_path = convert_vector(path, item_dir, catalog_root, reconvert=reconvert)
                metadata = extract_geoparquet_metadata(output_path)
    else:  # RASTER
        output_path = item_dir / f"{path.stem}.tif"
        if force and not reconvert and output_path.exists():
            _warn_if_source_newer(path, output_path)
            metadata = extract_cog_metadata(output_path)
        else:
            output_path = convert_raster(path, item_dir)
            metadata = extract_cog_metadata(output_path)
    return output_path, metadata


def _extract_statistics_best_effort(
    output_path: Path,
    format_type: FormatType,
    catalog_root: Path,
    collection_path: Path | None = None,
) -> tuple[list[Any], dict[str, Any]]:
    """Extract statistics with best-effort error handling.

    Args:
        output_path: Path to the converted file.
        format_type: Format type (RASTER or VECTOR).
        catalog_root: Catalog root for config lookup.
        collection_path: Collection directory for hierarchical config.

    Returns:
        Tuple of (band_stats, parquet_stats). Empty if disabled or failed.
    """
    band_stats: list[Any] = []
    parquet_stats: dict[str, Any] = {}
    stats_enabled = get_setting(
        "statistics.enabled",
        catalog_path=catalog_root,
        collection_path=collection_path,
    )
    if not stats_enabled:
        return band_stats, parquet_stats

    try:
        if format_type == FormatType.RASTER:
            raster_mode = get_setting(
                "statistics.raster_mode",
                catalog_path=catalog_root,
                collection_path=collection_path,
            )
            mode = raster_mode if raster_mode in ("cached", "approx", "exact") else "approx"
            band_stats = extract_band_statistics(output_path, mode=mode)  # type: ignore[arg-type]
        else:
            parquet_stats = extract_parquet_statistics(output_path)
    except Exception:  # nosec B110 - stats extraction is optional, failure is non-fatal
        # Statistics extraction failed - continue without stats
        pass
    return band_stats, parquet_stats


def _add_statistics_to_properties(
    stac_properties: dict[str, Any],
    format_type: FormatType,
    band_stats: list[Any],
    parquet_stats: dict[str, Any],
    stats_enabled: bool,
) -> None:
    """Add statistics to STAC properties in-place.

    Args:
        stac_properties: Properties dict to modify.
        format_type: Format type (RASTER or VECTOR).
        band_stats: Band statistics (for rasters).
        parquet_stats: Parquet column statistics (for vectors).
        stats_enabled: Whether stats are enabled.
    """
    if not stats_enabled:
        return

    if format_type == FormatType.RASTER and band_stats:
        for i, stats in enumerate(band_stats):
            if i < len(stac_properties.get("bands", [])):
                stac_properties["bands"][i]["statistics"] = stats.to_stac_dict()
    elif format_type == FormatType.VECTOR and parquet_stats:
        col_stats = {
            name: stat.to_stac_dict() for name, stat in parquet_stats.items() if stat.to_stac_dict()
        }
        if col_stats:
            stac_properties["table:column_statistics"] = col_stats


def _fix_collection_level_asset_hrefs(
    stac_assets: dict[str, pystac.Asset],
) -> dict[str, pystac.Asset]:
    """Fix asset hrefs and keys for collection-level assets.

    _scan_item_assets() computes hrefs relative to item.json, but for
    collection-level assets they should be relative to collection.json.
    Since both collection.json and assets are in the same directory,
    href should be ./filename (not ../filename).

    Also fixes asset keys: _scan_item_assets assigns "data" to primary files,
    but for collection-level assets we need unique keys to avoid collisions
    when multiple vectors exist in the same collection. Use file stem instead.

    Args:
        stac_assets: Assets with hrefs relative to item.json location.

    Returns:
        Assets with hrefs relative to collection.json location, with unique keys.
    """
    fixed_assets: dict[str, pystac.Asset] = {}
    for key, asset in stac_assets.items():
        href = asset.href

        # Normalize href: strip any ../ or ./ prefix, then add ./
        if href.startswith("../"):
            href = href[3:]
        elif href.startswith("./"):
            href = href[2:]
        fixed_href = f"./{href}"

        # Fix asset key: "data" → file stem for uniqueness across collection
        # e.g., "data" with href "./census.parquet" → key "census"
        if key == "data":
            fixed_key = Path(href).stem
        else:
            fixed_key = key

        fixed_assets[fixed_key] = pystac.Asset(
            href=fixed_href,
            media_type=asset.media_type,
            roles=asset.roles,
            title=asset.title,
            description=asset.description,
            extra_fields=asset.extra_fields,
        )
    return fixed_assets


def _create_and_save_item(
    *,
    item_id: str,
    bbox: list[float],
    item_datetime: datetime | None,
    stac_properties: dict[str, Any],
    stac_assets: dict[str, pystac.Asset],
    format_type: FormatType,
    metadata: AllMetadata,
    item_dir: Path,
) -> tuple[pystac.Item, Path]:
    """Create a STAC item with extensions and save it to disk.

    Helper to reduce complexity in prepare_item().

    Args:
        item_id: STAC item identifier.
        bbox: Bounding box [min_x, min_y, max_x, max_y].
        item_datetime: Acquisition/creation datetime.
        stac_properties: Properties to include in the item.
        stac_assets: Assets to attach to the item.
        format_type: Vector or raster format.
        metadata: Extracted metadata for extension fields.
        item_dir: Directory where item.json will be saved.

    Returns:
        Tuple of (item, item_json_path).
    """
    item = create_item(
        item_id=item_id,
        bbox=bbox,
        datetime=item_datetime,
        properties=stac_properties,
        assets=stac_assets,
    )
    add_projection_extension(item, metadata)
    if format_type == FormatType.VECTOR:
        add_vector_extension(item, metadata)
    elif format_type == FormatType.RASTER:
        add_raster_extension(item, metadata)

    item_json_path = item_dir / f"{item_id}.json"
    item.set_self_href(str(item_json_path))
    item.save_object()

    return item, item_json_path


def _apply_nodata_defaults_to_bands(
    stac_properties: dict[str, Any],
    metadata: COGMetadata,
    defaults: dict[str, Any],
    source_path: Path,
) -> None:
    """Apply nodata defaults from metadata.yaml to STAC band properties.

    Only applies defaults to bands that don't already have nodata values.
    Modifies stac_properties["bands"] in-place.

    Args:
        stac_properties: Properties dict to modify.
        metadata: COGMetadata with extraction results.
        defaults: The 'defaults' section from metadata.yaml.
        source_path: Path to source file (for error messages).

    Raises:
        NodataMismatchError: If per-band nodata list doesn't match band count.
    """
    bands = stac_properties.get("bands", [])
    if not bands:
        return

    # Get current nodatavals from metadata extraction
    current_nodatavals = (
        metadata.nodatavals if metadata.nodatavals else tuple(None for _ in range(len(bands)))
    )

    # Apply defaults with strict checking (raises NodataMismatchError on mismatch)
    try:
        updated_nodatavals = apply_raster_nodata_defaults(
            defaults, current_nodatavals, band_count=len(bands), strict=True
        )
    except NodataMismatchError as e:
        raise NodataMismatchError(
            f"Error applying nodata defaults to '{source_path.name}': {e}"
        ) from e

    # Update bands with defaults where extraction returned None
    for i, band in enumerate(bands):
        if i < len(updated_nodatavals) and updated_nodatavals[i] is not None:
            # Only set if band doesn't already have nodata
            if "nodata" not in band or band.get("nodata") is None:
                band["nodata"] = updated_nodatavals[i]


def prepare_item(
    *,
    path: Path,
    catalog_root: Path,
    collection_id: str,
    title: str | None = None,
    description: str | None = None,
    item_id: str | None = None,
    item_datetime: datetime | None = None,
    force: bool = False,
    reconvert: bool = False,
    exclude_sibling_names: frozenset[str] = frozenset(),
) -> PreparedItem:
    """Prepare files for addition (convert, extract metadata, create STAC item).

    This function does the GDAL-bound work (conversion, metadata extraction) but
    does NOT write to versions.json or update collection.json links. This enables
    O(n) versioning instead of O(n²) by batching writes in finalize_items().

    Per Issue #281: This is the parallelizable phase of the add workflow.
    Per Issue #386: force/reconvert control conversion skip behavior.

    Args:
        path: Path to the source file.
        catalog_root: Root directory of the catalog.
        collection_id: Collection to add the data to.
        title: Optional display title for the item.
        description: Optional description.
        item_id: Optional item ID (defaults to parent directory name).
        item_datetime: Optional acquisition/creation datetime.
        force: If True, bypass change detection (Issue #386).
        reconvert: If True, re-convert from source (requires force=True).
        exclude_sibling_names: Base filenames of other batch items (sources +
            converted outputs) to prune from a collection-level asset scan
            (issue #465). Forwarded to _scan_item_assets; see its docstring.

    Returns:
        PreparedItem with all metadata needed for finalization.

    Raises:
        ValueError: If the format is unsupported or collection_id is invalid.
        FileNotFoundError: If the source file doesn't exist.
        NoGeometryError: If the file has no valid geometry.
    """
    # Step 1: Validate inputs
    _validate_collection_id(collection_id)

    format_type = detect_format(path)
    if format_type == FormatType.UNKNOWN:
        raise ValueError(f"Unsupported format: {path.suffix}")

    _pre_validate_geometry(path, format_type)

    # Step 2: Set up paths
    collection_dir = catalog_root / Path(*collection_id.split("/"))
    item_id_resolved, is_collection_level_asset = _derive_item_id_and_asset_level(
        path=path,
        collection_dir=collection_dir,
        item_id=item_id,
        format_type=format_type,  # Issue #443: Handle Hive partitions
    )
    item_dir = path.parent

    # Verify item_dir is inside collection_dir (security check)
    try:
        item_dir.resolve().relative_to(collection_dir.resolve())
    except ValueError as err:
        raise ValueError(
            f"File '{path}' is not inside collection '{collection_id}'. "
            f"Expected path under '{collection_dir}'."
        ) from err

    # Step 3: Convert and extract metadata
    output_path, metadata = _convert_and_extract_metadata(
        path,
        item_dir,
        format_type,
        catalog_root=catalog_root,
        force=force,
        reconvert=reconvert,
    )

    # Step 3b: Load metadata.yaml defaults (for temporal/nodata when source lacks them)
    metadata_yaml = load_merged_metadata(collection_dir, catalog_root)
    defaults = metadata_yaml.get("defaults", {})

    # Validate defaults section if present (fail fast on invalid config)
    if defaults:
        validation_errors = validate_metadata({"defaults": defaults})
        # Filter to only defaults-related errors
        defaults_errors = [e for e in validation_errors if "defaults" in e.lower()]
        if defaults_errors:
            raise ValueError(
                "Invalid metadata.yaml defaults configuration:\n"
                + "\n".join(f" - {e}" for e in defaults_errors)
            )

    # Step 4: Extract and transform bbox
    if not metadata.bbox:
        _cleanup_orphaned_output(output_path, item_dir, path)
        raise NoGeometryError(
            path=metadata.id if hasattr(metadata, "id") else path.stem,
            reason="The source file may have no valid geometry.",
        )
    bbox = _extract_bbox_wgs84(metadata, output_path)

    # Step 4b: Generate the COG thumbnail sidecar so the scan below registers it
    # (Issue #657). The add path converts via convert_raster(), which does not
    # generate a thumbnail like convert_file() does, so without this rasters get
    # no thumbnail asset. The raster check lives in the helper, not here, because
    # prepare_item sits at the xenon complexity ceiling.
    _generate_raster_thumbnail(output_path, catalog_root, format_type)

    # Step 5: Scan assets and compute statistics
    stac_assets, asset_files, _asset_paths = _scan_item_assets(
        item_dir=item_dir,
        item_id=item_id_resolved,
        primary_file=output_path,
        collection_dir=collection_dir,
        exclude_names=exclude_sibling_names,
    )

    # Enrich COG assets with render extension properties (Issue #13)
    if format_type == FormatType.RASTER:
        enrich_cog_assets(stac_assets, catalog_root)

    band_stats, parquet_stats = _extract_statistics_best_effort(
        output_path, format_type, catalog_root, collection_path=collection_dir
    )

    # Step 6: Build STAC properties
    stac_properties = metadata.to_stac_properties()
    stats_enabled = bool(
        get_setting(
            "statistics.enabled",
            catalog_path=catalog_root,
            collection_path=collection_dir,
        )
    )
    _add_statistics_to_properties(
        stac_properties, format_type, band_stats, parquet_stats, stats_enabled
    )
    if title:
        stac_properties["title"] = title
    if description:
        stac_properties["description"] = description

    # Step 6b: Apply metadata.yaml defaults
    # Temporal defaults: applied when no --datetime flag was provided
    effective_datetime = item_datetime
    if effective_datetime is None and defaults:
        effective_datetime = apply_temporal_defaults(defaults)

    # Raster nodata defaults: applied to bands missing nodata values
    if format_type == FormatType.RASTER and defaults and isinstance(metadata, COGMetadata):
        _apply_nodata_defaults_to_bands(stac_properties, metadata, defaults, path)

    # Step 7: Create STAC item or collection-level assets
    # Collection-level vector assets: no item.json, assets go directly in collection.json
    # Item-level assets (rasters, partitioned vectors): create item.json as usual
    if is_collection_level_asset and format_type == FormatType.VECTOR:
        # Collection-level vector asset: no item.json
        return PreparedItem(
            item_id=item_id_resolved,
            collection_id=collection_id,
            format_type=format_type,
            bbox=bbox,
            asset_files=asset_files,
            item_json_path=None,  # No item.json for collection-level vector
            is_collection_level_asset=True,
            stac_item=None,
            stac_assets=_fix_collection_level_asset_hrefs(stac_assets),
            metadata=metadata,
        )

    # Item-level: create STAC item and save item.json
    item, item_json_path = _create_and_save_item(
        item_id=item_id_resolved,
        bbox=bbox,
        item_datetime=effective_datetime,
        stac_properties=stac_properties,
        stac_assets=stac_assets,
        format_type=format_type,
        metadata=metadata,
        item_dir=item_dir,
    )

    return PreparedItem(
        item_id=item_id_resolved,
        collection_id=collection_id,
        format_type=format_type,
        bbox=bbox,
        asset_files=asset_files,
        item_json_path=item_json_path,
        is_collection_level_asset=is_collection_level_asset,
        stac_item=item,
        metadata=metadata,
    )


# Marks the temporary file `_rewrite_parquet_in_place` swaps in. `add` collects
# a directory before it converts anything, so the marker keeps a file left by a
# killed run out of the next run's source list (issue #805).
REWRITE_TEMP_INFIX = ".portolan-rewrite"


def is_rewrite_temp(path: Path) -> bool:
    """Report whether a path is the scratch file of an in-place rewrite.

    :func:`_rewrite_parquet_in_place` writes a hidden sibling and swaps it in.
    A killed run skips the cleanup, so ``add`` must recognize a leftover and
    refuse to ingest it as a source (issue #805).

    Args:
        path: Candidate file path.

    Returns:
        True when the file name carries the rewrite marker.
    """
    return REWRITE_TEMP_INFIX in path.name


def _needs_spatial_rewrite(
    source: Path, settings: VectorSettings, *, reconvert: bool = False
) -> bool:
    """Decide whether an existing Parquet file must be rewritten on ``add``.

    ``add`` used to copy every ``.parquet`` source through untouched, which
    produced a catalog that failed Portolan's own ``check``: the file carried
    no bbox covering column, so it failed PTL-DAT-007 and left PTL-DAT-006
    unevaluated (issue #805).

    Three cases keep the copy:

    - The file is tabular Parquet, with no ``geo`` metadata. ``add_bbox()``
      needs a geometry column.
    - ``add_bbox`` is off. The footer is the only thing this function can read
      cheaply, so with the bbox column disabled there is nothing it can detect
      and no outcome a rewrite would change. Rewriting anyway would repeat on
      every ``add`` and report a reason the rewrite cannot fix.
    - The file already declares a bbox covering column. Rewriting a large
      conformant file would cost time and change nothing.

    The footer says nothing about row order, so a file with the column but
    unordered rows still fails PTL-DAT-006. ``reconvert`` is the operator's
    answer to that, and to a sort-only configuration: ``add --force
    --reconvert`` rewrites the file whatever its footer says.

    Args:
        source: Source Parquet file.
        settings: Vector conversion settings.
        reconvert: True when the operator asked to re-convert from source.

    Returns:
        True when the file must go through geoparquet-io again.
    """
    layout = read_spatial_layout(source)
    if not layout.is_geoparquet:
        return False
    if reconvert:
        return True
    if not settings.add_bbox:
        return False
    return not layout.has_bbox_covering


def _ensure_conforming_geoparquet(output_path: Path, catalog_root: Path | None) -> None:
    """Add the bbox covering column to an output ``--force`` did not reconvert.

    ``add --force`` skips conversion when the output already exists, because
    ``--force`` means "ignore change detection", not "convert again"
    (issue #386). For a single-file collection the output *is* the source, so
    that skip used to hand back a file with no covering column and no warning.
    The catalog then failed its own ``check`` on PTL-DAT-007, which is the
    failure issue #805 set out to remove.

    This runs the same footer test the conversion path runs, and rewrites the
    file when it fails. Source bytes are untouched: the file is already the
    catalog's own output.

    Args:
        output_path: Existing GeoParquet output.
        catalog_root: Catalog root, for reading ``conversion.vector`` settings.
            None uses the built-in defaults.
    """
    if output_path.suffix.lower() != ".parquet":
        return

    settings = get_vector_settings(catalog_root) if catalog_root else VectorSettings()
    if not _needs_spatial_rewrite(output_path, settings):
        return

    from portolan_cli.output import info as info_output
    from portolan_cli.utils import format_size

    size = format_size(output_path.stat().st_size)
    info_output(f"Rewriting {output_path.name} ({size}): it carries no bbox covering column")
    _rewrite_or_keep(output_path, settings)


def convert_vector(
    source: Path,
    dest_dir: Path,
    catalog_root: Path | None = None,
    *,
    reconvert: bool = False,
) -> Path:
    """Convert vector file to GeoParquet.

    Applies the catalog's ``conversion.vector`` settings, which sort rows and
    add a bbox covering column by default (issue #805). A ``.parquet`` source
    is copied unless :func:`_needs_spatial_rewrite` says it must be rewritten.

    Args:
        source: Source vector file.
        dest_dir: Destination directory.
        catalog_root: Catalog root, for reading ``conversion.vector`` settings.
            None uses the built-in defaults.
        reconvert: True when the operator asked to re-convert from source, which
            rewrites a Parquet file whatever its footer already declares.

    Returns:
        Path to the output GeoParquet file.
    """
    import geoparquet_io as gpio  # type: ignore[import-untyped]

    output_path = dest_dir / f"{source.stem}.parquet"
    settings = get_vector_settings(catalog_root) if catalog_root else VectorSettings()

    # Check if already GeoParquet
    if source.suffix.lower() == ".parquet":
        same_file = source.resolve() == output_path.resolve()
        if not _needs_spatial_rewrite(source, settings, reconvert=reconvert):
            if not same_file:
                shutil.copy2(source, output_path)
            return output_path

        from portolan_cli.output import info as info_output
        from portolan_cli.utils import format_size

        # Name the size. The rewrite is a full geoparquet-io read, sort and
        # write, not the copy this path used to do, and it needs room for a
        # second copy of the file while it runs.
        reason = "re-convert requested" if reconvert else "it carries no bbox covering column"
        size = format_size(source.stat().st_size)
        info_output(f"Rewriting {source.name} ({size}): {reason}")
        if same_file:
            # A single-file collection keeps its data where the operator put it,
            # so there is no separate destination to write to. Write a sibling
            # and swap it in, because geoparquet-io cannot write the file it
            # reads.
            _rewrite_or_keep(source, settings)
            return output_path

    # Convert using geoparquet-io fluent API. Wrapped in the shared retry so a
    # transient DuckDB "Query interrupted" does not fail a bulk add (Issue #339
    # nightly test_add_1000_files_* flake); this is the code path add uses.
    def _run() -> None:
        table = apply_vector_settings(gpio.convert(str(source)), settings)
        table.write(str(output_path))

    run_with_transient_convert_retry(_run, source_name=source.name)

    return output_path


def _rewrite_or_keep(source: Path, settings: VectorSettings) -> None:
    """Rewrite a GeoParquet in place, or keep it and say why the rewrite failed.

    ``add`` used to copy a ``.parquet`` source through untouched, so a file
    geoparquet-io cannot read or cannot write faithfully still reached the
    catalog. The rewrite must not turn that into a failed ``add``, and it must
    not replace the operator's data with worse data (issue #805).

    A file kept this way is still non-conformant, and ``check`` reports it. The
    operator sees the reason on the spot rather than a stack trace.

    Args:
        source: GeoParquet file to rewrite.
        settings: Vector conversion settings to apply.
    """
    from portolan_cli.output import warn

    try:
        _rewrite_parquet_in_place(source, settings)
    except RewriteFidelityError as err:
        warn(f"Kept {source.name} as it is: it would lose {err.lost}")
    except Exception as err:  # noqa: BLE001 - a copy is better than a failed add
        logger.warning("Rewrite of %s failed: %s", source, err, exc_info=True)
        warn(f"Kept {source.name} as it is: geoparquet-io could not rewrite it ({err})")


def _rewrite_parquet_in_place(source: Path, settings: VectorSettings) -> None:
    """Rewrite a GeoParquet file with the vector settings applied.

    Writes a sibling temporary file and swaps it in with :meth:`Path.replace`,
    which is atomic within one filesystem. The source keeps its bytes if the
    write raises, so a failed rewrite cannot destroy the operator's data.

    The temporary file is a hidden sibling and carries
    :data:`REWRITE_TEMP_INFIX` in its name. ``add`` collects the source
    directory before it converts anything, and a hard kill skips the cleanup
    below, so a leftover file would otherwise be collected as a source on the
    next run. :func:`portolan_cli.add.is_rewrite_temp` skips it.

    geoparquet-io writes a fresh ``geo`` key and drops every other schema
    metadata key, so any extra key the source carried is restored afterwards.
    That restore reads and rewrites the file a second time, so it runs only
    when the source has such a key.

    Args:
        source: GeoParquet file to rewrite.
        settings: Vector conversion settings to apply.
    """
    import geoparquet_io as gpio

    temp_path = source.with_name(f".{source.stem}{REWRITE_TEMP_INFIX}.parquet")
    preserved = read_extra_schema_metadata(source)
    before = read_rewrite_fidelity(source)

    def _run() -> None:
        table = apply_vector_settings(gpio.convert(str(source)), settings)
        table.write(str(temp_path))

    try:
        run_with_transient_convert_retry(_run, source_name=source.name)
        if preserved:
            _restore_schema_metadata(temp_path, preserved)
        _assert_rewrite_kept_everything(source, temp_path, before)
        temp_path.replace(source)
    finally:
        temp_path.unlink(missing_ok=True)


def _assert_rewrite_kept_everything(
    source: Path, rewritten: Path, before: RewriteFidelity | None
) -> None:
    """Refuse a rewrite that would replace good data with worse data.

    The rewrite overwrites the operator's own file, so it has to prove it lost
    nothing before the swap. geoparquet-io drops the CRS on every write, so a
    projected GeoParquet handed to ``add`` comes back declaring no CRS. The
    next ``add`` then reports a CRS mismatch, and a file whose coordinates fall
    inside the lon/lat range would be mislabeled as WGS84 instead.

    This is a gate on a destructive operation, not a repair. Portolan does not
    put the CRS back. It keeps the operator's file and says why, and ``check``
    still reports the missing covering column.

    Args:
        source: The file the rewrite would replace.
        rewritten: The candidate replacement.
        before: The source's fidelity fields, read before the rewrite ran.

    Raises:
        RewriteFidelityError: When the rewrite loses rows, columns, or the CRS.
    """
    if before is None:
        return
    after = read_rewrite_fidelity(rewritten)
    if after is None:
        raise RewriteFidelityError(source.name, "the ability to be read back")

    if after.row_count != before.row_count:
        raise RewriteFidelityError(
            source.name, f"rows: {before.row_count} became {after.row_count}"
        )

    dropped = before.columns - after.columns
    if dropped:
        raise RewriteFidelityError(source.name, f"the columns {sorted(dropped)}")

    if before.crs is not None and after.crs is None:
        raise RewriteFidelityError(
            source.name, "the CRS it declared, because geoparquet-io writes none"
        )


def _restore_schema_metadata(path: Path, preserved: dict[bytes, bytes]) -> None:
    """Put the source's extra schema metadata back on a rewritten file.

    geoparquet-io owns the ``geo`` key and writes its own, so ``preserved``
    must already exclude it. Every other key the publisher set is restored:
    geopandas writes ``pandas``, and provenance keys are common (issue #805).

    Copies row group by row group so memory does not scale with the file, and
    keeps the compression geoparquet-io chose.

    Args:
        path: Rewritten Parquet file to amend, in place.
        preserved: Schema metadata keys to restore.
    """
    import pyarrow.parquet as pq

    amended = path.with_name(f"{path.name}.meta")

    try:
        reader = pq.ParquetFile(path)
        try:
            schema = reader.schema_arrow
            # The writer's own keys win. Only keys it dropped come back.
            merged = {**preserved, **(schema.metadata or {})}
            groups = reader.num_row_groups
            compression = (
                reader.metadata.row_group(0).column(0).compression.lower() if groups else "snappy"
            )
            with pq.ParquetWriter(
                amended, schema.with_metadata(merged), compression=compression
            ) as writer:
                for index in range(groups):
                    writer.write_table(reader.read_row_group(index))
        finally:
            # Windows cannot replace a file that is still open.
            reader.close()
        amended.replace(path)
    finally:
        amended.unlink(missing_ok=True)


def convert_tabular(source: Path, dest_dir: Path) -> Path:
    """Convert tabular file to Parquet using geoparquet-io (Issue #432).

    Routes CSV/TSV/XLSX through gpio.convert().write() — the same pipeline
    as geo files but with geometry_column=None. This ensures consistent
    compression and row-group sizing across all Parquet outputs.

    For plain Parquet files, copies them directly (no re-conversion needed).

    Args:
        source: Source tabular file (CSV, TSV, XLSX, or plain Parquet).
        dest_dir: Destination directory.

    Returns:
        Path to the output Parquet file.
    """
    import geoparquet_io as gpio

    output_path = dest_dir / f"{source.stem}.parquet"

    # If already Parquet, just copy (no conversion needed)
    if source.suffix.lower() == ".parquet":
        if source.resolve() == output_path.resolve():
            return output_path
        shutil.copy2(source, output_path)
        return output_path

    # Convert CSV/TSV/XLSX using geoparquet-io
    # gpio.convert() auto-detects format and handles non-geo files correctly
    # (logs "Reading as plain table" and returns Table with geometry_column=None)
    # Write with standard Parquet settings (compression, row groups); gpio v1.2.0+
    # handles geometry_column=None correctly in all write strategies. Wrapped in
    # the shared retry so a transient DuckDB "Query interrupted" does not fail a
    # bulk add on the tabular path (Issue #339).
    def _run() -> None:
        table = gpio.convert(str(source))
        table.write(str(output_path))

    run_with_transient_convert_retry(_run, source_name=source.name)

    return output_path


def convert_raster(source: Path, dest_dir: Path) -> Path:
    """Convert raster file to COG.

    Uses Portolan's opinionated COG defaults (see convert command design):
    - DEFLATE compression (universal compatibility, lossless)
    - 512x512 tiles (matches rio-cogeo default, fewer HTTP requests)
    - Predictor and overview resampling derived from the source raster's dtype
      (see derive_cog_defaults)

    For fine-tuned control, power users should use rio_cogeo.cog_translate() directly.

    Args:
        source: Source raster file.
        dest_dir: Destination directory.

    Returns:
        Path to the output COG file.
    """
    from rio_cogeo.cogeo import cog_translate
    from rio_cogeo.profiles import cog_profiles

    from portolan_cli.conversion_config import derive_cog_defaults

    output_path = dest_dir / f"{source.stem}.tif"

    # Check if already a valid COG — skip conversion if so
    if source.suffix.lower() in (".tif", ".tiff") and is_cloud_optimized_geotiff(source):
        # If source is already at the destination, no copy needed (CodeRabbit review)
        if source.resolve() == output_path.resolve():
            return output_path
        # Already a COG, just copy to destination
        shutil.copy2(source, output_path)
        return output_path

    # Convert using rio-cogeo with Portolan's opinionated defaults
    profile = cog_profiles.get("deflate")  # type: ignore[no-untyped-call]

    # Derive predictor and overview resampling from the raster (Issue #690)
    # Note: profile is a copy of the deflate profile dict
    predictor, resampling = derive_cog_defaults(source)
    profile["predictor"] = predictor

    cog_translate(
        str(source),
        str(output_path),
        profile,
        quiet=True,
        overview_resampling=resampling,  # type: ignore[arg-type]
    )

    return output_path


def _generate_raster_thumbnail(cog_path: Path, catalog_root: Path, format_type: FormatType) -> None:
    """Write a ``{stem}.thumb.jpg`` next to a COG so the asset scan registers it.

    Mirrors the thumbnail step in ``convert.convert_file`` for the add path,
    which converts through the bare ``convert_raster`` wrapper and would
    otherwise leave rasters with no thumbnail asset (Issue #657). Gated on the
    ``generate_thumbnail`` COG setting and best-effort: a thumbnail failure must
    never fail the add. Skips generation when the sidecar already exists so a
    hand-curated thumbnail or a re-add is left untouched.

    Non-raster formats return immediately. The gate lives here rather than at the
    call site to keep ``prepare_item`` inside the xenon complexity ceiling.

    Args:
        cog_path: Path to the converted file.
        catalog_root: Catalog root, for loading COG settings.
        format_type: Format of the converted file; only RASTER is handled.
    """
    from portolan_cli.conversion_config import get_cog_settings
    from portolan_cli.convert import generate_cog_thumbnail

    if format_type != FormatType.RASTER:
        return

    settings = get_cog_settings(catalog_root)
    if not settings.generate_thumbnail:
        return
    if cog_path.with_name(f"{cog_path.stem}.thumb.jpg").exists():
        return
    try:
        generate_cog_thumbnail(
            cog_path,
            max_size=settings.thumbnail_max_size,
            quality=settings.thumbnail_quality,
        )
    except Exception as e:  # nosec B110 - thumbnail is optional, failure is non-fatal
        logger.warning("Thumbnail generation failed for %s: %s", cog_path.name, e)
