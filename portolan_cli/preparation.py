"""Per-item preparation and conversion routing for the add pipeline.

Extracted from ``add.py`` (issue #623). This module owns the parallelizable,
GDAL-bound phase of ``add``: format-specific conversion to cloud-native outputs
(GeoParquet / COG / plain Parquet), metadata + statistics extraction, asset
scanning, and STAC item construction. It writes item.json but performs no
versions.json or collection-link updates — those are batched in
``add.finalize_items`` to keep versioning O(n) (see Issue #281).

Per ADR-0007 the CLI stays a thin wrapper; ``add.py`` orchestrates on top of the
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
from portolan_cli.crs import transform_bbox_to_wgs84
from portolan_cli.errors import NoGeometryError
from portolan_cli.formats import FormatType, detect_format, is_cloud_optimized_geotiff
from portolan_cli.metadata import (
    extract_band_statistics,
    extract_cog_metadata,
    extract_flatgeobuf_metadata,
    extract_geoparquet_metadata,
    extract_parquet_statistics,
    extract_pmtiles_metadata,
)
from portolan_cli.metadata.cog import COGMetadata
from portolan_cli.metadata.flatgeobuf import FlatGeobufMetadata
from portolan_cli.metadata.geoparquet import GeoParquetMetadata
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
from portolan_cli.sync.checksums import compute_checksum, compute_dir_checksum, compute_dir_size
from portolan_cli.viz.style import enrich_cog_assets

logger = logging.getLogger(__name__)


# Files to ignore when scanning item directories for assets.
# These are STAC/Portolan structural files, not user data.
# AGENTS.md is referenced via a rel="agents" link, not tracked as an asset
# (ADR-0052: "AGENTS.md is a link, not an asset").
IGNORED_FILES: frozenset[str] = frozenset(
    {
        "catalog.json",
        "collection.json",
        "versions.json",
        "AGENTS.md",
    }
)


# Extension-to-MIME-type mapping for asset files. Derived from the extension
# registry (the single source, ADR-0055). Edit rows there, not this map.
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
# documentation). Derived from the extension registry (ADR-0055). Unknown
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
            ADR-0028. Ignored for item-level (subdirectory) scans.

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
        # Skip hidden files and symlinks unconditionally. The symlink check must
        # precede is_dir()/is_file() branching below: is_dir() follows symlinks,
        # so a symlinked .gdb directory would otherwise be checksummed as a
        # container asset, escaping the intended item boundary.
        if file_path.name.startswith("."):
            continue
        if file_path.is_symlink():
            continue

        # Issue #465: skip siblings that belong to OTHER items in this batch.
        # Keep the primary and its own same-stem source/sidecars; keep loose
        # companions (not in exclude_names) so ADR-0028 tracking is preserved.
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
        #   - Item JSON at {collection_dir}/{item_id}/{item_id}.json (subdirectory)
        #   - Href needs ../{filename} to reach parent (collection) directory
        #
        # Case 2: Data at {collection_dir}/{item_id}/data.parquet
        #   - item_dir == {collection_dir}/{item_id}/
        #   - Item JSON at same level: {collection_dir}/{item_id}/{item_id}.json
        #   - Href just needs {filename} (same directory)
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
            extra_fields={
                "file:size": file_size,
                "file:checksum": f"sha256:{file_checksum}",
            },
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
        item_json_path: Path to item.json (None for collection-level vector assets per ADR-0031).
        is_collection_level_asset: If True, asset is at collection level (ADR-0031).
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
            Vector formats in Hive partitions become collection-level assets
            per ADR-0031.

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
    # Per ADR-0032: forward slashes allowed for nested catalogs
    if (
        not collection_id
        or "\\" in collection_id
        or collection_id in {".", ".."}
        or any(part in {".", ".."} for part in collection_id.split("/"))
    ):
        raise ValueError(
            f"Invalid collection_id '{collection_id}': backslashes and . or .. segments not allowed"
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


def _extract_bbox_wgs84(metadata: AllMetadata) -> list[float]:
    """Extract bbox from metadata, transforming to WGS84 if needed.

    PMTiles bbox is already in WGS84 (4326). Other formats may need
    CRS transformation.

    Args:
        metadata: Metadata object with bbox attribute.

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
                metadata = extract_geoparquet_metadata(output_path)
            else:
                output_path = convert_vector(path, item_dir)
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
        collection_path: Collection directory for hierarchical config (ADR-0039).

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
    """Fix asset hrefs and keys for collection-level assets (ADR-0031).

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
        item_datetime: Optional acquisition/creation datetime (per ADR-0035).
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
        path, item_dir, format_type, force=force, reconvert=reconvert
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
                + "\n".join(f"  - {e}" for e in defaults_errors)
            )

    # Step 4: Extract and transform bbox
    if not metadata.bbox:
        _cleanup_orphaned_output(output_path, item_dir, path)
        raise NoGeometryError(
            path=metadata.id if hasattr(metadata, "id") else path.stem,
            reason="The source file may have no valid geometry.",
        )
    bbox = _extract_bbox_wgs84(metadata)

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

    # Step 7: Create STAC item or collection-level assets (per ADR-0031)
    # Collection-level vector assets: no item.json, assets go directly in collection.json
    # Item-level assets (rasters, partitioned vectors): create item.json as usual
    if is_collection_level_asset and format_type == FormatType.VECTOR:
        # Collection-level vector asset: no item.json per ADR-0031
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


def convert_vector(source: Path, dest_dir: Path) -> Path:
    """Convert vector file to GeoParquet.

    Args:
        source: Source vector file.
        dest_dir: Destination directory.

    Returns:
        Path to the output GeoParquet file.
    """
    import geoparquet_io as gpio  # type: ignore[import-untyped]

    output_path = dest_dir / f"{source.stem}.parquet"

    # Check if already GeoParquet
    if source.suffix.lower() == ".parquet":
        # If source is already at the destination, no copy needed
        if source.resolve() == output_path.resolve():
            return output_path
        shutil.copy2(source, output_path)
        return output_path

    # Convert using geoparquet-io fluent API
    gpio.convert(str(source)).write(str(output_path))

    return output_path


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
    table = gpio.convert(str(source))

    # Write with standard Parquet settings (compression, row groups)
    # gpio v1.2.0+ handles geometry_column=None correctly in all write strategies
    table.write(str(output_path))

    return output_path


def convert_raster(source: Path, dest_dir: Path) -> Path:
    """Convert raster file to COG.

    Uses Portolan's opinionated COG defaults (see convert command design):
    - DEFLATE compression (universal compatibility, lossless)
    - Predictor=2 (horizontal differencing, improves compression)
    - 512x512 tiles (matches rio-cogeo default, fewer HTTP requests)
    - Nearest resampling (safe for all data types: categorical, imagery, elevation)

    For fine-tuned control, power users should use rio_cogeo.cog_translate() directly.

    Args:
        source: Source raster file.
        dest_dir: Destination directory.

    Returns:
        Path to the output COG file.
    """
    from rio_cogeo.cogeo import cog_translate
    from rio_cogeo.profiles import cog_profiles

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

    # Apply predictor=2 for better compression
    # Note: profile is a copy of the deflate profile dict
    profile["predictor"] = 2

    cog_translate(
        str(source),
        str(output_path),
        profile,
        quiet=True,
        overview_resampling="nearest",  # Safe for all data types
    )

    return output_path
