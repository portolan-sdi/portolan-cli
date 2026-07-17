"""Add pipeline module - manages the add/list/info/remove workflow.

This module orchestrates the complete workflow for managing collections in a
Portolan catalog:
1. Format detection (route to vector or raster handler)
2. Conversion to cloud-native format (GeoParquet or COG)
3. Metadata extraction
4. STAC item/collection creation
5. versions.json update
6. File staging

Per ADR-0007, all logic lives here; the CLI is a thin wrapper.
"""

from __future__ import annotations

import json
import logging
import shutil
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import click
import pystac

from portolan_cli.collection import (
    _compute_union_bbox,
    _get_metadata_yaml_bbox,
    _get_sibling_collection_bboxes,
)
from portolan_cli.collection_id import (
    infer_nested_collection_id,
    resolve_collection_id,  # noqa: F401
)
from portolan_cli.config import get_setting
from portolan_cli.constants import (
    GEOSPATIAL_EXTENSIONS,
    TABULAR_EXTENSIONS,
)
from portolan_cli.conversion_config import get_vector_settings
from portolan_cli.convert import convert_multilayer_file
from portolan_cli.discovery import get_sidecars, iter_files_with_sidecars, iter_geospatial_files
from portolan_cli.errors import NoGeometryError

# Batch finalization (STAC-write + backend coordination) was extracted to
# finalization.py (issue #624). add.py orchestrates on top of it: add_files /
# add / add_directory call finalize_items, and _ensure_tabular_collection reuses
# _update_catalog_links. These names are re-exported so the finalize helpers
# resolve through add's namespace (keeping test patches of
# ``portolan_cli.add.finalize_items`` effective) and the test-suite keeps
# importing them from this module. The edge is one-directional (add ->
# finalization); finalization imports nothing from add.
from portolan_cli.finalization import (  # noqa: F401
    _asset_freshness_fields as _asset_freshness_fields,  # noqa: PLC0414
)
from portolan_cli.finalization import (  # noqa: F401
    _batch_update_versions as _batch_update_versions,  # noqa: PLC0414
)
from portolan_cli.finalization import (  # noqa: F401
    _ensure_partition_metadata as _ensure_partition_metadata,  # noqa: PLC0414
)
from portolan_cli.finalization import (  # noqa: F401
    _finalize_with_backend as _finalize_with_backend,  # noqa: PLC0414
)
from portolan_cli.finalization import (  # noqa: F401
    _get_or_create_collection as _get_or_create_collection,  # noqa: PLC0414
)
from portolan_cli.finalization import (  # noqa: F401
    _recompute_collection_extent_with_multibbox as _recompute_collection_extent_with_multibbox,  # noqa: PLC0414
)
from portolan_cli.finalization import (  # noqa: F401
    _save_collection_with_links as _save_collection_with_links,  # noqa: PLC0414
)
from portolan_cli.finalization import (
    _update_catalog_links as _update_catalog_links,  # noqa: PLC0414
)
from portolan_cli.finalization import (
    finalize_items as finalize_items,  # noqa: PLC0414
)
from portolan_cli.formats import (
    FormatType,
    is_multilayer,
    list_layers,
)
from portolan_cli.humanize import humanize_slug
from portolan_cli.preparation import (
    _MEDIA_TYPE_MAP as _MEDIA_TYPE_MAP,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    _ROLE_MAP as _ROLE_MAP,  # noqa: PLC0414
)

# Per-item preparation and conversion routing were extracted to preparation.py
# (issue #623). add.py orchestrates on top of it: some names below are used
# directly by the finalize/asset-update code that stays here, others are
# re-exported so external callers (external.py) and the test-suite keep importing
# them from this module. The dependency edge is one-directional (add ->
# preparation); preparation imports nothing from add.
from portolan_cli.preparation import (  # noqa: F401
    IGNORED_FILES as IGNORED_FILES,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    PreparedItem as PreparedItem,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    _convert_and_extract_metadata as _convert_and_extract_metadata,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    _create_and_save_item as _create_and_save_item,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    _derive_item_id_and_asset_level as _derive_item_id_and_asset_level,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    _get_asset_role as _get_asset_role,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    _get_media_type as _get_media_type,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    _handle_cloud_native_vector as _handle_cloud_native_vector,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    _pre_validate_geometry as _pre_validate_geometry,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    _scan_item_assets as _scan_item_assets,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    _warn_if_source_newer as _warn_if_source_newer,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    convert_raster as convert_raster,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    convert_tabular as convert_tabular,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    convert_vector as convert_vector,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    prepare_item as prepare_item,  # noqa: PLC0414
)
from portolan_cli.query import ItemInfo, get_item_info, is_current, list_items  # noqa: F401
from portolan_cli.remove import remove_files  # noqa: F401
from portolan_cli.stac import (
    MergeStrategy,
    create_collection,
    update_collection_file_statistics,
)
from portolan_cli.sync.checksums import compute_checksum
from portolan_cli.viz.style import enrich_cog_assets

logger = logging.getLogger(__name__)

# Error message patterns from geoparquet-io for non-geospatial CSV/TSV files.
# These specific patterns indicate the file lacks geometry columns (not other errors
# like permission denied, encoding issues, or memory errors).
# See: https://github.com/geoparquet/geoparquet-io (geometry detection logic)
_GEOPARQUET_IO_NO_GEOMETRY_PATTERNS: tuple[str, ...] = (
    "could not detect geometry columns",
    "geometry columns in csv",
    "geometry columns in tsv",
)

# Error message patterns for parquet files without geometry (Issue #177).
# These patterns indicate a parquet file lacks GeoParquet metadata (no 'geo' key),
# meaning it's tabular data that should be tracked as an auxiliary asset.
_PARQUET_NO_GEOMETRY_PATTERNS: tuple[str, ...] = (
    "missing bounding box",
    "no valid geometry",
)


def _is_parquet_no_geometry_error(err: ValueError) -> bool:
    """Check if a ValueError indicates a parquet file lacks geometry (Issue #177).

    This handles the case where a parquet file is valid but has no GeoParquet
    metadata (no 'geo' key in schema). Such files should be tracked as auxiliary
    assets per ADR-0028, not rejected.

    Args:
        err: The ValueError to check.

    Returns:
        True if the error is specifically about missing geometry in a parquet file.
    """
    err_msg = str(err).lower()
    return any(pattern in err_msg for pattern in _PARQUET_NO_GEOMETRY_PATTERNS)


def _batch_sibling_names(sources: list[Path]) -> frozenset[str]:
    """Base filenames of a batch's source files plus their converted outputs.

    Used to prune cross-item re-scanning of collection-level assets (issue #465).
    For each source we include its filename and its deterministic converted
    output ``{stem}.parquet`` (which equals the source for cloud-native parquet).

    Names, not paths, so the per-sibling membership check in ``_scan_item_assets``
    is a plain set lookup — no ``resolve()``/``stat`` syscall per (file × sibling),
    which would reintroduce O(n²) I/O. This is collision-safe: siblings only ever
    share the one collection directory, every batch name carries a geospatial or
    ``.parquet`` extension, and any real file with such a name is itself tracked
    by its own ``prepare_item`` (or the deferred non-geo pass). Loose companions
    that rely solely on the scan (``.txt``/``.png``/``.xml`` …) can never match.

    Multi-layer sources (GeoPackage/FileGDB) expand to one
    ``{stem}_{layer}.parquet`` output per layer (``convert_multilayer_file``),
    each tracked as its own layer item; their names are included so sibling
    layers skip each other too. ``list_layers`` is cheap for single-layer
    formats (extension check, no GDAL open) and reuses the exact naming
    ``convert_multilayer_file`` applies.
    """
    names: set[str] = set()
    for src in sources:
        names.add(src.name)
        names.add(f"{src.stem}.parquet")
        try:
            layers = list_layers(src)
        except Exception:
            # Pruning is a best-effort optimization; a listing failure only
            # means a few extra sibling scans, never incorrect output.
            layers = None
        if layers:
            for layer in layers:
                names.add(f"{src.stem}_{layer}.parquet")
    return frozenset(names)


@dataclass
class AddFailure:
    """Information about a failed add operation.

    Used by add_files to report files that could not be processed.

    Attributes:
        path: Path to the file that failed to add.
        error: Human-readable error message describing the failure.
    """

    path: Path
    error: str


def _maybe_partition_large_file(
    prepared: PreparedItem,
    catalog_root: Path,
    item_datetime: datetime | None,
    skip_partitioning: bool = False,
) -> list[PreparedItem]:
    """Partition a large GeoParquet file if it exceeds the size threshold.

    Per ADR-0031 and Issue #352: Files > 2GB should be spatially partitioned.
    Each partition becomes a STAC Item with its own bbox.

    Args:
        prepared: The prepared item to potentially partition.
        catalog_root: Root directory of the catalog.
        item_datetime: Optional datetime for created items.
        skip_partitioning: If True, skip partitioning even if file exceeds threshold.
            Used when user declines interactive prompt.

    Returns:
        List of PreparedItems. If partitioning occurred, contains multiple
        items (one per partition). Otherwise, returns [prepared] unchanged.
    """
    from portolan_cli.config import get_setting
    from portolan_cli.partitioning import (
        build_glob_pattern,
        get_partition_metadata,
        partition_geoparquet,
        should_partition,
    )

    # Only partition vector formats (GeoParquet)
    if prepared.format_type != FormatType.VECTOR:
        return [prepared]

    # Skip if user declined interactive prompt
    if skip_partitioning:
        return [prepared]

    # Only partition item-level assets (collection-level means single file < 2GB)
    # But wait - if file is > 2GB, it should NOT be collection-level, it should be partitioned
    # So we check the actual file, not the is_collection_level_asset flag

    # Find the primary parquet file in asset_files
    parquet_files = [
        path
        for filename, (path, _checksum, _size) in prepared.asset_files.items()
        if filename.endswith(".parquet")
    ]
    if not parquet_files:
        return [prepared]

    primary_parquet = parquet_files[0]

    # Check if partitioning is enabled and file exceeds threshold
    collection_dir = catalog_root / Path(*prepared.collection_id.split("/"))
    partitioning_enabled = get_setting(
        "partitioning.enabled",
        catalog_path=catalog_root,
        collection_path=collection_dir,
    )
    if partitioning_enabled is False:
        return [prepared]

    threshold_gb = (
        get_setting(
            "partitioning.threshold_gb",
            catalog_path=catalog_root,
            collection_path=collection_dir,
        )
        or 2.0
    )

    if not should_partition(primary_parquet, threshold_gb=float(threshold_gb)):
        return [prepared]

    # File needs partitioning
    strategy = (
        get_setting(
            "partitioning.strategy",
            catalog_path=catalog_root,
            collection_path=collection_dir,
        )
        or "kdtree"
    )

    target_rows = (
        get_setting(
            "partitioning.target_rows",
            catalog_path=catalog_root,
            collection_path=collection_dir,
        )
        or 120_000
    )

    # Create partition output directory (same level as original file)
    # Original: collection/data.parquet
    # Partitioned: collection/kdtree_cell=001/data.parquet, etc.
    partition_output_dir = primary_parquet.parent

    # Partition the file FIRST, before any cleanup
    # This ensures atomicity: if partitioning fails, original files remain intact
    # Rollback on failure is handled by partition_geoparquet itself
    partition_files = partition_geoparquet(
        input_path=primary_parquet,
        output_dir=partition_output_dir,
        strategy=str(strategy),
        target_rows=int(target_rows),
    )

    # Partitioning succeeded - now safe to clean up original artifacts
    # Delete the item.json that was created for the single file
    if prepared.item_json_path and prepared.item_json_path.exists():
        prepared.item_json_path.unlink()

    # Delete original large file (now replaced by partitions)
    if primary_parquet.exists():
        primary_parquet.unlink()

    # Create PreparedItem for each partition
    partitioned_items: list[PreparedItem] = []

    for partition_path in partition_files:
        # Create STAC item for this partition
        # item_id auto-derived from partition_path.parent.name (e.g., "kdtree_cell=0000000000")
        partition_prepared = prepare_item(
            path=partition_path,
            catalog_root=catalog_root,
            collection_id=prepared.collection_id,
            item_datetime=item_datetime,
        )
        partitioned_items.append(partition_prepared)

    # Add collection-level glob asset for bulk access (Issue #351)
    # This provides a single glob URL for DuckDB/PyArrow/GDAL to read all partitions
    glob_pattern = build_glob_pattern(str(strategy))
    glob_asset = pystac.Asset(
        href=glob_pattern,
        media_type="application/vnd.apache.parquet",
        roles=["data"],
        title="Partitioned GeoParquet",
        description=f"Glob pattern for {len(partition_files)} spatial partitions",
        # portolan:glob will be populated on push with remote URL
    )

    # Extract partition metadata for STAC partition extension (Issue #232)
    partition_meta = get_partition_metadata(partition_output_dir, str(strategy))

    # Create a PreparedItem for the glob asset (collection-level)
    # Use original item_id as base to avoid collisions across collections
    glob_item_id = f"{prepared.item_id}_partitioned"
    glob_prepared = PreparedItem(
        item_id=glob_item_id,
        collection_id=prepared.collection_id,
        format_type=FormatType.VECTOR,
        bbox=prepared.bbox,
        asset_files={},  # No physical files - glob is a pattern reference
        item_json_path=None,
        is_collection_level_asset=True,
        stac_item=None,
        stac_assets={glob_item_id: glob_asset},
        metadata=None,
        partition_metadata=partition_meta,
    )
    partitioned_items.append(glob_prepared)

    return partitioned_items


def add(
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
) -> ItemInfo:
    """Add files to a Portolan catalog.

    This is a convenience wrapper around prepare_item() + finalize_items()
    for adding a single file. For batch operations, use those functions directly
    to achieve O(n) versioning instead of O(n²). See Issue #281.

    Args:
        path: Path to the source file.
        catalog_root: Root directory of the catalog.
        collection_id: Collection to add the data to.
        title: Optional display title for the item.
        description: Optional description.
        item_id: Optional item ID (defaults to parent directory name).
        item_datetime: Optional acquisition/creation datetime (per ADR-0035).
            If None, uses null datetime with open interval (per ADR-0035).
        force: If True, bypass change detection and re-process (Issue #386).
        reconvert: If True, re-convert from source (requires force=True).

    Returns:
        ItemInfo with details about the added item.

    Raises:
        ValueError: If the format is unsupported or collection_id is invalid.
        FileNotFoundError: If the source file doesn't exist.
    """
    # Prepare: extract metadata, convert, create STAC item
    prepared = prepare_item(
        path=path,
        catalog_root=catalog_root,
        collection_id=collection_id,
        title=title,
        description=description,
        item_id=item_id,
        item_datetime=item_datetime,
        force=force,
        reconvert=reconvert,
    )

    # Finalize: batch write versions.json and collection.json
    results = finalize_items(catalog_root, [prepared])

    # Return the single result with title/description preserved
    result = results[0]
    return ItemInfo(
        item_id=result.item_id,
        collection_id=result.collection_id,
        format_type=result.format_type,
        bbox=result.bbox,
        asset_paths=result.asset_paths,
        title=title,
        description=description,
    )


def _ensure_tabular_collection(
    catalog_root: Path,
    collection_id: str,
    collection_dir: Path,
) -> None:
    """Ensure a collection exists for standalone tabular data (Issue #432).

    For tabular-only collections (no geometry), creates a collection with
    spatial extent determined by (in priority order per ADR-0047):
    1. Explicit bbox in metadata.yaml (manual override)
    2. Inherited from sibling geo collections (AOI inheritance)
    3. Global fallback [-180, -90, 180, 90]

    Per design decision: companion tabular data is almost always about the same
    area as the catalog's geo data, so inheriting the AOI is correct and zero-friction.

    Args:
        catalog_root: Root directory of the catalog.
        collection_id: Collection identifier.
        collection_dir: Path to the collection directory.
    """
    collection_json_path = collection_dir / "collection.json"

    if collection_json_path.exists():
        # Collection already exists (maybe from previous geo files)
        # Preserve existing extent
        return

    # Priority 1: Check metadata.yaml for explicit bbox (ADR-0047)
    explicit_bbox = _get_metadata_yaml_bbox(collection_dir)
    if explicit_bbox is not None:
        bbox_source = "metadata.yaml"
        final_bbox = explicit_bbox
        sibling_count = 0
    else:
        # Priority 2: AOI inheritance from sibling geo collections
        sibling_bboxes = _get_sibling_collection_bboxes(catalog_root)
        final_bbox = _compute_union_bbox(sibling_bboxes)  # Falls back to global if empty
        sibling_count = len(sibling_bboxes)
        bbox_source = "sibling" if sibling_count > 0 else "global"

    # Issue #502: human-readable title; description defaults to it.
    tabular_title = humanize_slug(collection_id)
    collection = create_collection(
        collection_id=collection_id,
        description=tabular_title,
        title=tabular_title,
        bbox=final_bbox,
    )

    # Mark as non-geospatial tabular collection (RULE-0090, ADR-0047)
    collection.extra_fields["portolan:geospatial"] = False

    # Save collection.json
    collection_dir.mkdir(parents=True, exist_ok=True)
    collection.set_self_href(str(collection_json_path))
    collection.save_object()

    # Update catalog links to include this collection
    _update_catalog_links(catalog_root, collection_id)

    # Issue #502: backfill the human-readable title onto the new child link.
    from portolan_cli.catalog import ensure_link_titles

    ensure_link_titles(catalog_root)

    # Log based on bbox source (ADR-0047 priority order)
    if bbox_source == "metadata.yaml":
        logger.info(
            "Created tabular collection %s with extent from metadata.yaml",
            collection_id,
        )
    elif bbox_source == "sibling":
        logger.info(
            "Created tabular collection %s with extent inherited from %d sibling collection(s)",
            collection_id,
            sibling_count,
        )
    else:
        logger.info(
            "Created tabular collection %s with global extent (no sibling collections)",
            collection_id,
        )


def _update_versions(
    collection_dir: Path,
    item_id: str,
    collection_id: str,
    output_path: Path | None = None,
    checksum: str | None = None,
    *,
    asset_files: dict[str, tuple[Path, str, int]] | None = None,
    is_collection_level_asset: bool = False,
    catalog_root: Path | None = None,
) -> None:
    """Update versions via the active backend.

    Supports both single-file (backward compat) and multi-file modes.
    Routes through version_ops.publish_version() so that the configured
    backend (file or plugin) handles storage.

    Args:
        collection_dir: Path to collection directory.
        item_id: Item identifier.
        collection_id: Collection identifier (full path like "climate/hittekaart" for nested).
        output_path: Path to single output file (legacy mode).
        checksum: SHA-256 checksum for single file (legacy mode).
        asset_files: Dict mapping filename to (path, checksum, size) tuples.
            If provided, output_path/checksum are ignored.
        is_collection_level_asset: If True, asset is at collection level (per ADR-0031).
            Affects href construction (no item_id in path).
        catalog_root: Root directory of the catalog. If None, derived from collection_dir.
    """
    from portolan_cli.version_ops import publish_version

    if catalog_root is None:
        catalog_root = collection_dir.parents[len(Path(collection_id).parts) - 1]

    # Build assets dict (asset_key -> file_path) for the backend.
    # The backend (file or plugin) handles checksum/size computation internally.
    assets: dict[str, str] = {}
    if asset_files is not None:
        # Multi-asset mode (per issue #133)
        for filename, (file_path, _checksum, _size) in asset_files.items():
            # For collection-level assets (Issue #250, ADR-0031), use filename only.
            # Both backends prepend collection/ when building the href,
            # so do NOT include collection_id here to avoid doubling.
            if is_collection_level_asset:
                asset_key = filename
            else:
                asset_key = f"{item_id}/{filename}"
            assets[asset_key] = str(file_path)
    elif output_path is not None and checksum is not None:
        # Legacy single-file mode (backward compatibility)
        assets[output_path.name] = str(output_path)
    else:
        raise ValueError("Either asset_files or (output_path, checksum) must be provided")

    published = publish_version(
        collection_id,
        assets=assets,
        catalog_root=catalog_root,
    )

    # Mirror the collection's new state into the catalog-level versions.json
    # index (ADR-0005), matching what finalize_items does for item-level and
    # geo collection-level assets. The deferred tabular / companion path reaches
    # versions.json only through here, so without this the collection is missing
    # from the catalog-level index (issue #650). update_catalog_versions no-ops
    # when there is no catalog-level versions.json (non-file backends).
    from portolan_cli.catalog import update_catalog_versions

    try:
        update_catalog_versions(
            catalog_root=catalog_root,
            collection_id=collection_id,
            current_version=published.version,
            asset_count=len(published.assets),
            total_size_bytes=sum(a.size_bytes for a in published.assets.values()),
        )
    except Exception:
        # The collection-level version was published successfully; a catalog-level
        # sync failure should not fail the add (mirrors _finalize_with_file_backend).
        logger.warning(
            "Failed to update catalog-level versions.json for collection '%s'. "
            "Collection version was published but catalog-level view may be stale.",
            collection_id,
            exc_info=True,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Directory handling
# ─────────────────────────────────────────────────────────────────────────────

# Note: GEOSPATIAL_EXTENSIONS imported from portolan_cli.constants
# Note: is_filegdb is imported from portolan_cli.scan.detect (canonical implementation).
# scan_detect.is_filegdb accepts either .gdbtable files OR a 'gdb' marker file, which
# matches the full FileGDB spec. Do not reimplement here.


def add_directory(
    *,
    path: Path,
    catalog_root: Path,
    collection_id: str,
    recursive: bool = True,
    force: bool = False,
    reconvert: bool = False,
) -> list[ItemInfo]:
    """Add all geospatial files in a directory to a collection.

    Uses batch versioning (Issue #281) for O(n) instead of O(n²) performance.

    Args:
        path: Directory containing geospatial files.
        catalog_root: Root directory containing .portolan/.
        collection_id: Collection to add files to.
        recursive: If True, process subdirectories recursively.
        force: If True, bypass change detection and re-process (Issue #386).
        reconvert: If True, re-convert from source (requires force=True).

    Returns:
        List of ItemInfo for each added item.
    """
    files = list(iter_geospatial_files(path, recursive=recursive))

    # Issue #465: skip cross-item siblings when scanning collection-level assets
    # so per-file work stays O(n) instead of O(n²).
    batch_exclude_names = _batch_sibling_names(files)

    # Phase 1: Prepare all items (GDAL work, parallelizable)
    prepared: list[PreparedItem] = []
    for file_path in files:
        result = prepare_item(
            path=file_path,
            catalog_root=catalog_root,
            collection_id=collection_id,
            force=force,
            reconvert=reconvert,
            exclude_sibling_names=batch_exclude_names,
        )
        prepared.append(result)

    # Phase 2: Finalize (batch write versions.json + collection.json)
    return finalize_items(catalog_root=catalog_root, prepared=prepared)


# ─────────────────────────────────────────────────────────────────────────────
# Sidecar auto-detection (per issue #97)
# ─────────────────────────────────────────────────────────────────────────────

# Note: SIDECAR_PATTERNS imported from portolan_cli.constants


def _is_no_geometry_error(err: click.ClickException) -> bool:
    """Check if a ClickException is specifically a 'no geometry columns' error from geoparquet-io.

    This narrows the exception handling to ONLY geometry detection errors,
    avoiding accidentally catching permission errors, encoding issues, or
    memory errors that might also be wrapped in ClickException.

    Args:
        err: The ClickException to check.

    Returns:
        True if the error is specifically about missing geometry columns.
    """
    err_msg = (str(err.message) if hasattr(err, "message") else str(err)).lower()
    return any(pattern in err_msg for pattern in _GEOPARQUET_IO_NO_GEOMETRY_PATTERNS)


def _copy_non_geo_to_item_dir(
    file_path: Path,
    item_dir: Path,
) -> Path:
    """Copy a non-geospatial file to an item directory as a companion asset.

    Per ADR-0028, ALL files in item directories should be tracked as STAC assets.
    Non-geospatial CSV/TSV files are copied (not converted) and tracked alongside
    the primary geospatial data.

    Args:
        file_path: Source file path.
        item_dir: Destination item directory.

    Returns:
        Path to the copied file in item_dir.
    """
    dest_path = item_dir / file_path.name
    if dest_path.exists() and dest_path.resolve() == file_path.resolve():
        # Already in place
        return dest_path
    shutil.copy2(file_path, dest_path)
    return dest_path


def _ensure_nested_catalogs(
    collection_id: str, catalog_root: Path, setup_collections: set[str]
) -> None:
    """Ensure intermediate catalogs exist for nested collection IDs (ADR-0032).

    Args:
        collection_id: The collection ID (may be nested like "climate/hittekaart").
        catalog_root: Root directory of the catalog.
        setup_collections: Set of already-setup collection IDs (mutated).
    """
    if collection_id in setup_collections:
        return

    from portolan_cli.catalog import create_intermediate_catalogs

    create_intermediate_catalogs(collection_id, catalog_root)
    setup_collections.add(collection_id)


def _collect_files_for_add(
    paths: list[Path],
    catalog_root: Path,
    collection_id: str | None,
    skipped: list[Path],
    setup_collections: set[str],
    *,
    force: bool = False,
) -> list[tuple[Path, str]]:
    """Collect and filter files for add operation (Phase 1).

    This is the fast, sequential phase that doesn't involve GDAL.
    Extracts from add_files() to reduce cyclomatic complexity.

    Args:
        paths: List of paths to add (files or directories).
        catalog_root: Root directory of the catalog.
        collection_id: Optional explicit collection ID.
        skipped: List to append skipped paths to (mutated).
        setup_collections: Set to track which collections have been set up (mutated).
        force: If True, bypass change detection (Issue #386).

    Returns:
        List of (file_path, collection_id) tuples to process.
    """
    processed_paths: set[Path] = set()
    files_to_process: list[tuple[Path, str]] = []

    for path in paths:
        if path.is_dir():
            files = iter_files_with_sidecars(path)
        else:
            files = [path] + get_sidecars(path)

        for file_path in files:
            # Resolve symlinks to track the real file
            if file_path.is_symlink():
                file_path = file_path.resolve()

            if file_path in processed_paths:
                continue
            processed_paths.add(file_path)

            # Skip non-geospatial files
            if file_path.suffix.lower() not in GEOSPATIAL_EXTENSIONS:
                continue

            # Determine collection ID (ADR-0032: use full nested path)
            coll_id = collection_id
            if coll_id is None:
                try:
                    coll_id = infer_nested_collection_id(file_path, catalog_root)
                except ValueError as err:
                    from portolan_cli.output import warn as warn_output

                    warn_output(f"Skipping {file_path.name}: {err}")
                    skipped.append(file_path)
                    continue

            # Check if unchanged (skip this check when force=True per Issue #386)
            if not force:
                versions_path = catalog_root / Path(*coll_id.split("/")) / "versions.json"
                if is_current(file_path, versions_path):
                    skipped.append(file_path)
                    continue

            # Set up nested catalog structure if needed (ADR-0032)
            _ensure_nested_catalogs(coll_id, catalog_root, setup_collections)

            files_to_process.append((file_path, coll_id))

    return files_to_process


@dataclass(frozen=True)
class _PrepareOptions:
    """Immutable per-batch inputs threaded through the phase-2 prep helpers.

    Bundling these keeps the prep functions module-level (not a closure over
    ``add_files``) so nesting stays shallow and each helper is independently
    unit-testable. The instance is read-only, so it is safe to share across
    worker threads in the parallel path.
    """

    catalog_root: Path
    item_id: str | None
    item_datetime: datetime | None
    force: bool
    reconvert: bool
    skip_partitioning: bool
    batch_exclude_names: frozenset[str]


@dataclass
class _ProcessResult:
    """Accumulated output of phase 2 (per-item preparation).

    ``prepared_items`` feed ``finalize_items``; ``deferred_non_geo`` plus the
    ``source_to_*`` maps drive phase 3 (deferred non-geo companion assets);
    ``failures`` are surfaced to the caller (Issue #175).
    """

    prepared_items: list[PreparedItem] = field(default_factory=list)
    failures: list[AddFailure] = field(default_factory=list)
    deferred_non_geo: list[tuple[Path, Path, str]] = field(default_factory=list)
    source_to_item_dir: dict[Path, tuple[Path, str, str]] = field(default_factory=dict)
    source_to_collection_dir: dict[Path, tuple[Path, str]] = field(default_factory=dict)


def _prepare_multilayer_file(
    file_path: Path,
    coll_id: str,
    opts: _PrepareOptions,
) -> tuple[list[PreparedItem], list[AddFailure]]:
    """Convert + prepare every layer of a multi-layer file (Issue #265).

    GeoPackage / FileGDB are split into one parquet (and one PreparedItem) per
    layer. A failure preparing a single layer is recorded and the remaining
    layers continue (Issue #175); a failure of the conversion itself fails the
    whole file with a single AddFailure.
    """
    prepared_list: list[PreparedItem] = []
    failure_list: list[AddFailure] = []

    try:
        # Load vector settings from catalog config, then convert all layers.
        vector_settings = get_vector_settings(opts.catalog_root)
        results = convert_multilayer_file(file_path, file_path.parent, settings=vector_settings)

        for result in results:
            if not (result.success and result.output):
                failure_list.append(
                    AddFailure(path=file_path, error=f"Layer {result.layer}: {result.error}")
                )
                continue
            try:
                prepared = prepare_item(
                    path=result.output,
                    catalog_root=opts.catalog_root,
                    collection_id=coll_id,
                    item_id=None,  # Derive from output filename
                    item_datetime=opts.item_datetime,
                    force=opts.force,
                    reconvert=opts.reconvert,
                    exclude_sibling_names=opts.batch_exclude_names,
                )
                # Apply partitioning to each layer (Issue #352)
                prepared_list.extend(
                    _maybe_partition_large_file(
                        prepared=prepared,
                        catalog_root=opts.catalog_root,
                        item_datetime=opts.item_datetime,
                        skip_partitioning=opts.skip_partitioning,
                    )
                )
            except Exception as err:
                failure_list.append(
                    AddFailure(path=result.output, error=f"Layer {result.layer}: {err}")
                )

        return (prepared_list, failure_list)

    except Exception as err:
        return ([], [AddFailure(path=file_path, error=str(err))])


def _prepare_single_file(
    file_path: Path,
    coll_id: str,
    opts: _PrepareOptions,
) -> tuple[list[PreparedItem], list[AddFailure], tuple[Path, Path, str] | None]:
    """Prepare one source file for finalization (phase 2, parallelizable).

    Runs ``prepare_item()`` (GDAL work + item.json) but writes no versions.json
    or collection.json — those are batched in ``finalize_items`` (Issue #281).
    Returns ``(prepared_items, failures, deferred)`` where ``deferred`` is a
    ``(file, source_dir, collection_id)`` tuple for a non-geo tabular file to be
    tracked as a companion asset in phase 3 (ADR-0028), else ``None``.
    """
    # Multi-layer files (GeoPackage, FileGDB) split into one item per layer.
    if is_multilayer(file_path):
        prepared_list, failure_list = _prepare_multilayer_file(file_path, coll_id, opts)
        return (prepared_list, failure_list, None)

    # Single-layer file - original behavior
    try:
        prepared = prepare_item(
            path=file_path,
            catalog_root=opts.catalog_root,
            collection_id=coll_id,
            item_id=opts.item_id,
            item_datetime=opts.item_datetime,
            force=opts.force,
            reconvert=opts.reconvert,
            exclude_sibling_names=opts.batch_exclude_names,
        )
        # Check if file should be partitioned (Issue #352)
        # Returns multiple PreparedItems if partitioned, else [prepared]
        partitioned = _maybe_partition_large_file(
            prepared=prepared,
            catalog_root=opts.catalog_root,
            item_datetime=opts.item_datetime,
            skip_partitioning=opts.skip_partitioning,
        )
        return (partitioned, [], None)

    except click.ClickException as err:
        if _is_no_geometry_error(err) and file_path.suffix.lower() in TABULAR_EXTENSIONS:
            return ([], [], (file_path, file_path.parent, coll_id))
        return ([], [AddFailure(path=file_path, error=str(err))], None)

    except NoGeometryError as err:
        if file_path.suffix.lower() in TABULAR_EXTENSIONS:
            return ([], [], (file_path, file_path.parent, coll_id))
        return ([], [AddFailure(path=file_path, error=str(err))], None)

    except ValueError as err:
        if _is_parquet_no_geometry_error(err) and file_path.suffix.lower() in TABULAR_EXTENSIONS:
            return ([], [], (file_path, file_path.parent, coll_id))
        return ([], [AddFailure(path=file_path, error=str(err))], None)

    except Exception as err:
        return ([], [AddFailure(path=file_path, error=str(err))], None)


def _record_prepared(
    result: _ProcessResult,
    prepared_list: list[PreparedItem],
    file_path: Path,
    coll_id: str,
    catalog_root: Path,
) -> None:
    """Fold one file's prepared items into the running phase-2 accumulator.

    Maps the file's source dir to its item dir (item-level) or collection dir
    (collection-level, Issue #383) so phase 3 can place any deferred non-geo
    companion in the right location.
    """
    source_dir = file_path.parent
    collection_dir = catalog_root / Path(*coll_id.split("/"))
    for prepared in prepared_list:
        result.prepared_items.append(prepared)
        if prepared.is_collection_level_asset:
            # Collection-level: map source to collection dir (Issue #383)
            result.source_to_collection_dir[source_dir] = (collection_dir, coll_id)
        else:
            # Item-level: map source to item dir
            item_dir = collection_dir / prepared.item_id
            result.source_to_item_dir[source_dir] = (item_dir, coll_id, prepared.item_id)


def _phase_process(
    files_to_process: list[tuple[Path, str]],
    opts: _PrepareOptions,
    *,
    workers: int,
    json_mode: bool,
    on_progress: Callable[[Path], None] | None,
) -> _ProcessResult:
    """Phase 2: prepare every collected file, sequentially or across workers.

    Each file is prepared independently (writes only its own item.json), so the
    work parallelizes cleanly (Issue #281); results are folded into a single
    ``_ProcessResult`` on the main thread. GDAL preparation dominates, so worker
    threads overlap those reads.
    """
    result = _ProcessResult()

    if workers == 1:
        # Sequential execution (original behavior)
        for file_path, coll_id in files_to_process:
            if on_progress is not None:
                on_progress(file_path)
            prepared_list, failure_list, deferred = _prepare_single_file(file_path, coll_id, opts)
            _record_prepared(result, prepared_list, file_path, coll_id, opts.catalog_root)
            result.failures.extend(failure_list)
            if deferred is not None:
                result.deferred_non_geo.append(deferred)
        return result

    # Parallel execution with ThreadPoolExecutor
    from concurrent.futures import ThreadPoolExecutor, as_completed

    from portolan_cli.output import info

    if not json_mode:
        info(f"Using {workers} parallel workers for {len(files_to_process)} files")

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_file = {
            executor.submit(_prepare_single_file, fp, cid, opts): (fp, cid)
            for fp, cid in files_to_process
        }
        # Process results as they complete (main thread)
        for future in as_completed(future_to_file):
            file_path, coll_id = future_to_file[future]
            prepared_list, failure_list, deferred = future.result()
            # Call progress callback from the main thread (thread-safe) so the
            # CLI's AddProgressReporter works in parallel mode.
            if on_progress is not None:
                on_progress(file_path)
            _record_prepared(result, prepared_list, file_path, coll_id, opts.catalog_root)
            result.failures.extend(failure_list)
            if deferred is not None:
                result.deferred_non_geo.append(deferred)

    return result


def _recompute_stats_for_collections(affected_collections: set[Path]) -> None:
    """Phase 3.5: refresh file statistics for collections that got deferred assets.

    Deferred non-geo assets are added after ``finalize_items``, so their
    collection file statistics must be recomputed to include them (Issue #501).
    """
    for collection_dir in affected_collections:
        collection_json_path = collection_dir / "collection.json"
        if collection_json_path.exists():
            collection = pystac.Collection.from_file(str(collection_json_path))
            update_collection_file_statistics(collection)
            collection.save_object(include_self_link=False)


def add_files(
    *,
    paths: list[Path],
    catalog_root: Path,
    collection_id: str | None = None,
    item_id: str | None = None,
    item_datetime: datetime | None = None,
    verbose: bool = False,
    on_progress: Callable[[Path], None] | None = None,
    workers: int = 1,
    json_mode: bool = False,
    force: bool = False,
    reconvert: bool = False,
    skip_partitioning: bool = False,
    merge_strategy: MergeStrategy = MergeStrategy.SMART,
) -> tuple[list[ItemInfo], list[Path], list[AddFailure]]:
    """Add files to a Portolan catalog.

    This is the main entry point for the `portolan add` command.
    Handles single files, directories, and sidecar auto-detection.

    Per ADR-0028 ("Track ALL files in item directories as assets"):
    - Geospatial files (with geometry) are converted to cloud-native format
    - Non-geospatial CSV/TSV files are tracked as companion assets (no conversion)
    - Files must be in a directory with at least one geospatial file to be tracked

    Per Issue #175 ("Continue on errors and report all failures at end"):
    - Continues processing all files even when some fail
    - Collects all errors and reports them at the end
    - Enables batch processing without stopping on first error

    Per Issue #386 ("--force flag for re-tracking files"):
    - force=True bypasses mtime-based change detection
    - reconvert=True also re-converts from source (requires force=True)

    Args:
        paths: List of paths to add (files or directories).
        catalog_root: Root directory of the catalog.
        collection_id: Optional explicit collection ID.
            If not provided (None), the collection is inferred per-file from
            the first directory component relative to catalog_root. This is
            used by `portolan add .` to process multiple collections at once.
            Files at the catalog root level (not in a subdirectory) are skipped
            with a warning when collection_id=None.
        item_id: Optional explicit item ID. If provided, overrides automatic
            derivation from parent directory name. Must be a single path segment
            (no '/', '\\', '.', or '..').
        item_datetime: Optional acquisition/creation datetime (per ADR-0035).
            If None, defaults to current time but marks item as provisional.
        verbose: If True, return skipped files info.
        on_progress: Optional callback invoked before processing each geo file.
            Receives the file path being processed. Use for progress display.
        workers: Number of parallel workers for metadata extraction.
            Default is 1 (sequential). Higher values parallelize GDAL reads.
        json_mode: If True, suppress progress bar output.
        force: If True, bypass change detection and re-process all files.
        reconvert: If True, re-convert from source files (requires force=True).

    Returns:
        Tuple of (added_items, skipped_paths, failures).
        added_items: List of ItemInfo for newly added/updated files.
        skipped_paths: List of paths that were skipped (unchanged or non-geospatial).
        failures: List of AddFailure for files that could not be processed.
    """
    skipped: list[Path] = []

    # Track which nested collections have had their catalogs set up (ADR-0032)
    setup_collections: set[str] = set()

    # Phase 1: Collect + filter the files to process (fast, no GDAL).
    files_to_process = _collect_files_for_add(
        paths, catalog_root, collection_id, skipped, setup_collections, force=force
    )
    if not files_to_process:
        return [], skipped, []

    # Issue #465: filenames of every batch item (sources + converted outputs) so a
    # collection-level asset scan can skip siblings that are tracked as their own
    # items instead of re-scanning the whole flat collection dir (O(n²) → O(n)).
    opts = _PrepareOptions(
        catalog_root=catalog_root,
        item_id=item_id,
        item_datetime=item_datetime,
        force=force,
        reconvert=reconvert,
        skip_partitioning=skip_partitioning,
        batch_exclude_names=_batch_sibling_names([fp for fp, _ in files_to_process]),
    )

    # Phase 2: Prepare each file (GDAL work), accumulating prepared items (Issue #281).
    proc = _phase_process(
        files_to_process,
        opts,
        workers=workers,
        json_mode=json_mode,
        on_progress=on_progress,
    )
    failures = proc.failures

    # Phase 2.5: Batch finalize — ONE write per collection instead of O(n) (Issue #281).
    added: list[ItemInfo] = (
        finalize_items(catalog_root, proc.prepared_items, merge_strategy)
        if proc.prepared_items
        else []
    )

    # Phase 3: Track deferred non-geo files as companion assets (ADR-0028).
    affected_collections = _process_deferred_non_geo_files(
        deferred_non_geo=proc.deferred_non_geo,
        source_to_item_dir=proc.source_to_item_dir,
        source_to_collection_dir=proc.source_to_collection_dir,
        catalog_root=catalog_root,
        skipped=skipped,
        failures=failures,
    )

    # Phase 3.5: Recompute file statistics for collections with deferred assets (Issue #501).
    _recompute_stats_for_collections(affected_collections)

    return added, skipped, failures


def _process_deferred_non_geo_files(
    *,
    deferred_non_geo: list[tuple[Path, Path, str]],
    source_to_item_dir: dict[Path, tuple[Path, str, str]],
    source_to_collection_dir: dict[Path, tuple[Path, str]],
    catalog_root: Path,
    skipped: list[Path],
    failures: list[AddFailure],
) -> set[Path]:
    """Process deferred non-geospatial files (ADR-0028).

    These files were deferred during the main add loop because they lack
    geometry. They are tracked as auxiliary assets alongside geo files.

    Args:
        deferred_non_geo: List of (file_path, source_dir, collection_id) tuples.
        source_to_item_dir: Mapping from source dirs to (item_dir, coll_id, item_id).
        source_to_collection_dir: Mapping from source dirs to (collection_dir, coll_id)
            for collection-level assets (Issue #383).
        catalog_root: Root directory of the catalog.
        skipped: List to append skipped files to (modified in place).
        failures: List to append failures to (modified in place).

    Returns:
        Set of collection directories that received deferred assets (for statistics recomputation).
    """
    affected_collections: set[Path] = set()
    for file_path, source_dir, coll_id in deferred_non_geo:
        try:
            if source_dir in source_to_item_dir:
                # Item-level: existing behavior
                resolved_item_dir, _, resolved_item_id = source_to_item_dir[source_dir]

                # Copy non-geo file to item directory as companion asset
                dest_path = _copy_non_geo_to_item_dir(file_path, resolved_item_dir)

                # Log info message (expected behavior per ADR-0028)
                ext = file_path.suffix.upper().lstrip(".")
                logger.info(
                    "Tracking %s as non-geospatial %s asset (no conversion): %s",
                    file_path,
                    ext,
                    dest_path.name,
                )

                # Update the STAC item to include this new asset
                _update_item_with_asset(
                    catalog_root=catalog_root,
                    collection_id=coll_id,
                    item_id=resolved_item_id,
                    asset_path=dest_path,
                )

                # Track for statistics recomputation
                affected_collections.add(catalog_root / coll_id)

                # Add to skipped (tracked but not converted)
                skipped.append(file_path)

            elif source_dir in source_to_collection_dir:
                # Collection-level: new behavior for Issue #383
                resolved_collection_dir, resolved_coll_id = source_to_collection_dir[source_dir]

                # For collection-level, file is already in place (same as geo file)
                # Register it as an asset in collection.json AND versions.json
                ext = file_path.suffix.upper().lstrip(".")
                logger.info(
                    "Tracking %s as non-geospatial %s collection-level asset: %s",
                    file_path,
                    ext,
                    file_path.name,
                )

                # Update collection.json with the non-geo asset
                _update_collection_with_asset(
                    collection_dir=resolved_collection_dir,
                    asset_path=file_path,
                )

                # Update versions.json so is_current() finds the asset
                file_checksum = compute_checksum(file_path)
                file_size = file_path.stat().st_size
                asset_files = {file_path.name: (file_path, file_checksum, file_size)}
                _update_versions(
                    collection_dir=resolved_collection_dir,
                    item_id=file_path.stem,  # Use file stem as item_id for collection-level
                    collection_id=resolved_coll_id,
                    asset_files=asset_files,
                    is_collection_level_asset=True,
                    catalog_root=catalog_root,
                )

                # Track for statistics recomputation
                affected_collections.add(resolved_collection_dir)

                # Add to skipped (tracked but not converted)
                skipped.append(file_path)

            else:
                # No geo file in same dir - check if tabular support is enabled
                ext = file_path.suffix.upper().lstrip(".")
                collection_dir = catalog_root / Path(*coll_id.split("/"))

                # Check tabular.enabled config (Issue #432)
                tabular_enabled = get_setting(
                    "tabular.enabled",
                    catalog_path=catalog_root,
                    collection_path=collection_dir,
                )

                if not tabular_enabled:
                    # tabular.enabled=false (default): fail with helpful hint
                    failures.append(
                        AddFailure(
                            path=file_path,
                            error=(
                                f"Tabular data support is disabled. "
                                f"File '{file_path.name}' has no geometry and no companion "
                                f"geospatial file in the same directory. "
                                f"To track standalone tabular data as collection-level assets, "
                                f"set 'tabular.enabled: true' in .portolan/config.yaml"
                            ),
                        )
                    )
                else:
                    # tabular.enabled=true: track as standalone collection-level asset
                    # Check if conversion is enabled (Issue #432)
                    tabular_convert = get_setting(
                        "tabular.convert",
                        catalog_path=catalog_root,
                        collection_path=collection_dir,
                    )

                    # Ensure collection exists first (AOI inheritance from siblings)
                    _ensure_tabular_collection(
                        catalog_root=catalog_root,
                        collection_id=coll_id,
                        collection_dir=collection_dir,
                    )

                    # Determine the final asset path (convert if needed)
                    if tabular_convert and file_path.suffix.lower() != ".parquet":
                        # Convert CSV/TSV/XLSX to Parquet via gpio (Issue #432)
                        logger.info(
                            "Converting %s to Parquet via geoparquet-io: %s",
                            ext,
                            file_path.name,
                        )
                        asset_path = convert_tabular(file_path, collection_dir)
                        logger.info(
                            "Tracking %s as standalone tabular collection-level asset: %s",
                            file_path,
                            asset_path.name,
                        )
                        # Track BOTH converted Parquet and source file (consistent with
                        # vector behavior per ADR-0020: side-by-side, both tracked)
                        source_tracked = True
                    else:
                        # Already Parquet or conversion disabled - track as-is
                        asset_path = file_path
                        logger.info(
                            "Tracking %s as standalone tabular %s collection-level asset: %s",
                            file_path,
                            ext,
                            file_path.name,
                        )
                        source_tracked = False

                    # Update collection.json with the tabular asset(s)
                    # Primary asset: the Parquet file (or source if no conversion)
                    _update_collection_with_asset(
                        collection_dir=collection_dir,
                        asset_path=asset_path,
                    )

                    # If converted, also track source file as companion asset
                    # (consistent with vector conversion behavior per ADR-0020)
                    if source_tracked:
                        _update_collection_with_asset(
                            collection_dir=collection_dir,
                            asset_path=file_path,
                        )

                    # Update versions.json so is_current() finds the asset(s)
                    asset_checksum = compute_checksum(asset_path)
                    asset_size = asset_path.stat().st_size
                    asset_files = {asset_path.name: (asset_path, asset_checksum, asset_size)}
                    if source_tracked:
                        source_checksum = compute_checksum(file_path)
                        source_size = file_path.stat().st_size
                        asset_files[file_path.name] = (file_path, source_checksum, source_size)
                    _update_versions(
                        collection_dir=collection_dir,
                        item_id=asset_path.stem,  # Use file stem as item_id
                        collection_id=coll_id,
                        asset_files=asset_files,
                        is_collection_level_asset=True,
                        catalog_root=catalog_root,
                    )

                    # Track for statistics recomputation
                    affected_collections.add(collection_dir)

                    # Add to skipped (tracked, possibly converted)
                    skipped.append(file_path)
        except Exception as err:
            # Record failure and continue (Issue #175).
            failures.append(AddFailure(path=file_path, error=str(err)))

    return affected_collections


def _update_item_with_asset(
    catalog_root: Path,
    collection_id: str,
    item_id: str,
    asset_path: Path,
) -> None:
    """Update a STAC item to include a new asset file.

    Re-scans the item directory and updates the item.json with all assets.
    This is used to add non-geospatial companion files to existing items.

    Args:
        catalog_root: Root directory of the catalog.
        collection_id: Collection identifier.
        item_id: Item identifier.
        asset_path: Path to the new asset file.
    """
    collection_dir = catalog_root / collection_id
    item_dir = collection_dir / item_id
    item_json_path = item_dir / f"{item_id}.json"

    if not item_json_path.exists():
        logger.warning("Item JSON not found: %s", item_json_path)
        return

    # Load existing item
    with open(item_json_path) as f:
        item_data = json.load(f)

    # Find the primary data file by checking existing assets first (Issue #190).
    # Prefer the existing "data" asset to avoid reselecting a tabular parquet
    # that was just copied as the primary geo-asset.
    primary_file: Path | None = None

    # First: Check existing assets for one with "data" role
    existing_assets = item_data.get("assets", {})
    for _asset_key, asset_info in existing_assets.items():
        roles = asset_info.get("roles", [])
        if "data" in roles:
            # Found existing primary asset - use its href
            href = asset_info.get("href", "")
            if href:
                candidate = item_dir / href
                if candidate.exists():
                    primary_file = candidate
                    break

    # Fallback: scan directory for .parquet or .tif (original behavior)
    if primary_file is None:
        for file in item_dir.iterdir():
            if file.suffix.lower() in {".parquet", ".tif", ".tiff"}:
                primary_file = file
                break

    if primary_file is None:
        # Use the first non-json file as primary
        for file in item_dir.iterdir():
            if file.is_file() and file.suffix.lower() != ".json":
                primary_file = file
                break

    if primary_file is None:
        logger.warning("No primary file found in item directory: %s", item_dir)
        return

    # Re-scan assets
    stac_assets, asset_files, _ = _scan_item_assets(
        item_dir=item_dir,
        item_id=item_id,
        primary_file=primary_file,
        collection_dir=collection_dir,
    )

    # Enrich COG assets with render extension properties (Issue #13)
    enrich_cog_assets(stac_assets, catalog_root)

    # Update item assets - include extra_fields for style properties
    # Merge with existing asset metadata to preserve title/description
    existing_assets = item_data.get("assets", {})
    item_data["assets"] = {
        key: {
            **existing_assets.get(key, {}),  # Preserve existing metadata
            "href": asset.href,
            "type": asset.media_type,
            "roles": asset.roles,
            **(asset.extra_fields or {}),
        }
        for key, asset in stac_assets.items()
    }

    # Write updated item
    with open(item_json_path, "w") as f:
        json.dump(item_data, f, indent=2)

    # Detect if this is a collection-level asset
    is_collection_level = item_dir.resolve() == collection_dir.resolve()

    # Update versions.json with new asset
    _update_versions(
        collection_dir=collection_dir,
        item_id=item_id,
        collection_id=collection_id,
        asset_files=asset_files,
        is_collection_level_asset=is_collection_level,
        catalog_root=catalog_root,
    )


def _update_collection_with_asset(
    collection_dir: Path,
    asset_path: Path,
) -> None:
    """Update a collection.json to include a new non-geo asset file (Issue #383).

    For collection-level non-geospatial files, this adds them as assets directly
    to collection.json rather than an item.json.

    Args:
        collection_dir: Path to the collection directory.
        asset_path: Path to the non-geo asset file.
    """
    collection_json_path = collection_dir / "collection.json"

    if not collection_json_path.exists():
        logger.warning("collection.json not found: %s", collection_json_path)
        return

    # Load existing collection
    with open(collection_json_path) as f:
        collection_data = json.load(f)

    # Add asset to collection
    assets = collection_data.setdefault("assets", {})
    media_type = _get_media_type(asset_path)
    role = _get_asset_role(asset_path)

    # Use stem as key, but fall back to full filename on collision
    # (consistent with _scan_item_assets behavior for vectors)
    asset_key = asset_path.stem
    if asset_key in assets:
        # Check if it's the same file (idempotent update) or a different file
        existing_href = assets[asset_key].get("href", "")
        if existing_href != f"./{asset_path.name}":
            # Different file with same stem - use full filename to avoid collision
            asset_key = asset_path.name

    # Compute file size and checksum for file extension metadata
    file_size = asset_path.stat().st_size
    file_checksum = compute_checksum(asset_path)

    assets[asset_key] = {
        "href": f"./{asset_path.name}",
        "type": media_type,
        "roles": [role],
        "file:size": file_size,
        "file:checksum": f"sha256:{file_checksum}",
    }

    # Write updated collection
    with open(collection_json_path, "w") as f:
        json.dump(collection_data, f, indent=2)
