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
import os
import shutil
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import click
import pystac
from pystac.layout import AsIsLayoutStrategy

from portolan_cli.checksums import compute_checksum
from portolan_cli.collection import (
    _compute_union_bbox,
    _get_metadata_yaml_bbox,
    _get_sibling_collection_bboxes,
)
from portolan_cli.collection_id import (
    infer_nested_collection_id,
    resolve_collection_id,  # noqa: F401
)
from portolan_cli.config import get_setting, load_merged_metadata
from portolan_cli.constants import (
    GEOSPATIAL_EXTENSIONS,
    TABULAR_EXTENSIONS,
)
from portolan_cli.conversion_config import get_vector_settings
from portolan_cli.convert import convert_multilayer_file
from portolan_cli.discovery import get_sidecars, iter_files_with_sidecars, iter_geospatial_files
from portolan_cli.errors import NoGeometryError
from portolan_cli.formats import (
    FormatType,
    is_multilayer,
    list_layers,
)
from portolan_cli.humanize import humanize_slug
from portolan_cli.metadata import (
    extract_geoparquet_metadata,
)
from portolan_cli.metadata.geoparquet import GeoParquetMetadata
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
    add_asset_to_collection,
    add_collection_extensions_from_summaries,
    add_collection_properties_from_metadata,
    add_item_to_collection,
    add_partition_metadata_to_collection,
    add_table_extension,
    aggregate_table_metadata,
    apply_human_titles,
    create_collection,
    load_catalog,
    update_catalog_file_statistics,
    update_collection_file_statistics,
    update_collection_summaries,
)
from portolan_cli.style import enrich_cog_assets
from portolan_cli.versions import (
    Asset,
    VersionsFile,
    _increment_version,
    add_version,
    read_versions,
    write_versions,
)

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


def _deduplicate_collection_item_links(collection: pystac.Collection) -> None:
    """De-duplicate item links in a PySTAC collection.

    PySTAC adds duplicate links when the same item is added multiple times.
    This modifies collection.links in place.
    """
    seen_item_ids: set[str] = set()
    unique_links: list[pystac.Link] = []
    for link in collection.links:
        if link.rel == "item":
            # For item links, de-duplicate by target item's ID
            target = link.target
            if isinstance(target, pystac.Item):
                item_id_key = target.id
            else:
                # If target is a string (href), use it directly
                item_id_key = str(target) if target else ""
            if item_id_key in seen_item_ids:
                continue
            seen_item_ids.add(item_id_key)
        unique_links.append(link)
    collection.links = unique_links


def _fix_collection_links(
    collection_json_path: Path,
    catalog_root: Path,
    collection_dir: Path,
) -> None:
    """Fix root/parent links and deduplicate item links in collection JSON.

    PySTAC sets root to self by default; we need to point to catalog root.
    Also deduplicates item links that can occur when add is called
    multiple times on the same collection.
    """
    if not collection_json_path.exists():
        return

    collection_data = json.loads(collection_json_path.read_text(encoding="utf-8"))
    relative_root = os.path.relpath(catalog_root / "catalog.json", collection_dir)

    # Update root link to point to catalog
    for link in collection_data.get("links", []):
        if link.get("rel") == "root":
            link["href"] = relative_root
            break
    else:
        # No root link found, add one
        collection_data.setdefault("links", []).append(
            {"rel": "root", "href": relative_root, "type": "application/json"}
        )

    # Add parent link if missing
    has_parent = any(link.get("rel") == "parent" for link in collection_data.get("links", []))
    if not has_parent:
        collection_data["links"].append(
            {"rel": "parent", "href": relative_root, "type": "application/json"}
        )

    # Deduplicate item links (can occur when add is called multiple times)
    seen_item_hrefs: set[str] = set()
    deduped_links: list[dict[str, Any]] = []
    for link in collection_data.get("links", []):
        if link.get("rel") == "item":
            href = link.get("href", "")
            if href in seen_item_hrefs:
                continue
            seen_item_hrefs.add(href)
        deduped_links.append(link)
    collection_data["links"] = deduped_links

    collection_json_path.write_text(json.dumps(collection_data, indent=2), encoding="utf-8")


def _save_collection_with_links(
    collection: pystac.Collection,
    collection_dir: Path,
    catalog_root: Path,
    collection_id: str,
) -> None:
    """Save collection and fix links.

    Args:
        collection: PySTAC collection to save.
        collection_dir: Collection directory path.
        catalog_root: Catalog root path.
        collection_id: Collection identifier.
    """
    _deduplicate_collection_item_links(collection)
    collection.set_self_href(str(collection_dir / "collection.json"))
    # Trailing slash required: pystac treats paths with dots in final component as files
    collection.normalize_hrefs(f"{collection_dir}/", strategy=AsIsLayoutStrategy())
    collection.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)

    collection_json_path = collection_dir / "collection.json"
    _fix_collection_links(collection_json_path, catalog_root, collection_dir)
    _update_catalog_links(catalog_root, collection_id)


def _recompute_collection_extent_with_multibbox(collection: pystac.Collection) -> None:
    """Recompute collection spatial extent with anti-meridian handling (issue #516).

    Collects all item and asset bboxes from the collection and recomputes the
    spatial extent using proper multi-bbox support for anti-meridian crossing.

    If anti-meridian crossing is detected, the extent will use multiple bboxes
    per the STAC spec (one for the western portion, one for the eastern portion).

    Args:
        collection: The pystac Collection to update.
    """
    from portolan_cli.bbox import compute_bbox_union

    # Collect all bboxes from items
    all_bboxes: list[list[float]] = []

    # Get bboxes from linked items
    for link in collection.links:
        if link.rel == "item" and hasattr(link, "target") and link.target is not None:
            item = link.target
            if hasattr(item, "bbox") and item.bbox is not None:
                all_bboxes.append(list(item.bbox))

    # Get bboxes from collection-level assets (if they have proj:bbox)
    if collection.assets:
        for asset in collection.assets.values():
            if hasattr(asset, "extra_fields") and asset.extra_fields:
                proj_bbox = asset.extra_fields.get("proj:bbox")
                if proj_bbox:
                    all_bboxes.append(list(proj_bbox))

    # Also use the existing collection extent as a fallback
    if collection.extent and collection.extent.spatial:
        for bbox in collection.extent.spatial.bboxes:
            if bbox not in all_bboxes:
                all_bboxes.append(list(bbox))

    if not all_bboxes:
        return  # No bboxes to process

    # Compute union with anti-meridian handling
    result = compute_bbox_union(all_bboxes)

    if result.bbox is None:
        logger.warning(
            "Collection '%s': all bboxes are invalid, keeping existing extent",
            collection.id,
        )
        return

    # Update collection extent
    if result.is_multi_bbox and result.bboxes:
        # Use multi-bbox for anti-meridian crossing
        collection.extent.spatial = pystac.SpatialExtent(bboxes=result.bboxes)
        logger.info(
            "Collection '%s': using multi-bbox extent for anti-meridian crossing (%d bboxes)",
            collection.id,
            len(result.bboxes),
        )
    else:
        # Single bbox
        collection.extent.spatial = pystac.SpatialExtent(bboxes=[result.bbox])


def _add_prepared_items_to_collection(
    collection: pystac.Collection,
    items: list[PreparedItem],
    merge_strategy: MergeStrategy = MergeStrategy.SMART,
) -> None:
    """Add prepared items or collection-level assets to a collection.

    Per ADR-0031: Collection-level vector assets go directly in collection.assets.
    Item-level assets get linked via add_item_to_collection.

    Args:
        collection: The pystac Collection to add to.
        items: List of PreparedItem objects.
        merge_strategy: How to merge auto-detected metadata with existing values.
    """
    for p in items:
        if p.is_collection_level_asset and p.stac_assets is not None:
            # Collection-level asset: add directly to collection.assets
            for asset_key, asset in p.stac_assets.items():
                add_asset_to_collection(
                    collection,
                    asset_key,
                    asset,
                    update_extent_from_bbox=p.bbox,
                    merge_strategy=merge_strategy,
                )
            # Add format-specific properties (proj:epsg, pmtiles:*, flatgeobuf:*)
            if p.metadata is not None:
                add_collection_properties_from_metadata(collection, p.metadata)
        elif p.stac_item is not None:
            # Item-level: add item link to collection
            add_item_to_collection(
                collection, p.stac_item, update_extent=True, merge_strategy=merge_strategy
            )


def _collect_parquet_metadata_from_disk(
    collection_dir: Path,
    collection: pystac.Collection,
) -> list[GeoParquetMetadata]:
    """Scan collection directory and extract metadata from tracked parquet assets.

    Issue #447: Used to recompute row counts from disk instead of carrying forward
    potentially stale aggregated counts. This ensures correctness when:
    - Re-adding the same file (no double-count)
    - Files are replaced on disk with different content
    - Files are deleted and re-added

    IMPORTANT: Only counts files that are tracked as assets in collection.json.
    Untracked parquet files (temp files, work-in-progress) are ignored to prevent
    inflating row counts.

    Args:
        collection_dir: Path to the collection directory.
        collection: The collection to check tracked assets against.

    Returns:
        List of GeoParquetMetadata for tracked parquet assets found on disk.
    """
    # Build set of tracked asset hrefs (normalized without ./ prefix)
    tracked_hrefs: set[str] = set()
    for asset in collection.assets.values():
        if asset.href:
            # Normalize: strip ./ prefix for comparison
            href = asset.href.lstrip("./")
            tracked_hrefs.add(href)

    metadata_list: list[GeoParquetMetadata] = []

    for parquet_file in collection_dir.glob("**/*.parquet"):
        # Skip files in .portolan directory (internal state)
        if ".portolan" in parquet_file.parts:
            continue

        # Only include files that are tracked as assets
        # Use as_posix() for cross-platform consistency (Windows uses backslashes,
        # but STAC asset hrefs always use forward slashes)
        relative_path = parquet_file.relative_to(collection_dir).as_posix()
        if relative_path not in tracked_hrefs:
            logger.debug(f"Skipping untracked parquet file: {relative_path}")
            continue

        try:
            meta = extract_geoparquet_metadata(parquet_file)
            metadata_list.append(meta)
        except Exception as e:
            # Log but don't fail - file might be corrupted or not a valid parquet
            logger.warning(f"Could not read metadata from {parquet_file}: {e}")

    return metadata_list


def _warn_about_stale_assets(
    collection: pystac.Collection,
    collection_dir: Path,
) -> list[str]:
    """Check for assets that reference missing files and return warnings.

    Issue #447: Emits warnings for assets pointing to files that no longer exist.
    Does NOT remove them - that's the job of `check --fix`.

    Args:
        collection: The collection to check.
        collection_dir: Path to the collection directory.

    Returns:
        List of warning messages for stale assets.
    """
    warnings: list[str] = []

    for key, asset in collection.assets.items():
        if not asset.href:
            continue

        # Resolve href relative to collection directory
        if asset.href.startswith("./"):
            asset_path = collection_dir / asset.href[2:]
        elif asset.href.startswith("/"):
            asset_path = Path(asset.href)
        else:
            asset_path = collection_dir / asset.href

        if not asset_path.exists():
            warnings.append(f"Asset '{key}' references missing file: {asset.href}")

    return warnings


def _ensure_partition_metadata(
    collection: pystac.Collection,
    collection_dir: Path,
    items: list[PreparedItem],
) -> list[str]:
    """Add partition metadata to collection from items or auto-detection.

    Issue #232: Adds partition extension if any items have partition metadata.
    Issue #443: Auto-detects pre-existing Hive partitions if no metadata was set
    from items. This handles the case where users add pre-partitioned data not
    created by Portolan. Also creates glob assets for bulk access.

    Args:
        collection: The STAC collection to update.
        collection_dir: Directory containing the collection.
        items: List of prepared items for this collection.

    Returns:
        List of warning messages (e.g., schema inconsistency warnings).
    """
    from portolan_cli.partitioning import (
        build_glob_pattern,
        detect_partitioning,
        validate_partition_schemas,
    )

    warnings: list[str] = []

    # First, check if any items have explicit partition metadata
    for p in items:
        if p.partition_metadata is not None:
            add_partition_metadata_to_collection(collection, p.partition_metadata)
            # Validate schema consistency for partitioned data
            validation = validate_partition_schemas(collection_dir)
            if not validation.is_consistent and validation.partition_count > 0:
                warnings.append(
                    f"Schema inconsistency in partitioned data: {validation.error_message}"
                )
            return warnings  # Only one partition metadata per collection

    # No explicit metadata - try auto-detection for pre-existing Hive partitions
    detected = detect_partitioning(collection_dir)
    if detected:
        add_partition_metadata_to_collection(collection, detected)
        partition_keys = detected.get("partition:keys", [])
        partition_columns = [k["name"] for k in partition_keys]
        file_count = detected.get("partition:file_count", 0)

        logger.debug(f"Auto-detected Hive partitions in {collection_dir}: {partition_columns}")

        # Create glob asset for bulk access (Issue #443)
        # Only add if not already present (avoid duplicates on re-add)
        glob_pattern = build_glob_pattern(partition_columns=partition_columns)
        glob_asset_key = "partitioned_data"

        # Check if glob asset already exists (any asset with * in href)
        existing_glob = None
        for key, asset in collection.assets.items():
            if asset.href and "*" in asset.href:
                existing_glob = key
                break

        if existing_glob is None:
            # Check if target key is occupied by a non-glob asset (avoid clobbering)
            # Per Issue #443: Don't overwrite user-defined assets at this key
            existing_at_key = collection.assets.get(glob_asset_key)
            if existing_at_key is not None and (
                not existing_at_key.href or "*" not in existing_at_key.href
            ):
                # Key is occupied by non-glob asset - use alternate key
                glob_asset_key = "partitioned_data_glob"
                logger.debug(
                    f"Key 'partitioned_data' occupied by non-glob asset, "
                    f"using '{glob_asset_key}' instead"
                )

            import pystac

            glob_asset = pystac.Asset(
                href=glob_pattern,
                media_type="application/vnd.apache.parquet",
                roles=["data"],
                title="Partitioned GeoParquet",
                description=f"Glob pattern for {file_count} partitioned files",
            )
            collection.assets[glob_asset_key] = glob_asset
            logger.debug(f"Added glob asset with pattern: {glob_pattern}")

        # Validate schema consistency for auto-detected partitions
        validation = validate_partition_schemas(collection_dir)
        if not validation.is_consistent and validation.partition_count > 0:
            warnings.append(f"Schema inconsistency in partitioned data: {validation.error_message}")

    return warnings


def finalize_items(
    catalog_root: Path,
    prepared: list[PreparedItem],
    merge_strategy: MergeStrategy = MergeStrategy.SMART,
) -> list[ItemInfo]:
    """Finalize prepared items by writing versions.json and collection.json.

    This function batches all writes by collection, enabling O(n) versioning
    instead of O(n²). See Issue #281.

    Args:
        catalog_root: Root directory of the catalog.
        prepared: List of PreparedItem objects from prepare_item().
        merge_strategy: How to merge auto-detected metadata with existing values.

    Returns:
        List of ItemInfo for each finalized item.
    """
    if not prepared:
        return []

    # Group by collection for efficient batch writes
    from collections import defaultdict

    by_collection: dict[str, list[PreparedItem]] = defaultdict(list)
    for p in prepared:
        by_collection[p.collection_id].append(p)

    results: list[ItemInfo] = []

    for collection_id, items in by_collection.items():
        collection_dir = catalog_root / Path(*collection_id.split("/"))

        # Get or create collection, then add all items at once
        first_item = items[0]
        collection = _get_or_create_collection(
            catalog_root=catalog_root,
            collection_id=collection_id,
            initial_bbox=first_item.bbox,
        )

        # Issue #502: apply human title/description overrides from
        # metadata.yaml (highest precedence over the auto-derived defaults).
        apply_human_titles(collection, load_merged_metadata(collection_dir, catalog_root))

        # Add items or collection-level assets to collection (in memory)
        _add_prepared_items_to_collection(collection, items, merge_strategy)

        # Issue #447: Check for stale assets (reference missing files)
        # Warn but don't remove - removal is handled by `check --fix`
        stale_warnings = _warn_about_stale_assets(collection, collection_dir)
        if stale_warnings:
            from portolan_cli.output import warn as warn_output

            warn_output(
                f"{len(stale_warnings)} asset(s) reference missing files "
                "(run `portolan check --fix` to clean up)"
            )
            for warning_msg in stale_warnings:
                logger.debug(warning_msg)

        # Add table extension if any items are GeoParquet format (Issue #304)
        # Issue #447 FIX: Recompute metadata from ALL parquet files on disk
        # instead of carrying forward stale aggregated counts. This prevents:
        # - Double-counting when re-adding the same file
        # - Stale counts when files are replaced with different content
        #
        # Important: Only run aggregation if there's at least one NEW GeoParquet item
        # in this batch. PMTiles/FlatGeobuf are collection-level assets without
        # table schema, so exclude them from table extension aggregation.
        new_geoparquet_metadata: list[GeoParquetMetadata] = [
            p.metadata
            for p in items
            if p.format_type == FormatType.VECTOR and isinstance(p.metadata, GeoParquetMetadata)
        ]
        if new_geoparquet_metadata:
            # Recompute from disk: scan tracked parquet assets in collection
            # This is O(n) file metadata reads but always correct
            all_parquet_metadata = _collect_parquet_metadata_from_disk(collection_dir, collection)
            if all_parquet_metadata:
                aggregated = aggregate_table_metadata(all_parquet_metadata)
                add_table_extension(collection, aggregated, merge_strategy=merge_strategy)

        # Add partition extension if any items have partition metadata (Issue #232)
        # Issue #443: Also auto-detect pre-existing Hive partitions and validate schemas
        partition_warnings = _ensure_partition_metadata(collection, collection_dir, items)
        if partition_warnings:
            from portolan_cli.output import warn as warn_output

            for warning_msg in partition_warnings:
                warn_output(warning_msg)

        # Compute collection summaries from items (per ADR-0036)
        # Moved here from push.py for separation of concerns - summaries are now
        # available immediately after add, not just after push.
        update_collection_summaries(collection)

        # Compute aggregate file statistics (Issue #501)
        update_collection_file_statistics(collection)

        # Add extension declarations based on summaries (Issue #336)
        # Collections should declare extensions used by their items
        if collection.summaries is not None:
            add_collection_extensions_from_summaries(collection, collection.summaries.to_dict())

        # Issue #516: Recompute spatial extent with anti-meridian handling
        # This step collects all item/asset bboxes and computes proper multi-bbox
        # when anti-meridian crossing is detected.
        _recompute_collection_extent_with_multibbox(collection)

        # Save collection.json ONCE for all items in this collection
        _save_collection_with_links(collection, collection_dir, catalog_root, collection_id)

        # Resolve active backend for versioning routing
        from portolan_cli.config import get_setting

        active_backend = get_setting("backend", catalog_path=catalog_root)

        if active_backend is not None and active_backend != "file":
            # Plugin backend: publish version snapshot and run post-add hooks
            _finalize_with_backend(
                catalog_root=catalog_root,
                collection_id=collection_id,
                collection_dir=collection_dir,
                collection=collection,
                items=items,
                active_backend=active_backend,
            )
        else:
            # File backend: use optimized batch write (O(1) per collection)
            current_version, asset_count, total_size = _batch_update_versions(
                collection_dir=collection_dir,
                collection_id=collection_id,
                items=items,
            )

            # Update catalog-level versions.json (ADR-0005)
            # This keeps the catalog-level view in sync with collection state.
            # Wrap in try/except to avoid failing the add if catalog update fails
            # (collection-level versions.json was already written successfully).
            from portolan_cli.catalog import update_catalog_versions

            try:
                update_catalog_versions(
                    catalog_root=catalog_root,
                    collection_id=collection_id,
                    current_version=current_version,
                    asset_count=asset_count,
                    total_size_bytes=total_size,
                )
            except Exception:
                # Collection-level versions.json was written successfully.
                # Log warning but don't fail the add operation.
                logger.warning(
                    "Failed to update catalog-level versions.json for collection '%s'. "
                    "Collection version was published but catalog-level view may be stale.",
                    collection_id,
                    exc_info=True,
                )

        # Build results
        for p in items:
            results.append(
                ItemInfo(
                    item_id=p.item_id,
                    collection_id=p.collection_id,
                    format_type=p.format_type,
                    bbox=p.bbox,
                    asset_paths=[
                        str(path) for _name, (path, _checksum, _size) in p.asset_files.items()
                    ],
                )
            )

    # Issue #502: backfill human-readable titles onto child/item links so STAC
    # Browser renders names without fetching every child. Done once per batch
    # (O(catalog), not per-collection) after all collections are written.
    from portolan_cli.catalog import ensure_link_titles

    ensure_link_titles(catalog_root)

    # Issue #501: update catalog-level aggregate file statistics
    # Done after all collections are finalized so totals are accurate.
    try:
        update_catalog_file_statistics(catalog_root)
    except Exception:
        logger.warning(
            "Failed to update catalog-level file statistics. "
            "Catalog may have stale or missing aggregate size data.",
            exc_info=True,
        )

    return results


def _finalize_with_backend(
    catalog_root: Path,
    collection_id: str,
    collection_dir: Path,
    collection: object,
    items: list[PreparedItem],
    active_backend: str,
) -> None:
    """Run backend versioning and post-add hooks for a non-file backend.

    Handles both publish_version() and on_post_add() calls so that
    finalize_items() stays within complexity rank C.

    This is backend routing logic added by the iceberg-backend-integration
    branch.
    """
    from portolan_cli.backends import get_backend
    from portolan_cli.config import get_setting
    from portolan_cli.version_ops import publish_version

    # Publish version snapshot via the plugin backend
    assets: dict[str, str] = {}
    for p in items:
        for filename, (file_path, _checksum, _size) in p.asset_files.items():
            if p.is_collection_level_asset:
                asset_key = filename
            else:
                asset_key = f"{p.item_id}/{filename}"
            assets[asset_key] = str(file_path)
    publish_version(collection_id, assets=assets, catalog_root=catalog_root)

    # NOTE: Plugin backends (e.g. Iceberg) may override table:* STAC extension
    # fields in collection.json via on_post_add, since the backend's table state
    # (actual row counts, schema excluding derived columns) is authoritative.
    backend = get_backend(active_backend, catalog_root=catalog_root)
    if not hasattr(backend, "on_post_add"):
        return

    remote = get_setting("remote", catalog_path=catalog_root, collection=collection_id)
    first = items[0]
    # For collection-level assets, item_json_path is None; use collection_dir
    first_item_dir = first.item_json_path.parent if first.item_json_path else collection_dir
    context = {
        "catalog_root": catalog_root,
        "collection_id": collection_id,
        "collection_dir": collection_dir,
        "collection": collection,
        "item_id": first.item_id,
        "item_dir": first_item_dir,
        "asset_files": first.asset_files,
        "items": [
            {
                "item_id": p.item_id,
                "item_dir": (p.item_json_path.parent if p.item_json_path else collection_dir),
                "asset_files": p.asset_files,
            }
            for p in items
        ],
        "remote": remote,
    }
    try:
        backend.on_post_add(context)
    except Exception:
        # Version was already published successfully. Log warning but don't fail
        # the entire add operation. The backend hook is for optional enrichment
        # (e.g., uploading STAC metadata to remote).
        logger.warning(
            "Backend on_post_add hook failed for collection '%s'. "
            "Version was published but post-add actions may be incomplete.",
            collection_id,
            exc_info=True,
        )


_FRESHNESS_CHECKABLE_SUFFIXES = frozenset({".parquet", ".tif", ".tiff"})


def _asset_freshness_fields(
    file_path: Path,
    *,
    is_collection_level: bool,
) -> tuple[float | None, int | None, str | None]:
    """Compute (source_mtime, feature_count, schema_fingerprint) for tracking.

    Only collection-level, freshness-checkable assets (GeoParquet / COG) get a
    baseline: they have no companion item.json, so the metadata_fresh check
    reads their freshness straight from versions.json (#512). Item-level assets
    are left to their existing item.json / ``--fix`` path.

    Best-effort: any extraction failure falls back to ``(None, None, None)`` so
    tracking never blocks an otherwise successful ``add``. The asset's own mtime
    doubles as ``source_mtime`` — mirroring ``update_versions_tracking`` — since
    the freshness fast path compares the asset file's current mtime to it.
    """
    if not is_collection_level:
        return (None, None, None)
    if file_path.suffix.lower() not in _FRESHNESS_CHECKABLE_SUFFIXES:
        return (None, None, None)

    from portolan_cli.metadata.detection import get_current_metadata

    try:
        current = get_current_metadata(file_path)
        return (
            file_path.stat().st_mtime,
            current.current_feature_count,
            current.current_schema_fingerprint,
        )
    except (ValueError, OSError):
        logger.debug("Could not extract freshness heuristics for %s", file_path, exc_info=True)
        return (None, None, None)


def _batch_update_versions(
    collection_dir: Path,
    collection_id: str,
    items: list[PreparedItem],
) -> tuple[str, int, int]:
    """Batch update versions.json for multiple items in a single read-modify-write.

    This is the key optimization for Issue #281: instead of O(n) writes
    (one per item), we do O(1) writes per collection.

    Args:
        collection_dir: Path to collection directory.
        collection_id: Collection identifier.
        items: List of PreparedItem objects to add versions for.

    Returns:
        Tuple of (current_version, asset_count, total_size_bytes) for catalog-level
        versioning updates (ADR-0005).
    """
    versions_path = collection_dir / "versions.json"

    # Read existing versions (or create new)
    if versions_path.exists():
        versions_file = read_versions(versions_path)
    else:
        versions_file = VersionsFile(
            spec_version="1.0.0",
            current_version=None,
            versions=[],
        )

    # Compute new version string using the helper (handles prerelease versions)
    if versions_file.current_version is None:
        new_version = "1.0.0"
    else:
        new_version = _increment_version(versions_file.current_version)

    # Build assets dict from ALL items (batch)
    all_assets: dict[str, Asset] = {}
    for p in items:
        for filename, (file_path, file_checksum, file_size) in p.asset_files.items():
            # For collection-level assets (ADR-0031), omit item_id from path
            # asset_key is collection-relative; href is catalog-relative
            if p.is_collection_level_asset:
                href = f"{collection_id}/{filename}"
                asset_key = filename  # Issue #354: collection-relative, not doubled
            else:
                href = f"{collection_id}/{p.item_id}/{filename}"
                asset_key = f"{p.item_id}/{filename}"

            stat = file_path.stat()
            # Collection-level assets (ADR-0031) have no companion item.json, so
            # the freshness check reads their baseline straight from
            # versions.json. Persist source_mtime + heuristics here so a plain
            # `add` produces a FRESH asset instead of a perpetual STALE (#512),
            # and so a touched-but-identical asset stays FRESH via the ADR-0017
            # heuristic fallback rather than flipping to STALE/BREAKING.
            source_mtime, feature_count, schema_fingerprint = _asset_freshness_fields(
                file_path, is_collection_level=p.is_collection_level_asset
            )
            # Use pre-computed file_size (handles directories like FileGDB correctly)
            all_assets[asset_key] = Asset(
                sha256=file_checksum,
                size_bytes=file_size,
                href=href,
                mtime=stat.st_mtime,
                source_mtime=source_mtime,
                feature_count=feature_count,
                schema_fingerprint=schema_fingerprint,
            )

    # Add single version with all assets
    updated = add_version(
        versions_file,
        version=new_version,
        assets=all_assets,
        breaking=False,
    )

    # Single write for all items
    write_versions(versions_path, updated)

    # Return info for catalog-level versioning (ADR-0005)
    # Get latest version's asset info
    latest = updated.versions[-1] if updated.versions else None
    if latest:
        asset_count = len(latest.assets)
        total_size = sum(a.size_bytes for a in latest.assets.values())
        return (updated.current_version or new_version, asset_count, total_size)
    return (new_version, 0, 0)


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


def _get_or_create_collection(
    catalog_root: Path,
    collection_id: str,
    initial_bbox: list[float],
) -> pystac.Collection:
    """Load existing collection or create new one.

    Args:
        catalog_root: Root directory of the catalog.
        collection_id: Collection identifier (may be nested path like "climate/hittekaart").
        initial_bbox: Initial bounding box for new collections.

    Returns:
        pystac.Collection object.
    """
    # STAC at root level (per ADR-0023), handle nested paths (per ADR-0032)
    collection_path = catalog_root / Path(*collection_id.split("/")) / "collection.json"

    if collection_path.exists():
        return pystac.Collection.from_file(str(collection_path))

    # Create new collection. Issue #502: derive a human-readable title from
    # the collection id and default the description to it (no "Collection:
    # <slug>" placeholder). create_collection fills both in when omitted.
    title = humanize_slug(collection_id)
    return create_collection(
        collection_id=collection_id,
        description=title,
        title=title,
        bbox=initial_bbox,
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


def _update_catalog_links(catalog_root: Path, collection_id: str) -> None:
    """Ensure catalog has link to collection.

    For nested collection IDs (ADR-0032), delegates to update_catalog_links_for_nested
    which properly links through the catalog hierarchy.

    Args:
        catalog_root: Root directory of the catalog.
        collection_id: Collection identifier (may be nested like "climate/hittekaart").
    """
    # For nested collection IDs, use the nested catalog link updater (ADR-0032)
    if "/" in collection_id:
        from portolan_cli.catalog import update_catalog_links_for_nested

        update_catalog_links_for_nested(catalog_root, collection_id)
        return

    # For single-level collections, add direct link from root
    catalog_path = catalog_root / "catalog.json"
    catalog = load_catalog(catalog_path)

    # Trailing slash required: pystac treats paths with dots in final component as files
    catalog.normalize_hrefs(f"{catalog_root}/")

    # Extract collection IDs from existing child links
    # Links are in format: "./{collection_id}/collection.json"
    existing_collection_ids: set[str] = set()
    for link in catalog.links:
        if link.rel != "child":
            continue
        href = link.href or ""
        # Extract collection ID from href pattern: ./{collection_id}/collection.json
        if href.endswith("/collection.json"):
            # Parse: ./{collection_id}/collection.json or {collection_id}/collection.json
            parts = href.replace("./", "").split("/")
            if len(parts) >= 2:
                coll_id = parts[0]
                existing_collection_ids.add(coll_id)

    if collection_id not in existing_collection_ids:
        collection_href = f"./{collection_id}/collection.json"
        catalog.add_link(pystac.Link(rel="child", target=collection_href))
        # Re-save catalog
        catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)


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

    publish_version(
        collection_id,
        assets=assets,
        catalog_root=catalog_root,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Directory handling
# ─────────────────────────────────────────────────────────────────────────────

# Note: GEOSPATIAL_EXTENSIONS imported from portolan_cli.constants
# Note: is_filegdb is imported from portolan_cli.scan_detect (canonical implementation).
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
    added: list[ItemInfo] = []
    skipped: list[Path] = []
    failures: list[AddFailure] = []

    # Track which nested collections have had their catalogs set up (ADR-0032)
    setup_collections: set[str] = set()

    # Track source_dir -> item_dir mappings for non-geo file placement (ADR-0028)
    source_to_item_dir: dict[Path, tuple[Path, str, str]] = {}

    # Track source_dir -> collection_dir mappings for collection-level assets (Issue #383)
    source_to_collection_dir: dict[Path, tuple[Path, str]] = {}

    # Deferred non-geo files: (file_path, source_dir, collection_id)
    deferred_non_geo: list[tuple[Path, Path, str]] = []

    # Phase 1: Collect files (extracted to reduce complexity)
    files_to_process = _collect_files_for_add(
        paths, catalog_root, collection_id, skipped, setup_collections, force=force
    )

    # Phase 2: Process files
    if not files_to_process:
        return added, skipped, failures

    # Issue #465: filenames of every batch item (sources + converted outputs) so a
    # collection-level asset scan can skip siblings that are tracked as their own
    # items instead of re-scanning the whole flat collection dir (O(n²) → O(n)).
    batch_exclude_names = _batch_sibling_names([fp for fp, _ in files_to_process])

    # Import here to avoid circular imports

    # Accumulate prepared items for batch finalization (Issue #281)
    prepared_items: list[PreparedItem] = []

    def prepare_single_file(
        file_path: Path, coll_id: str
    ) -> tuple[
        list[PreparedItem],
        list[AddFailure],
        tuple[Path, Path, str] | None,  # deferred non-geo
    ]:
        """Prepare a single file. Returns (prepared_list, failures, deferred).

        This runs prepare_item() which does GDAL work but does NOT write
        versions.json or collection.json. Those writes are batched in finalize.

        Per Issue #281: This is the parallelizable phase. Each item writes to
        its own item.json (no conflict). versions.json and collection.json
        are written once at the end via finalize_items().

        Per Issue #265: Multi-layer files (GeoPackage, FileGDB) are split into
        separate parquet files, one per layer.
        """
        prepared_list: list[PreparedItem] = []
        failure_list: list[AddFailure] = []

        # Check for multi-layer files (GeoPackage, FileGDB) - Issue #265
        if is_multilayer(file_path):
            try:
                # Load vector settings from catalog config
                vector_settings = get_vector_settings(catalog_root)
                # Convert all layers to separate parquet files
                results = convert_multilayer_file(
                    file_path, file_path.parent, settings=vector_settings
                )

                for result in results:
                    if result.success and result.output:
                        # Prepare each converted layer
                        try:
                            prepared = prepare_item(
                                path=result.output,
                                catalog_root=catalog_root,
                                collection_id=coll_id,
                                item_id=None,  # Derive from output filename
                                item_datetime=item_datetime,
                                force=force,
                                reconvert=reconvert,
                                exclude_sibling_names=batch_exclude_names,
                            )
                            # Apply partitioning to each layer (Issue #352)
                            partitioned = _maybe_partition_large_file(
                                prepared=prepared,
                                catalog_root=catalog_root,
                                item_datetime=item_datetime,
                                skip_partitioning=skip_partitioning,
                            )
                            prepared_list.extend(partitioned)
                        except Exception as err:
                            failure_list.append(
                                AddFailure(
                                    path=result.output,
                                    error=f"Layer {result.layer}: {err}",
                                )
                            )
                    else:
                        failure_list.append(
                            AddFailure(
                                path=file_path,
                                error=f"Layer {result.layer}: {result.error}",
                            )
                        )

                return (prepared_list, failure_list, None)

            except Exception as err:
                return ([], [AddFailure(path=file_path, error=str(err))], None)

        # Single-layer file - original behavior
        try:
            prepared = prepare_item(
                path=file_path,
                catalog_root=catalog_root,
                collection_id=coll_id,
                item_id=item_id,
                item_datetime=item_datetime,
                force=force,
                reconvert=reconvert,
                exclude_sibling_names=batch_exclude_names,
            )
            # Check if file should be partitioned (Issue #352)
            # Returns multiple PreparedItems if partitioned, else [prepared]
            partitioned = _maybe_partition_large_file(
                prepared=prepared,
                catalog_root=catalog_root,
                item_datetime=item_datetime,
                skip_partitioning=skip_partitioning,
            )
            return (partitioned, [], None)

        except click.ClickException as err:
            is_tabular = file_path.suffix.lower() in TABULAR_EXTENSIONS
            if _is_no_geometry_error(err) and is_tabular:
                return ([], [], (file_path, file_path.parent, coll_id))
            return ([], [AddFailure(path=file_path, error=str(err))], None)

        except NoGeometryError as err:
            if file_path.suffix.lower() in TABULAR_EXTENSIONS:
                return ([], [], (file_path, file_path.parent, coll_id))
            return ([], [AddFailure(path=file_path, error=str(err))], None)

        except ValueError as err:
            if (
                _is_parquet_no_geometry_error(err)
                and file_path.suffix.lower() in TABULAR_EXTENSIONS
            ):
                return ([], [], (file_path, file_path.parent, coll_id))
            return ([], [AddFailure(path=file_path, error=str(err))], None)

        except Exception as err:
            return ([], [AddFailure(path=file_path, error=str(err))], None)

    total_files = len(files_to_process)

    if workers == 1:
        # Sequential execution (original behavior)
        for file_path, coll_id in files_to_process:
            if on_progress is not None:
                on_progress(file_path)

            prepared_list, failure_list, deferred = prepare_single_file(file_path, coll_id)
            for prepared in prepared_list:
                prepared_items.append(prepared)
                source_dir = file_path.parent
                collection_dir = catalog_root / Path(*coll_id.split("/"))
                if prepared.is_collection_level_asset:
                    # Collection-level: map source to collection dir (Issue #383)
                    source_to_collection_dir[source_dir] = (collection_dir, coll_id)
                else:
                    # Item-level: map source to item dir
                    item_dir = collection_dir / prepared.item_id
                    source_to_item_dir[source_dir] = (item_dir, coll_id, prepared.item_id)
            failures.extend(failure_list)
            if deferred is not None:
                deferred_non_geo.append(deferred)
    else:
        # Parallel execution with ThreadPoolExecutor
        from concurrent.futures import ThreadPoolExecutor, as_completed

        from portolan_cli.output import info

        # Show worker count
        if not json_mode:
            info(f"Using {workers} parallel workers for {total_files} files")

        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(prepare_single_file, fp, cid): (fp, cid)
                for fp, cid in files_to_process
            }

            # Process results as they complete (main thread)
            for future in as_completed(future_to_file):
                file_path, coll_id = future_to_file[future]
                prepared_list, failure_list, deferred = future.result()

                # Call progress callback from main thread (thread-safe)
                # This ensures CLI's AddProgressReporter works in parallel mode
                if on_progress is not None:
                    on_progress(file_path)

                for prepared in prepared_list:
                    prepared_items.append(prepared)
                    source_dir = file_path.parent
                    collection_dir = catalog_root / Path(*coll_id.split("/"))
                    if prepared.is_collection_level_asset:
                        # Collection-level: map source to collection dir (Issue #383)
                        source_to_collection_dir[source_dir] = (collection_dir, coll_id)
                    else:
                        # Item-level: map source to item dir
                        item_dir = collection_dir / prepared.item_id
                        source_to_item_dir[source_dir] = (item_dir, coll_id, prepared.item_id)
                failures.extend(failure_list)
                if deferred is not None:
                    deferred_non_geo.append(deferred)

    # ========================================================================
    # PHASE 2.5: Batch finalize all prepared items (Issue #281)
    # ========================================================================
    # This is the key optimization: ONE write per collection instead of O(n)
    if prepared_items:
        added.extend(finalize_items(catalog_root, prepared_items, merge_strategy))

    # ========================================================================
    # PHASE 3: Process deferred non-geo files (sequential)
    # ========================================================================
    affected_collections = _process_deferred_non_geo_files(
        deferred_non_geo=deferred_non_geo,
        source_to_item_dir=source_to_item_dir,
        source_to_collection_dir=source_to_collection_dir,
        catalog_root=catalog_root,
        skipped=skipped,
        failures=failures,
    )

    # ========================================================================
    # PHASE 3.5: Recompute file statistics for collections with deferred assets
    # ========================================================================
    # Deferred assets are added after finalize_items, so file statistics
    # must be recomputed to include them (Issue #501)
    for collection_dir in affected_collections:
        collection_json_path = collection_dir / "collection.json"
        if collection_json_path.exists():
            collection = pystac.Collection.from_file(str(collection_json_path))
            update_collection_file_statistics(collection)
            collection.save_object(include_self_link=False)

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
