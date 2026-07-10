"""Batch finalization for the add pipeline: STAC-write + backend coordination.

Extracted from ``add.py`` (issue #624). This module owns the second phase of
``add``: taking the per-item :class:`PreparedItem` objects produced by
``preparation.prepare_item`` and writing the *collection-level* artifacts exactly
once per collection — ``collection.json`` (assets, summaries, extents,
extensions), ``versions.json``, and the parent ``catalog.json`` links — then
routing the version snapshot to the active backend (file or plugin).

Batching all writes by collection is the key optimization behind Issue #281:
one write per collection instead of O(n) per item. A per-item preparation
failure therefore leaves no partial version entry (see ``.claude/rules``:
"add must be atomic").

Per ADR-0007 the CLI stays a thin wrapper; ``add.py`` orchestrates on top of the
routines here. This module deliberately imports nothing from ``add`` so the
dependency edge is one-directional (add -> finalization). Imports of
``catalog`` / ``backends`` / ``version_ops`` are kept **function-local** on
purpose: ``catalog`` re-exports ``add_files`` at import time, so a module-level
``finalization -> catalog`` edge would reintroduce a load-time cycle.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import pystac
from pystac.layout import AsIsLayoutStrategy

from portolan_cli.config import load_merged_metadata
from portolan_cli.formats import FormatType
from portolan_cli.humanize import humanize_slug
from portolan_cli.metadata import extract_geoparquet_metadata
from portolan_cli.metadata.geoparquet import GeoParquetMetadata
from portolan_cli.preparation import PreparedItem
from portolan_cli.query import ItemInfo
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
from portolan_cli.versions import (
    Asset,
    VersionsFile,
    _increment_version,
    add_version,
    read_versions,
    write_versions,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Collection link management
# ─────────────────────────────────────────────────────────────────────────────


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

    # Scaffold AGENTS.md and add the rel="agents" link (ADR-0052, RULE-0081).
    # Idempotent and merge-safe: never overwrites an existing AGENTS.md, only
    # adds the link when absent, so re-running `add` preserves human edits.
    from portolan_cli.agents_md import ensure_agents_md

    ensure_agents_md(collection_json_path)


# ─────────────────────────────────────────────────────────────────────────────
# Spatial extent recomputation
# ─────────────────────────────────────────────────────────────────────────────


def _gather_collection_bboxes(collection: pystac.Collection) -> list[list[float]]:
    """Collect all candidate bboxes for a collection's spatial extent (issue #516).

    Gathers bboxes from linked items, collection-level assets (``proj:bbox``),
    and the existing collection extent (as a fallback), de-duplicating the
    existing-extent entries against what the items/assets already contributed.

    Args:
        collection: The pystac Collection to read bboxes from.

    Returns:
        List of bboxes (each a ``[minx, miny, maxx, maxy]`` list).
    """
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

    return all_bboxes


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

    all_bboxes = _gather_collection_bboxes(collection)
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


# ─────────────────────────────────────────────────────────────────────────────
# Collection assembly helpers
# ─────────────────────────────────────────────────────────────────────────────


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
            # Normalize to match relative_to(...).as_posix() below: drop only an
            # exact "./" prefix. lstrip("./") would also strip leading dots from
            # hidden paths (".hidden/x.parquet" -> "hidden/x.parquet") and never
            # match, silently dropping the asset from row-count aggregation.
            href = asset.href.removeprefix("./")
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


# ─────────────────────────────────────────────────────────────────────────────
# Versioning (file backend) + freshness tracking
# ─────────────────────────────────────────────────────────────────────────────


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


def _finalize_with_file_backend(
    catalog_root: Path,
    collection_id: str,
    collection_dir: Path,
    items: list[PreparedItem],
) -> None:
    """Update versions.json and catalog-level versions via the file backend.

    File backend fast path (O(1) write per collection). Updates the
    collection-level versions.json first, then mirrors the summary into the
    catalog-level versions.json (ADR-0005). A catalog-level failure is logged
    but never fails the add: the collection version was already published.
    """
    from portolan_cli.catalog import update_catalog_versions

    current_version, asset_count, total_size = _batch_update_versions(
        collection_dir=collection_dir,
        collection_id=collection_id,
        items=items,
    )

    # Update catalog-level versions.json (ADR-0005)
    # This keeps the catalog-level view in sync with collection state.
    # Wrap in try/except to avoid failing the add if catalog update fails
    # (collection-level versions.json was already written successfully).
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


def _publish_collection_version(
    catalog_root: Path,
    collection_id: str,
    collection_dir: Path,
    collection: pystac.Collection,
    items: list[PreparedItem],
) -> None:
    """Route the collection's version snapshot to the active backend.

    Plugin backends (e.g. Iceberg) publish + run post-add hooks; the default
    file backend uses the optimized batch write. Backend selection follows the
    resolved ``backend`` setting (ADR-0046).
    """
    from portolan_cli.config import get_setting

    active_backend = get_setting("backend", catalog_path=catalog_root)

    if active_backend is not None and active_backend != "file":
        _finalize_with_backend(
            catalog_root=catalog_root,
            collection_id=collection_id,
            collection_dir=collection_dir,
            collection=collection,
            items=items,
            active_backend=active_backend,
        )
    else:
        _finalize_with_file_backend(
            catalog_root=catalog_root,
            collection_id=collection_id,
            collection_dir=collection_dir,
            items=items,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Per-collection assembly + batch entry point
# ─────────────────────────────────────────────────────────────────────────────


def _emit_stale_asset_warnings(
    collection: pystac.Collection,
    collection_dir: Path,
) -> None:
    """Warn about assets referencing missing files (issue #447).

    Warns but does not remove; removal is handled by ``check --fix``.
    """
    stale_warnings = _warn_about_stale_assets(collection, collection_dir)
    if not stale_warnings:
        return

    from portolan_cli.output import warn as warn_output

    warn_output(
        f"{len(stale_warnings)} asset(s) reference missing files "
        "(run `portolan check --fix` to clean up)"
    )
    for warning_msg in stale_warnings:
        logger.debug(warning_msg)


def _apply_table_extension_from_disk(
    collection: pystac.Collection,
    collection_dir: Path,
    items: list[PreparedItem],
    merge_strategy: MergeStrategy,
) -> None:
    """Aggregate table:* metadata from tracked parquet on disk (issues #304/#447).

    Only runs when the batch contains at least one NEW GeoParquet item.
    Recomputes aggregates from ALL tracked parquet files on disk instead of
    carrying forward stale counts — preventing double-counting on re-add and
    stale counts when files are replaced. PMTiles/FlatGeobuf are collection-level
    assets without table schema and are excluded.
    """
    new_geoparquet_metadata: list[GeoParquetMetadata] = [
        p.metadata
        for p in items
        if p.format_type == FormatType.VECTOR and isinstance(p.metadata, GeoParquetMetadata)
    ]
    if not new_geoparquet_metadata:
        return

    # Recompute from disk: scan tracked parquet assets in collection
    # This is O(n) file metadata reads but always correct
    all_parquet_metadata = _collect_parquet_metadata_from_disk(collection_dir, collection)
    if all_parquet_metadata:
        aggregated = aggregate_table_metadata(all_parquet_metadata)
        add_table_extension(collection, aggregated, merge_strategy=merge_strategy)


def _emit_partition_warnings(
    collection: pystac.Collection,
    collection_dir: Path,
    items: list[PreparedItem],
) -> None:
    """Add partition metadata (issues #232/#443) and surface any schema warnings."""
    partition_warnings = _ensure_partition_metadata(collection, collection_dir, items)
    if not partition_warnings:
        return

    from portolan_cli.output import warn as warn_output

    for warning_msg in partition_warnings:
        warn_output(warning_msg)


def _finalize_collection(
    catalog_root: Path,
    collection_id: str,
    items: list[PreparedItem],
    merge_strategy: MergeStrategy,
) -> list[ItemInfo]:
    """Assemble and persist a single collection from its prepared items.

    Builds collection.json (assets, summaries, extents, extensions) in memory,
    saves it once, and publishes the version snapshot via the active backend.
    See ``finalize_items`` for the batch orchestration around this.

    Args:
        catalog_root: Root directory of the catalog.
        collection_id: Collection identifier (may be nested per ADR-0032).
        items: Prepared items belonging to this collection.
        merge_strategy: How to merge auto-detected metadata with existing values.

    Returns:
        List of ItemInfo for each finalized item in this collection.
    """
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
    _emit_stale_asset_warnings(collection, collection_dir)

    # Add table extension if any items are GeoParquet format (Issue #304/#447)
    _apply_table_extension_from_disk(collection, collection_dir, items, merge_strategy)

    # Add partition extension if any items have partition metadata (Issue #232/#443)
    _emit_partition_warnings(collection, collection_dir, items)

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
    _recompute_collection_extent_with_multibbox(collection)

    # Save collection.json ONCE for all items in this collection
    _save_collection_with_links(collection, collection_dir, catalog_root, collection_id)

    # Route version snapshot to the active backend (plugin or file)
    _publish_collection_version(catalog_root, collection_id, collection_dir, collection, items)

    # Build results
    return [
        ItemInfo(
            item_id=p.item_id,
            collection_id=p.collection_id,
            format_type=p.format_type,
            bbox=p.bbox,
            asset_paths=[str(path) for _name, (path, _checksum, _size) in p.asset_files.items()],
        )
        for p in items
    ]


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
        results.extend(_finalize_collection(catalog_root, collection_id, items, merge_strategy))

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
