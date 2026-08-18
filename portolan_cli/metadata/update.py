"""Metadata update functions for STAC items, collections, and versions.

This module provides functions to update existing metadata when files change:
- update_item_metadata(): Re-extract metadata and update existing STAC item
- create_missing_item(): Create new STAC item for file without metadata
- update_collection_extent(): Recalculate extent from child items
- update_versions_tracking(): Update source_mtime in versions.json

Part of Phase 2c: Update Functions (check-metadata-handling feature).
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import rasterio

from portolan_cli.collection import (
    read_collection_json,
    write_collection_json,
)
from portolan_cli.formats import RASTER_EXTENSIONS, FormatType
from portolan_cli.item import (
    _extract_geometry_from_file,
    _get_media_type,
    create_item,
    write_item_json,
)
from portolan_cli.json_io import write_json_atomic
from portolan_cli.metadata.cog import extract_cog_metadata
from portolan_cli.models.collection import (
    CollectionModel,
    ExtentModel,
    SpatialExtent,
)
from portolan_cli.models.item import ItemModel
from portolan_cli.versions import (
    Asset,
    Version,
    VersionsFile,
    read_versions,
    write_versions,
)

logger = logging.getLogger(__name__)

#: The band name ``extract_cog_metadata`` generates. A band called anything else
#: was named by a human, so the band refresh below keeps that name rather than
#: writing the placeholder back over it.
_GENERATED_BAND_NAME = re.compile(r"^band_\d+$")


def update_item_metadata(item_path: Path, file_path: Path) -> ItemModel:
    """Re-extract metadata from file and update existing STAC item.

    Refreshes only the fields this metadata pass owns, the item's bbox,
    geometry, datetime, the data asset's href, and, for a raster, the data
    asset's ``bands`` array. Everything else on disk is preserved. That
    deliberately includes the item's ``stac_extensions``, every non-``data``
    asset such as the ``thumbnail`` that ``portolan add`` registers for COGs
    (#657), a non-raster asset's ``bands`` (#659), and the data asset's media
    type, which the refresh backfills only when it is missing.

    A raster's ``bands`` array is the exception, because it describes the shape
    of the file and formats.md makes the file authoritative for band statistics.
    Re-shape a 4-band COG into a 1-band COG and a preserved array documents
    bands that no longer exist (#737), on a command whose job is freshness. So
    the refresh re-reads the array from the bytes with the extractors
    ``portolan add`` uses. A failed or empty extraction keeps what is on disk,
    because a refresh that cannot read the file must not destroy the record
    (#659).

    The update edits the raw item JSON in place rather than round-tripping
    through :class:`ItemModel`. The model only carries href/type/roles/title for
    assets and drops ``stac_extensions`` entirely, so serializing through it
    would silently destroy those fields (#659).

    Args:
        item_path: Path to existing item JSON file.
        file_path: Path to the data file (GeoParquet or COG).

    Returns:
        Updated ItemModel (a lossy view, the on-disk JSON is the full record).

    Raises:
        FileNotFoundError: If item_path or file_path doesn't exist.
    """
    # Validate both files exist
    if not file_path.exists():
        raise FileNotFoundError(f"Data file not found: {file_path}")
    if not item_path.exists():
        raise FileNotFoundError(f"Item file not found: {item_path}")

    # Load the raw item as the source of truth so nothing outside the refresh's
    # scope is dropped.
    item_data: dict[str, Any] = json.loads(item_path.read_text(encoding="utf-8"))

    # Extract fresh metadata from file
    bbox, geometry = _extract_geometry_from_file(file_path)
    item_data["bbox"] = bbox
    item_data["geometry"] = geometry

    # Bump datetime, preserving every other property.
    properties = item_data.setdefault("properties", {})
    properties["datetime"] = datetime.now(timezone.utc).isoformat()

    # Refresh the data asset's href/type in place, leaving any human-authored
    # title and all other assets untouched.
    assets: dict[str, Any] = item_data.setdefault("assets", {})
    data_key = _find_data_asset_key(assets)
    data_asset = assets.setdefault(data_key, {})
    data_asset["href"] = file_path.name
    # Keep the media type `add` registered. `add` derives it from the extension
    # registry, which gives a COG the `profile=cloud-optimized` suffix that
    # PORTO-CORE-026 makes a MUST; the local extension map here does not, so
    # overwriting would downgrade a valid COG asset to plain GeoTIFF. Only
    # backfill when the field is absent or blank.
    if not data_asset.get("type"):
        data_asset["type"] = _get_media_type(file_path)
    data_asset.setdefault("roles", ["data"])
    _refresh_raster_bands(data_asset, file_path)

    # Write the full record back to disk. Atomic, so a kill mid-write cannot
    # leave a truncated item.json where a valid one used to be (issue #687).
    write_json_atomic(item_path, item_data)

    return ItemModel.from_dict(item_data)


def _refresh_raster_bands(data_asset: dict[str, Any], file_path: Path) -> None:
    """Re-extract a raster data asset's ``bands`` array in place (#737).

    A non-raster asset is untouched, so a GeoParquet asset's array still survives
    the refresh verbatim (#659). So does a raster's array when the extraction
    fails or reads no bands, because a refresh that cannot see the file has
    nothing better to write than what is already recorded.

    When the band count is unchanged the fresh values are merged over the
    existing band, keeping the descriptive fields extraction cannot derive. When
    the count changes the file has been re-shaped, so a label written for one of
    four bands cannot be assumed to describe the one band that is left, and the
    fresh array replaces the old one outright.
    """
    if file_path.suffix.lower() not in RASTER_EXTENSIONS:
        return

    fresh = _extract_raster_bands(file_path)
    if not fresh:
        logger.warning(
            "Could not re-extract bands from '%s'; keeping the recorded array",
            file_path.name,
        )
        return

    existing = data_asset.get("bands")
    if isinstance(existing, list) and len(existing) == len(fresh):
        fresh = [
            _merge_band(new, old) if isinstance(old, dict) else new
            for new, old in zip(fresh, existing, strict=True)
        ]

    data_asset["bands"] = fresh


def _extract_raster_bands(file_path: Path) -> list[dict[str, Any]]:
    """The ``bands`` array ``portolan add`` would write for this raster.

    Orchestrates the extractors the add path uses, ``extract_cog_metadata`` for
    the per-band shape and ``preparation``'s statistics helpers for the
    ``statistics`` sub-object, so a refreshed array and a freshly added one are
    built by the same code and carry the same STAC field names. Returns an empty
    list when the raster cannot be read, which the caller treats as "keep what is
    on disk".

    Reads with ``GDAL_PAM_ENABLED=NO`` so a ``.aux.xml`` sidecar cannot answer
    for the file. formats.md is explicit that statistics MUST be embedded in the
    file and that a PAM sidecar does not satisfy that, and GDAL never invalidates
    a sidecar when the raster underneath is replaced. `add` leaves one next to
    every raster it computes statistics for, so a refresh that trusted it would
    copy the old raster's numbers onto the new bands. It is also how rashid reads
    a COG for PTL-DAT-009.
    """
    # Imported inside the function on purpose: `preparation` imports the
    # `metadata` package this module belongs to, so a module-level import would
    # close an import cycle.
    from portolan_cli.preparation import (
        _add_statistics_to_properties,
        _extract_statistics_best_effort,
    )

    with rasterio.Env(GDAL_PAM_ENABLED="NO"):
        try:
            stac_properties = extract_cog_metadata(file_path).to_stac_properties()
        except Exception:
            logger.warning("Raster metadata extraction failed for '%s'", file_path, exc_info=True)
            return []

        # Statistics are a configured enrichment, so they need the catalog whose
        # settings gate them. A raster outside any catalog gets the band shape only.
        catalog_root, collection_dir = _statistics_scope(file_path)
        if catalog_root is not None:
            band_stats, parquet_stats = _extract_statistics_best_effort(
                file_path, FormatType.RASTER, catalog_root, collection_path=collection_dir
            )
            # `_extract_statistics_best_effort` already applied the
            # `statistics.enabled` gate, returning nothing when the setting is
            # off, so the flag here is always True.
            _add_statistics_to_properties(
                stac_properties, FormatType.RASTER, band_stats, parquet_stats, stats_enabled=True
            )

    bands = stac_properties.get("bands")
    if not isinstance(bands, list):
        return []
    return [band for band in bands if isinstance(band, dict)]


def _statistics_scope(file_path: Path) -> tuple[Path | None, Path | None]:
    """The (catalog root, collection dir) a statistics setting resolves against.

    The refresh is handed one file, so the scope `add` receives as arguments has
    to be recovered from the path. The catalog root comes from the usual sentinel
    walk, the collection from the nearest ancestor holding a ``collection.json``.
    Either may be None, and `get_setting` then falls back a level.
    """
    from portolan_cli.catalog import find_catalog_root

    catalog_root = find_catalog_root(file_path.parent)
    if catalog_root is None:
        return None, None

    for parent in file_path.parents:
        if (parent / "collection.json").exists():
            return catalog_root, parent
        if parent == catalog_root:
            break
    return catalog_root, None


def _merge_band(fresh: dict[str, Any], existing: dict[str, Any]) -> dict[str, Any]:
    """Fresh band values over the recorded ones, keeping what pixels cannot say.

    The split follows what is derivable. ``data_type``, ``nodata`` and
    ``statistics`` come from the file, which formats.md makes the authority for
    them. A ``name`` or ``description`` a human wrote is descriptive, not
    derivable, so it survives. A name is kept unless it is the generated
    ``band_N`` placeholder, and any other recorded key the extractor does not
    produce carries over. That also means a value extraction failed to read,
    including a ``nodata`` that came from ``metadata.yaml`` defaults the refresh
    never sees, is preserved rather than dropped.
    """
    merged = {**existing, **fresh}

    name = existing.get("name")
    if isinstance(name, str) and name and not _GENERATED_BAND_NAME.match(name):
        merged["name"] = name

    return merged


def _find_data_asset_key(assets: dict[str, Any]) -> str:
    """Return the key of the item's data asset, defaulting to ``"data"``.

    Prefers an asset whose ``roles`` include ``"data"``, then a literal
    ``"data"`` key, so the href/type refresh lands on the real data asset
    without disturbing sibling assets (thumbnail, metadata, ...).
    """
    for key, asset in assets.items():
        roles = asset.get("roles") if isinstance(asset, dict) else None
        if isinstance(roles, list) and "data" in roles:
            return key
    return "data"


def create_missing_item(file_path: Path, collection_path: Path) -> Path:
    """Create new STAC item for a file without metadata.

    Writes item.json into the data file's parent directory at
    `{item_dir}/{item_id}.json`, matching the hierarchical convention
    that `add` produces ( and add.py:_create_and_save_item).

    Args:
        file_path: Path to the data file (GeoParquet or COG).
        collection_path: Path to the parent collection directory.

    Returns:
        Path to the created item JSON file.

    Raises:
        FileNotFoundError: If file_path doesn't exist or collection.json not found.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Data file not found: {file_path}")

    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        raise FileNotFoundError(f"collection.json not found: {collection_json_path}")

    collection = read_collection_json(collection_json_path)
    item_id = file_path.stem
    item_dir = file_path.parent

    item = create_item(
        item_id=item_id,
        data_path=file_path,
        collection_id=collection.id,
    )

    return write_item_json(item, item_dir)


def update_collection_extent(collection_path: Path) -> CollectionModel:
    """Recalculate collection extent from child items.

    Reads all items in the collection directory, computes the union
    bounding box, updates the collection extent, and writes the
    updated collection.json.

    Args:
        collection_path: Path to the collection directory.

    Returns:
        Updated CollectionModel.

    Raises:
        FileNotFoundError: If collection_path doesn't exist or has no collection.json.
    """
    # Validate collection.json exists
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        raise FileNotFoundError(f"collection.json not found: {collection_json_path}")

    # Read existing collection
    collection = read_collection_json(collection_json_path)

    # Find all item JSON files in collection directory
    item_files = list(collection_path.glob("*.json"))
    item_files = [f for f in item_files if f.name not in ("collection.json", "schema.json")]

    # Collect bboxes from all items
    bboxes: list[list[float]] = []
    for item_file in item_files:
        try:
            with open(item_file, encoding="utf-8") as f:
                data = json.load(f)
            # Check if this is a valid STAC item (has bbox)
            if "bbox" in data and data.get("type") == "Feature":
                bboxes.append(data["bbox"])
        except (json.JSONDecodeError, KeyError):
            # Skip invalid JSON files
            continue

    # If no items found, return collection unchanged
    if not bboxes:
        return collection

    # Compute union bbox
    union_bbox = _compute_union_bbox(bboxes)
    if union_bbox is None:
        # No valid child bboxes; keep the existing extent rather than persisting
        # a synthetic global [-180, -90, 180, 90] that would mask the failure.
        logger.warning(
            "Collection '%s': all item bboxes invalid; keeping existing extent",
            collection.id,
        )
        return collection

    # Update collection extent
    updated_extent = ExtentModel(
        spatial=SpatialExtent(bbox=[union_bbox]),
        temporal=collection.extent.temporal,  # Keep temporal extent
    )

    # Create updated collection
    updated_collection = CollectionModel(
        id=collection.id,
        description=collection.description,
        extent=updated_extent,
        license=collection.license,
        title=collection.title,
        summaries=collection.summaries,
        providers=collection.providers,
        keywords=collection.keywords,
        created=collection.created,
        updated=datetime.now(timezone.utc),
        links=collection.links,
    )

    # Write updated collection
    write_collection_json(updated_collection, collection_path)

    return updated_collection


def _compute_union_bbox(bboxes: list[list[float]]) -> list[float] | None:
    """Compute the union bounding box from multiple bboxes.

    Filters out invalid bboxes (inf/nan/out-of-range) with warnings (issue #516).

    Args:
        bboxes: List of bounding boxes, each as [west, south, east, north].

    Returns:
        Union bounding box [west, south, east, north], or None when there are no
        valid bboxes. Callers must not substitute a synthetic global extent, as
        that would mask the failure (issue #516).
    """
    from portolan_cli.bbox import compute_bbox_union

    if not bboxes:
        return None

    result = compute_bbox_union(bboxes)
    return result.bbox  # None when all bboxes are invalid


def update_versions_tracking(file_path: Path, versions_path: Path) -> None:
    """Update source_mtime in versions.json for a file.

    Reads versions.json, finds the asset entry for the file,
    updates the source_mtime to the current file modification time,
    and writes the updated versions.json.

    Args:
        file_path: Path to the data file.
        versions_path: Path to the versions.json file.

    Raises:
        FileNotFoundError: If file_path or versions_path doesn't exist.
        KeyError: If asset not found in versions.json.
    """
    # Validate file exists
    if not file_path.exists():
        raise FileNotFoundError(f"Data file not found: {file_path}")

    # Read versions.json
    versions_file = read_versions(versions_path)

    # Get current version
    if not versions_file.versions:
        raise KeyError("No versions in versions.json")

    current_version = versions_file.versions[-1]

    # Find the asset
    asset_name = file_path.name
    if asset_name not in current_version.assets:
        raise KeyError(f"Asset not found in versions.json: {asset_name}")

    # Get current mtime from file
    current_mtime = file_path.stat().st_mtime

    # Refresh both mtime fields to the current file so the entry stays
    # internally consistent — carrying the current `source_mtime` next to a
    # stale `mtime` would leave the two fields describing different on-disk
    # states. Preserve the freshness heuristics (feature_count,
    # schema_fingerprint) so refreshing mtime does not wipe them (#512).
    old_asset = current_version.assets[asset_name]
    updated_asset = Asset(
        sha256=old_asset.sha256,
        size_bytes=old_asset.size_bytes,
        href=old_asset.href,
        source_path=old_asset.source_path,
        source_mtime=current_mtime,
        mtime=current_mtime,
        feature_count=old_asset.feature_count,
        schema_fingerprint=old_asset.schema_fingerprint,
    )

    # Create updated assets dict
    updated_assets = dict(current_version.assets)
    updated_assets[asset_name] = updated_asset

    # Create updated version
    updated_version = Version(
        version=current_version.version,
        created=current_version.created,
        breaking=current_version.breaking,
        assets=updated_assets,
        changes=current_version.changes,
    )

    # Create updated versions list
    updated_versions = list(versions_file.versions[:-1])
    updated_versions.append(updated_version)

    # Create updated versions file
    updated_versions_file = VersionsFile(
        spec_version=versions_file.spec_version,
        current_version=versions_file.current_version,
        versions=updated_versions,
    )

    # Write updated versions.json
    write_versions(versions_path, updated_versions_file)
