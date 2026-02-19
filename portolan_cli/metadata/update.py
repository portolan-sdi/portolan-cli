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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from portolan_cli.collection import (
    read_collection_json,
    write_collection_json,
)
from portolan_cli.item import (
    _extract_geometry_from_file,
    _get_media_type,
    create_item,
    read_item_json,
    write_item_json,
)
from portolan_cli.models.collection import (
    CollectionModel,
    ExtentModel,
    SpatialExtent,
)
from portolan_cli.models.item import AssetModel, ItemModel
from portolan_cli.versions import (
    Asset,
    Version,
    VersionsFile,
    read_versions,
    write_versions,
)


def update_item_metadata(item_path: Path, file_path: Path) -> ItemModel:
    """Re-extract metadata from file and update existing STAC item.

    Reads the existing item, extracts fresh metadata from the data file,
    updates bbox, geometry, datetime, and assets while preserving
    user-added fields (title, description).

    Args:
        item_path: Path to existing item JSON file.
        file_path: Path to the data file (GeoParquet or COG).

    Returns:
        Updated ItemModel.

    Raises:
        FileNotFoundError: If item_path or file_path doesn't exist.
    """
    # Validate both files exist
    if not file_path.exists():
        raise FileNotFoundError(f"Data file not found: {file_path}")
    if not item_path.exists():
        raise FileNotFoundError(f"Item file not found: {item_path}")

    # Read existing item
    existing_item = read_item_json(item_path)

    # Extract fresh metadata from file
    bbox, geometry = _extract_geometry_from_file(file_path)

    # Create new datetime
    new_datetime = datetime.now(timezone.utc).isoformat()

    # Create updated properties (preserve existing, update datetime)
    updated_properties: dict[str, Any] = dict(existing_item.properties)
    updated_properties["datetime"] = new_datetime

    # Determine media type
    media_type = _get_media_type(file_path)

    # Create updated assets
    assets = {
        "data": AssetModel(
            href=str(file_path.name),
            type=media_type,
            roles=["data"],
            title="Data file",
        )
    }

    # Create updated item, preserving user-added fields
    updated_item = ItemModel(
        id=existing_item.id,
        geometry=geometry,
        bbox=bbox,
        properties=updated_properties,
        assets=assets,
        collection=existing_item.collection,
        title=existing_item.title,  # Preserve user field
        description=existing_item.description,  # Preserve user field
        links=existing_item.links,  # Preserve links
    )

    # Write updated item back to disk
    write_item_json(updated_item, item_path.parent)

    return updated_item


def create_missing_item(file_path: Path, collection_path: Path) -> Path:
    """Create new STAC item for a file without metadata.

    Uses existing create_item() from item.py, ensures proper linking
    to parent collection, and writes item.json alongside the data file.

    Args:
        file_path: Path to the data file (GeoParquet or COG).
        collection_path: Path to the parent collection directory.

    Returns:
        Path to the created item JSON file.

    Raises:
        FileNotFoundError: If file_path doesn't exist or collection.json not found.
    """
    # Validate file exists
    if not file_path.exists():
        raise FileNotFoundError(f"Data file not found: {file_path}")

    # Validate collection.json exists
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        raise FileNotFoundError(f"collection.json not found: {collection_json_path}")

    # Read collection to get its ID
    collection = read_collection_json(collection_json_path)

    # Use filename stem as item ID
    item_id = file_path.stem

    # Create item using existing function
    item = create_item(
        item_id=item_id,
        data_path=file_path,
        collection_id=collection.id,
    )

    # Write item to collection directory
    item_path = write_item_json(item, collection_path)

    return item_path


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
            with open(item_file) as f:
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


def _compute_union_bbox(bboxes: list[list[float]]) -> list[float]:
    """Compute the union bounding box from multiple bboxes.

    Args:
        bboxes: List of bounding boxes, each as [west, south, east, north].

    Returns:
        Union bounding box [west, south, east, north].
    """
    if not bboxes:
        return [-180.0, -90.0, 180.0, 90.0]  # Global default

    min_west = min(bbox[0] for bbox in bboxes)
    min_south = min(bbox[1] for bbox in bboxes)
    max_east = max(bbox[2] for bbox in bboxes)
    max_north = max(bbox[3] for bbox in bboxes)

    return [min_west, min_south, max_east, max_north]


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

    # Create updated asset with new mtime
    old_asset = current_version.assets[asset_name]
    updated_asset = Asset(
        sha256=old_asset.sha256,
        size_bytes=old_asset.size_bytes,
        href=old_asset.href,
        source_path=old_asset.source_path,
        source_mtime=current_mtime,
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
