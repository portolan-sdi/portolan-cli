"""STAC generation module - wraps pystac for Portolan's conventions.

Provides opinionated helpers for creating STAC catalogs, collections, and items
with consistent defaults and conventions for Portolan-managed catalogs.

Key conventions:
- Self-contained catalog type (relative links, portable)
- WGS84 (EPSG:4326) as default CRS
- Consistent asset naming and roles
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pystac

# STAC version we generate
STAC_VERSION = "1.0.0"

# Default license when not specified
DEFAULT_LICENSE = "proprietary"


def create_collection(
    *,
    collection_id: str,
    description: str,
    title: str | None = None,
    license: str = DEFAULT_LICENSE,
    bbox: list[float] | None = None,
    temporal_extent: tuple[datetime | None, datetime | None] | None = None,
) -> pystac.Collection:
    """Create a STAC Collection with Portolan conventions.

    Args:
        collection_id: Unique identifier for the collection.
        description: Human-readable description.
        title: Optional display title (defaults to None).
        license: SPDX license identifier (default: "proprietary").
        bbox: Spatial extent as [min_x, min_y, max_x, max_y] in WGS84.
              Defaults to global extent if not specified.
        temporal_extent: Temporal extent as (start, end) datetimes.
                        Use None for open-ended intervals.

    Returns:
        A pystac.Collection object.
    """
    # Default to global extent if not specified
    if bbox is None:
        bbox = [-180, -90, 180, 90]

    # Default to open temporal interval
    if temporal_extent is None:
        temporal_interval: list[datetime | None] = [None, None]
    else:
        temporal_interval = list(temporal_extent)

    extent = pystac.Extent(
        spatial=pystac.SpatialExtent(bboxes=[bbox]),
        temporal=pystac.TemporalExtent(intervals=[temporal_interval]),
    )

    collection = pystac.Collection(
        id=collection_id,
        description=description,
        extent=extent,
        title=title,
        license=license,
    )

    return collection


def create_item(
    *,
    item_id: str,
    bbox: list[float],
    datetime: datetime | None = None,
    properties: dict[str, object] | None = None,
    assets: dict[str, pystac.Asset] | None = None,
) -> pystac.Item:
    """Create a STAC Item with Portolan conventions.

    Args:
        item_id: Unique identifier for the item.
        bbox: Bounding box as [min_x, min_y, max_x, max_y] in WGS84.
        datetime: Acquisition/creation datetime. Defaults to current UTC time.
        properties: Additional properties to include.
        assets: Asset dictionary to attach to the item.

    Returns:
        A pystac.Item object.
    """
    # Generate polygon geometry from bbox
    geometry = _bbox_to_polygon(bbox)

    # Default to current time if not specified
    if datetime is None:
        datetime = _now_utc()

    # Merge any custom properties
    item_properties = properties or {}

    item = pystac.Item(
        id=item_id,
        geometry=geometry,
        bbox=bbox,
        datetime=datetime,
        properties=item_properties,
    )

    # Add assets if provided
    if assets:
        for asset_key, asset in assets.items():
            item.add_asset(asset_key, asset)

    return item


def _bbox_to_polygon(bbox: list[float]) -> dict[str, object]:
    """Convert a bounding box to a GeoJSON Polygon geometry.

    Args:
        bbox: [min_x, min_y, max_x, max_y]

    Returns:
        GeoJSON Polygon dict.
    """
    min_x, min_y, max_x, max_y = bbox
    return {
        "type": "Polygon",
        "coordinates": [
            [
                [min_x, min_y],
                [min_x, max_y],
                [max_x, max_y],
                [max_x, min_y],
                [min_x, min_y],  # Close the ring
            ]
        ],
    }


def _now_utc() -> datetime:
    """Get current UTC datetime."""
    return datetime.now(timezone.utc)


def load_catalog(catalog_path: Path) -> pystac.Catalog:
    """Load an existing STAC catalog from disk.

    Args:
        catalog_path: Path to the catalog.json file.

    Returns:
        A pystac.Catalog object.

    Raises:
        FileNotFoundError: If the catalog file doesn't exist.
    """
    if not catalog_path.exists():
        raise FileNotFoundError(f"Catalog not found: {catalog_path}")

    return pystac.Catalog.from_file(str(catalog_path))


def save_catalog(catalog: pystac.Catalog, dest_dir: Path) -> None:
    """Save a STAC catalog to disk.

    Saves as a self-contained catalog with relative links.

    Args:
        catalog: The catalog to save.
        dest_dir: Directory to save the catalog to (will contain catalog.json).
    """
    dest_dir.mkdir(parents=True, exist_ok=True)

    # Normalize the catalog before saving (sets up hrefs)
    catalog.normalize_hrefs(str(dest_dir))

    # Save as self-contained (relative links)
    catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)


def add_collection_to_catalog(
    catalog: pystac.Catalog,
    collection: pystac.Collection,
) -> None:
    """Add a collection as a child of a catalog.

    Args:
        catalog: The parent catalog.
        collection: The collection to add.
    """
    catalog.add_child(collection)


def add_item_to_collection(
    collection: pystac.Collection,
    item: pystac.Item,
    *,
    update_extent: bool = False,
) -> None:
    """Add an item to a collection.

    Args:
        collection: The parent collection.
        item: The item to add.
        update_extent: If True, update collection's spatial extent to
                      encompass the item's bbox.
    """
    collection.add_item(item)

    if update_extent:
        _update_collection_extent(collection, item)


def _update_collection_extent(
    collection: pystac.Collection,
    item: pystac.Item,
) -> None:
    """Update a collection's spatial extent to include an item's bbox.

    Args:
        collection: The collection to update.
        item: The item whose bbox should be included.
    """
    if item.bbox is None:
        return

    current_bbox = collection.extent.spatial.bboxes[0]
    new_bbox = [
        min(current_bbox[0], item.bbox[0]),  # min_x
        min(current_bbox[1], item.bbox[1]),  # min_y
        max(current_bbox[2], item.bbox[2]),  # max_x
        max(current_bbox[3], item.bbox[3]),  # max_y
    ]

    collection.extent.spatial = pystac.SpatialExtent(bboxes=[new_bbox])
