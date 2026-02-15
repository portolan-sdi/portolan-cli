"""Item creation and management.

Provides functions to create ItemModel from data files,
extract geometry and assets, and write item metadata to JSON.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from portolan_cli.metadata.geoparquet import extract_geoparquet_metadata
from portolan_cli.models.item import AssetModel, ItemModel


def create_item(
    item_id: str,
    data_path: Path,
    collection_id: str,
    *,
    title: str | None = None,
    description: str | None = None,
) -> ItemModel:
    """Create an ItemModel from a data file.

    Extracts geometry, bbox, and creates asset reference.

    Args:
        item_id: Unique item identifier.
        data_path: Path to data file (GeoParquet or COG).
        collection_id: Parent collection ID.
        title: Optional human-readable title.
        description: Optional item description.

    Returns:
        ItemModel with extracted metadata.

    Raises:
        FileNotFoundError: If data_path doesn't exist.
    """
    if not data_path.exists():
        raise FileNotFoundError(f"Data file not found: {data_path}")

    # Extract metadata based on file type
    bbox, geometry = _extract_geometry_from_file(data_path)

    # Create properties with datetime
    properties: dict[str, Any] = {
        "datetime": datetime.now(timezone.utc).isoformat(),
    }

    # Determine media type
    media_type = _get_media_type(data_path)

    # Create asset
    assets = {
        "data": AssetModel(
            href=str(data_path.name),
            type=media_type,
            roles=["data"],
            title="Data file",
        )
    }

    # Create item
    item = ItemModel(
        id=item_id,
        geometry=geometry,
        bbox=bbox,
        properties=properties,
        assets=assets,
        collection=collection_id,
        title=title,
        description=description,
    )

    return item


def _extract_geometry_from_file(
    path: Path,
) -> tuple[list[float], dict[str, Any]]:
    """Extract bbox and geometry from a data file.

    Args:
        path: Path to data file.

    Returns:
        Tuple of (bbox, geometry) where geometry is a GeoJSON polygon.
    """
    suffix = path.suffix.lower()
    bbox: list[float]

    if suffix in (".parquet", ".geoparquet"):
        gp_metadata = extract_geoparquet_metadata(path)
        if gp_metadata.bbox:
            bbox = list(gp_metadata.bbox)
        else:
            # Default to global extent
            bbox = [-180.0, -90.0, 180.0, 90.0]
    elif suffix in (".tif", ".tiff"):
        from portolan_cli.metadata.cog import extract_cog_metadata

        cog_metadata = extract_cog_metadata(path)
        bbox = list(cog_metadata.bbox)
    else:
        # Default to global extent
        bbox = [-180.0, -90.0, 180.0, 90.0]

    # Create GeoJSON polygon from bbox
    geometry = _bbox_to_polygon(bbox)

    return bbox, geometry


def _bbox_to_polygon(bbox: list[float]) -> dict[str, Any]:
    """Convert bbox to GeoJSON polygon.

    Args:
        bbox: Bounding box [west, south, east, north].

    Returns:
        GeoJSON Polygon geometry.
    """
    west, south, east, north = bbox[0], bbox[1], bbox[2], bbox[3]

    return {
        "type": "Polygon",
        "coordinates": [
            [
                [west, south],
                [east, south],
                [east, north],
                [west, north],
                [west, south],
            ]
        ],
    }


def _get_media_type(path: Path) -> str:
    """Get IANA media type for a file.

    Args:
        path: Path to file.

    Returns:
        Media type string.
    """
    suffix = path.suffix.lower()

    media_types = {
        ".parquet": "application/x-parquet",
        ".geoparquet": "application/x-parquet",
        ".tif": "image/tiff; application=geotiff",
        ".tiff": "image/tiff; application=geotiff",
        ".json": "application/json",
        ".geojson": "application/geo+json",
    }

    return media_types.get(suffix, "application/octet-stream")


def write_item_json(item: ItemModel, path: Path) -> Path:
    """Write item metadata to JSON file.

    Args:
        item: ItemModel to write.
        path: Directory to write to.

    Returns:
        Path to written file.
    """
    path.mkdir(parents=True, exist_ok=True)
    output_path = path / f"{item.id}.json"

    with open(output_path, "w") as f:
        json.dump(item.to_dict(), f, indent=2)

    return output_path


def read_item_json(path: Path) -> ItemModel:
    """Read item metadata from JSON file.

    Args:
        path: Path to item JSON file.

    Returns:
        ItemModel loaded from file.

    Raises:
        FileNotFoundError: If file doesn't exist.
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    with open(path) as f:
        data = json.load(f)

    return ItemModel.from_dict(data)
