"""Item creation and management."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from portolan_cli.crs import transform_bbox_to_wgs84
from portolan_cli.metadata.geoparquet import extract_geoparquet_metadata
from portolan_core import extension_registry as _reg
from portolan_core.json_io import write_json_atomic
from portolan_core.models.item import AssetModel, ItemModel

# Sourced from the registry so this cannot drift from what `add` writes. A COG
# missing "profile=cloud-optimized" is invisible to rashid's COG detection gate.
_MEDIA_TYPE_MAP: dict[str, str] = _reg.field_map("media_type")

_MEDIA_TYPE_MAP.setdefault(".geoparquet", _MEDIA_TYPE_MAP[".parquet"])


def create_item(
    item_id: str,
    data_path: Path,
    collection_id: str,
    *,
    title: str | None = None,
    description: str | None = None,
) -> ItemModel:
    """Create an ItemModel from a data file."""
    if not data_path.exists():
        raise FileNotFoundError(f"Data file not found: {data_path}")

    # Extract metadata based on file type
    bbox, geometry = _extract_geometry_from_file(data_path)

    # Create properties with datetime
    properties: dict[str, Any] = {
        "datetime": datetime.now(UTC).isoformat(),
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
    return ItemModel(
        id=item_id,
        geometry=geometry,
        bbox=bbox,
        properties=properties,
        assets=assets,
        collection=collection_id,
        title=title,
        description=description,
    )


def _extract_geometry_from_file(
    path: Path,
) -> tuple[list[float], dict[str, Any]]:
    """Extract bbox and geometry from a data file."""
    suffix = path.suffix.lower()
    bbox: list[float]

    if suffix in (".parquet", ".geoparquet"):
        gp_metadata = extract_geoparquet_metadata(path)
        # Default to global extent
        bbox = list(gp_metadata.bbox) if gp_metadata.bbox else [-180.0, -90.0, 180.0, 90.0]
    elif suffix in (".tif", ".tiff"):
        from portolan_cli.metadata.cog import extract_cog_metadata

        cog_metadata = extract_cog_metadata(path)
        bbox = list(transform_bbox_to_wgs84(cog_metadata.bbox, cog_metadata.crs))
    else:
        # Default to global extent
        bbox = [-180.0, -90.0, 180.0, 90.0]

    # Create GeoJSON polygon from bbox
    geometry = _bbox_to_polygon(bbox)

    return bbox, geometry


def _bbox_to_polygon(bbox: list[float]) -> dict[str, Any]:
    """Convert bbox to GeoJSON polygon."""
    from portolan_core.bbox import to_2d_bbox

    west, south, east, north = to_2d_bbox(bbox)

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
    """Get IANA media type for a file."""
    return _MEDIA_TYPE_MAP.get(path.suffix.lower(), "application/octet-stream")


def write_item_json(item: ItemModel, path: Path) -> Path:
    """Write item metadata to JSON file."""
    path.mkdir(parents=True, exist_ok=True)
    output_path = path / f"{item.id}.json"

    write_json_atomic(output_path, item.to_dict())

    return output_path


def read_item_json(path: Path) -> ItemModel:
    """Read item metadata from JSON file."""
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    with Path(path).open(encoding="utf-8") as f:
        data = json.load(f)

    return ItemModel.from_dict(data)
