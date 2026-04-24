"""PMTiles metadata extraction.

Uses the pmtiles package to read PMTiles header metadata.
PMTiles store bounds in WGS84 (4326) but tiles are Web Mercator (3857).

Per ADR-0031, PMTiles are collection-level assets when added to a catalog.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pmtiles.reader import MmapSource, Reader
from pmtiles.tile import MagicNumberNotFound


@dataclass
class PMTilesMetadata:
    """Metadata extracted from a PMTiles file.

    Attributes:
        bbox: Bounding box as (min_lon, min_lat, max_lon, max_lat) in WGS84.
        min_zoom: Minimum zoom level.
        max_zoom: Maximum zoom level.
        tile_type: Tile type ("mvt", "png", "jpeg", "webp", "avif").
        center: Optional center point as (lon, lat, zoom).
    """

    bbox: tuple[float, float, float, float] | None
    min_zoom: int
    max_zoom: int
    tile_type: str
    center: tuple[float, float, int] | None

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        return {
            "bbox": list(self.bbox) if self.bbox else None,
            "min_zoom": self.min_zoom,
            "max_zoom": self.max_zoom,
            "tile_type": self.tile_type,
            "center": list(self.center) if self.center else None,
        }

    def to_stac_properties(self) -> dict[str, Any]:
        """Convert to STAC Item/Collection properties format.

        PMTiles are always in Web Mercator (3857) internally, even though
        bounds are stored in WGS84 (4326) for discoverability.
        """
        props: dict[str, Any] = {
            "proj:epsg": 3857,  # PMTiles tiles are always Web Mercator
            "pmtiles:min_zoom": self.min_zoom,
            "pmtiles:max_zoom": self.max_zoom,
            "pmtiles:tile_type": self.tile_type,
        }

        if self.center:
            props["pmtiles:center"] = list(self.center)

        return props


# Map pmtiles TileType enum to string
_TILE_TYPE_MAP = {
    0: "unknown",
    1: "mvt",
    2: "png",
    3: "jpeg",
    4: "webp",
    5: "avif",
}


def extract_pmtiles_metadata(path: Path) -> PMTilesMetadata:
    """Extract metadata from a PMTiles file.

    Uses the pmtiles package to read header metadata without loading tiles.

    Args:
        path: Path to PMTiles file.

    Returns:
        PMTilesMetadata with extracted information.

    Raises:
        FileNotFoundError: If file doesn't exist.
        ValueError: If file is not a valid PMTiles file.
    """
    if not path.exists():
        raise FileNotFoundError(f"PMTiles file not found: {path}")

    try:
        with open(path, "rb") as f:
            reader = Reader(MmapSource(f))  # type: ignore[no-untyped-call]
            header = reader.header()  # type: ignore[no-untyped-call]
    except MagicNumberNotFound as e:
        raise ValueError(f"Invalid PMTiles file: {path}") from e
    except Exception as e:
        raise ValueError(f"Invalid PMTiles file: {path} - {e}") from e

    # Convert E7 (fixed-point) to degrees
    bbox = (
        header["min_lon_e7"] / 1e7,
        header["min_lat_e7"] / 1e7,
        header["max_lon_e7"] / 1e7,
        header["max_lat_e7"] / 1e7,
    )

    # Extract center if present
    center = None
    if header.get("center_lon_e7") and header.get("center_lat_e7"):
        center = (
            header["center_lon_e7"] / 1e7,
            header["center_lat_e7"] / 1e7,
            header.get("center_zoom", 0),
        )

    # Get tile type as string
    tile_type_value = header.get("tile_type")
    if hasattr(tile_type_value, "value"):
        tile_type = _TILE_TYPE_MAP.get(tile_type_value.value, "unknown")
    else:
        tile_type = _TILE_TYPE_MAP.get(tile_type_value, "unknown")

    return PMTilesMetadata(
        bbox=bbox,
        min_zoom=header["min_zoom"],
        max_zoom=header["max_zoom"],
        tile_type=tile_type,
        center=center,
    )
