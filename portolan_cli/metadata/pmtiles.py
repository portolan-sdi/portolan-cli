"""PMTiles metadata extraction."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class PMTilesMetadata:
    """Metadata extracted from a PMTiles file."""

    bbox: tuple[float, float, float, float] | None
    min_zoom: int
    max_zoom: int
    tile_type: str
    center: tuple[float, float, int] | None
    layer_name: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        result: dict[str, Any] = {
            "bbox": list(self.bbox) if self.bbox else None,
            "min_zoom": self.min_zoom,
            "max_zoom": self.max_zoom,
            "tile_type": self.tile_type,
            "center": list(self.center) if self.center else None,
        }
        if self.layer_name:
            result["layer_name"] = self.layer_name
        return result

    def to_stac_properties(self) -> dict[str, Any]:
        """Convert to STAC Item/Collection properties format."""
        props: dict[str, Any] = {
            "pmtiles:min_zoom": self.min_zoom,
            "pmtiles:max_zoom": self.max_zoom,
            "pmtiles:tile_type": self.tile_type,
        }

        if self.center:
            props["pmtiles:center"] = list(self.center)

        if self.layer_name:
            props["pmtiles:layers"] = [self.layer_name]

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
        ImportError: If pmtiles package is not installed.
    """
    # Lazy import - pmtiles is an optional dependency
    try:
        from pmtiles.reader import MmapSource, Reader
        from pmtiles.tile import MagicNumberNotFound
    except ImportError as e:
        raise ImportError("pmtiles package not installed. Install with: pip install pmtiles") from e

    if not path.exists():
        raise FileNotFoundError(f"PMTiles file not found: {path}")

    try:
        with Path(path).open("rb") as f:
            reader = Reader(MmapSource(f))  # type: ignore[no-untyped-call]
            header = reader.header()  # type: ignore[no-untyped-call]
            metadata = reader.metadata()  # type: ignore[no-untyped-call]
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

    # Extract center if present (explicit None check to preserve valid zero coords)
    center = None
    if header.get("center_lon_e7") is not None and header.get("center_lat_e7") is not None:
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

    # Multi-layer PMTiles use the first layer and warn.
    layer_name: str | None = None
    if isinstance(metadata, dict):
        # TileJSON format: {"vector_layers": [{"id": "layer_name", ...}]}
        vector_layers = metadata.get("vector_layers", [])
        if isinstance(vector_layers, list) and vector_layers:
            layer_ids: list[str] = []
            for layer in vector_layers:
                if isinstance(layer, dict):
                    layer_id = layer.get("id")
                    if isinstance(layer_id, str):
                        layer_ids.append(layer_id)
            if layer_ids:
                layer_name = layer_ids[0]
                if len(layer_ids) > 1:
                    logger.warning(
                        "PMTiles has %d layers %s; using first layer '%s' for style",
                        len(layer_ids),
                        layer_ids,
                        layer_name,
                    )

    return PMTilesMetadata(
        bbox=bbox,
        min_zoom=header["min_zoom"],
        max_zoom=header["max_zoom"],
        tile_type=tile_type,
        center=center,
        layer_name=layer_name,
    )
