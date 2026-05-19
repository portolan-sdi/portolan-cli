"""FlatGeobuf metadata extraction.

Uses pyogrio to read FlatGeobuf header metadata without loading all data.
Extracts bbox, CRS, schema, geometry type, and feature count.

Per ADR-0031, FlatGeobuf files are collection-level assets when added to a catalog.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyogrio  # type: ignore[import-untyped]
from pyproj import CRS
from pyproj.exceptions import CRSError


@dataclass
class FlatGeobufMetadata:
    """Metadata extracted from a FlatGeobuf file.

    Attributes:
        bbox: Bounding box as (min_x, min_y, max_x, max_y).
        crs: CRS as string (e.g., "EPSG:4326").
        geometry_type: Geometry type (Point, Polygon, etc.).
        feature_count: Number of features.
        schema: Field names and types.
    """

    bbox: tuple[float, float, float, float] | None
    crs: str | None
    geometry_type: str | None
    feature_count: int
    schema: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        return {
            "bbox": list(self.bbox) if self.bbox else None,
            "crs": self.crs,
            "geometry_type": self.geometry_type,
            "feature_count": self.feature_count,
            "schema": self.schema,
        }

    def to_stac_properties(self) -> dict[str, Any]:
        """Convert to STAC Item/Collection properties format."""
        props: dict[str, Any] = {}

        # Parse CRS using pyproj (handles EPSG:, WKT, OGC URN, etc.)
        if self.crs:
            try:
                crs = CRS.from_user_input(self.crs)
                epsg = crs.to_epsg()
                if epsg is not None:
                    props["proj:epsg"] = epsg
            except CRSError:
                pass  # Unknown CRS format, skip proj:epsg

        if self.geometry_type:
            props["flatgeobuf:geometry_type"] = self.geometry_type

        if self.feature_count is not None:
            props["flatgeobuf:feature_count"] = self.feature_count

        return props


def extract_flatgeobuf_metadata(path: Path) -> FlatGeobufMetadata:
    """Extract metadata from a FlatGeobuf file.

    Uses pyogrio to read header metadata without loading all data.

    Args:
        path: Path to FlatGeobuf file.

    Returns:
        FlatGeobufMetadata with extracted information.

    Raises:
        FileNotFoundError: If file doesn't exist.
        ValueError: If file is not a valid FlatGeobuf file.
    """
    if not path.exists():
        raise FileNotFoundError(f"FlatGeobuf file not found: {path}")

    try:
        info = pyogrio.read_info(str(path))
    except Exception as e:
        raise ValueError(f"Invalid FlatGeobuf file: {path} - {e}") from e

    # Extract bounds
    bounds = info.get("total_bounds")
    bbox = tuple(bounds) if bounds is not None else None

    # Build schema from fields and dtypes
    fields = info.get("fields", [])
    dtypes = info.get("dtypes", [])
    schema = dict(zip(fields, [str(dt) for dt in dtypes], strict=False))

    return FlatGeobufMetadata(
        bbox=bbox,
        crs=info.get("crs"),
        geometry_type=info.get("geometry_type"),
        feature_count=info.get("features", 0),
        schema=schema,
    )
