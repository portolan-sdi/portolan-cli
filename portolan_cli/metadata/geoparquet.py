"""GeoParquet metadata extraction.

Uses pyarrow to read GeoParquet metadata without loading full data.
Extracts bbox, CRS, schema, and geometry type from file metadata.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq


@dataclass
class GeoParquetMetadata:
    """Metadata extracted from a GeoParquet file.

    Attributes:
        bbox: Bounding box as (minx, miny, maxx, maxy) or None.
        crs: CRS as EPSG code, WKT, or PROJJSON dict.
        geometry_type: Geometry type (Point, Polygon, etc.) or None.
        geometry_column: Name of the geometry column.
        feature_count: Number of features (rows).
        schema: Column names and types.
    """

    bbox: tuple[float, float, float, float] | None
    crs: str | dict[str, Any] | None
    geometry_type: str | None
    geometry_column: str | None
    feature_count: int
    schema: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        return {
            "bbox": list(self.bbox) if self.bbox else None,
            "crs": self.crs,
            "geometry_type": self.geometry_type,
            "geometry_column": self.geometry_column,
            "feature_count": self.feature_count,
            "schema": self.schema,
        }

    def to_stac_properties(self) -> dict[str, Any]:
        """Convert to STAC Item properties format."""
        props: dict[str, Any] = {}

        if self.geometry_type:
            props["geoparquet:geometry_type"] = self.geometry_type

        if self.feature_count:
            props["geoparquet:feature_count"] = self.feature_count

        return props


def extract_geoparquet_metadata(path: Path) -> GeoParquetMetadata:
    """Extract metadata from a GeoParquet file.

    Uses pyarrow to read file metadata without loading all data.
    Parses GeoParquet geo metadata from file's custom metadata.

    Args:
        path: Path to GeoParquet file.

    Returns:
        GeoParquetMetadata with extracted information.

    Raises:
        FileNotFoundError: If file doesn't exist.
        ValueError: If file is not valid GeoParquet.
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    # Open parquet file (metadata only)
    pf = pq.ParquetFile(path)
    metadata = pf.schema_arrow.metadata or {}

    # Parse GeoParquet geo metadata
    geo_metadata = _parse_geo_metadata(metadata)

    # Extract schema
    schema = {field.name: str(field.type) for field in pf.schema_arrow}

    # Get feature count from row groups
    feature_count = pf.metadata.num_rows

    # Extract geometry column info
    geometry_column = geo_metadata.get("primary_column", "geometry")
    column_meta = geo_metadata.get("columns", {}).get(geometry_column, {})

    # Extract bbox
    bbox = _extract_bbox(column_meta)

    # Extract CRS
    crs = _extract_crs(column_meta)

    # Extract geometry type
    geometry_type = _extract_geometry_type(column_meta)

    return GeoParquetMetadata(
        bbox=bbox,
        crs=crs,
        geometry_type=geometry_type,
        geometry_column=geometry_column,
        feature_count=feature_count,
        schema=schema,
    )


def _parse_geo_metadata(metadata: dict[bytes, bytes]) -> dict[str, Any]:
    """Parse GeoParquet geo metadata from Arrow schema metadata."""
    geo_key = b"geo"
    if geo_key not in metadata:
        # Not a GeoParquet file, return empty
        return {}

    try:
        result = json.loads(metadata[geo_key].decode("utf-8"))
        return dict(result) if isinstance(result, dict) else {}
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {}


def _extract_bbox(column_meta: dict[str, Any]) -> tuple[float, float, float, float] | None:
    """Extract bbox from column metadata."""
    bbox = column_meta.get("bbox")
    if bbox and len(bbox) >= 4:
        return (float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3]))
    return None


def _extract_crs(column_meta: dict[str, Any]) -> str | dict[str, Any] | None:
    """Extract CRS from column metadata."""
    crs = column_meta.get("crs")
    if crs is None:
        return None

    # CRS can be PROJJSON dict or WKT string
    if isinstance(crs, dict):
        # Try to extract EPSG code from PROJJSON
        epsg = crs.get("id", {}).get("code")
        if epsg:
            return f"EPSG:{epsg}"
        return crs
    return str(crs)


def _extract_geometry_type(column_meta: dict[str, Any]) -> str | None:
    """Extract geometry type from column metadata."""
    geom_types = column_meta.get("geometry_types", [])
    if geom_types:
        # Return the first/primary type
        return geom_types[0] if isinstance(geom_types, list) else str(geom_types)

    # Fallback to geometry_type field
    return column_meta.get("geometry_type")
