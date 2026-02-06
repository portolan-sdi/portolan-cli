"""Format detection for routing to appropriate conversion library.

This module provides minimal format detection to route inputs to either
geoparquet-io (vector) or rio-cogeo (raster). Per ADR-0010, actual
validation and conversion are delegated to these upstream libraries.
"""

from __future__ import annotations

from enum import Enum
from pathlib import Path


class FormatType(Enum):
    """Detected format type for routing to conversion library."""

    VECTOR = "vector"  # Route to geoparquet-io
    RASTER = "raster"  # Route to rio-cogeo
    UNKNOWN = "unknown"  # Cannot determine format


# Extensions that indicate vector formats (handled by geoparquet-io)
VECTOR_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".geojson",
        ".parquet",
        ".shp",
        ".gpkg",
        ".fgb",  # FlatGeobuf
        ".csv",
    }
)

# Extensions that indicate raster formats (handled by rio-cogeo)
RASTER_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".tif",
        ".tiff",
        ".jp2",  # JPEG2000
    }
)


def detect_format(path: Path) -> FormatType:
    """Detect whether a file is vector, raster, or unknown.

    This provides minimal detection to route files to the correct
    conversion library. It does NOT validate file contents—that is
    delegated to geoparquet-io or rio-cogeo per ADR-0010.

    Args:
        path: Path to the file to detect.

    Returns:
        FormatType indicating vector, raster, or unknown.

    Raises:
        FileNotFoundError: If the file does not exist.
        IsADirectoryError: If the path is a directory.
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    if path.is_dir():
        raise IsADirectoryError(f"Path is a directory: {path}")

    extension = path.suffix.lower()

    # Check extension-based detection first
    if extension in VECTOR_EXTENSIONS:
        return FormatType.VECTOR
    if extension in RASTER_EXTENSIONS:
        return FormatType.RASTER

    # Special case: .json files might be GeoJSON
    if extension == ".json":
        return _detect_json_type(path)

    return FormatType.UNKNOWN


def _detect_json_type(path: Path) -> FormatType:
    """Check if a .json file is GeoJSON.

    Uses prefix reading (first 8KB) to avoid OOM on large files.
    Searches for GeoJSON type tokens without full JSON parsing.

    Args:
        path: Path to JSON file.

    Returns:
        VECTOR if GeoJSON, UNKNOWN otherwise.
    """
    # GeoJSON type tokens to search for in file prefix
    geojson_tokens = (
        '"type":"FeatureCollection"',
        '"type": "FeatureCollection"',
        '"type":"Feature"',
        '"type": "Feature"',
        '"type":"Point"',
        '"type": "Point"',
        '"type":"MultiPoint"',
        '"type": "MultiPoint"',
        '"type":"LineString"',
        '"type": "LineString"',
        '"type":"MultiLineString"',
        '"type": "MultiLineString"',
        '"type":"Polygon"',
        '"type": "Polygon"',
        '"type":"MultiPolygon"',
        '"type": "MultiPolygon"',
        '"type":"GeometryCollection"',
        '"type": "GeometryCollection"',
    )
    try:
        # Read only first 8KB to avoid OOM on large files
        with open(path, encoding="utf-8") as f:
            prefix = f.read(8192)
            if any(token in prefix for token in geojson_tokens):
                return FormatType.VECTOR
    except OSError:
        pass
    return FormatType.UNKNOWN
