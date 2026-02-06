"""Format detection for routing to appropriate conversion library.

This module provides minimal format detection to route inputs to either
geoparquet-io (vector) or rio-cogeo (raster). Per ADR-0010, actual
validation and conversion are delegated to these upstream libraries.
"""

from __future__ import annotations

import json
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

    Args:
        path: Path to JSON file.

    Returns:
        VECTOR if GeoJSON, UNKNOWN otherwise.
    """
    try:
        with open(path) as f:
            # Read just enough to check the type field
            data = json.load(f)
            if isinstance(data, dict) and data.get("type") in (
                "FeatureCollection",
                "Feature",
                "Point",
                "MultiPoint",
                "LineString",
                "MultiLineString",
                "Polygon",
                "MultiPolygon",
                "GeometryCollection",
            ):
                return FormatType.VECTOR
    except (json.JSONDecodeError, OSError):
        pass
    return FormatType.UNKNOWN
