"""Extension-based format detection with no third-party imports.

This leaf holds the part of format detection that reads only a file name or
the first 8 KB of a JSON file. The catalog read modules (``query``,
``catalog_list``) need it, and they must not load pyarrow, rasterio, or
rio-cogeo (issue #944). ``formats.py`` re-exports every name here and adds the
content inspection that does load those packages.
"""

from __future__ import annotations

from enum import Enum
from pathlib import Path

from portolan_cli import extension_registry as _reg

FORMAT_DISPLAY_NAMES: dict[str, str] = _reg.field_map("display_name")


class FormatType(Enum):
    """Detected format type for routing to conversion library."""

    VECTOR = "vector"  # Route to geoparquet-io
    RASTER = "raster"  # Route to rio-cogeo
    UNKNOWN = "unknown"  # Cannot determine format


# Extensions that route to geoparquet-io (vector) / rio-cogeo (raster). Derived
# from the registry. Cloud-native vectors like .fgb/.pmtiles are
# included in VECTOR_EXTENSIONS so detect_format() returns VECTOR, letting
# convert_vector() then check cloud-native status and skip. .gdb is a FileGDB
# directory handled specially in detect_format().
VECTOR_EXTENSIONS: frozenset[str] = _reg.extensions_where(routes_as="vector")

RASTER_EXTENSIONS: frozenset[str] = _reg.extensions_where(routes_as="raster")


def detect_format(path: Path) -> FormatType:
    """Detect whether a file is vector, raster, or unknown.

    This provides minimal detection to route files to the correct
    conversion library. It does NOT validate file contents—that is
    delegated to geoparquet-io or rio-cogeo.

    Args:
        path: Path to the file or directory to detect.

    Returns:
        FormatType indicating vector, raster, or unknown.

    Raises:
        FileNotFoundError: If the file does not exist.
        IsADirectoryError: If the path is a non-geospatial directory.
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    extension = path.suffix.lower()

    # Special case: FileGDB directories (.gdb) are treated as vector format
    if path.is_dir():
        if extension == ".gdb":
            return FormatType.VECTOR
        raise IsADirectoryError(f"Path is a directory: {path}")

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

    STAC items/collections/catalogs are NOT GeoJSON even though they have
    "type": "Feature". They are identified by "stac_version".

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
        with Path(path).open(encoding="utf-8") as f:
            prefix = f.read(8192)
            # STAC metadata has stac_version - NOT GeoJSON
            if "stac_version" in prefix:
                return FormatType.UNKNOWN
            if any(token in prefix for token in geojson_tokens):
                return FormatType.VECTOR
    except (OSError, UnicodeDecodeError):
        # OSError: permission denied, file not found, etc.
        # UnicodeDecodeError: binary file with .json extension
        pass
    return FormatType.UNKNOWN
