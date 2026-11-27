"""Built-in format families from the file name, with no third-party imports."""

from __future__ import annotations

from pathlib import Path

from portolan_core import extension_registry as _reg

VECTOR = "vector"
RASTER = "raster"

FORMAT_DISPLAY_NAMES: dict[str, str] = _reg.field_map("display_name")

# .gdb is a FileGDB directory, which detect_family handles on its own.
VECTOR_EXTENSIONS: frozenset[str] = _reg.extensions_where(routes_as="vector")

RASTER_EXTENSIONS: frozenset[str] = _reg.extensions_where(routes_as="raster")

_GEOJSON_TOKENS = tuple(
    f'"type":{space}"{kind}"'
    for kind in (
        "FeatureCollection",
        "Feature",
        "Point",
        "MultiPoint",
        "LineString",
        "MultiLineString",
        "Polygon",
        "MultiPolygon",
        "GeometryCollection",
    )
    for space in ("", " ")
)


def detect_family(path: Path) -> str | None:
    """Return the built-in family of a file: ``vector``, ``raster``, or None."""
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    extension = path.suffix.lower()

    # Special case: FileGDB directories (.gdb) are treated as vector format
    if path.is_dir():
        if extension == ".gdb":
            return VECTOR
        raise IsADirectoryError(f"Path is a directory: {path}")

    if extension in VECTOR_EXTENSIONS:
        return VECTOR
    if extension in RASTER_EXTENSIONS:
        return RASTER
    if extension == ".json" and is_geojson(path):
        return VECTOR
    return None


def family_of_asset(media_type: str | None, href: str) -> str | None:
    """Return the built-in family of a STAC asset from its media type."""
    media = (media_type or "").lower()
    suffix = Path(href).suffix.lower()
    if "parquet" in media or (not media and suffix == ".parquet"):
        return VECTOR
    if "tiff" in media or (not media and suffix in (".tif", ".tiff")):
        return RASTER
    return None


def is_geojson(path: Path) -> bool:
    """Check if a .json file is GeoJSON from its first 8 KB.

    A STAC object has "type": "Feature" too, but its "stac_version" marks it as not GeoJSON.

    Args:
        path: Path to JSON file.

    Returns:
        True if the prefix looks like GeoJSON.
    """
    try:
        # Read only first 8KB to avoid OOM on large files
        with Path(path).open(encoding="utf-8") as f:
            prefix = f.read(8192)
    except (OSError, UnicodeDecodeError):
        # OSError: permission denied, file not found, etc.
        # UnicodeDecodeError: binary file with .json extension
        return False
    # STAC metadata has stac_version - NOT GeoJSON
    if "stac_version" in prefix:
        return False
    return any(token in prefix for token in _GEOJSON_TOKENS)
