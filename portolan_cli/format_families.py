"""Built-in format families from the file name, with no third-party imports.

A format family groups formats: ``vector`` routes to geoparquet-io and
``raster`` to rio-cogeo. A plugin format handler brings its own family string
(#947), so a family is free text, not an enum.

This leaf reads only a file name or the first 8 KB of a JSON file. The catalog
read modules (``query``, ``catalog_list``) need it, and they must not load
pyarrow, rasterio, rio-cogeo, or a plugin (issue #944). It knows the built-in
formats only. ``formats.detect_format`` also asks the plugin format handlers.
"""

from __future__ import annotations

from pathlib import Path

from portolan_cli import extension_registry as _reg

VECTOR = "vector"
RASTER = "raster"

FORMAT_DISPLAY_NAMES: dict[str, str] = _reg.field_map("display_name")

# Extensions that route to geoparquet-io (vector) / rio-cogeo (raster). Derived
# from the registry. Cloud-native vectors like .fgb/.pmtiles are included in
# VECTOR_EXTENSIONS. .gdb is a FileGDB directory handled specially below.
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
    """Return the built-in family of a file: ``vector``, ``raster``, or None.

    This provides minimal detection to route files to the correct conversion
    library. It does NOT validate file contents. That is delegated to
    geoparquet-io or rio-cogeo.

    Args:
        path: Path to the file or directory to detect.

    Returns:
        ``vector``, ``raster``, or None when no built-in format matches.

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
    """Return the built-in family of a STAC asset from its media type.

    The media type comes first. A plugin format declares its own media type,
    so the read layer does not need to load the plugin. The href suffix is
    the fallback for an asset with no ``type``.

    Args:
        media_type: The asset ``type``, or None.
        href: The asset ``href``.

    Returns:
        ``vector``, ``raster``, or None.
    """
    media = (media_type or "").lower()
    suffix = Path(href).suffix.lower()
    if "parquet" in media or (not media and suffix == ".parquet"):
        return VECTOR
    if "tiff" in media or (not media and suffix in (".tif", ".tiff")):
        return RASTER
    return None


def is_geojson(path: Path) -> bool:
    """Check if a .json file is GeoJSON.

    Uses prefix reading (first 8KB) to avoid OOM on large files.
    Searches for GeoJSON type tokens without full JSON parsing.

    STAC items/collections/catalogs are NOT GeoJSON even though they have
    "type": "Feature". They are identified by "stac_version".

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
