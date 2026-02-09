"""Format detection for routing to appropriate conversion library.

This module provides minimal format detection to route inputs to either
geoparquet-io (vector) or rio-cogeo (raster). Per ADR-0010, actual
validation and conversion are delegated to these upstream libraries.

Additionally, it provides cloud-native status classification (see issue #10):
- CLOUD_NATIVE: Already cloud-optimized (GeoParquet, COG, FlatGeobuf, etc.)
- CONVERTIBLE: Can be converted (Shapefile, GeoJSON, GeoPackage, etc.)
- UNSUPPORTED: Not yet supported (NetCDF, HDF5, non-COPC LAS/LAZ)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path

# =============================================================================
# Cloud-Native Status Classification (Issue #10)
# =============================================================================


class CloudNativeStatus(Enum):
    """Classification of file format for cloud-native data handling.

    Used to determine how to handle a file during add_dataset():
    - CLOUD_NATIVE: Accept silently, no conversion needed
    - CONVERTIBLE: Warn then convert to cloud-native format
    - UNSUPPORTED: Reject with helpful error message
    """

    CLOUD_NATIVE = "cloud_native"
    CONVERTIBLE = "convertible"
    UNSUPPORTED = "unsupported"


@dataclass(frozen=True)
class FormatInfo:
    """Information about a detected file format.

    Provides metadata needed for user-facing messages during dataset add.

    Attributes:
        status: Cloud-native classification status.
        display_name: Human-readable format name (e.g., "SHP", "GeoJSON").
        target_format: Target cloud-native format if convertible (e.g., "GeoParquet").
        error_message: Error message if unsupported.
    """

    status: CloudNativeStatus
    display_name: str
    target_format: str | None
    error_message: str | None


class UnsupportedFormatError(ValueError):
    """Raised when attempting to add an unsupported file format.

    This error is raised for formats that cannot be converted to cloud-native
    formats (e.g., NetCDF, HDF5, non-COPC LAS/LAZ).
    """

    pass


# =============================================================================
# Cloud-Native Format Constants
# =============================================================================

# Extensions for cloud-native formats (pass through without conversion)
CLOUD_NATIVE_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".fgb",  # FlatGeobuf
        ".pmtiles",  # PMTiles
        ".raquet",  # Raquet (raster parquet)
        # Note: .parquet, .tif, .tiff require content inspection
        # Note: .copc.laz is handled specially (compound extension)
        # Note: .zarr is a directory, not a file extension
    }
)

# Extensions for convertible vector formats
CONVERTIBLE_VECTOR_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".shp",  # Shapefile
        ".geojson",  # GeoJSON
        ".gpkg",  # GeoPackage
        ".csv",  # CSV with geometry
    }
)

# Extensions for convertible raster formats
CONVERTIBLE_RASTER_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".jp2",  # JPEG2000
    }
)

# Extensions for unsupported formats
UNSUPPORTED_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".nc",  # NetCDF
        ".netcdf",  # NetCDF alternate
        ".h5",  # HDF5
        ".hdf5",  # HDF5 alternate
        ".las",  # LAS (non-COPC)
        ".laz",  # LAZ (non-COPC, unless .copc.laz)
    }
)

# Display names for formats
FORMAT_DISPLAY_NAMES: dict[str, str] = {
    # Cloud-native
    ".parquet": "GeoParquet",
    ".fgb": "FlatGeobuf",
    ".pmtiles": "PMTiles",
    ".raquet": "Raquet",
    ".tif": "COG",
    ".tiff": "COG",
    # Convertible vector
    ".shp": "SHP",
    ".geojson": "GeoJSON",
    ".gpkg": "GPKG",
    ".csv": "CSV",
    ".json": "GeoJSON",
    # Convertible raster
    ".jp2": "JP2",
    # Unsupported
    ".nc": "NetCDF",
    ".netcdf": "NetCDF",
    ".h5": "HDF5",
    ".hdf5": "HDF5",
    ".las": "LAS",
    ".laz": "LAZ",
}

# Error messages for unsupported formats
UNSUPPORTED_ERROR_MESSAGES: dict[str, str] = {
    ".nc": "NetCDF is not yet supported. Support coming soon.",
    ".netcdf": "NetCDF is not yet supported. Support coming soon.",
    ".h5": "HDF5 is not yet supported. Support coming soon.",
    ".hdf5": "HDF5 is not yet supported. Support coming soon.",
    ".las": "LAS/LAZ point clouds require COPC format. Use pdal or other tools to convert.",
    ".laz": "LAS/LAZ point clouds require COPC format. Use pdal or other tools to convert.",
}


# =============================================================================
# Cloud-Native Detection Functions
# =============================================================================


def is_geoparquet(path: Path) -> bool:
    """Check if a Parquet file has GeoParquet metadata.

    GeoParquet files have a 'geo' key in their schema metadata that contains
    geospatial column information.

    Args:
        path: Path to the Parquet file.

    Returns:
        True if the file is a valid GeoParquet, False otherwise.
    """
    try:
        import pyarrow.parquet as pq

        metadata = pq.read_metadata(str(path))
        schema_metadata = metadata.schema.to_arrow_schema().metadata or {}
        # GeoParquet files have 'geo' key in schema metadata
        return b"geo" in schema_metadata
    except Exception:
        return False


def is_cloud_optimized_geotiff(path: Path) -> bool:
    """Check if a TIFF file is a Cloud-Optimized GeoTIFF.

    Uses rio-cogeo's validation to determine if the file meets COG requirements.
    Per ADR-0010, this delegates to upstream library rather than reimplementing.

    Args:
        path: Path to the TIFF file.

    Returns:
        True if the file is a valid COG, False otherwise.
    """
    try:
        from rio_cogeo.cogeo import cog_validate

        is_valid, _errors, _warnings = cog_validate(str(path))
        return is_valid
    except Exception:
        return False


def get_cloud_native_status(path: Path) -> FormatInfo:
    """Determine cloud-native status and format info for a file.

    Classifies a file as CLOUD_NATIVE, CONVERTIBLE, or UNSUPPORTED based on
    its format. Uses content inspection for ambiguous cases (e.g., .tif files
    that may or may not be COGs, .parquet files that may or may not have geo metadata).

    Args:
        path: Path to the file to classify.

    Returns:
        FormatInfo with status, display name, target format, and error message.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    # Handle Zarr directories (special case: directory with .zarr extension)
    if path.is_dir():
        if path.suffix.lower() == ".zarr":
            return FormatInfo(
                status=CloudNativeStatus.CLOUD_NATIVE,
                display_name="Zarr",
                target_format=None,
                error_message=None,
            )
        raise IsADirectoryError(f"Path is a directory: {path}")

    # Get the file extension (lowercase)
    extension = path.suffix.lower()

    # Handle compound extensions like .copc.laz
    if path.name.lower().endswith(".copc.laz"):
        return FormatInfo(
            status=CloudNativeStatus.CLOUD_NATIVE,
            display_name="COPC",
            target_format=None,
            error_message=None,
        )

    # Check for cloud-native formats (extension-based)
    if extension in CLOUD_NATIVE_EXTENSIONS:
        display_name = FORMAT_DISPLAY_NAMES.get(extension, extension.upper().lstrip("."))
        return FormatInfo(
            status=CloudNativeStatus.CLOUD_NATIVE,
            display_name=display_name,
            target_format=None,
            error_message=None,
        )

    # Check Parquet files - need content inspection for geo metadata
    if extension == ".parquet":
        if is_geoparquet(path):
            return FormatInfo(
                status=CloudNativeStatus.CLOUD_NATIVE,
                display_name="GeoParquet",
                target_format=None,
                error_message=None,
            )
        # Plain Parquet is also cloud-native (just not geo-aware)
        return FormatInfo(
            status=CloudNativeStatus.CLOUD_NATIVE,
            display_name="Parquet",
            target_format=None,
            error_message=None,
        )

    # Check TIFF files - need content inspection for COG validation
    if extension in (".tif", ".tiff"):
        if is_cloud_optimized_geotiff(path):
            return FormatInfo(
                status=CloudNativeStatus.CLOUD_NATIVE,
                display_name="COG",
                target_format=None,
                error_message=None,
            )
        # Non-COG TIFF is convertible
        return FormatInfo(
            status=CloudNativeStatus.CONVERTIBLE,
            display_name="TIFF",
            target_format="COG",
            error_message=None,
        )

    # Check for unsupported formats (before convertible to catch .laz)
    if extension in UNSUPPORTED_EXTENSIONS:
        display_name = FORMAT_DISPLAY_NAMES.get(extension, extension.upper().lstrip("."))
        error_message = UNSUPPORTED_ERROR_MESSAGES.get(
            extension,
            f"{display_name} is not yet supported. Support coming soon.",
        )
        return FormatInfo(
            status=CloudNativeStatus.UNSUPPORTED,
            display_name=display_name,
            target_format=None,
            error_message=error_message,
        )

    # Check for convertible vector formats
    if extension in CONVERTIBLE_VECTOR_EXTENSIONS:
        display_name = FORMAT_DISPLAY_NAMES.get(extension, extension.upper().lstrip("."))
        return FormatInfo(
            status=CloudNativeStatus.CONVERTIBLE,
            display_name=display_name,
            target_format="GeoParquet",
            error_message=None,
        )

    # Check for convertible raster formats
    if extension in CONVERTIBLE_RASTER_EXTENSIONS:
        display_name = FORMAT_DISPLAY_NAMES.get(extension, extension.upper().lstrip("."))
        return FormatInfo(
            status=CloudNativeStatus.CONVERTIBLE,
            display_name=display_name,
            target_format="COG",
            error_message=None,
        )

    # Check for .json files (might be GeoJSON)
    if extension == ".json":
        if _detect_json_type(path) == FormatType.VECTOR:
            return FormatInfo(
                status=CloudNativeStatus.CONVERTIBLE,
                display_name="GeoJSON",
                target_format="GeoParquet",
                error_message=None,
            )

    # Unknown format - treat as unsupported
    if extension:
        error_msg = f"Unknown format '{extension}' is not supported."
    else:
        error_msg = "File has no extension and format could not be detected."
    return FormatInfo(
        status=CloudNativeStatus.UNSUPPORTED,
        display_name=extension.upper().lstrip(".") if extension else "Unknown",
        target_format=None,
        error_message=error_msg,
    )


# =============================================================================
# Format Type Detection (existing functionality)
# =============================================================================


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
