# Data Model: Cloud-Native Dataset Warnings

**Feature**: 002-cloud-native-warnings
**Date**: 2025-02-09

## Entities

### CloudNativeStatus (Enum)

Represents the cloud-native classification of a file format.

```python
class CloudNativeStatus(Enum):
    """Classification of file format for cloud-native data handling."""

    CLOUD_NATIVE = "cloud_native"  # Accept silently, no conversion needed
    CONVERTIBLE = "convertible"     # Warn then convert to cloud-native format
    UNSUPPORTED = "unsupported"     # Reject with helpful error message
```

**Behaviors**:
- CLOUD_NATIVE: File passes through unchanged, no terminal output
- CONVERTIBLE: Single-line warning emitted before conversion begins
- UNSUPPORTED: Error emitted, processing stops immediately

### FormatInfo (DataClass)

Metadata about a detected format for generating user messages.

```python
@dataclass(frozen=True)
class FormatInfo:
    """Information about a detected file format."""

    status: CloudNativeStatus       # Classification status
    display_name: str               # Human-readable format name (e.g., "SHP", "GeoJSON")
    target_format: str | None       # Target cloud-native format if convertible (e.g., "GeoParquet")
    error_message: str | None       # Error message if unsupported
```

**Examples**:
```python
# Cloud-native (GeoParquet)
FormatInfo(
    status=CloudNativeStatus.CLOUD_NATIVE,
    display_name="GeoParquet",
    target_format=None,
    error_message=None
)

# Convertible (Shapefile)
FormatInfo(
    status=CloudNativeStatus.CONVERTIBLE,
    display_name="SHP",
    target_format="GeoParquet",
    error_message=None
)

# Unsupported (NetCDF)
FormatInfo(
    status=CloudNativeStatus.UNSUPPORTED,
    display_name="NetCDF",
    target_format=None,
    error_message="NetCDF is not yet supported. Support coming soon."
)
```

## Format Classification Tables

### Cloud-Native Formats (CLOUD_NATIVE)

| Display Name | Extensions | Detection Method |
|--------------|------------|------------------|
| GeoParquet | .parquet | Has 'geo' schema metadata |
| Parquet | .parquet | Valid Parquet, no geo metadata |
| COG | .tif, .tiff | Passes rio-cogeo validation |
| FlatGeobuf | .fgb | Extension |
| COPC | .copc.laz | Extension |
| PMTiles | .pmtiles | Extension |
| Zarr | .zarr (directory) | Path detection |
| Raquet | .raquet | Extension |

### Convertible Formats (CONVERTIBLE)

| Display Name | Extensions | Target Format |
|--------------|------------|---------------|
| SHP | .shp | GeoParquet |
| GeoJSON | .geojson, .json* | GeoParquet |
| GPKG | .gpkg | GeoParquet |
| CSV | .csv | GeoParquet |
| TIFF | .tif, .tiff** | COG |
| JP2 | .jp2 | COG |

\* .json files detected as GeoJSON via content inspection
\** Only if rio-cogeo validation fails (not already a COG)

### Unsupported Formats (UNSUPPORTED)

| Display Name | Extensions | Error Message |
|--------------|------------|---------------|
| NetCDF | .nc, .netcdf | "NetCDF is not yet supported. Support coming soon." |
| HDF5 | .h5, .hdf5 | "HDF5 is not yet supported. Support coming soon." |
| LAS | .las, .laz* | "LAS/LAZ point clouds require COPC format. Use pdal or other tools to convert." |

\* Non-COPC only; .copc.laz is CLOUD_NATIVE

## State Transitions

```
File Input
    │
    ▼
┌─────────────────┐
│ detect_format() │  (existing)
└────────┬────────┘
         │
         ▼
┌─────────────────────────┐
│ get_cloud_native_status()│  (new)
└────────┬────────────────┘
         │
    ┌────┴────┬─────────────┐
    │         │             │
    ▼         ▼             ▼
CLOUD_NATIVE  CONVERTIBLE   UNSUPPORTED
    │         │             │
    ▼         ▼             ▼
 (silent)   warn()        error()
    │         │             │
    ▼         ▼             ▼
 process    convert        stop
```

## Validation Rules

1. **File must exist** - FileNotFoundError if missing
2. **Path must be file** - IsADirectoryError for directories
3. **Extension or content must be recognized** - UNSUPPORTED for unknown formats
4. **COG detection requires valid TIFF** - Invalid TIFFs fail with rasterio error
5. **GeoParquet detection requires valid Parquet** - Invalid Parquet fails with pyarrow error

## Integration Points

### In formats.py

New function:
```python
def get_cloud_native_status(path: Path) -> FormatInfo:
    """Determine cloud-native status and format info for a file."""
```

### In dataset.py

Modification to `add_dataset()`:
```python
# Before conversion step
format_info = get_cloud_native_status(path)
if format_info.status == CloudNativeStatus.UNSUPPORTED:
    error(format_info.error_message)
    raise UnsupportedFormatError(format_info.error_message)
if format_info.status == CloudNativeStatus.CONVERTIBLE:
    warn(f"{format_info.display_name} is not cloud-native. Converting to {format_info.target_format}.")
# Continue with existing conversion logic...
```
