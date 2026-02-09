# Quickstart: Cloud-Native Dataset Warnings

**Feature**: 002-cloud-native-warnings
**Date**: 2025-02-09

## Overview

When adding datasets to a Portolan catalog, files are classified by their cloud-native status:

- **Cloud-native formats** are accepted silently
- **Convertible formats** show a warning, then convert automatically
- **Unsupported formats** are rejected with a helpful error

## Usage Examples

### Cloud-Native Files (Silent Passthrough)

```bash
# GeoParquet - accepted silently
$ portolan dataset add data.parquet --collection mydata
✓ Added data to collection 'mydata'

# COG - accepted silently (no re-conversion)
$ portolan dataset add satellite.tif --collection imagery
✓ Added satellite to collection 'imagery'

# FlatGeobuf - accepted silently
$ portolan dataset add boundaries.fgb --collection admin
✓ Added boundaries to collection 'admin'
```

### Convertible Files (Warning + Convert)

```bash
# Shapefile - warns then converts
$ portolan dataset add buildings.shp --collection mydata
⚠ SHP is not cloud-native. Converting to GeoParquet.
✓ Added buildings to collection 'mydata'

# GeoJSON - warns then converts
$ portolan dataset add points.geojson --collection mydata
⚠ GeoJSON is not cloud-native. Converting to GeoParquet.
✓ Added points to collection 'mydata'

# Non-COG TIFF - warns then converts
$ portolan dataset add orthophoto.tif --collection imagery
⚠ TIFF is not cloud-native. Converting to COG.
✓ Added orthophoto to collection 'imagery'
```

### Unsupported Files (Rejected)

```bash
# NetCDF - rejected with helpful message
$ portolan dataset add climate.nc --collection weather
✗ NetCDF is not yet supported. Support coming soon.

# HDF5 - rejected
$ portolan dataset add satellite.h5 --collection raw
✗ HDF5 is not yet supported. Support coming soon.

# LAS (non-COPC) - rejected with conversion guidance
$ portolan dataset add lidar.laz --collection terrain
✗ LAS/LAZ point clouds require COPC format. Use pdal or other tools to convert.
```

## Cloud-Native Format Reference

| Format | Extension | Behavior |
|--------|-----------|----------|
| GeoParquet | .parquet | ✓ Cloud-native |
| COG | .tif, .tiff | ✓ Cloud-native (if validated) |
| FlatGeobuf | .fgb | ✓ Cloud-native |
| COPC | .copc.laz | ✓ Cloud-native |
| PMTiles | .pmtiles | ✓ Cloud-native |
| Zarr | .zarr | ✓ Cloud-native |
| Raquet | .raquet | ✓ Cloud-native |
| Shapefile | .shp | ⚠ Converts to GeoParquet |
| GeoJSON | .geojson | ⚠ Converts to GeoParquet |
| GeoPackage | .gpkg | ⚠ Converts to GeoParquet |
| CSV | .csv | ⚠ Converts to GeoParquet |
| TIFF (non-COG) | .tif | ⚠ Converts to COG |
| JP2 | .jp2 | ⚠ Converts to COG |
| NetCDF | .nc | ✗ Not yet supported |
| HDF5 | .h5 | ✗ Not yet supported |
| LAS/LAZ | .las, .laz | ✗ Requires COPC format |

## API Usage

```python
from pathlib import Path
from portolan_cli.formats import get_cloud_native_status, CloudNativeStatus

# Check a file's status
path = Path("data.shp")
info = get_cloud_native_status(path)

if info.status == CloudNativeStatus.CLOUD_NATIVE:
    print(f"{info.display_name} is cloud-native, no conversion needed")
elif info.status == CloudNativeStatus.CONVERTIBLE:
    print(f"{info.display_name} will convert to {info.target_format}")
elif info.status == CloudNativeStatus.UNSUPPORTED:
    print(f"Error: {info.error_message}")
```

## Programmatic Format Detection

```python
from portolan_cli.formats import (
    CloudNativeStatus,
    get_cloud_native_status,
    is_cloud_optimized_geotiff,
    is_geoparquet,
)

# Check if a TIFF is already a COG
if is_cloud_optimized_geotiff(Path("image.tif")):
    print("Already a COG - no conversion needed")

# Check if a Parquet file has geo metadata
if is_geoparquet(Path("data.parquet")):
    print("GeoParquet detected")
```
