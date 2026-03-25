# Format Support

Portolan converts data to cloud-native formats (GeoParquet, COG) for efficient cloud storage and querying.

## Supported Formats

| Input Format | Output Format | Notes |
|--------------|---------------|-------|
| Shapefile | GeoParquet | Auto-converted |
| GeoJSON | GeoParquet | Auto-converted |
| GeoPackage | GeoParquet | Auto-converted |
| CSV (with geometry) | GeoParquet | Auto-converted |
| TIFF/GeoTIFF | COG | Auto-converted |
| JPEG2000 | COG | Auto-converted |
| GeoParquet | GeoParquet | Already cloud-native |
| COG | COG | Already cloud-native |
| FlatGeobuf | — | Accepted as-is (cloud-native) |

## ESRI File Geodatabase Rasters

Raster data stored in ESRI File Geodatabases (`.gdb`) **cannot be converted by Portolan**. The format was reverse-engineered exclusively for GDAL—no pure Python library can read it.

**Workaround:** Pre-convert to COG using GDAL before adding to your catalog:

```bash
# List rasters in the geodatabase
gdalinfo input.gdb

# Convert to COG
gdal_translate input.gdb/raster_name output.tif -of COG
```

Then add the resulting COG to your catalog as usual.

!!! note "Vector GDB data is supported"
    This limitation applies only to **raster** data in geodatabases. Vector layers in `.gdb` files work normally.
