# Format Support

Portolan converts data to cloud-native formats (GeoParquet, COG) for efficient cloud storage and querying.

## Supported Formats

The complete extension-by-extension vocabulary — what each input format is,
how it routes, and what it converts to — lives in
[Input Formats](input-formats.md), a page generated from the code's extension
registry so it can never drift. This page covers the conversion behavior and
edge cases around it.

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

## Multi-Layer Formats

GeoPackage and FileGDB files can contain multiple vector layers. Portolan handles these specially:

| Format | Layer Detection | Notes |
|--------|-----------------|-------|
| GeoPackage | ✅ geoparquet-io | No external dependencies |
| FileGDB | ✅ geoparquet-io | No external GDAL required |

### API Functions

- `list_layers(path)` — Returns list of layer names, or `None` for single-layer formats
- `is_multilayer(path)` — Returns `True` if file has more than one layer
- `convert_multilayer_file(source, output_dir)` — Converts each layer to a separate GeoParquet file

### Output Naming

Each layer becomes a separate file: `{source_stem}_{layer_name}.parquet`

```
multilayer.gpkg (3 layers)
├── points
├── lines
└── polygons

→ multilayer_points.parquet
→ multilayer_lines.parquet
→ multilayer_polygons.parquet
```

## COG Conversion Settings

COG (Cloud-Optimized GeoTIFF) conversion can be configured via `config.yaml`:

```yaml
conversion:
  cog:
    compression: DEFLATE  # DEFLATE, LZW, ZSTD, JPEG, WEBP
    tile_size: 512        # 256, 512, 1024
    predictor: auto       # auto, 1=none, 2=horizontal, 3=floating-point
    resampling: auto      # auto, nearest, bilinear, cubic, lanczos, average
    quality: 75           # JPEG/WEBP quality (1-100)
```

`auto` derives the setting from the source raster's dtype: floats get predictor 3
and `average` overviews, integers get predictor 2 and `nearest`, and multi-band
uint8 imagery gets no predictor. See
[COG settings](configuration.md#cog-settings) for the full table.

See `get_cog_settings()` and `CogSettings` for programmatic access.
