# Recognized File Extensions

Portolan tools classify files by extension to determine how they should be handled during import and validation.

## Primary Geospatial Formats

These extensions are recognized as importable geospatial data:

| Extension | Format | Type | Cloud-Native | Notes |
|-----------|--------|------|--------------|-------|
| `.parquet` | GeoParquet | Vector | Yes | Requires geo metadata (content inspection) |
| `.geojson` | GeoJSON | Vector | No | Converts to GeoParquet |
| `.json` | GeoJSON | Vector | No | Content inspected for GeoJSON structure |
| `.shp` | Shapefile | Vector | No | Converts to GeoParquet |
| `.gpkg` | GeoPackage | Vector | No | Converts to GeoParquet |
| `.fgb` | FlatGeobuf | Vector | Yes | Cloud-native, passed through |
| `.csv` | CSV | Vector/Tabular | No | Content inspected: geometry columns → GeoParquet; no geometry → tabular (if enabled) |
| `.tif`, `.tiff` | GeoTIFF/COG | Raster | Depends | Content inspected for COG compliance |
| `.jp2` | JPEG2000 | Raster | No | Converts to COG |

Files with these extensions are candidates for `portolan dataset add`.

### Non-Cloud-Native Format Handling

Portolan **accepts** non-cloud-native formats (GeoJSON, Shapefile, GeoPackage, etc.) but emits warnings encouraging conversion to cloud-native formats.

**Behavior**:
- Non-cloud-native files are imported with a warning
- Users are encouraged to convert to GeoParquet (vector) or COG (raster)
- Validation passes but reports the warning

**Rationale**: Many users have legacy data in non-cloud-native formats. Rejecting these files would create friction. Instead, we warn and accept, guiding users toward best practices over time.

See [ADR-0014: Accept non-cloud-native formats](https://github.com/portolan-sdi/portolan-cli/blob/main/context/shared/adr/0014-accept-non-cloud-native-formats.md) for the full decision record.

### Content Inspection

Some formats require content inspection to determine type and cloud-native status:

- **`.parquet`**: Checked for GeoParquet metadata (`geo` key in schema metadata)
  - If `geo` key present → GeoParquet (geospatial pipeline)
  - If `geo` key absent → Plain Parquet (tabular pipeline, requires `tabular.enabled: true`)
- **`.csv`**: Checked for geometry columns (lat/lon, WKT, WKB)
  - If geometry columns found → GeoParquet conversion
  - If no geometry columns → tabular (requires `tabular.enabled: true`)
- **`.json`**: Checked for GeoJSON structure (FeatureCollection, Feature, or geometry)
- **`.tif`/`.tiff`**: Validated against COG spec (internal tiling, overviews)

Files that fail content inspection are treated as convertible (vector), tabular (if enabled), or rejected (raster without geo info).

## Sidecar Files

These extensions are recognized as sidecar files belonging to a primary asset:

| Extension | Associated Format |
|-----------|-------------------|
| `.dbf` | Shapefile attribute table |
| `.shx` | Shapefile index |
| `.prj` | Shapefile projection |
| `.cpg` | Shapefile code page |
| `.sbn`, `.sbx` | Shapefile spatial index |
| `.ovr` | Raster overview (pyramid) |
| `.xml` | Auxiliary metadata (aux.xml, etc.) |

Sidecar files are **not** imported directly. When importing a `.shp` file, its sidecars are read automatically.

## Visualization Formats

| Extension | Format | Description |
|-----------|--------|-------------|
| `.pmtiles` | PMTiles | Cloud-native vector tiles for web rendering |
| `.mbtiles` | MBTiles | SQLite-based tile archive |

These are **derivatives**, not primary data. PMTiles **SHOULD** be generated from GeoParquet for web display.

## Metadata Files

| Filename | Description |
|----------|-------------|
| `catalog.json` | STAC Catalog (root) |
| `collection.json` | STAC Collection |
| `versions.json` | Portolan version manifest |
| `styles/*.json` | Mapbox GL / MapLibre style definitions (see [best-practices.md#visualization-styles](best-practices.md#visualization-styles)) |

These files have semantic meaning and are not imported as datasets.

## Thumbnail/Preview

| Extension | Handling |
|-----------|----------|
| `.png`, `.jpg`, `.jpeg`, `.webp`, `.gif` | Treated as thumbnails if < 1 MiB (1,048,576 bytes) |

Small images are assumed to be previews, not raster data.

## Unsupported Formats

These formats are explicitly rejected with informative error messages:

| Extension | Format | Reason |
|-----------|--------|--------|
| `.nc`, `.netcdf` | NetCDF | Not yet supported |
| `.h5`, `.hdf5` | HDF5 | Not yet supported |
| `.las`, `.laz` | LAS/LAZ | Use COPC format instead |

## Tabular Formats

These extensions are recognized as tabular data when `tabular.enabled: true` in `.portolan/config.yaml`:

| Extension | Format | Handling |
|-----------|--------|----------|
| `.parquet` | Plain Parquet | Content inspection — no `geo` metadata key |
| `.csv` | CSV | Content inspection — no geometry columns |
| `.tsv` | TSV | Converts to Parquet |
| `.xlsx`, `.xls` | Excel | Converts to Parquet |

When `tabular.enabled` is `false` (default):
- Tabular files with a companion geo file → tracked as companion assets
- Standalone tabular files → rejected with helpful error message

See [formats/tabular.md](formats/tabular.md) for full requirements.

## Ignored Files

The following are skipped during directory scans:

| Pattern | Reason |
|---------|--------|
| `.exe`, `.dll`, `.so`, `.dylib` | Executables |
| `.pyc`, `.pyo`, `.class`, `.o`, `.obj` | Compiled files |
| `__pycache__/`, `.git/`, `.svn/`, `.hg/` | Build/VCS directories |
| `.idea/`, `.vscode/`, `node_modules/`, `.tox/`, `.pytest_cache/` | IDE/tooling directories |
| `.md`, `.txt`, `.rst`, `.html`, `.htm` | Documentation |

## Extension vs. Role

STAC uses **asset roles** to describe purpose, not file extensions. The extensions above are used for:
1. **Import classification** — determining if a file can be added as a dataset
2. **Format detection** — deciding which conversion pipeline to use

Once imported, the STAC asset's `roles` field (e.g., `["data"]`, `["thumbnail"]`) provides machine-readable semantics.
