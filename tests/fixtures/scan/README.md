# Scan Fixtures

Test fixtures for `portolan scan` — the directory scanning and validation command.

## Purpose

These fixtures test **directory structure detection**, not file content parsing. The `scan` command analyzes directories to identify:

- Geospatial files that can be imported
- Structural issues (multiple primary assets, invalid filenames, etc.)
- Files that should be skipped (unsupported formats, sidecars)

**What we're testing:**
- Directory traversal and file discovery
- Issue detection and severity classification
- Sidecar file association (`.dbf`, `.prj`, `.shx` with `.shp`)
- Filename validation

**What we're NOT testing:**
- File content validity (that's `dataset add`'s job)
- Geometry correctness (upstream library's job)
- Format conversion (geoparquet-io/rio-cogeo's job)

## Fixture Directories

| Directory | Scenario | Expected Behavior |
|-----------|----------|-------------------|
| `clean_flat/` | 3 independent files | Clean scan, no warnings |
| `multiple_primaries/` | 3 primary assets in one dir | Warning: ambiguous structure |
| `complete_shapefile/` | Full shapefile set | Recognized as single dataset |
| `incomplete_shapefile/` | `.shp` missing `.dbf` | Error: incomplete shapefile |
| `invalid_chars/` | Filenames with `()` and accents | Warning: invalid characters |
| `mixed_formats/` | Raster + vector together | Info: mixed format types |
| `nested/` | Hierarchical `category/year/` structure | Depth detection |
| `unsupported/` | `.csv`, `.mxd` + one valid file | Skip unsupported, find valid |
| `duplicate_basenames/` | `argentina.geojson` + `Argentina.geojson` | Warning: case collision |

## Fixture Details

### `clean_flat/`

Happy path: a directory with independent, valid geospatial files.

| File | Format | Size | Source |
|------|--------|------|--------|
| `example.parquet` | GeoParquet | 30KB | Unknown origin (from Downloads) |
| `argentina.geojson` | GeoJSON | 10KB | Unknown origin (from Downloads) |
| `seaLevelDistanceGeoJSON.geojson` | GeoJSON | 2KB | Unknown origin (from Downloads) |

**Tests:** Basic file discovery, extension filtering, no false positives.

### `multiple_primaries/`

Three GeoJSON files that could each be a "primary" dataset — ambiguous whether they should be separate datasets or one collection.

| File | Format | Size | Source |
|------|--------|------|--------|
| `la_plata_L1.geojson` | GeoJSON | ~400B | Derived from La Plata, Argentina admin boundaries (Level 1) |
| `la_plata_L2.geojson` | GeoJSON | ~550B | Derived from La Plata, Argentina admin boundaries (Level 2) |
| `la_plata_L3.geojson` | GeoJSON | ~400B | Derived from La Plata, Argentina admin boundaries (Level 3) |

**Note:** These were simplified from larger source files using `ST_Simplify(geom, 0.01)` and limited to 1 feature each.

**Tests:** Detection of multiple primary assets in same directory, appropriate warning generation.

### `complete_shapefile/`

A valid, complete ESRI Shapefile with all required sidecar files.

| File | Purpose |
|------|---------|
| `radios_sample.shp` | Geometry |
| `radios_sample.dbf` | Attributes |
| `radios_sample.shx` | Spatial index |
| `radios_sample.prj` | Projection/CRS |

**Source:** Subset of Argentina census radios (Radios_2022), limited to 5 features via `ogr2ogr`.

**Tests:** Shapefile sidecar association, recognition as single dataset (not 4 separate files).

### `incomplete_shapefile/`

An ESRI Shapefile missing its `.dbf` file (attribute table).

| File | Present |
|------|---------|
| `radios_sample.shp` | Yes |
| `radios_sample.dbf` | **No** |
| `radios_sample.shx` | Yes |
| `radios_sample.prj` | Yes |

**Tests:** Error detection for incomplete shapefile, clear error message.

### `invalid_chars/`

Files with characters that may cause issues on some filesystems or in URLs.

| File | Issue |
|------|-------|
| `data (copy).parquet` | Parentheses and spaces |
| `données.geojson` | Non-ASCII character (French accent) |

**Tests:** Filename validation, warning generation, suggested fixes.

### `mixed_formats/`

Raster and vector files in the same directory — not necessarily wrong, but worth noting.

| File | Format |
|------|--------|
| `example.parquet` | Vector (GeoParquet) |
| `ID1_N80_W170_RP10_depth_reclass.tif` | Raster (GeoTIFF) |

**Source:** Raster is flood depth data from unknown source (from Downloads).

**Tests:** Mixed format detection, info-level suggestion (not error or warning).

### `nested/`

Hierarchical directory structure mimicking real-world organization.

```
nested/
├── census/
│   ├── 2020/
│   │   └── boundaries.geojson
│   └── 2022/
│       └── boundaries.geojson
└── imagery/
    └── 2024/
        └── flood_depth.tif
```

**Tests:** Recursive scanning, depth detection, `--depth` flag mapping.

### `unsupported/`

Mix of unsupported and supported formats.

| File | Format | Supported? |
|------|--------|------------|
| `argentina.geojson` | GeoJSON | Yes |
| `metadata.csv` | CSV | No |
| `project.mxd` | ArcMap project | No |

**Tests:** Skip unsupported formats, still find valid files, report skipped count.

### `duplicate_basenames/`

Two files with the same base name but different case.

| File | Notes |
|------|-------|
| `argentina.geojson` | Lowercase |
| `Argentina.geojson` | Uppercase |

**Note:** On case-insensitive filesystems (macOS, Windows), these would collide. On Linux they coexist but may cause confusion when generating IDs.

**Tests:** Case collision detection, unique ID generation.

## Fixture Size

Total size: ~470KB (25 files)

All fixtures are small enough to commit to git. No network dependencies during tests.

## Adding New Fixtures

1. Identify the structural scenario you need to test
2. Create minimal files (prefer <10KB per file, <100KB per directory)
3. Add directory to this README with explanation
4. Update `tests/conftest.py` with pytest fixtures if needed
5. Ensure fixture tests **structure**, not **content** (content testing belongs in `tests/fixtures/vector/` etc.)

## Provenance Notes

Most source data came from `~/Downloads/spatial/` — a collection of geospatial files accumulated over time. Original sources include:

- **La Plata boundaries:** Administrative boundaries for La Plata, Argentina (unknown exact source)
- **Radios_2022:** Argentina 2022 census radios (INDEC)
- **Flood depth raster:** Unknown source, appears to be flood modeling output
- **Other files:** Various small files of unknown provenance, kept because they're valid examples of their formats

If exact provenance matters for a specific test, document it when you add the test.
