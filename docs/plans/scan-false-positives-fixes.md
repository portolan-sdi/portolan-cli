# Scan Pre-Import Directory Model

**Date:** 2025-02-11
**Status:** IN PROGRESS
**Related PR:** #63

## Purpose

The `portolan scan` command validates directories **before import** to identify geospatial files ready for cataloging and detect structural issues that would cause import failures.

## The Leaf Directory Model (Happy Path)

A well-structured pre-import directory follows the **Leaf Directory Model**:

```
raw-data/
├── 2010/
│   └── radios/                    # Leaf directory = one dataset
│       ├── radios.parquet         # PRIMARY: GeoParquet (exactly ONE)
│       ├── census-data.parquet    # SIDECAR: regular Parquet (tabular)
│       ├── metadata.parquet       # SIDECAR: regular Parquet (tabular)
│       └── overview.pmtiles       # SIDECAR: derivative/visualization
│
├── 2022/
│   └── radios/
│       ├── radios.parquet         # PRIMARY
│       ├── census-data.parquet    # SIDECAR
│       └── overview.pmtiles       # SIDECAR
│
└── boundaries/
    └── provinces/
        └── provinces.geojson      # PRIMARY
```

### Rules

1. **Leaf directories contain exactly ONE primary geospatial file**
   - Primary formats: GeoParquet, GeoJSON, Shapefile (.shp), GeoPackage (.gpkg), FlatGeoBuf (.fgb), COG (.tif)

2. **Sidecars are non-geospatial supporting files**
   - Regular Parquet (tabular data, no `geo` metadata)
   - PMTiles (visualization/overview)
   - JSON (styles, metadata)
   - PNG/JPEG (thumbnails)

3. **Intermediate directories are organizational only**
   - They group datasets but don't contain data files directly
   - Examples: `2010/`, `2022/`, `boundaries/`

4. **Directory name = dataset name**
   - The leaf directory name typically becomes the dataset identifier

### What Scan Should Detect

| Condition | Severity | Message | Suggestion |
|-----------|----------|---------|------------|
| Leaf with 2+ primary geospatial files | WARNING | "Multiple primary assets in directory" | "Split into separate subdirectories" |
| Primary file at intermediate level | INFO | "Geospatial file not in leaf directory" | "Create subdirectory for this dataset" |
| No primary in leaf (only sidecars) | INFO | "Directory has sidecars but no primary" | "Add primary geospatial file or remove directory" |

### File Classification

| Extension | Classification | Notes |
|-----------|----------------|-------|
| `.parquet` | PRIMARY if GeoParquet, SIDECAR if regular | Check for `geo` key in schema metadata |
| `.geojson` | PRIMARY | Always geospatial |
| `.shp` | PRIMARY | With `.dbf`, `.shx` sidecars |
| `.gpkg` | PRIMARY | Self-contained |
| `.fgb` | PRIMARY | FlatGeoBuf |
| `.tif`, `.tiff` | PRIMARY | COG/GeoTIFF |
| `.pmtiles` | SIDECAR | Visualization derivative |
| `.json` | SIDECAR | Metadata/styles |
| `.png`, `.jpg` | SIDECAR | Thumbnails |
| `.dbf`, `.shx`, `.prj` | SIDECAR | Shapefile components |

---

## Implementation Changes

### Issue 1: Distinguish GeoParquet from Regular Parquet ✅

**Problem:** All `.parquet` files were treated as primary assets.

**Fix:** Check for GeoParquet metadata (`geo` key in schema):

```python
def _is_geoparquet(path: Path) -> bool:
    """Check if a Parquet file is GeoParquet."""
    try:
        import pyarrow.parquet as pq
        pq_file = pq.ParquetFile(path)
        schema_metadata = pq_file.schema_arrow.metadata
        return b"geo" in (schema_metadata or {})
    except Exception:
        return False
```

### Issue 2: PMTiles as Sidecar ✅

**Problem:** `.pmtiles` files weren't recognized.

**Fix:** Add to `OVERVIEW_EXTENSIONS` and skip as sidecars (not primary assets).

### Issue 3: Duplicate Basenames Scope ✅

**Problem:** Warned about `2010/radios.parquet` and `2022/radios.parquet`.

**Fix:** Only warn about duplicates within the SAME directory. Sibling directories with matching names are intentional organization.

---

## Test Cases

1. ✅ Directory with 1 GeoParquet + 2 regular Parquet → no "multiple primaries" warning
2. ✅ Sibling directories with same filenames → no "duplicate basename" warning
3. ✅ Same directory with duplicate basenames → SHOULD warn
4. ✅ Directory with .pmtiles → recognized as sidecar, not primary

---

## Future Work (Phase 2)

- `portolan scan --fix` to automatically restructure directories
- Detection of primary files at intermediate levels
- Suggested directory structure for non-conforming layouts
