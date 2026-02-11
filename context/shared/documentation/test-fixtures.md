# Real-World Test Fixtures

## Purpose

**Portolan orchestrates — it does not convert or validate geometry.**

These fixtures test that Portolan's orchestration layer correctly handles real-world data:
- Passes files to upstream libraries (geoparquet-io, rio-cogeo) without corruption
- Extracts correct metadata for STAC catalog entries
- Computes accurate bounding boxes (including edge cases like antimeridian)
- Handles various file sizes and feature counts without OOM or timeout

**What we're NOT testing:**
- Geometry validity (geoparquet-io's job)
- Format conversion correctness (upstream library's job)
- Spatial operations (not our domain)

## Fixture Location

**In repo:** `tests/fixtures/realdata/`

**Canonical source:** `s3://us-west-2.opendata.source.coop/nlebovits/portolan-test-fixtures/sources/`

Fixtures are committed to git for robustness — no network dependency during tests.

| File | Size | Features | Source |
|------|------|----------|--------|
| `nwi-wetlands.parquet` | 1.5MB | 1,000 | [National Wetlands Inventory](https://source.coop/giswqs/nwi) |
| `open-buildings.parquet` | 146KB | 1,000 | [Google-Microsoft-OSM Open Buildings](https://source.coop/vida/google-microsoft-osm-open-buildings) |
| `road-detections.parquet` | 92KB | 1,000 | [Microsoft ML Road Detections](https://source.coop/nlebovits/microsoft-ml-road-detections) |
| `fieldmaps-boundaries.parquet` | 2.3MB | 3 | [FieldMaps](https://fieldmaps.io/) |
| `rapidai4eo-sample.tif` | 205KB | N/A | [RapidAI4EO](https://source.coop/planet/rapidai4eo) |
| `natural-earth-non-cog.tif` | 1.3MB | N/A | [Natural Earth 50m Shaded Relief](https://www.naturalearthdata.com/downloads/50m-raster-data/50m-shaded-relief/) |
| `open-buildings-utm31n.parquet` | 147KB | 1,000 | Derived from `open-buildings.parquet`, reprojected to EPSG:32631 |

**Total size:** ~5.8MB

## What Each Fixture Tests

### Vector Datasets

#### `nwi-wetlands.parquet` (1.5MB, 1,000 features)
- **Data:** National Wetlands Inventory polygons from DC
- **Why:** Complex polygons with holes
- **Portolan tests:**
  - Metadata extraction works on complex geometries
  - Feature count reported correctly
  - File passes through to geoparquet-io without corruption

#### `open-buildings.parquet` (146KB, 1,000 features)
- **Data:** Building footprints from Andorra (Google-Microsoft-OSM)
- **Why:** High feature count, simple polygons, Hive-partitioned source
- **Portolan tests:**
  - Bulk file handling doesn't OOM
  - Metadata extraction completes in reasonable time

#### `road-detections.parquet` (92KB, 1,000 features)
- **Data:** Road detections from Saint Lucia (Microsoft ML)
- **Why:** LineString geometries (not polygons)
- **Portolan tests:**
  - Non-polygon geometry types handled correctly
  - Geometry type detected and reported accurately

#### `fieldmaps-boundaries.parquet` (2.3MB, 3 features)
- **Data:** Fiji and Kiribati administrative boundaries
- **Why:** **Crosses the antimeridian (±180° longitude)**
- **Portolan tests:**
  - Bounding box computation handles antimeridian correctly
  - STAC bbox field is valid (doesn't produce impossible coordinates)

### Raster Dataset

#### `rapidai4eo-sample.tif` (205KB)
- **Data:** Planet satellite imagery tile
- **Why:** Real Cloud-Optimized GeoTIFF from production source
- **Portolan tests:**
  - File passes to rio-cogeo without corruption
  - Raster metadata extraction works on real COG

#### `natural-earth-non-cog.tif` (1.3MB)
- **Data:** Natural Earth 50m shaded relief, cropped to 1000x1000 pixels
- **Why:** **NOT a Cloud-Optimized GeoTIFF** (non-tiled, overviews in wrong order)
- **Portolan tests:**
  - "Not cloud-native" warning is triggered
  - Raster validation correctly identifies non-COG files

### Projected Vector Dataset

#### `open-buildings-utm31n.parquet` (147KB, 1,000 features)
- **Data:** Same as `open-buildings.parquet`, reprojected to UTM Zone 31N (EPSG:32631)
- **Why:** **Non-WGS84 CRS** — tests CRS extraction for projected coordinate systems
- **Portolan tests:**
  - CRS extraction works with PROJJSON format
  - Non-EPSG:4326 coordinates handled correctly in metadata

## CI Integration

### Marker

Tests using these fixtures are marked with `@pytest.mark.realdata`:

```python
@pytest.mark.realdata
def test_antimeridian_bbox_computation(fieldmaps_fixture):
    """Bounding box for Fiji/Kiribati computed correctly."""
    ...
```

### When Tests Run

| CI Tier | Runs `realdata` tests? | Notes |
|---------|------------------------|-------|
| Pre-commit | No | Too slow for local hooks |
| PR CI | **Yes** | Fixtures in repo, no network needed |
| Nightly | Yes | Same as PR CI |

Fixtures are committed to git — no caching infrastructure needed.

## Fixture Provenance

### NWI Wetlands
```sql
-- Source: s3://us-west-2.opendata.source.coop/giswqs/nwi/wetlands/DC_Wetlands.parquet
COPY (
  SELECT * FROM read_parquet('s3://us-west-2.opendata.source.coop/giswqs/nwi/wetlands/DC_Wetlands.parquet')
  LIMIT 1000
) TO 'nwi-wetlands.parquet' (FORMAT PARQUET);
```

### Open Buildings
```sql
-- Source: Hive-partitioned by country, Andorra subset
COPY (
  SELECT * FROM read_parquet('s3://us-west-2.opendata.source.coop/vida/google-microsoft-osm-open-buildings/geoparquet/by_country/country_iso=AND/AND.parquet')
  LIMIT 1000
) TO 'open-buildings.parquet' (FORMAT PARQUET);
```

### Road Detections
```sql
-- Source: Hive-partitioned by country, Saint Lucia subset
COPY (
  SELECT * FROM read_parquet('s3://us-west-2.opendata.source.coop/nlebovits/microsoft-ml-road-detections/by_country/country=LCA/LCA.parquet')
  LIMIT 1000
) TO 'road-detections.parquet' (FORMAT PARQUET);
```

### FieldMaps Boundaries
```sql
-- Source: https://data.fieldmaps.io/edge-matched/open/intl/adm1_polygons.parquet
-- Filter: Only features spanning the antimeridian
COPY (
  SELECT * FROM read_parquet('https://data.fieldmaps.io/edge-matched/open/intl/adm1_polygons.parquet')
  WHERE geometry_bbox.xmin < -170 AND geometry_bbox.xmax > 170
    AND adm0_name IN ('Fiji', 'Kiribati')
) TO 'fieldmaps-boundaries.parquet' (FORMAT PARQUET);
```

### RapidAI4EO
```bash
# Direct download of a single COG tile
curl -o rapidai4eo-sample.tif \
  "https://data.source.coop/planet/rapidai4eo/imagery/33N/15E-200N/33N_15E-200N_01_09/PF-SR/2018-01-03.tif"
```

### Natural Earth Non-COG
```bash
# Source: https://www.naturalearthdata.com/downloads/50m-raster-data/50m-shaded-relief/
# Download 50m shaded relief (SR_50M.zip)
curl -L -o SR_50M.zip "https://naciscdn.org/naturalearth/50m/raster/SR_50M.zip"
unzip SR_50M.zip

# Crop to 1000x1000, non-tiled, then add overviews in wrong order to break COG validity
gdal_translate -co TILED=NO -srcwin 0 0 1000 1000 SR_50M.tif natural-earth-non-cog.tif
gdaladdo -r average natural-earth-non-cog.tif 2 4 8

# Verify it's NOT a COG
rio cogeo validate natural-earth-non-cog.tif
# Should report: "is NOT a valid cloud optimized GeoTIFF"
```

### Open Buildings UTM 31N
```sql
-- Derived from open-buildings.parquet
-- Reprojected using DuckDB spatial extension, then CRS metadata added via PyArrow
LOAD spatial;
COPY (
  SELECT ST_Transform(geometry, 'EPSG:4326', 'EPSG:32631') as geometry, * EXCLUDE geometry
  FROM read_parquet('open-buildings.parquet')
) TO 'open-buildings-utm31n.parquet' (FORMAT PARQUET);

-- Note: DuckDB doesn't write CRS to GeoParquet metadata, so we add it manually:
-- See tests/fixtures/generate.py or use PyArrow to add PROJJSON CRS metadata
```

## Adding New Fixtures

1. **Identify the edge case** — What orchestration behavior needs testing?
2. **Find minimal real data** — Prefer <5MB, <10K features
3. **Document provenance** — Add SQL/command to this file under "Fixture Provenance"
4. **Add fixture to repo** — Place in `tests/fixtures/realdata/`
5. **Add pytest fixture** — Update `tests/realdata/conftest.py`
6. **Write the test** — Focus on Portolan's orchestration, not upstream behavior
7. **Update pre-commit if needed** — Large files may need exclusion in `.pre-commit-config.yaml`

Optionally mirror to S3 for backup: `aws s3 cp fixture.parquet s3://us-west-2.opendata.source.coop/nlebovits/portolan-test-fixtures/sources/`
