---
name: portolan-consume
description: Guide users through querying and exploring Portolan/STAC catalogs with optimized GeoParquet and COGs
triggers:
  - "how do I query this catalog"
  - "consume this data"
  - "duckdb query"
  - "read the parquet"
  - "access this dataset"
  - "query the geoparquet"
---

<!-- freshness: last-verified: 2026-05-08, maps-to: ADR-0044 -->

# Portolan Catalog Consumption Skill

You are helping a user query and explore data from a Portolan catalog. Portolan produces optimized cloud-native geospatial data (GeoParquet for vectors, COG for rasters) with rich STAC metadata.

## Portolan Catalog Consumption Guide

Help users query and explore Portolan catalogs. Portolan produces optimized cloud-native geospatial data (GeoParquet for vectors, COG for rasters) with rich STAC metadata.

### Step 1: Detect User's Environment

Before suggesting tools, check what's installed:

```bash
# Check for DuckDB
which duckdb 2>/dev/null && echo "DuckDB CLI available"
python -c "import duckdb; print(f'DuckDB Python: {duckdb.__version__}')" 2>/dev/null

# Check for Python geospatial stack
python -c "import geopandas; print(f'GeoPandas: {geopandas.__version__}')" 2>/dev/null
python -c "import rioxarray; print('rioxarray available')" 2>/dev/null
python -c "import rasterio; print(f'rasterio: {rasterio.__version__}')" 2>/dev/null
```

**Recommendations based on environment:**

| Installed | For Vectors | For Rasters |
|-----------|-------------|-------------|
| DuckDB | DuckDB + spatial extension (best) | N/A |
| GeoPandas only | `gpd.read_parquet()` | N/A |
| rioxarray | N/A | `rioxarray.open_rasterio()` (best) |
| rasterio only | N/A | `rasterio.open()` |
| Nothing | Suggest DuckDB installation | Suggest rioxarray |

If user has nothing installed, explain options and suggest cloud-native approach (DuckDB for vectors, rioxarray for rasters). Offer to guide through installation but respect if they don't want to install.

### Step 2: Understand the Catalog Structure

Read STAC metadata before querying data:

```bash
# For remote catalogs
curl -s "https://data.source.coop/user/catalog/collection/collection.json" | jq '.id, .title, .description'

# Check assets
curl -s "https://data.source.coop/user/catalog/collection/collection.json" | jq '.assets | keys'

# Check schema (table:columns)
curl -s "https://data.source.coop/user/catalog/collection/collection.json" | jq '."table:columns"'
```

For local catalogs, read the JSON files directly.

**Key STAC locations:**
- `catalog.json` — root, lists collections
- `collection.json` — collection metadata, schema, **vector assets live here**
- `item.json` — item metadata, **raster assets live here**

### Step 3: URL Protocol Handling

Convert remote storage URLs to consumption URLs:

| Storage URL | Consumption URL | Notes |
|-------------|-----------------|-------|
| `s3://data.source.coop/...` | `https://data.source.coop/...` | Source Coop serves HTTPS |
| `s3://private-bucket/...` | `s3://private-bucket/...` | Needs credential config |
| `gs://bucket/...` | `https://storage.googleapis.com/bucket/...` | For public GCS |
| Local path | `file:///path/to/...` | Works with DuckDB |

For private S3, configure credentials:

```sql
-- DuckDB
SET s3_region = 'us-east-1';
SET s3_access_key_id = getenv('AWS_ACCESS_KEY_ID');
SET s3_secret_access_key = getenv('AWS_SECRET_ACCESS_KEY');
```

### Step 4: Portolan GeoParquet Optimizations

**Portolan produces optimized GeoParquet files.** Understand these to write efficient queries:

| Optimization | Description | How to Leverage |
|--------------|-------------|-----------------|
| **Hilbert spatial ordering** | Features sorted by Hilbert curve | Spatial queries read sequentially |
| **Row groups (~100K rows)** | Data chunked for parallel access | Predicate pushdown skips irrelevant groups |
| **ZSTD compression** | Fast decompression, small files | Automatic, no action needed |
| **bbox struct column** | `bbox.xmin/xmax/ymin/ymax` | Use for fast spatial pre-filter |

**Fast spatial filtering with bbox struct:**

```sql
-- Step 1: Fast filter using bbox (doesn't parse geometry)
SELECT * FROM read_parquet('https://...')
WHERE bbox.xmin > -58.5 AND bbox.xmax < -58.3
  AND bbox.ymin > -34.7 AND bbox.ymax < -34.5
LIMIT 100;

-- Step 2: Refine with actual geometry if needed
SELECT * FROM (
  SELECT * FROM read_parquet('https://...')
  WHERE bbox.xmin > -58.5 AND bbox.xmax < -58.3
        AND bbox.ymin > -34.7 AND bbox.ymax < -34.5
)
WHERE ST_Intersects(geometry, ST_GeomFromText('POLYGON(...)'));
```

### Step 5: Exploration Workflow (Vectors)

Always explore before full queries:

```sql
-- 1. Install spatial extension (once)
INSTALL spatial; LOAD spatial;

-- 2. Check schema
DESCRIBE SELECT * FROM read_parquet('https://...') LIMIT 0;

-- 3. Row count (uses metadata, fast)
SELECT COUNT(*) FROM read_parquet('https://...');

-- 4. Dry run (always LIMIT first!)
SELECT * FROM read_parquet('https://...') LIMIT 10;

-- 5. Spatial extent
SELECT
  MIN(bbox.xmin) as west,
  MIN(bbox.ymin) as south,
  MAX(bbox.xmax) as east,
  MAX(bbox.ymax) as north
FROM read_parquet('https://...');

-- 6. Sample values for key columns
SELECT DISTINCT column_name FROM read_parquet('https://...') LIMIT 20;
```

### Step 6: Exploration Workflow (Rasters)

For COGs, STAC metadata has most info. Then use rioxarray:

```python
import rioxarray

# Open COG (lazy - doesn't load data yet)
da = rioxarray.open_rasterio(
    "https://data.source.coop/user/catalog/collection/item/image.tif"
)

# Check metadata
print(f"Shape: {da.shape}")
print(f"CRS: {da.rio.crs}")
print(f"Bounds: {da.rio.bounds()}")
print(f"Resolution: {da.rio.resolution()}")

# Read a small window (efficient - only fetches needed tiles)
window = da.isel(x=slice(0, 512), y=slice(0, 512))
data = window.compute()  # Actually loads data
```

### Step 7: Common Query Patterns

**Spatial filter:**
```sql
SELECT * FROM read_parquet('https://...')
WHERE ST_Intersects(
  geometry,
  ST_GeomFromText('POLYGON((-58.5 -34.7, -58.3 -34.7, -58.3 -34.5, -58.5 -34.5, -58.5 -34.7))')
);
```

**Attribute filter:**
```sql
SELECT * FROM read_parquet('https://...')
WHERE population > 10000;
```

**Aggregation:**
```sql
SELECT
  province,
  SUM(population) as total_pop,
  COUNT(*) as num_areas
FROM read_parquet('https://...')
GROUP BY province;
```

**Join multiple assets:**
```sql
-- When collection has related tables (check STAC metadata for relationships)
SELECT r.*, c.population, c.households
FROM read_parquet('https://.../radios.parquet') r
JOIN read_parquet('https://.../census-data.parquet') c
  ON r.cod_2022 = c.id_geo;
```

### Step 8: Partitioned Datasets

For datasets with `partition:glob` in STAC:

```sql
-- Query all partitions (use glob pattern from STAC)
SELECT * FROM read_parquet(
  'https://data.source.coop/user/catalog/collection/kdtree_cell=*/*.parquet'
) LIMIT 10;

-- Query single partition first (faster for exploration)
SELECT * FROM read_parquet(
  'https://data.source.coop/user/catalog/collection/kdtree_cell=0/*.parquet'
);

-- DuckDB automatically prunes partitions for Hive-style paths
SELECT * FROM read_parquet(
  'https://.../kdtree_cell=*/*.parquet'
) WHERE kdtree_cell = '42';
```

### Troubleshooting

**403 Forbidden:**
- Check if bucket is public
- For private buckets, configure S3 credentials
- Source Coop uses HTTPS, not S3 protocol

**Slow queries:**
- Always LIMIT during exploration
- Use bbox struct for spatial pre-filtering
- Check file size in STAC (`file:size`) before querying large files
- For partitioned data, query one partition first

**Schema mismatch:**
- Read `table:columns` from STAC first
- Column names are case-sensitive
- Geometry column is usually `geometry` (check STAC)

**Memory issues:**
- Use DuckDB (streams data, low memory)
- For GeoPandas, read with `columns=` parameter to limit columns
- For rasters, use window reads

### Tips

- **Always read STAC metadata first** — it has schema, extent, file sizes
- **Always LIMIT during exploration** — don't load full dataset until you know what you need
- **Use bbox struct** — faster than full geometry intersection
- **Portolan's Hilbert ordering** means spatial queries are I/O efficient
- **Check for partitioning** — `partition:glob` in STAC indicates partitioned dataset
