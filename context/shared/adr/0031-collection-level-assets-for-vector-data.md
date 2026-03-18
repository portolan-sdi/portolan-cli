# ADR-0031: Collection-Level Assets for Vector Data

## Status
Proposed

## Context

Portolan catalogs geospatial data using STAC (SpatioTemporal Asset Catalog), but the STAC specification treats vector and raster data differently at a fundamental level.

**The STAC best practices state explicitly:**

> "A shapefile or geopackage corresponds to a **Collection, not a single Item**. The ideal thing to do with one of those is to serve it with OGC API - Features standard. This allows each feature in the shapefile/geopackage to be represented online, and enables querying of the actual data."

([Source: STAC Best Practices - Representing Vector Layers](https://github.com/radiantearth/stac-spec/blob/master/best-practices.md#representing-vector-layers-in-stac))

Currently, Portolan enforces item-level organization for all data (ADR-0022: Git-Style Implicit Tracking). This creates friction:

1. **User confusion**: Users place vector files directly in collection directories, then see "deleted" warnings because Portolan expects items
2. **Semantic mismatch**: A GeoParquet file representing municipal boundaries is a complete dataset (collection), not an individual observation (item)
3. **STAC non-compliance**: Item-level vector organization contradicts STAC best practices

**Vector vs Raster Semantics:**

| Data Type | Semantic Unit | STAC Entity | Example |
|-----------|---------------|-------------|---------|
| **Vector layer** | Entire dataset (all features) | Collection | Municipal boundaries (1 GeoParquet with 500 polygons) |
| **Raster scene** | Single observation | Item | Landsat scene from 2024-01-15 |

A vector file contains **many features** (rows in a table), not a single spatiotemporal observation. The abstraction level matches a **collection** of features, not an individual **item**.

**Supporting evidence:**

1. **STAC Table Extension** explicitly supports collection-level assets for tabular data
2. **Chris Holmes** (STAC co-author) confirmed vector data belongs at collection level
3. **Overture Maps** (billion-feature dataset) uses collection-level GeoParquet with partitioning
4. **Real-world catalogs** serve vector via OGC API - Features (not STAC items), but when cataloged in STAC, it's at collection level

## Decision

**Vector datasets (GeoParquet, Shapefile, GeoPackage, FlatGeobuf) are collection-level assets by default.**

**Raster datasets (GeoTIFF, COG, NetCDF) are item-level assets (unchanged from current behavior).**

### Decision Criteria

When `portolan add` encounters a file:

| File Type | Detection | Recommended Organization |
|-----------|-----------|--------------------------|
| GeoParquet (`.parquet` with `geo` metadata) | Via geoparquet-io | **Collection-level asset** |
| Shapefile (`.shp`) | File extension | **Collection-level asset** |
| GeoPackage (`.gpkg`) | File signature | **Collection-level asset** |
| FlatGeobuf (`.fgb`) | File extension | **Collection-level asset** |
| GeoTIFF (`.tif` with georeferencing) | Via rasterio | **Item-level asset** |
| COG (cloud-optimized GeoTIFF) | Via rasterio | **Item-level asset** |

**Exceptions:**

- **Time-series vector data** (e.g., daily traffic observations in GeoParquet) → Use items (one per time slice)
- **User override** → Allow `--item` flag to force item-level organization if user has a specific reason

### Collection Metadata

For vector data, collections include STAC Table Extension fields:

```json
{
  "type": "Collection",
  "stac_extensions": [
    "https://stac-extensions.github.io/table/v1.2.0/schema.json",
    "https://stac-extensions.github.io/projection/v1.1.0/schema.json"
  ],
  "table:columns": [
    {"name": "geometry", "type": "byte_array", "description": "WGS84 boundaries"},
    {"name": "municipality", "type": "string", "description": "Municipality name"},
    {"name": "population", "type": "int64", "description": "Total population"}
  ],
  "table:primary_geometry": "geometry",
  "table:row_count": 500,
  "proj:epsg": 4326,
  "assets": {
    "data": {
      "href": "./boundaries.parquet",
      "type": "application/vnd.apache.parquet",
      "roles": ["data"]
    }
  }
}
```

Metadata is extracted from:
- **GeoParquet**: `geo` metadata (CRS, geometry types, bbox), Parquet schema (columns, types, row count)
- **Shapefile/GeoPackage**: Via GDAL/OGR (geometry type, CRS, feature count)

### Example Structures

**Vector data (collection-level):**
```
catalog-root/
├── catalog.json
└── municipalities/
    ├── collection.json          ← table:columns metadata
    └── boundaries.parquet       ← Collection-level asset
```

**Raster data (item-level, unchanged):**
```
catalog-root/
├── catalog.json
└── landsat/
    ├── collection.json
    └── scene-2024-01-15/
        ├── item.json
        └── B1.tif               ← Item-level asset
```

**Mixed (vector collection + raster items):**
```
catalog-root/
├── catalog.json
├── boundaries/
│   ├── collection.json
│   └── admin.parquet            ← Vector (collection-level)
└── imagery/
    ├── collection.json
    └── scene-2024-01-15/
        ├── item.json
        └── red.tif              ← Raster (item-level)
```

## Consequences

### Positive

1. **STAC compliance**: Aligns with best practices for vector layer representation
2. **User intuition**: Users can place GeoParquet files directly in collection directories (matches natural organization)
3. **Semantic clarity**: Vector layer = collection, raster scene = item (clear conceptual model)
4. **Table Extension unlocked**: Can use `table:columns`, `table:primary_geometry`, `table:row_count` for rich metadata
5. **Partitioning support**: Collection-level assets enable Hive-style partitioning (see ADR-TBD for partitioning design)
6. **Interoperability**: Matches how Overture Maps and other cloud-native vector datasets are organized

### Negative

1. **Breaking change**: Existing item-level vector catalogs need migration (or can stay as-is with deprecation warning)
2. **Format detection required**: `portolan add` must reliably distinguish vector from raster (dependency on geoparquet-io, rasterio)
3. **Mixed patterns**: Collections can have both assets (vector) AND items (raster time-series), adding complexity
4. **Documentation burden**: Users need to understand vector vs raster distinction

### Neutral

1. **versions.json tracking**: Both collection-level and item-level assets are tracked; implementation is similar
2. **Cloud sync**: S3/GCS sync works identically for both patterns
3. **Validation**: Both require schema validation, just at different STAC levels

## Alternatives Considered

### Alternative 1: Item-Level for All Data (Status Quo)

**Rejected.**

**Pros:**
- Consistent organization (everything is an item)
- No format detection needed

**Cons:**
- Violates STAC best practices
- Semantic mismatch (vector layer ≠ single observation)
- Can't use Table Extension at collection level
- User confusion (why does a single GeoParquet file need an item directory?)

### Alternative 2: User-Specified on Every Add

Force users to explicitly choose:
```bash
portolan add boundaries.parquet --as-collection
portolan add scene.tif --as-item
```

**Rejected.**

**Pros:**
- Maximum user control
- No auto-detection errors

**Cons:**
- Poor UX (extra flag on every operation)
- Users must understand STAC semantics
- Violates "interactive + automatable" principle (should have smart defaults)

### Alternative 3: Collection-Level for Everything

Treat all data as collection-level assets.

**Rejected.**

**Pros:**
- Simple (no items at all)
- Matches some static catalogs

**Cons:**
- Loses spatiotemporal specificity for raster scenes
- Can't represent time-series properly
- Violates STAC conventions (items are standard for imagery)
- Breaks with existing STAC ecosystem tools (expect items for rasters)

### Alternative 4: Hybrid with Manual Override

Auto-detect vector vs raster, recommend pattern, allow override.

**Accepted (this is the decision).**

**Pros:**
- Smart defaults reduce cognitive load
- Users can override for edge cases
- Aligns with STAC best practices
- Supports both patterns when needed (e.g., time-series vector data)

**Cons:**
- Format detection can fail (mitigated by clear error messages)
- Adds complexity to `portolan add` (acceptable trade-off)

## Implementation Notes

### Format Detection

**GeoParquet:**
```python
import pyarrow.parquet as pq

# Read Parquet metadata
parquet_file = pq.ParquetFile("data.parquet")
metadata = parquet_file.schema_arrow.metadata

# Check for 'geo' key (GeoParquet spec)
if b'geo' in metadata:
    # Vector data → collection-level
    geo_metadata = json.loads(metadata[b'geo'])
    primary_geometry = geo_metadata.get('primary_column')
```

**Raster (GeoTIFF/COG):**
```python
import rasterio

with rasterio.open("data.tif") as src:
    if src.crs is not None:
        # Georeferenced raster → item-level
```

### Schema Extraction

Leverage existing tools:
- **geoparquet-io**: Extract `geo` metadata (CRS, geometry types, primary column)
- **PyArrow**: Read Parquet schema (column names, types, row count)
- **Projection Extension**: Map GeoParquet CRS to `proj:epsg`

### Migration Path

Existing item-level vector catalogs:
1. **Option A**: Leave as-is (valid STAC, just not optimal)
2. **Option B**: `portolan migrate flatten-items` command (future enhancement)
3. **Option C**: `portolan scan` warns about non-recommended patterns

For MVP: Option A (no forced migration).

### Testing

- Unit tests: Format detection (GeoParquet, Shapefile, GeoTIFF)
- Integration tests: Collection creation with Table Extension
- Validation tests: STAC validators (pystac, stac-validator)
- Real-world tests: Den Haag datasets, Overture Maps samples

## Related

- **ADR-0022** (Git-Style Implicit Tracking): Clarified to allow collection-level assets for vector data
- **ADR-0032** (Nested Catalogs with Flat Collections): Defines organizational structure
- **Issue #226**: Catalog structure patterns (parent issue)
- **Issue #231**: Collection-level assets and nested collections (research ticket)
- **Issue #233**: STAC Table Extension integration

## References

1. [STAC Best Practices - Representing Vector Layers](https://github.com/radiantearth/stac-spec/blob/master/best-practices.md#representing-vector-layers-in-stac)
2. [STAC Table Extension v1.2.0](https://github.com/stac-extensions/table)
3. [GeoParquet Specification](https://geoparquet.org/)
4. [Overture Maps Data Access](https://docs.overturemaaps.org/guides/geoparquet/)
5. [STAC Collection Specification](https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md)
