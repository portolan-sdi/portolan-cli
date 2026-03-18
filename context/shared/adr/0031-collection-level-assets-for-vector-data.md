# ADR-0031: Collection-Level Assets for Vector Data

## Status
Adopted

## Context

Portolan catalogs geospatial data using STAC (SpatioTemporal Asset Catalog), but the STAC specification treats vector and raster data differently at a fundamental level. Per the ([STAC Best Practices](https://github.com/radiantearth/stac-spec/blob/master/best-practices.md#representing-vector-layers-in-stac)):

> "A shapefile or geopackage corresponds to a **Collection, not a single Item**. The ideal thing to do with one of those is to serve it with OGC API - Features standard. This allows each feature in the shapefile/geopackage to be represented online, and enables querying of the actual data."

Basically, raster data should be treated as an item with assets, and vector data as a collection. This conflicts with our current enforcement of item-level organization for all data (ADR-0022: Git-Style Implicit Tracking).

## Decision

Vector datasets are collection-level assets by default. The only exception is _partitioned_ vector datasets (e.g., hive partitioning), in which case partitions are treated as items. Raster datasets remain item-level assets, so no change from the existing behavior.

Generally, for vector data, we favor a single file, especially if it's below ~2GB. For larger datasets, we recommend partitioning, in which case each partition becomes an item. (Partitions can be Hive-style (`date=2024-01-15/`) or simple file splits.) If a provider splits by year/theme, each is treated as a separate dataset, i.e., its own collection, even if the schema is the same.

### Organization Patterns

| Pattern | Detection | Organization |
|---------|-----------|--------------|
| Single GeoParquet/Shapefile/GeoPackage | Via format detection | **Collection-level asset** |
| Partitioned GeoParquet (Hive or file splits) | Detect partition structure | **Item per partition** |
| GeoTIFF/COG | Via rasterio | **Item-level asset** |

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

### Example Structures

**Small vector dataset (collection-level):**
```
catalog-root/
├── catalog.json
└── municipalities/
    ├── collection.json          ← table:columns metadata
    └── boundaries.parquet       ← Collection-level asset (< 2GB)
```

**Large partitioned vector dataset (items per partition):**
```
catalog-root/
├── catalog.json
└── building-footprints/
    ├── collection.json          ← Describes whole dataset
    ├── country=NL/
    │   ├── item.json            ← Item for this partition
    │   └── data.parquet
    └── country=BE/
        ├── item.json            ← Item for this partition
        └── data.parquet
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

## Consequences

This decision makes us more STAC-compliant, is more intuitive, addresses the question of partitioning, and makes it easier to incorporate the STAC table extension.

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

### Alternative 2: Always Use Items for Partitioned Data

Treat each partition file as collection-level asset (not item).

**Rejected per Chris Holmes' guidance.**

**Pros:**
- Simpler (no items for vector data ever)
- Matches "vector = collection" rule strictly

**Cons:**
- Loses spatiotemporal discovery (can't search by date/region)
- Partitions are conceptually "slices" of the dataset (items)
- Makes STAC API search impossible for partitioned data

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

Auto-detect vector vs raster, recommend pattern, allow user override flags.

**Considered but rejected in favor of automatic detection only.**

During design, we considered adding flags like `--collection-level` or `--item-level` to allow users to override automatic detection. However, this was intentionally removed (see Decision section) because:

1. STAC best practices are clear and unambiguous for most cases
2. The four rules (single file → collection, partitioned → items per partition, raster → items, provider splits → separate collections) cover the valid patterns
3. Manual overrides would allow STAC-non-compliant catalogs
4. Edge cases should be resolved through better detection logic, not user flags

**Pros (if implemented):**
- Could handle ambiguous edge cases
- Gives users explicit control

**Cons (why rejected):**
- Violates STAC spec clarity (the patterns are well-defined)
- Creates maintenance burden (support invalid structures)
- Detection logic should be improved instead of bypassed
- No legitimate use case identified that Rules 1-4 don't cover

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
