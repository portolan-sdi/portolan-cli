# ADR-0047: Non-Geo Tabular Data Support

## Status
Accepted

## Context

Portolan is a geospatial catalog tool. However, local governments and organizations often have tabular data (CSV, TSV, Excel, plain Parquet) that relates to the same geographic area as their geospatial layers but contains no geometry itself. Examples include census demographics, permit records, or budget allocations linked by tract IDs or parcel numbers.

Prior to this change, Portolan had three inconsistent definitions of "tabular":
- `scan_classify.py`: `{.csv, .tsv, .xlsx, .xls}` (no `.parquet`)
- `constants.py`: `{.csv, .tsv, .parquet}`
- `GEO_ASSET_EXTENSIONS`: included `.parquet` (regardless of whether it had geo metadata)

This led to confusion: a plain Parquet file (no geometry) was classified as `GEO_ASSET` but had no path to become a collection asset.

### Forces at play

1. **Scope boundary**: Portolan should remain a geospatial-first tool, not become a general data catalog
2. **User friction**: Users shouldn't need a separate tool for companion tabular data
3. **STAC compliance**: STAC Collections require spatial extent, but tabular data has none intrinsically
4. **Consistency**: Conversion should use the same pipeline (geoparquet-io) for both geo and tabular data
5. **Opt-in safety**: Sweeping in stray `.csv` files would be disruptive

## Decision

### 1. Scope: Geospatial catalog that tolerates companion tabular

Portolan is NOT becoming a tabular-first data catalog. The bar for tabular support is "a local government has mostly geospatial layers plus a few tables about the same place." General-purpose tabular cataloging is out of scope.

### 2. Classification: GeoParquet vs Plain Parquet via metadata peeking

`.parquet` files are no longer in `GEO_ASSET_EXTENSIONS`. Instead, `is_geoparquet()` peeks at the Parquet schema metadata:
- `b"geo"` key present → `GEO_ASSET` (route through geo pipeline)
- No `b"geo"` key → `TABULAR_DATA` (route through tabular pipeline)

This is a fast O(1) check that reads only the Parquet footer.

### 3. Opt-in via config: `tabular.enabled` (default: false)

Tabular support is opt-in at the catalog or collection level via `.portolan/config.yaml`:

```yaml
tabular:
  enabled: true   # Track standalone tabular files as collection-level assets
  convert: true   # Convert CSV/TSV/Excel to Parquet (default)
```

When `tabular.enabled` is false (default):
- Tabular files WITH a companion geo file → tracked as companion assets (ADR-0028 behavior)
- Tabular files WITHOUT a companion geo file → **fail with helpful hint** explaining how to enable

When `tabular.enabled` is true:
- Standalone tabular files → tracked as collection-level assets
- A collection can be tabular-only (no geo data)

### 4. Conversion: Route through geoparquet-io

Tabular files are converted via `gpio.convert().write()` — the same pipeline as geo files. This ensures:
- Consistent compression (ZSTD by default)
- Consistent row-group sizing
- No code duplication (per ADR-0010)

geoparquet-io v1.2.0+ handles `geometry_column=None` correctly:
- `gpio.convert()` on a CSV without geometry logs "Reading as plain table"
- `gpio.Table.write()` produces valid plain Parquet (no `geo` metadata key)

### 5. Spatial extent: AOI inheritance from sibling collections

STAC requires spatial extent for collections, but tabular data has none intrinsically. Resolution order:

1. **Explicit bbox** in `metadata.yaml` (manual override)
2. **Inherit from sibling geo collections** — compute union bbox (the default)
3. **Global fallback** `[-180, -90, 180, 90]` when no siblings exist

Rationale for inherit-by-default: companion tabular data is almost always *about* the same area as the catalog's geo data. "Extent" for tabular collections means "the AOI this data pertains to," not "the geometry of the data."

### 6. Modeling: Collection-level assets

Tabular files become collection-level assets (per ADR-0031), not items. This avoids the complexity of null-geometry STAC items and matches the vector data pattern.

Exception: **Partitioned tabular data** (e.g., temporally partitioned) will use items, where the partition key becomes the item's temporal interval and `geometry: null`. This follows the existing partition extension (ADR-0042).

## Consequences

### What becomes easier

- Users can catalog tabular companion data without a separate tool
- Consistent Parquet output across all file types (compression, row groups)
- Clear error messages guide users toward `tabular.enabled: true` when needed
- Tabular collections automatically inherit sensible spatial extents

### What becomes harder

- Maintaining the classification boundary between GeoParquet and plain Parquet
- Users expecting tabular-first features may be confused by the geospatial framing

### Trade-offs

| Decision | Pro | Con |
|----------|-----|-----|
| Opt-in default | Doesn't sweep in stray CSVs | Extra config step for tabular-only use |
| AOI inheritance | Zero-friction default extent | "Extent" semantics differ from geo collections |
| gpio routing | Single conversion pipeline | Dependency on geoparquet-io for non-geo files |

## Alternatives considered

### 1. Separate tabular catalog tool
Rejected: Creates tool fragmentation for a common use case (mixed geo + tabular).

### 2. Tabular enabled by default
Rejected: Would sweep in stray CSV/Excel files, causing confusion and catalog bloat.

### 3. Require explicit bbox for tabular collections
Rejected: Too much friction. Most tabular data relates to the catalog's AOI.

### 4. Use items with null geometry for all tabular
Rejected: Adds complexity without benefit. Collection-level assets are simpler for the common case of small tabular files.

### 5. Reimplement Parquet conversion with PyArrow directly
Rejected: Violates ADR-0010 (delegate to upstream libraries). gpio already handles compression, row-group sizing, and format detection.
