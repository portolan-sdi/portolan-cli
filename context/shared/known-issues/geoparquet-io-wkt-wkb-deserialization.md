# geoparquet-io WKT/WKB Deserialization Issue

## Status
Fixed in geoparquet-io v0.4.0+ (PR #233)

## Summary
Geometry deserialization behavior differed between DuckDB and PyArrow backends, causing inconsistent results when reading GeoParquet files with WKT or WKB encoded geometries.

## Details

When reading GeoParquet files, geoparquet-io supports multiple backends:
- **DuckDB**: Fast, SQL-based reading
- **PyArrow**: Native Parquet reading

Prior to the fix, these backends handled WKT (Well-Known Text) and WKB (Well-Known Binary) geometry encodings differently, leading to:
- Different geometry representations depending on which backend was used
- Potential data inconsistencies in downstream processing
- Subtle bugs that only appeared with certain backend/encoding combinations

## Resolution

Fixed in [geoparquet-io PR #233](https://github.com/geoparquet/geoparquet-io/pull/233).

**Action required**: Ensure Portolan uses geoparquet-io >= 0.4.0 (or whichever version includes this fix).

## Impact on Portolan

- **Conversion**: When converting vector formats to GeoParquet, geometry encoding is now consistent
- **Validation**: Metadata extraction will return consistent geometry types regardless of backend
- **Testing**: Integration tests should verify both backends produce identical results

## Workaround (if using older versions)

If stuck on an older geoparquet-io version:
1. Explicitly specify the backend when reading
2. Normalize geometries after reading
3. Prefer PyArrow backend for consistency (DuckDB had the divergent behavior)

## References

- [geoparquet-io PR #233](https://github.com/geoparquet/geoparquet-io/pull/233)
- [geoparquet-io GitHub](https://github.com/geoparquet/geoparquet-io)
