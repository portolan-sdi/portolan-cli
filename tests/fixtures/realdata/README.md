# Real-World Test Fixtures

Production data samples for testing Portolan's orchestration layer.

**Full documentation:** `context/shared/documentation/test-fixtures.md`

## Files

| File | Size | Features | Tests |
|------|------|----------|-------|
| `nwi-wetlands.parquet` | 1.5MB | 1,000 | Complex polygons with holes |
| `open-buildings.parquet` | 146KB | 1,000 | Bulk polygon handling |
| `road-detections.parquet` | 92KB | 1,000 | LineString geometries |
| `fieldmaps-boundaries.parquet` | 2.3MB | 3 | Antimeridian crossing |
| `rapidai4eo-sample.tif` | 205KB | N/A | COG raster handling |

## Important

**Portolan orchestrates — it does not validate geometry.**

These fixtures test that Portolan correctly:
- Passes files to upstream libraries without corruption
- Extracts metadata for STAC catalog entries
- Computes bounding boxes (including antimeridian edge case)

They do NOT test geometry validity or format conversion (that's geoparquet-io/rio-cogeo's job).

## Source

Canonical copies at `s3://us-west-2.opendata.source.coop/nlebovits/portolan-test-fixtures/sources/`

See `context/shared/documentation/test-fixtures.md` for provenance (SQL/commands to regenerate).
