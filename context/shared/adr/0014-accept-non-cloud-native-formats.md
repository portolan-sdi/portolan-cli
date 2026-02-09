# ADR-0014: Accept Non-Cloud-Native Formats

## Status
Accepted

## Context
Users have valid reasons to store non-cloud-native data:

- **Interoperability**: Downstream tools may require Shapefile/GeoJSON
- **Organizational mandates**: Some organizations standardize on specific formats
- **Incremental adoption**: Users want to explore Portolan before committing to conversion
- **Archival fidelity**: Exact byte preservation matters for some workflows
- **Small files**: Conversion overhead may exceed benefit for tiny datasets

Rejecting these formats creates adoption friction and forces premature decisions.

## Decision
1. **All formats accepted and stored as-is by default** - no surprise transformations
2. **Warning displayed for non-cloud-native formats** - encourages conversion without forcing it
3. **`--convert` flag available** for simple legacy formats (Shapefile → GeoParquet, TIFF → COG)
4. **Complex formats (NetCDF, HDF5) accepted but not auto-convertible** - these require domain-specific decisions
5. **Thumbnails/previews skipped for non-cloud-native rasters** - feature degradation, not rejection

Format classification:
| Status | Formats |
|--------|---------|
| Cloud-native (silent) | GeoParquet, Parquet, COG, FlatGeobuf, COPC, PMTiles, Zarr |
| Convertible (warn) | Shapefile, GeoJSON, GeoPackage, CSV, JP2, non-COG TIFF |
| Unsupported (reject) | NetCDF, HDF5, LAS/LAZ (non-COPC) |

## Consequences
- ✅ Lower adoption barrier - users can start with existing data
- ✅ No surprise transformations - explicit conversion only
- ✅ Clear upgrade path - warnings guide users toward cloud-native
- ⚠️ Catalog may mix formats - documented trade-off
- ⚠️ Some features unavailable for legacy formats - preview, streaming

## Alternatives Considered

### Reject non-cloud-native formats entirely
Rejected: Too high an adoption barrier. Users would need to convert before even trying Portolan.

### Silent acceptance (no warnings)
Rejected: Users should understand the trade-offs. Warnings educate without blocking.

### Auto-convert everything
Rejected: Violates principle of no surprise transformations. Users may have valid reasons for original format.
