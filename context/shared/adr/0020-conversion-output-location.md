# ADR-0020: Conversion Output Location Strategy

## Status
Accepted

## Context

When converting files to cloud-native formats, where should output go? Options:
1. Side-by-side (new file next to original)
2. In-place (overwrite original)
3. Separate output directory

## Decision

**Format-dependent strategy:**

| Format | Strategy | Example |
|--------|----------|---------|
| Vector | Side-by-side | `roads.shp` → `roads.parquet` (both exist) |
| Raster | In-place | `elevation.tif` → `elevation.tif` (COG replaces original) |

**Flags:**
- `--replace` — Delete original vector files after successful conversion
- Default behavior preserves originals for vectors

## Consequences

### Why in-place for rasters
- Same `.tif` extension for COG and non-COG
- Users expect TIFF files; changing extension breaks workflows
- COG is strictly better; no reason to keep non-COG version

### Why side-by-side for vectors
- Different extensions (`.shp` vs `.parquet`)
- Users may need originals for legacy tools
- Explicit `--replace` makes deletion intentional

### Safety
- Conversion must succeed AND validate before any deletion
- Original never deleted on failure
