# Research: Cloud-Native Dataset Warnings

**Feature**: 002-cloud-native-warnings
**Date**: 2025-02-09

## Research Tasks

### 1. COG Detection Pattern

**Question**: How to reliably detect if a TIFF is a COG vs a regular TIFF?

**Decision**: Use rio-cogeo's `cog_validate` function.

**Rationale**:
- rio-cogeo is already a project dependency (used for COG creation)
- `cog_validate` returns validation info that indicates if a file meets COG requirements
- Avoids reimplementing COG detection logic (aligns with ADR-0010: delegate to upstream)

**Alternatives Considered**:
- Manual check for overview levels and tiling → Rejected (reimplements upstream logic)
- Extension-based detection (.cog.tif) → Rejected (unreliable, many COGs use .tif)
- GDAL metadata inspection → Rejected (rio-cogeo already wraps this properly)

**Implementation Pattern**:
```python
from rio_cogeo.cogeo import cog_validate

def is_cloud_optimized_geotiff(path: Path) -> bool:
    """Check if a TIFF file is a Cloud-Optimized GeoTIFF."""
    try:
        is_valid, errors, warnings = cog_validate(str(path))
        return is_valid
    except Exception:
        return False  # Any validation error means it's not a valid COG
```

### 2. GeoParquet Detection Pattern

**Question**: How to detect if a Parquet file is GeoParquet (has geo metadata)?

**Decision**: Use geoparquet-io's metadata reading or check for geoparquet-io compatible loading.

**Rationale**:
- geoparquet-io is already a project dependency
- GeoParquet files have specific metadata in the Parquet schema
- The library already handles this detection internally

**Alternatives Considered**:
- Manual parquet metadata inspection → Rejected (reimplements upstream)
- Extension-based (.geoparquet vs .parquet) → Rejected (unreliable)

**Implementation Pattern**:
```python
import pyarrow.parquet as pq

def is_geoparquet(path: Path) -> bool:
    """Check if a Parquet file has GeoParquet metadata."""
    try:
        metadata = pq.read_metadata(str(path))
        schema_metadata = metadata.schema.to_arrow_schema().metadata or {}
        # GeoParquet files have 'geo' key in schema metadata
        return b'geo' in schema_metadata
    except Exception:
        return False
```

### 3. Format Display Abbreviations

**Question**: What abbreviations to use in warning messages?

**Decision**: Use standard, recognizable abbreviations.

| Format | Abbreviation | Target |
|--------|--------------|--------|
| Shapefile | SHP | GeoParquet |
| GeoJSON | GeoJSON | GeoParquet |
| GeoPackage | GPKG | GeoParquet |
| CSV | CSV | GeoParquet |
| TIFF (non-COG) | TIFF | COG |
| JPEG2000 | JP2 | COG |
| NetCDF | NetCDF | (unsupported) |
| HDF5 | HDF5 | (unsupported) |
| LAS/LAZ (non-COPC) | LAS | (unsupported) |

**Rationale**:
- Uses common industry abbreviations
- GeoJSON kept full (commonly written this way)
- Avoids confusion (e.g., "SHP" universally recognized)

### 4. Cloud-Native Format List

**Question**: What formats are considered cloud-native (no warning)?

**Decision**: Based on spec input, the following are cloud-native:

| Format | Extension(s) | Detection Method |
|--------|--------------|------------------|
| GeoParquet | .parquet | Has 'geo' metadata |
| Parquet (non-geo) | .parquet | Valid Parquet, no geo metadata |
| COG | .tif, .tiff | Passes cog_validate |
| FlatGeobuf | .fgb | Extension-based |
| COPC | .copc.laz | Extension-based |
| PMTiles | .pmtiles | Extension-based |
| Zarr | directory/.zarr | Path-based |
| Raquet | .raquet | Extension-based |

**Rationale**: These formats are designed for cloud-native access (HTTP range requests, columnar access, etc.)

### 5. Unsupported Format List

**Question**: What formats are explicitly unsupported?

**Decision**: Based on spec input:

| Format | Extension(s) | Error Message |
|--------|--------------|---------------|
| NetCDF | .nc, .netcdf | "NetCDF is not yet supported. Support coming soon." |
| HDF5 | .h5, .hdf5 | "HDF5 is not yet supported. Support coming soon." |
| LAS/LAZ (non-COPC) | .las, .laz | "LAS/LAZ point clouds require COPC format. Use pdal or other tools to convert." |

**Rationale**:
- These formats are common in geospatial but not yet handled
- "Coming soon" sets user expectations for future support
- LAS/LAZ message is more specific (COPC is the cloud-native alternative)

## Resolved Clarifications

No NEEDS CLARIFICATION markers in the spec. All technical decisions resolved above.

## Dependencies for Implementation

1. **rio-cogeo** (already installed) - for COG detection
2. **pyarrow** (already installed) - for GeoParquet metadata inspection
3. No new dependencies required
