# ADR-0010: Delegate Conversion and Validation to Upstream Libraries

## Status
Accepted

## Context
Portolan orchestrates cloud-native geospatial data workflows. Two key operations—format conversion and input validation—could be implemented within Portolan or delegated to upstream libraries.

The libraries we depend on already provide these capabilities:
- **geoparquet-io**: Converts vector formats (Shapefile, GeoJSON, GeoPackage, CSV) to GeoParquet with validation
- **rio-cogeo**: Converts rasters to Cloud-Optimized GeoTIFF with validation

Duplicating this functionality would:
1. Increase maintenance burden
2. Risk divergence from upstream behavior
3. Add complexity without adding value

## Decision
Portolan delegates all format conversion and input validation to upstream libraries:

1. **Vector conversion**: geoparquet-io handles GeoJSON/Shapefile/GeoPackage → GeoParquet
2. **Raster conversion**: rio-cogeo handles GeoTIFF → COG
3. **Validation**: Errors from upstream libraries propagate directly to users

Portolan role is limited to:
- **Format detection**: Routing inputs to the correct library (vector vs raster)
- **Orchestration**: Calling the right library with appropriate parameters
- **Output management**: Placing converted files in the .portolan/ structure

## Consequences

**Easier:**
- Less code to maintain
- Automatic benefit from upstream improvements
- Consistent behavior with standalone GPIO/rio-cogeo usage
- Clear separation of concerns

**Harder:**
- Error messages reference upstream libraries users may not know
- Cannot customize validation rules without forking upstream
- Dependent on upstream release cycles for bug fixes

## Alternatives Considered

### Wrap upstream errors in Portolan exceptions
Rejected because:
- Adds abstraction layer without clear benefit
- Hides useful debugging information
- Maintenance burden tracking upstream error types

### Implement custom validation
Rejected because:
- Duplicates upstream functionality
- Risk of divergence (works in GPIO but not Portolan)
- Violates DRY principle
