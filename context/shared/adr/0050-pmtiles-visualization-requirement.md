# ADR-0050: PMTiles as Visualization Requirement

## Status
Accepted

> Migrated from spec/DECISIONS.md ADR-005 (2025-02-13)

## Context

Large GeoParquet files (100+ MB) are impractical to render directly on web maps. Clients would need to download the entire file before rendering, resulting in unacceptable load times.

PMTiles is a single-file vector tile format that enables efficient HTTP range-request access, making it ideal for static hosting and web map visualization.

## Decision

PMTiles derivatives are:

1. **Best practice** for vector datasets >10 MB
2. **Required** for vector datasets >100 MB

The threshold balances:
- Tippecanoe dependency (platform-specific installation)
- Generation time for large datasets
- Visualization performance benefit

## Consequences

### What becomes easier
- Large vector datasets render efficiently on web maps
- No tile server required (static files + range requests)
- Progressive loading for better UX

### What becomes harder
- Tippecanoe must be installed (C++ dependency, not pip-installable)
- Generation can be slow for complex geometries
- Tile settings affect visualization fidelity (zoom levels, simplification)

### Trade-offs
- We accept the Tippecanoe dependency for visualization performance
- We phase in the requirement (best practice → required) to ease adoption

## Alternatives Considered

### 1. MBTiles instead of PMTiles
**Rejected:** MBTiles requires a tile server or SQLite access. PMTiles works with pure HTTP range requests on static hosting.

### 2. No tile requirement
**Rejected:** Large vector datasets would be unusable for web visualization, a core use case for geospatial catalogs.

### 3. Lower threshold (10 MB)
**Considered but deferred:** 10 MB is the "best practice" threshold; 100 MB is the "required" threshold. This balances accessibility with performance.

## References

- [PMTiles spec](https://github.com/protomaps/PMTiles)
- [Tippecanoe](https://github.com/felt/tippecanoe)
- [spec/best-practices.md](../../spec/best-practices.md#pmtiles)
