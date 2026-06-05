# ADR-0049: STAC-GeoParquet as Scalability Requirement

## Status
Accepted

> Migrated from spec/DECISIONS.md ADR-004 (2025-02-13)

## Context

Large STAC collections with thousands of items are slow to search without a STAC API server. STAC-GeoParquet enables efficient queries over static files by encoding STAC item metadata in a columnar format that supports predicate pushdown and spatial filtering.

The question is whether STAC-GeoParquet should be optional, recommended, or required.

## Decision

STAC-GeoParquet (`items.parquet`) is:

1. **Best practice** for all collections with >100 items
2. **Required** for collections with >1000 items

The threshold balances:
- Overhead of generating/maintaining the file
- Performance benefit for consumers
- Tooling maturity (still evolving)

## Consequences

### What becomes easier
- Consumers can query large catalogs without a STAC API
- Spatial filtering, temporal filtering, and property queries work efficiently
- Static hosting remains viable for large catalogs

### What becomes harder
- Tooling must generate and maintain `items.parquet`
- Schema changes require regenerating the file
- Two representations of item metadata to keep in sync (JSON + Parquet)

### Trade-offs
- We accept generation overhead for query performance
- We phase in the requirement (best practice → required) as tooling matures

## Alternatives Considered

### 1. No STAC-GeoParquet requirement
**Rejected:** Large catalogs would be unusable without a STAC API, defeating the static-first design.

### 2. Require for all collections
**Rejected:** Overkill for small catalogs. Adds friction for new users.

### 3. Different threshold
1000 items is a reasonable balance—below this, JSON file enumeration is tolerable; above it, query performance degrades noticeably.

## References

- [STAC-GeoParquet spec](https://github.com/stac-utils/stac-geoparquet)
- [spec/best-practices.md](../../spec/best-practices.md#stac-geoparquet)
