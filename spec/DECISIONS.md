# Architectural Decision Records

This document captures key architectural decisions made during the development of the Portolan specification.

## Format

Each decision includes:
- **Decision**: What was decided
- **Context**: Why this decision was needed
- **Rationale**: Why this approach was chosen
- **Date**: When the decision was made
- **Status**: Accepted, Superseded, or Under Review

---

## Template for New Decisions

**Date**: YYYY-MM-DD
**Status**: Under Review | Accepted | Superseded

**Decision**: [What was decided]

**Context**: [Why this decision was needed]

**Rationale**: [Why this approach was chosen over alternatives]

**Consequences**: [What this decision enables or constrains]

---

## Decisions

### ADR-001: Flat Catalog Hierarchy

**Date**: 2025-02-13
**Status**: Accepted

**Decision**: Portolan catalogs use a flat hierarchy—collections contain items directly, with no nested sub-collections.

**Context**: STAC supports arbitrary nesting of catalogs and collections. We needed to decide whether to allow this flexibility or constrain the structure.

**Rationale**:
- Simplifies tooling (no recursive traversal needed)
- Clear versioning boundaries (each collection has one versions.json)
- Easier to understand and navigate
- Avoids ambiguity about where metadata belongs

**Consequences**: Users who want nested organization must flatten their structure (e.g., `census-2020-tracts` instead of `census/2020/tracts`).

---

### ADR-002: versions.json as Single Source of Truth

**Date**: 2025-02-13
**Status**: Accepted

**Decision**: Each collection has a single `versions.json` file that serves as version history, sync manifest, and integrity checksums.

**Context**: We needed to track what files exist, what changed between versions, and what's synced to remote storage.

**Rationale**:
- Single file to understand (no reconciling multiple sources)
- Atomic updates (one file write per version bump)
- Simple sync (diff one JSON file to know what to push)
- Corruption detection (checksums catch tampering or bit rot)

**Consequences**:
- File grows with version count (mitigated by future prune command)
- Single-writer constraint (multi-user deferred to portolake plugin)

See: [versions.md](versions.md)

---

### ADR-003: Accept Non-Cloud-Native Formats with Warnings

**Date**: 2025-02-13
**Status**: Accepted

**Decision**: Portolan accepts non-cloud-native formats (GeoJSON, Shapefile, GeoPackage) but emits warnings encouraging conversion.

**Context**: Many users have legacy data in non-cloud-native formats. We needed to decide whether to reject these or accept them.

**Rationale**:
- Reduces friction for new users
- Guides users toward best practices over time
- Validation still passes (warns, doesn't fail)
- Conversion can happen later

**Consequences**: Catalogs may contain non-optimal formats. Tooling should encourage (not force) conversion.

See: [ADR-0014 in portolan-cli](https://github.com/portolan-sdi/portolan-cli/blob/main/context/shared/adr/0014-accept-non-cloud-native-formats.md)

---

### ADR-004: STAC-GeoParquet as Scalability Best Practice

**Date**: 2025-02-13
**Status**: Accepted

**Decision**: STAC-GeoParquet is a best practice for collections with many items, becoming required at >1000 items.

**Context**: Large collections with thousands of STAC items are slow to search without an API. STAC-GeoParquet enables efficient queries.

**Rationale**:
- Best practice now, required later (phased approach)
- Tooling still maturing
- Enables search without STAC API server
- Threshold of 1000 items balances overhead vs. benefit

**Consequences**: Large catalogs must include `items.parquet`. Tooling needs to generate and maintain this file.

See: [best-practices.md#stac-geoparquet](best-practices.md#stac-geoparquet)

---

### ADR-005: PMTiles as Visualization Best Practice

**Date**: 2025-02-13
**Status**: Accepted

**Decision**: PMTiles derivatives are a best practice for vector datasets, becoming required at >100 MB.

**Context**: Large GeoParquet files are slow to render on web maps. PMTiles provide efficient tile-based rendering.

**Rationale**:
- Best practice now, required later (phased approach)
- Tippecanoe dependency has platform-specific installation requirements
- Threshold of 100 MB balances generation cost vs. rendering benefit

**Consequences**: Large vector datasets must include PMTiles. Tooling needs to handle tippecanoe installation.

See: [best-practices.md#pmtiles](best-practices.md#pmtiles)

---

### ADR-006: SELF_CONTAINED Catalog Type

**Date**: 2025-02-13
**Status**: Accepted

**Decision**: Portolan catalogs must use pystac's `SELF_CONTAINED` catalog type with relative links.

**Context**: STAC supports multiple catalog types with different linking strategies.

**Rationale**:
- Portability: catalogs can be moved between buckets/hosts
- No absolute filesystem paths leak into metadata
- Simpler to understand and debug

**Consequences**: All links are relative. Tooling must normalize hrefs before saving.
