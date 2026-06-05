# ADR-0051: SELF_CONTAINED Catalog Type

## Status
Accepted

> Migrated from spec/DECISIONS.md ADR-006 (2025-02-13)

## Context

PySTAC supports multiple catalog types with different linking strategies:

- **ABSOLUTE_PUBLISHED**: All links are absolute URLs
- **RELATIVE_PUBLISHED**: Links are relative, hrefs point to published location
- **SELF_CONTAINED**: All links are relative, catalog is fully portable

We need to choose which type Portolan catalogs use.

## Decision

Portolan catalogs **MUST** use PySTAC's `SELF_CONTAINED` catalog type with relative links.

This means:
- All `rel: "self"`, `"root"`, `"parent"`, `"child"`, `"item"` links use relative paths
- Asset `href` values are relative within the catalog structure
- No absolute filesystem paths appear in metadata

## Consequences

### What becomes easier
- **Portability**: Catalogs can be moved between buckets, hosts, or local paths without rewriting links
- **Debugging**: No leaked local paths (e.g., `/home/user/...`) in published metadata
- **Simplicity**: One linking strategy, no ambiguity

### What becomes harder
- Tooling must normalize hrefs before saving (convert absolute to relative)
- Consumers must resolve relative paths against their access method

### Trade-offs
- We prioritize portability over convenience of absolute URLs
- This is the standard practice for static STAC catalogs

## Alternatives Considered

### 1. ABSOLUTE_PUBLISHED
**Rejected:** Hardcodes the hosting location. Moving the catalog requires rewriting all links.

### 2. RELATIVE_PUBLISHED
**Rejected:** Hybrid approach that still has some absolute paths. More complex, less portable.

### 3. Allow user choice
**Rejected:** Inconsistency across catalogs. Tooling would need to handle all cases.

## References

- [PySTAC CatalogType](https://pystac.readthedocs.io/en/latest/api/catalog.html#pystac.CatalogType)
- [STAC Best Practices: Self-Contained](https://github.com/radiantearth/stac-spec/blob/master/best-practices.md#self-contained-stac)
