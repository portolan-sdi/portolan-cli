# Portolan Specification

This directory contains the canonical Portolan specification.

The **portolan-cli repository is the source of truth** for the spec. The
[portolan-spec](https://github.com/portolan-sdi/portolan-spec) repository is a
read-only mirror, automatically synced via CI on every merge to main.

## What is Portolan?

Portolan is a STAC profile—not a competing specification. It adds requirements
and best practices on top of [STAC](https://stacspec.org/) for publishing
cloud-native geospatial data.

## Specification

- [Core requirements](core.md) - Mandatory requirements for all Portolan catalogs
- [Catalog structure](structure.md) - Directory layout and file organization
- [Version manifest](versions.md) - `versions.json` schema for version tracking
- [File extensions](extensions.md) - Recognized file types and classification
- [Format addenda](formats/) - Per-format specifications
  - [Vector data](formats/vector.md)
  - [Raster data](formats/raster.md)
  - [Point clouds](formats/pointcloud.md)
- [Best practices](best-practices.md) - Recommended conventions
- [AI & LLM integration](ai-integration.md) - llms.txt requirements for agent discoverability

## Architectural Decisions

Spec-related decisions are tracked in the CLI repository's ADR directory:

- [ADR-0005: versions.json as Single Source of Truth](../context/shared/adr/0005-versions-json-source-of-truth.md)
- [ADR-0032: Nested Catalogs with Flat Collections](../context/shared/adr/0032-nested-catalogs-with-flat-collections.md)
- [ADR-0049: STAC-GeoParquet as Scalability Requirement](../context/shared/adr/0049-stac-geoparquet-scalability.md)
- [ADR-0050: PMTiles as Visualization Requirement](../context/shared/adr/0050-pmtiles-visualization-requirement.md)
- [ADR-0051: SELF_CONTAINED Catalog Type](../context/shared/adr/0051-self-contained-catalog-type.md)
- [ADR-0052: Require llms.txt for AI/LLM Integration](../context/shared/adr/0052-llms-txt-requirement.md)

For all architectural decisions, see [context/shared/adr/](../context/shared/adr/).

## Machine-Readable Schemas

- `schema/` — JSON schemas and validation rules for `versions.json`, `catalog.json`, etc.

## Examples

See [examples/](examples/) for reference implementations.

## Making Changes

To propose spec changes:

1. Open a PR in this repository (portolan-cli)
2. Changes to `spec/` trigger review from spec maintainers
3. On merge, CI syncs to portolan-spec automatically

See [ADR-0048](../context/shared/adr/0048-cli-as-spec-source.md) for rationale.
