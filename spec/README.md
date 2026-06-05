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
- [Architectural decisions](DECISIONS.md) - Key design decisions and rationale

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
