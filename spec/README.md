# Portolan Specification

This directory contains the canonical Portolan specification.

**Spec version: `0.1.1`** ([SemVer](https://semver.org/), pre-1.0). The
canonical machine-readable home is
[`schema/spec-version.json`](schema/spec-version.json). See
[Versioning](#versioning) below for the bump policy.

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
  - [Tabular (non-geospatial) data](formats/tabular.md)
- [Best practices](best-practices.md) - Recommended conventions
- [AI & LLM integration](ai-integration.md) - llms.txt requirements for agent discoverability

## Architectural Decisions

Spec-related decisions are tracked in the CLI repository's ADR directory:

- [ADR-0005: versions.json as Single Source of Truth](../context/shared/adr/0005-versions-json-source-of-truth.md)
- [ADR-0032: Nested Catalogs with Flat Collections](../context/shared/adr/0032-nested-catalogs-with-flat-collections.md)
- [ADR-0047: Non-Geo Tabular Data Support](../context/shared/adr/0047-non-geo-tabular-data-support.md)
- [ADR-0049: STAC-GeoParquet as Scalability Requirement](../context/shared/adr/0049-stac-geoparquet-scalability.md)
- [ADR-0050: PMTiles as Visualization Requirement](../context/shared/adr/0050-pmtiles-visualization-requirement.md)
- [ADR-0051: SELF_CONTAINED Catalog Type](../context/shared/adr/0051-self-contained-catalog-type.md)
- [ADR-0052: Require llms.txt for AI/LLM Integration](../context/shared/adr/0052-llms-txt-requirement.md)

For all architectural decisions, see [context/shared/adr/](../context/shared/adr/).

## Machine-Readable Schemas

- `schema/` — JSON schemas and validation rules for `versions.json`, `catalog.json`, etc.

## Examples

See [examples/](examples/) for reference implementations.

## Versioning

The specification is versioned with [SemVer](https://semver.org/), starting
pre-1.0. The version lives in exactly one canonical, machine-readable place:

- [`schema/spec-version.json`](schema/spec-version.json) — read `spec_version`
  from here to claim or verify conformance against a version of the Portolan
  spec. Everything else (this README's header, the CLI's
  `portolan_cli.constants.PORTOLAN_SPEC_VERSION`, and the
  `portolan_spec_version` field in `portolan check --json` output) mirrors this
  value.

This is distinct from the `spec_version` field inside a `versions.json`
manifest, which versions the [manifest schema](schema/versions.schema.json), not
the specification as a whole. The check output deliberately names its field
`portolan_spec_version` to avoid colliding with that manifest key.

### Bump policy

While the spec is pre-1.0, **any breaking change bumps the MINOR** version
(e.g. `0.1.0` → `0.2.0`); non-breaking changes bump the PATCH. Once the spec
reaches `1.0.0`, normal SemVer applies (breaking changes bump MAJOR).

A change is **breaking** when a catalog that conformed to the previous version
may no longer conform, or a tool built against the previous version may
misvalidate. Examples:

- Raising a rule's severity (e.g. `warning` → `error`) in `schema/rules.yaml`.
- Adding a new `error`-level rule, or a new required field/constraint in any
  schema (schema tightening).
- Removing or renaming a field, rule id, or accepted value.
- Changing the meaning of an existing field.

A change is **non-breaking** (PATCH) when previously-conforming catalogs still
conform. Examples:

- Adding a new `warning`-level rule or an optional field.
- Relaxing a constraint (e.g. `error` → `warning`, widening accepted values).
- Editorial/documentation-only changes and clarifications.

When you change the spec in a way that trips the criteria above, bump
`spec_version` in `schema/spec-version.json` **and** the mirrored
`PORTOLAN_SPEC_VERSION` constant in `portolan_cli/constants.py` in the same PR
(a spec-compliance test fails if they drift).

## Making Changes

To propose spec changes:

1. Open a PR in this repository (portolan-cli)
2. Changes to `spec/` trigger review from spec maintainers
3. On merge, CI syncs to portolan-spec automatically
4. If the change is normative, bump `spec_version` per the
   [bump policy](#bump-policy) above

See [ADR-0048](../context/shared/adr/0048-cli-as-spec-source.md) for rationale.
