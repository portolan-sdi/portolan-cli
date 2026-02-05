# ADR-0004: Iceberg as Plugin, Not Core

## Status
Accepted

## Context

Apache Iceberg is a table format gaining traction in the data lakehouse world. Some users have asked whether Portolan should support Iceberg natively, potentially replacing STAC as the catalog layer.

This decision has significant architectural implications.

## Decision

Iceberg is a **plugin**, not a core dependency. Portolan's catalog layer remains STAC.

### Rationale

**1. Wrong problem space**

Iceberg solves concurrent writes, ACID transactions, and schema evolution on petabyte tables. Portolan users publish static datasets. They don't need any of that.

**2. Wrong infrastructure model**

Iceberg requires a catalog server (REST API, at minimum). Portolan's whole value proposition is static files on object storage, no running services, $5/month hosting.

**3. Governance risk**

Databricks owns the Iceberg creators (Tabular acquisition). The spec is shaped by billion-dollar warehouse companies, and the catalog layer is an active corporate battleground. Portolan shouldn't tie its fate to that.

**4. Geospatial support isn't there**

Iceberg V3 geometry types exist in spec but not in tooling — DuckDB's Iceberg extension, PyIceberg, none of them support it yet. Even the spec itself deferred edge cases to V4.

**5. STAC already does what we need**

A manifest saying "here are files, here's what's in them, here's their spatial extent" — that's STAC. No reason to replace it with something heavier.

**6. Interoperability without dependency**

GeoParquet and COG files are the same regardless of catalog layer. If someone wants to register Portolan data in their Iceberg catalog, they can — the formats are compatible. That's a plugin use case, not a core requirement.

### Plugin path

A `portolan-iceberg` plugin could:
- Export Portolan catalogs to Iceberg format
- Register GeoParquet files in an existing Iceberg catalog
- Sync Iceberg metadata alongside STAC

This keeps Iceberg users happy without compromising Portolan's simplicity.

## Consequences

### What becomes easier
- **Simple deployment** — No catalog server required
- **Low barrier to entry** — Static hosting works
- **Stable foundation** — STAC is mature, well-documented, widely adopted in geospatial
- **Independence** — Not subject to lakehouse vendor politics

### What becomes harder
- **Lakehouse integration** — Users with Iceberg-native stacks need the plugin
- **"Modern data stack" marketing** — Iceberg is trendy; not having it core may seem behind

### Trade-offs
- We accept reduced lakehouse integration for architectural simplicity
- We accept marketing friction for governance independence

## Alternatives Considered

### 1. Iceberg as core catalog layer
**Rejected:** Wrong problem space, wrong infrastructure model, governance risk, immature geospatial support.

### 2. Dual catalog (STAC + Iceberg)
**Rejected:** Complexity explosion. Two sources of truth, sync issues, doubled maintenance.

### 3. Iceberg-first, STAC export
**Rejected:** Inverts the dependency. Portolan is for static publishing, not lakehouse management.

### 4. Wait and see
**Considered:** Could revisit if Iceberg geospatial support matures and a catalog-free mode emerges. Current decision doesn't preclude future change — plugin can always be promoted to core.
