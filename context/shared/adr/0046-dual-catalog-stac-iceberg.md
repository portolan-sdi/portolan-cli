# ADR-0046: Dual serialization for the STAC catalog — JSON files and static Iceberg REST

## Status

Proposed (DRAFT)

> **Note on numbering:** PR #342 (portolake → portolan-cli merge) also adds a superseding ADR. Final number may shift to 0047+ depending on merge order.

## Context

### Terminology

This ADR is precise about the spec/serialization distinction, because conflating them obscures the actual decision:

- **STAC** — the SpatioTemporal Asset Catalog *specification*. Defines fields, semantics, and relationships (Catalogs → Collections → Items → Assets). STAC is a data model; it does not mandate a storage format.
- **STAC JSON** — the canonical serialization of STAC as a tree of JSON files. This is what Portolan publishes today, and what most STAC tooling expects.
- **STAC in Iceberg** — an alternative serialization of STAC, where Items are stored as rows in an Iceberg table. Same spec, same field names, same semantics — different storage format. Analogous to how `pgstac` serializes STAC in PostgreSQL.
- **Iceberg REST catalog** — a catalog format defined by the Iceberg spec. Can contain both *STAC-in-Iceberg* tables (the metadata layer) and *data tables* (e.g., GeoParquet collections exposed for direct SQL access). Hostable as a tree of static JSON files conforming to the REST spec.
- **STAC Iceberg Extension** — a separate STAC extension (already extracted to its own repo) that lets a STAC Item *describe* an Iceberg table. Different concept from "STAC in Iceberg": the extension describes Iceberg tables *from* STAC; "STAC in Iceberg" stores STAC items *in* Iceberg.

The decision this ADR addresses is **how Portolan serializes its STAC catalog metadata**, and what additional data tables (if any) are exposed via Iceberg REST for SQL query.

### Current state and its limits

Portolan publishes STAC catalogs as JSON files. This serialization has broad community adoption, accommodates all asset formats (vector, raster, point cloud, tiles), and aligns with Portolan's cloud-native, static-files thesis.

However, the JSON-only serialization has structural limits that increasingly bind real use cases:

1. **No native query access.** To do a spatial/temporal query over a STAC catalog as JSON files you need a STAC API server (pgstac, stac-fastapi) or you walk JSON files manually. DuckDB, Trino, Spark cannot read a STAC catalog without a custom extension.

2. **STAC's relational structure is implicit in JSON nesting, not queryable.** Items belong to Collections; Items have Assets; Items have spatial/temporal extents. These are real relations but JSON serialization expresses them as nested objects, not joinable tables.

3. **Multi-table relational data models are not expressible.** Use cases like `sensors → scenes → bands` (star schemas, time-series with dimensions) must be modeled as separate Collections with prose-documented relationships, because JSON-serialized STAC has no notion of cross-table joins.

4. **No native JOIN across catalogs.** Federation between Portolans (e.g., joining imagery from one publisher with administrative boundaries from another) requires custom application code on top of STAC clients.

These are limits of the **JSON serialization**, not of STAC itself. Serializing the same STAC catalog in Iceberg, alongside a few data tables, gets us:

- **Spec-conformant REST catalog materialized as static JSON files** (no server required)
- **Tables queryable from any tool that speaks Iceberg REST** (DuckDB, Trino, Spark, etc.) — including a `items` table containing STAC items as rows
- **Multi-table relational semantics** native to Iceberg
- **`ATTACH` multiple Iceberg catalogs** in a single DuckDB session — cross-catalog `JOIN`s

What STAC-in-Iceberg cannot fully replace is **STAC JSON itself as a human-editable, JSON-tool-friendly serialization**:

- JSON files are inspectable, hand-editable, diffable, and supported by every STAC tool today
- Non-tabular item descriptions with format-specific extensions (`pmtiles:*`, `cog:*`, `raster:*`) are awkward to schematize cleanly in a single Iceberg table
- The STAC ecosystem (STAC Browser, pystac, stactools) expects JSON

**The decision is therefore not "Iceberg replaces STAC." STAC remains the data model. The question is whether to add a second serialization (STAC-in-Iceberg) for query workloads, and what to treat as source of truth.**

## Decision

### Source of truth: STAC JSON, with STAC-in-Iceberg as projection

Portolan publishes **two serializations of the same STAC catalog** (plus, separately, data tables in Iceberg — covered below). The architectural question to settle is which serialization is source of truth — and why. Three framings are possible:

| Framing | What it says | Trade-offs |
|---|---|---|
| **A. STAC JSON is canonical; STAC-in-Iceberg is generated from it** | The JSON serialization is source of truth; the Iceberg serialization is a projection | Matches Portolan's current architecture; preserves STAC community comfort (JSON tools work unchanged); non-tabular item descriptions stay in JSON where they belong; clear write path; lowest migration cost |
| **B. STAC-in-Iceberg is canonical; STAC JSON is generated from it** | Iceberg becomes the primary serialization; JSON is rendered for clients that expect it | Gains Iceberg's rigorous schema enforcement, ACID, time travel, native query. But: non-tabular item descriptions with format-specific extensions are awkward to schematize; JSON-first STAC tools become second-class consumers (read derived files that may lag); commits the project to an Iceberg-first identity prematurely |
| **C. Both serializations are generated from an internal canonical model** | Neither is source; an explicit internal representation is | Most honest about reality — the underlying data files are the only true source, and both STAC serializations are views of the metadata. Maximum flexibility for future evolution. But: introduces an explicit "internal model" that doesn't exist today; harder to communicate to users than picking a side |

**Choice: A.** Given Portolan's current state — STAC JSON is the primary, hand-editable, human-readable output today, and Iceberg generation is a downstream step — the JSON serialization remains source of truth. STAC-in-Iceberg becomes a derived projection that consumers can use for SQL query without changing how publishers write or describe their catalogs.

This is a *contingent* decision tied to the project's current architecture, not an eternal one. If Iceberg-side use cases become operationally central — concurrent writes, transactional updates, schema-rigorous publishing pipelines, multi-writer workflows — the project may revisit and shift toward framing B or C. For now, A keeps the change additive, the migration cost low, and the STAC community comfortable.

The remainder of this ADR assumes framing A.

### 1. STAC JSON catalog (primary)

Unchanged from current ADRs:

- Source of truth for all asset description
- Covers every supported format (GeoParquet, COG, LAZ, PMTiles, etc.)
- Self-describing JSON tree rooted at `catalog.json`
- All existing STAC tooling continues to work

### 2. Iceberg REST catalog (projection)

A second catalog materialized **as a tree of static JSON files conforming to the [Iceberg REST spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)**:

- Hostable on R2, S3, GCS, Azure, or any HTTP-accessible static-file host
- No server, no DB, CDN-friendly by default
- Generated *from* the STAC JSON catalog; the JSON serialization remains source of truth (see "Source of truth" section above)
- Hosting cost for a typical public catalog: ~$5/month

The Iceberg catalog exposes two kinds of tables:

#### (a) Data tables for tabular collections

One Iceberg table per GeoParquet (or Parquet) collection. Native SELECT works against the underlying data:

```sql
SELECT * FROM portolan.demographics.census_2020 LIMIT 10;
```

Schema is preserved from the underlying Parquet via Iceberg's name-mapping (no Parquet rewrite required).

#### (b) STAC items as an Iceberg table (catalog-wide discovery)

A single `items` table at the catalog root — **this is STAC in Iceberg serialization** — containing one row per STAC Item, **including non-tabular items** (COG, LAZ, tiles).

Schema (subject to refinement in a follow-up ADR):

```
id              VARCHAR
collection_id   VARCHAR
geometry        BLOB     -- WKB, nullable
bbox_west       DOUBLE   -- nullable
bbox_south      DOUBLE   -- nullable
bbox_east       DOUBLE   -- nullable
bbox_north      DOUBLE   -- nullable
datetime        TIMESTAMP -- nullable
start_datetime  TIMESTAMP -- nullable
end_datetime    TIMESTAMP -- nullable
properties      VARCHAR  -- JSON
assets          VARCHAR  -- JSON: { asset_key: { href, type, roles, ... } }
links           VARCHAR  -- JSON
created_at      TIMESTAMP
updated_at      TIMESTAMP
```

This enables **STAC search without a server**:

```sql
SELECT id, datetime, assets['cog'].href
FROM portolan.items
WHERE collection_id = 'sentinel-2-l2a'
  AND ST_Intersects(geometry, ST_MakeEnvelope(-10, 40, 0, 50))
  AND datetime BETWEEN '2024-01-01' AND '2024-12-31';
```

For non-tabular items (COG, LAZ), the `assets` JSON contains the URLs. Consumers fetch the actual data via HTTP range requests — the existing cloud-native pattern.

### Format-agnostic substrate (non-binding)

The `items` table's spatial fields (`geometry`, `bbox_*`) are nullable. **Non-spatial datasets (e.g., open-data CSVs) can be cataloged as items with NULL geometry** without any schema redesign. This is not the primary use case but is a natural property of the design and should not be artificially restricted. It opens the door to joining a Portolan to non-spatial data (Socrata-style budgets, census tables, health records) inside the same DuckDB session.

## Consequences

### Easier

- **Native SQL discovery from DuckDB / Trino / Spark.** No custom extension, no JSON walking, no STAC API server.
- **Cross-catalog federation in one session:** `ATTACH` multiple Portolans, `JOIN` across them, including private + public mixed.
- **Multi-table relational publishing** becomes expressible (star schemas, fact + dimension).
- **STAC consumers are unaffected.** Primary catalog is unchanged JSON; all existing tooling works.
- **Non-spatial datasets fit naturally** without redesign.
- **Hosting cost stays in the "static files" regime.** No server tier introduced.

### Harder

- **Two serializations to keep in sync.** STAC JSON and STAC-in-Iceberg must agree. *Mitigation:* single generator pipeline; the Iceberg serialization is fully derived from STAC JSON; STAC JSON is the only source of truth.
- **Schema evolution requires care.** Adding fields to the `items` table interacts with Iceberg snapshot semantics. *Mitigation:* use Iceberg's schema evolution primitives; document required vs optional fields per a follow-up ADR.
- **Iceberg REST spec compatibility burden.** The generator must conform to the spec to remain queryable by future versions of DuckDB / Trino / Spark. *Mitigation:* pin to a known-good Iceberg spec version; version the static catalog accordingly.
- **No write coordination at the catalog level.** This is by design — matches the publishing model, not the warehouse model. Concurrent-write use cases are explicitly covered by the existing server-based `IcebergBackend` (`portolan-cli[iceberg]`).

### Coexistence with `portolan-cli[iceberg]`

This ADR proposes a **second flavor** of the Iceberg backend in `portolan_cli/backends/iceberg/`:

| Module | Role |
|---|---|
| `backend.py` (existing, from portolake merge per #342) | Server-based `IcebergBackend` for lakehouse use cases (concurrent writes, ACID) |
| `static_backend.py` (new) | Static REST catalog generator for publishing use cases |

Both share `spatial.py`, `stac_generator.py`, `config.py`, and `export.py`. The user picks one via configuration (e.g., `--backend iceberg-static` vs `--backend iceberg`). The split is along *deployment model*, not capability.

## Alternatives considered

### A. Stop publishing STAC JSON entirely; only publish STAC in Iceberg

Drop the JSON serialization; the only catalog Portolan emits is STAC-in-Iceberg, with the existing STAC tooling ecosystem expected to adapt.

**Rejected:** Massive social cost — every STAC tool today (STAC Browser, pystac, stactools, STAC validators) expects JSON files; non-tabular item descriptions with format-specific extensions (`pmtiles:*`, `cog:*`, `raster:*`) are awkward to schematize cleanly in a single Iceberg table; loses the human-readability of STAC JSON for hand-editing and inspection. The dual-serialization model (Choice A in the Decision section) gets the SQL query benefits without forcing this loss.

### B. STAC API server (pgstac, stac-fastapi) for queryability

Stand up a STAC API server to make STAC JSON queryable via CQL2.

**Rejected:** Adds server operational burden; breaks Portolan's static-files thesis; doesn't enable cross-catalog DuckDB federation (each STAC API is its own endpoint with its own query language, not joinable in SQL).

### C. Custom DuckDB extension for STAC JSON

Write a DuckDB extension that reads STAC JSON natively.

**Rejected:** Significant maintenance burden; users of Trino / Spark / others still excluded; doesn't enable cross-catalog `ATTACH`/`JOIN`; STAC JSON isn't designed for query workloads anyway. STAC-in-Iceberg sidesteps this by using a serialization that *is* designed for query.

### D. Tabular Iceberg only (no `items` table for non-tabular)

Iceberg exposes only data tables; COGs and other non-tabular assets are invisible to DuckDB users.

**Rejected:** Misses the "STAC-search-without-a-server" property — the most valuable feature of the proposal. COG-heavy catalogs (imagery archives) get no value from the Iceberg layer.

## Follow-up ADRs

- **ADR-XXXX: Per-catalog authentication model.** Each Portolan has its own identity; configured client-side at `ATTACH` time. Three tiers: fully public, fully private (storage-layer auth), hybrid (lightweight auth endpoint signs URLs for private subset).
- **ADR-XXXX: Federation conventions for cross-Portolan DuckDB queries.** Naming, schema alignment, when to publish a shared `items` table schema vs custom.
- **ADR-XXXX: Items table schema specification.** Required vs optional fields; STAC fidelity vs DuckDB query ergonomics; JSON-typed properties vs flattened columns.

## Implementation notes

A working prototype exists on the `add-sdi-experiment` branch of the original `portolan` repository (latest commit `d645577`). Audit findings:

| Capability | Status |
|---|---|
| Tabular Iceberg tables for Parquet-backed resources | Implemented (`output_generators.py:1754`) |
| Partial metadata index (`_meta.resources`, 24 cols) | Implemented (`output_generators.py:1577`) |
| Full STAC+ISO 19115 items table (43 cols) | Scaffolded in `sdi_catalog.py:415`, not yet wired into output pipeline |
| COG metadata extractor (bounds, CRS, bands) | Not implemented; ~1-2 days with DuckDB + GDAL |
| Sync to R2 / S3 / GCS / Azure via obstore | Implemented (`catalog_state.py:217`) |
| End-to-end test with DuckDB `ATTACH` | Verified manually |

**Estimated effort to land a working implementation in `portolan-cli[iceberg]` after PR #342 merges: 3-4 focused days.**

The scope is mostly plumbing — most of the Iceberg generator code already exists; what's missing is wiring `generate_sdi_catalog()` into the output pipeline and adding the COG metadata extractor.
