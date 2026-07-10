# ADR-0057: Portolan as a pure STAC profile — thin index, derived capabilities, no Portolan extension

## Status

Proposed

> RFC for team discussion. Detailed proposal, draft docs, and a validated reference example set live in
> [`spec/rfc/pure-profile-v0.2.0/`](../../../spec/rfc/pure-profile-v0.2.0/). Nothing in the canonical
> `spec/` is rewritten by this PR; the breaking edits land only if the direction is accepted.

## Context

A post-workshop review (2026-07-09) found the emerging design overloading STAC:

- Parquet partitions modelled as STAC `Item`s.
- A relational (multi-table) model pushed into STAC JSON links.
- A new Portolan STAC extension (`stac-portolan-extension`, created 2026-07-09) whose only field is a
  version marker.
- Format-specific mandates (PMTiles required).
- `portolan:geospatial` as a required boolean, plus a validator special-case for tabular bbox.

Underneath sits a real tension between two goals the spec is trying to satisfy as a **mandatory
union**: AI/analytics access (agents querying with DuckDB) and cheap, browsable catalogs
(visualization, the portal experience). Satisfying both as mandatory taxes both.

Applying one rule exposes how much is ceremony — **keep a custom field only if it is (a) not derivable
from ground truth, (b) has no home in an existing extension, and (c) is consumer-needed.** Under it,
almost every Portolan-specific field fails.

## Decision

Reframe Portolan as a **pure STAC profile**: a set of requirements + a conformance vocabulary,
expressed entirely through STAC core + existing extensions (`table`, `projection`, `alternate-assets`,
`osi`, `iceberg`). Concretely:

1. **No Portolan STAC extension.** No `stac_extensions` entry, no `portolan:` fields. Repurpose the
   `stac-portolan-extension` repo as the profile / conformance-vocabulary home.
2. **Thin index, layered concerns.** STAC = discovery (Catalog → Collection → data assets; the `Item`
   is vestigial for vector/tabular data). Tables = GeoParquet or Iceberg (hidden partitioning; the
   `static` catalog mode stays serverless-on-object-store). Meaning + relations = OSI. Presentation =
   optional.
3. **Identity + profile version via `conformsTo`** (reused OGC/STAC-API idiom; version in the URI).
   Distinct from STAC `stac_version` and the data version in `versions.json`.
4. **Capabilities are derived, not declared.** Presence is read from the artifacts
   (`roles:["visual"]` ⇒ visualization; OSI link ⇒ semantic); the spec defines a **contract** per
   capability, enforced by `portolan check`.
5. **Drop `portolan:geospatial`** — arbitrary + derivable; `bbox` is uniformly an AOI; map-viewability
   is the visualization capability.
6. **Drop `portolan:datetime_provisional` as a stored field** — validator-computed WARNING.
7. **`AGENTS.md` (agents, hierarchical) + `README.md` (humans)** replace `llms.txt`
   (supersedes ADR-0052).
8. **Multi-table models = one collection, tables as assets;** relations in OSI (`osi:relations`,
   incl. spatial-predicate relations), not Items or links.
9. **Visualization is a capability + contract, not a mandated format.** PMTiles = recommended default.

The mandatory **core contract** (PC-01…PC-18) and the derived **capability contracts** are drafted in
[`capabilities.md`](../../../spec/rfc/pure-profile-v0.2.0/capabilities.md).

## Consequences

- **Breaking** (spec `0.1.1 → 0.2.0`): removed fields, `llms.txt → AGENTS.md`, schema tightening
  (machine-readable schema required), `SELF_CONTAINED` omits `self` links.
- **Simpler + more portable:** the design validates as plain STAC and renders in STAC Browser today
  (see evidence below); Portolan "invents nothing."
- **Companion changes** in sibling repos: `stac-osi-extension` adds `osi:relations` (spatial-predicate
  aware; current schema already accepts it); `stac-iceberg-extension` allows asset-scoped `iceberg:*`
  for multi-table collections.
- `portolan check` / `reis` (`schema/rules.yaml`) becomes the operative definition of conformance.

## Evidence

Reference set in [`spec/rfc/pure-profile-v0.2.0/examples/`](../../../spec/rfc/pure-profile-v0.2.0/examples/):
`buildings` (single-table, H3-partitioned, not itemised, viz) + `cadastre` (multi-table model with OSI
semantics). Validated with `stac-node-validator` v2.0.0-rc.3 (**3/3 pass**, all extensions incl. the
proposed `osi:relations`) and rendered in **STAC Browser v5.0.0-rc.1** (multi-table assets, OSI
metrics/relations, `table:columns` schema, and the VISUAL-badged tiles asset all display).

## Open questions

- Schema-in-STAC (`table:columns`): SHOULD (CLI-generated) vs MUST.
- `versions.json`: minimal manifest in core vs a `versioning` capability.
- Accepted-format allowlist for the data asset.
- `AGENTS.md`/`README.md` required per collection vs root-only.
- PMTiles: capability + contract vs mandatory format.
