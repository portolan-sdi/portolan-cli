# RFC: Portolan as a pure STAC profile — thin index, derived capabilities, no Portolan extension (v0.2.0)

> Target: `portolan-cli` `spec/` (source of truth; syncs to `portolan-spec`). Proposed spec bump:
> **0.1.1 → 0.2.0** (breaking: schema tightening + requirement changes + removed fields).
> This RFC can be merged as one change or split into the focused PRs listed at the end.

## Summary

Reframe Portolan as a **thin STAC discovery index** over cloud-native data, with meaning/relations in
**OSI** and tables in **GeoParquet/Iceberg**. The result removes the bespoke Portolan STAC extension
entirely and expresses everything through existing standards + a conformance vocabulary.

## Motivation

Post-workshop review found we were overloading STAC: partitions modelled as Items, relations pushed
into JSON, a new marker extension, and format-specific mandates (PMTiles). Applying one rule — *keep a
custom field only if it is (a) not derivable, (b) has no home in an existing extension, and (c) is
consumer-needed* — collapses almost all of it.

## Normative changes

### Removed
- **The Portolan STAC extension** (`stac-portolan-extension`) — no fields survive the rule. Repurpose
  that repo as the **profile / conformance-vocabulary home** (hosts the `conformsTo` tier URIs + prose
  spec), not a fields extension.
- **`portolan:version`** — replaced by a `conformsTo` core URI (version in the path).
- **`portolan:geospatial`** — arbitrary + derivable; replaced by *uniform bbox = AOI* semantics and
  the visualization capability. (Update `core.md` "Spatial Extent for Tabular Collections" and
  `formats/tabular.md` accordingly.)
- **`portolan:datetime_provisional`** as a stored field — becomes a validator-computed WARNING.

### Changed
- **`core.md`** — adopt the **Core Contract** (PC-01…PC-18, see `capabilities.md`). Notable:
  - `SELF_CONTAINED` now **omits `self` links** (schema requires `self.href` to be an absolute IRI;
    relative `root`/`parent`/`child` are fine). *Verified against `stac-node-validator`.*
  - Every data asset MUST be a **self-describing, range-readable** format (schema readable from the
    asset); STAC `table:columns` restatement is **SHOULD** (CLI-generated).
  - `bbox` is uniformly an **AOI**; no `geospatial` branching.
- **`ai-integration.md`** — replace the `llms.txt` requirement with **`AGENTS.md`** at catalog root
  and each collection (hierarchical, `rel:"agents"`); keep **`README.md`** for humans. *Supersedes
  ADR-0052.*
- **`extensions.md`** — document that Portolan adds **no STAC extension**; list the ecosystem
  extensions used (`table`, `projection`, `alternate-assets`, `osi`, `iceberg`).
- **`schema/rules.yaml`** — encode PC-01…PC-18 + the capability contracts as validator rules;
  remove rules referencing the removed fields.
- **`schema/spec-version.json`** — bump to `0.2.0`.

### Added
- **`conformance.md`** (new) — identity + profile version via `conformsTo`; the versioned core URI;
  three-versions distinction. *(File included in this packet.)*
- **`capabilities.md`** (new) — the Core Contract table + the derived capability contracts
  (visualization / semantic / iceberg / versioning). *(File included in this packet.)*
- **Multi-table guidance** (in `formats/vector.md` or `structure.md`) — a data model = one collection,
  tables as **assets**; relations in OSI (`osi:relations`), not Items or links.

## Companion changes (separate repos, referenced here)
- **`stac-osi-extension`** — add `osi:relations` (denormalized summary mirroring `osi:metrics`),
  including **spatial-predicate** relations (`predicate`, `spatial`). *Validated: current OSI schema
  already accepts the field.*
- **`stac-iceberg-extension`** — allow **asset-scoped** `iceberg:*` so one collection can hold
  multiple Iceberg tables (currently collection-singular).

## Examples & evidence
- Reference set under `spec/examples/portolan/` (from this work's `example/`): `buildings`
  (single-table, not itemised, viz) + `cadastre` (multi-table model, OSI semantics).
- **`stac-node-validator` v2.0.0-rc.3: 3/3 pass**, all extensions pass.
- **STAC Browser v5.0.0-rc.1**: catalogue, multi-table assets, OSI metrics/relations, `table:columns`
  schema, and the VISUAL badge all render.

## Open decisions for the group (please weigh in)
- **A** — schema-in-STAC (`table:columns`): SHOULD (CLI-generated, proposed) vs MUST.
- **B** — `versions.json`: minimal manifest in core (proposed) vs a `versioning` capability.
- **C** — accepted-format allowlist for the data asset (Parquet/Iceberg required; GeoJSON/CSV only for
  tiny data?).
- **D** — `AGENTS.md`/`README.md` required per collection (proposed, CLI-scaffolded) vs root-only.
- **PMTiles** — capability + contract (proposed) vs mandatory format.

## If we'd rather split it
1. Remove Portolan extension + `portolan:*` fields → `conformsTo`.
2. `AGENTS.md`/`README.md` (supersede ADR-0052).
3. Capabilities + contracts (`capabilities.md`, `rules.yaml`).
4. Multi-table model guidance + OSI companion changes.
5. Visualization capability reframe.
