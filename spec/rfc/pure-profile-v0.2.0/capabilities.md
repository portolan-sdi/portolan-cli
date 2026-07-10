# Core Contract & Capabilities

Conformance is **"passes the checks,"** not "claims to." This document defines the mandatory **core
contract** every Portolan node satisfies, and the **optional capability contracts** that are verified
only when a capability is present (presence is *derived* from the artifacts, never declared).

## Core (MUST unless noted) — `portolan check` verifies

| ID | Requirement | Level |
|---|---|---|
| PC-01 | Valid STAC 1.1.0 (Catalog / Collection / Item as used) | MUST |
| PC-02 | `SELF_CONTAINED`: structural links (`root`/`parent`/`child`) are relative and resolve; **`self` links are omitted** (schema requires `self.href` to be an absolute IRI) | MUST |
| PC-03 | Data asset hrefs are absolute object-storage URLs | MUST |
| PC-04 | Every Catalog/Collection has non-empty, human-readable `title` + `description` | MUST |
| PC-05 | Every `child`/`item` link has a `title` | MUST |
| PC-06 | `AGENTS.md` at catalog root and each collection, linked `rel:"agents"` (hierarchical) | MUST |
| PC-07 | `README.md` at catalog root (title, description, license, provenance) | MUST |
| PC-08 | Every Collection has `extent.spatial.bbox`, interpreted uniformly as **AOI** | MUST |
| PC-09 | Every bbox valid (no NaN/Inf/sentinels; WGS84 ranges; south ≤ north) | MUST |
| PC-10 | Items/collections carry `datetime` or a start/end interval | SHOULD (absent → WARNING) |
| PC-11 | Every dataset has ≥1 asset `roles:["data"]`, resolvable href, correct media type | MUST |
| PC-12 | Data is a self-describing, cloud-native, range-readable format so schema+types(+CRS) are machine-readable from the asset | MUST |
| PC-13 | Schema restated in STAC as asset/collection `table:columns` (+ `proj:code`), CLI-generated | SHOULD |
| PC-14 | Collection declares a `license` | MUST |
| PC-15 | `providers` with roles | SHOULD |
| PC-16 | `rel:"via"` to the canonical external source when data is derived from one | MUST (if derived) |
| PC-17 | `versions.json` with ≥ current version id + asset checksum(s) | MUST |
| PC-18 | Declare profile version via one `conformsTo` core URI | SHOULD |

Core deliberately **excludes** visualization, semantics, iceberg and relations.

**Open judgment calls:** PC-13 SHOULD vs MUST · PC-17 core vs a `versioning` capability · PC-12
accepted-format allowlist · PC-06/07 per-collection vs root-only. See the RFC.

## Capabilities — optional, derived from presence, contract-guaranteed

A capability is present iff its **signal** is present; the **contract** is what makes it reliable.
Nothing is declared in `conformsTo`.

| Capability | Signal (how a client detects it) | Contract (`portolan check` when present) |
|---|---|---|
| **Visualization** | a `roles:["visual"]` asset (or renderable-from-source) | zero-infra renderable; covers the dataset; declared derived from its data source (staleness-detectable). Format-agnostic; PMTiles = recommended default |
| **Semantic** | OSI model link / `osi:*` fields | model resolves; `osi:relations`/`osi:metrics` consistent with it; referenced columns exist in the schema |
| **Iceberg / table-format** | Iceberg asset / `iceberg:*` fields | `metadata.json` resolvable and `iceberg_scan()`-able; declared `iceberg:*` match the metadata |
| **Versioning / time-travel** *(candidate)* | multi-version `versions.json` / Iceberg snapshots | predecessor/successor resolvable; checksums verify |

**Why derived, not declared:** capability presence is already in the artifacts, so a declaration would
duplicate ground truth (the same reason `portolan:geospatial` is dropped) — and a declaration can lie
(claim `viz` over a broken tile) where inspection + contract cannot. A viewer's reliability comes from
the contract on the artifact, which is stronger than a self-reported flag.
