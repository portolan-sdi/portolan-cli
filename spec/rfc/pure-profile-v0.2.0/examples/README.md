# Finland — Portolan reference catalogue

A small, self-contained reference catalogue demonstrating the **converged Portolan design**
(post-2026-07-09 workshop). It is a *thin STAC index* over cloud-native data; the data itself lives
in object storage as GeoParquet.

## What this demonstrates

- **STAC as a thin index.** Catalog → Collections → data assets. No STAC `Item`s (they're vestigial
  for vector/tabular data); partitions are *not* itemised.
- **No bespoke Portolan STAC extension.** Everything rides on STAC core + `table` + `projection` +
  `alternate-assets` + `osi`. Identity and profile version are declared once via `conformsTo`.
- **Capabilities are derived, not declared.** A client learns what a dataset supports by looking:
  a `roles:["visual"]` asset ⇒ visualization; an OSI model link ⇒ semantics.
- **Two shapes:**
  - `buildings/` — a single-table dataset, GeoParquet Hive-partitioned by H3 cell, **not** itemised;
    ships a visualization capability (PMTiles).
  - `cadastre/` — a **multi-table data model** (parcels · buildings · ownerships) in one collection;
    relations and meaning live in the linked **OSI** model (semantic capability), including a
    spatial-predicate relation.

## Access

Data is queried directly with DuckDB over object storage — see each collection's `AGENTS.md` for the
exact SQL. Structural links are relative, so this catalogue can be browsed locally (e.g. in STAC
Browser) without a live bucket.

## License & provenance

Source: National Land Survey of Finland (NLS) Topographic Database, CC-BY-4.0. See the `via` link in
`catalog.json` and each collection's `providers`.
