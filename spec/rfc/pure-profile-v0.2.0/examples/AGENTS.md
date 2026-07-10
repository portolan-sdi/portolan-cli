# Portolan catalogue — agent guide

This is a **Portolan** catalogue: a thin STAC index over cloud-native GeoParquet on object storage.
This file is the catalogue-wide guide; each collection has its own `AGENTS.md` (nearest one wins).

## How to use this catalogue
- The tree is `catalog.json` → `{collection}/collection.json`. Structural links are relative.
- **Data is not in STAC.** STAC points at it. Query the GeoParquet directly with DuckDB.
- **Capabilities are derived, not declared.** To know if a dataset can be mapped, look for an asset
  with `roles:["visual"]`. To know if it has a semantic model, look for `osi:*` fields / an OSI link.
- Profile: this catalogue declares `conformsTo: [".../spec/v0.2.0/core"]`. That is the only Portolan
  identity/version marker — there is no `portolan:` field and no Portolan STAC extension.

## Setup for queries
```sql
INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;
```

## Collections
- `buildings/` — building footprints, one row per building (~5.65M), GeoParquet partitioned by H3.
- `cadastre/` — a multi-table model (parcels, buildings, ownerships); see its `AGENTS.md` and the
  linked OSI model for the join graph.

License: CC-BY-4.0, National Land Survey of Finland. Cite NLS.
