# Finland cadastre (Uusimaa sample)

A multi-table cadastral **data model** published as a single Portolan collection: parcels, buildings,
and ownership records. The tables are related (buildings and ownerships reference parcels); the
relationships, metrics and meaning are described in the linked **OSI semantic model**
(`semantics.osi.yaml`).

- **Tables:** `parcels`, `buildings` (both GeoParquet, EPSG:3067), `ownerships` (Parquet, non-spatial).
- **Access & joins:** see `AGENTS.md` for DuckDB recipes, including a spatial-predicate join.
- **License:** CC-BY-4.0 — cite the National Land Survey of Finland.
