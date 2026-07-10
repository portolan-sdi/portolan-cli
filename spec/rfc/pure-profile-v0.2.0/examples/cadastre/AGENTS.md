# Cadastre (multi-table model) — agent guide

A **data model published as one collection**: three tables, each a data asset. GeoParquet, EPSG:3067.
The canonical join graph and meaning live in the **OSI model** (`semantics.osi.yaml`); STAC carries a
denormalized summary in `osi:relations` / `osi:metrics`.

## Tables (assets)
- `parcels` — PK `parcel_id`, `geometry` (polygon).
- `buildings` — PK `building_id`, FK `parcel_id` → parcels, `geometry` (polygon).
- `ownerships` — PK `ownership_id`, FK `parcel_id` → parcels, `owner_name`, `share_pct` (no geometry).

## Relations (from the OSI model)
- `buildings.parcel_id → parcels.parcel_id` (N:1)
- `ownerships.parcel_id → parcels.parcel_id` (N:1)
- `buildings ST_Within parcels` (N:1, **spatial predicate** — no stored key). This is the
  spatial-relation case OSI's spatial extension is being designed for; it is a **proposed** field.

## Query (DuckDB)
```sql
INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;

-- buildings with their parcel + owner (key join)
SELECT b.building_id, p.parcel_id, o.owner_name, o.share_pct
FROM read_parquet('s3://example-bucket/fi/cadastre/buildings/**/*.parquet') b
JOIN read_parquet('s3://example-bucket/fi/cadastre/parcels/**/*.parquet')    p USING (parcel_id)
JOIN read_parquet('s3://example-bucket/fi/cadastre/ownerships/**/*.parquet') o USING (parcel_id);

-- spatial-predicate relation (no stored key)
SELECT b.building_id, p.parcel_id
FROM read_parquet('s3://example-bucket/fi/cadastre/buildings/**/*.parquet') b
JOIN read_parquet('s3://example-bucket/fi/cadastre/parcels/**/*.parquet')   p
  ON ST_Within(ST_GeomFromWKB(b.geometry), ST_GeomFromWKB(p.geometry));
```

## Capabilities present
- **Semantic** — yes (OSI model linked; `osi:*` fields present).
- **Visualization** — none shipped for this model (query/analysis dataset).

License: CC-BY-4.0, National Land Survey of Finland.
