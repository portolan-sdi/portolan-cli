# Finland building footprints

Building footprints (one row per building, ~5.65M) from the National Land Survey of Finland
Topographic Database. Stored as GeoParquet in EPSG:3067, Hive-partitioned by H3 resolution-3 cell.

- **Access:** query directly with DuckDB (see `AGENTS.md`).
- **Visualization:** PMTiles vector tiles (the `tiles` asset).
- **License:** CC-BY-4.0 — cite the National Land Survey of Finland.
