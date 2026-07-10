# Building footprints — agent guide

One row per building from the NLS Topographic Database (`rakennus`). GeoParquet, EPSG:3067,
~5.65M rows, Hive-partitioned by `h3_3` (H3 resolution-3 cell). **Not itemised** — there is no STAC
Item per partition; use the glob.

## Query (DuckDB)
```sql
INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;

-- whole dataset (wildcard the partitions)
SELECT count(*)
FROM read_parquet('s3://example-bucket/fi/buildings/**/*.parquet', hive_partitioning = true)
WHERE municipality_code = '091';

-- one partition (substitute the h3_3 value)
SELECT * FROM read_parquet('s3://example-bucket/fi/buildings/h3_3=831f5cfffffffff/data.parquet');
```
Within a partition, GeoParquet's bbox covering column prunes row-groups.

## Columns
`mtk_id` (PK) · `municipality_code` · `use` · `h3_3` (partition key) · `geometry` (WKB, EPSG:3067).
Also restated machine-readably in the collection's `table:columns`.

## Capabilities present
- **Visualization** — yes (`tiles` asset, PMTiles, `roles:["visual"]`).
- **Semantic** — no OSI model for this dataset.

License: CC-BY-4.0, National Land Survey of Finland.
