# Philadelphia zoning table on Carto

The rashid row `extract-carto` in
`tests/integration/test_generated_catalog_conformance.py` uses this scenario.
The run is `extract carto <base_url>/api/v2/sql --tables zoning_basedistricts
--limit 3`.

## Source

- The upstream endpoint is `https://phl.carto.com/api/v2/sql`.
- The responses date from 2026-10-23.
- The account refuses `CDB_UserTables()` with HTTP 403. The extractor then
  uses the table that `--tables` gives.
- The GeoJSON response contains the 3 rows of `LIMIT 3`. The count response
  is the real count of 29208.
- geoparquet-io 1.7.0 reads the SRID of `the_geom` before it reads the rows.
  The SRID response dates from 2026-10-27.
- The responses contain no credential.
