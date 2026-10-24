# Fixtures from production data

Production data samples for tests marked `realdata`. They test Portolan's
orchestration: metadata extraction, bounding boxes, and handoff to
geoparquet-io and rio-cogeo. They do not test geometry validity or format
conversion, which belong to the upstream libraries.

Git holds the files, so the tests need no network. A backup copy is at
`s3://us-west-2.opendata.source.coop/nlebovits/portolan-test-fixtures/sources/`.

| File | Edge case | Source |
|------|-----------|--------|
| `nwi-wetlands.parquet` | Complex polygons with holes | [National Wetlands Inventory](https://source.coop/giswqs/nwi), DC |
| `open-buildings.parquet` | 1,000 simple polygons | [Open Buildings](https://source.coop/vida/google-microsoft-osm-open-buildings), Andorra |
| `road-detections.parquet` | LineString geometry | [Microsoft ML Road Detections](https://source.coop/nlebovits/microsoft-ml-road-detections), Saint Lucia |
| `fieldmaps-boundaries.parquet` | Crosses the antimeridian | [FieldMaps](https://fieldmaps.io/), Fiji, Kiribati |
| `rapidai4eo-sample.tif` | A production cloud-optimized GeoTIFF | [RapidAI4EO](https://source.coop/planet/rapidai4eo) |

`philadelphia-housing/` and `filegdb/` have their own provenance in the
directory or in the tests that read them.

## Provenance

Each Parquet file is the output of one DuckDB query:

```sql
COPY (SELECT * FROM read_parquet(
  's3://us-west-2.opendata.source.coop/giswqs/nwi/wetlands/DC_Wetlands.parquet')
  LIMIT 1000) TO 'nwi-wetlands.parquet' (FORMAT PARQUET);

COPY (SELECT * FROM read_parquet(
  's3://us-west-2.opendata.source.coop/vida/google-microsoft-osm-open-buildings/geoparquet/by_country/country_iso=AND/AND.parquet')
  LIMIT 1000) TO 'open-buildings.parquet' (FORMAT PARQUET);

COPY (SELECT * FROM read_parquet(
  's3://us-west-2.opendata.source.coop/nlebovits/microsoft-ml-road-detections/by_country/country=LCA/LCA.parquet')
  LIMIT 1000) TO 'road-detections.parquet' (FORMAT PARQUET);

COPY (SELECT * FROM read_parquet(
  'https://data.fieldmaps.io/edge-matched/open/intl/adm1_polygons.parquet')
  WHERE geometry_bbox.xmin < -170 AND geometry_bbox.xmax > 170
    AND adm0_name IN ('Fiji', 'Kiribati'))
  TO 'fieldmaps-boundaries.parquet' (FORMAT PARQUET);
```

The DuckDB copy drops the `crs` of the source `geo` metadata. The NWI source
declares EPSG:5070 (NAD83 / Conus Albers), and a missing `crs` means OGC:CRS84,
so `add` refused the fixture (issue #996). The source's `crs` object is copied
onto the fixture after the query:

```python
import json
import pyarrow.parquet as pq
from geoparquet_io.core.duckdb_metadata import get_geo_metadata

source = get_geo_metadata(
    "https://data.source.coop/giswqs/nwi/wetlands/DC_Wetlands.parquet")
table = pq.read_table("nwi-wetlands.parquet")
metadata = dict(table.schema.metadata)
geo = json.loads(metadata[b"geo"])
geo["columns"]["geometry"]["crs"] = source["columns"]["geometry"]["crs"]
metadata[b"geo"] = json.dumps(geo).encode()
pq.write_table(table.replace_schema_metadata(metadata), "nwi-wetlands.parquet")
```

The raster is one tile, downloaded as-is:

```bash
curl -o rapidai4eo-sample.tif \
  "https://data.source.coop/planet/rapidai4eo/imagery/33N/15E-200N/33N_15E-200N_01_09/PF-SR/2018-01-03.tif"
```

## Add a fixture

Use real data under 5 MB that shows one orchestration edge case. Add the
source and the command to this file. Add a session fixture to
`tests/conftest.py`, and mark the tests that read it `realdata`.
