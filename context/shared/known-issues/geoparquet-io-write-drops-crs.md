# geoparquet-io drops the CRS on write

**Status:** open upstream. Portolan guards against it.
**Upstream:** geoparquet-io, git main `a23dafa` (PyPI 1.3.0).
**Portolan issue:** #805.

## What happens

`gpio.convert(<geoparquet>).write(<out>)` writes no `crs` key for the primary
geometry column. The source's `crs` value does not reach the output.

The key is absent, not `null`. The GeoParquet specification reads an absent
`crs` as OGC:CRS84. A projected file therefore comes back labelled as
longitude and latitude. The coordinates do not change. Only the label does.

## Reproduction

```python
import json

import geopandas as gpd
import geoparquet_io as gpio
import pyarrow.parquet as pq
from shapely.geometry import Point


def crs_of(path: str) -> object:
    geo = json.loads((pq.ParquetFile(path).schema_arrow.metadata or {})[b"geo"])
    return geo["columns"][geo["primary_column"]].get("crs")


gdf = gpd.GeoDataFrame(
    {"n": [1, 2]},
    geometry=[Point(-8238310, 4970072), Point(-8237000, 4971000)],
    crs="EPSG:3857",
)
gdf.to_parquet("in.parquet")
print("source  :", crs_of("in.parquet"))   # full PROJJSON for EPSG:3857

gpio.convert("in.parquet").write("out.parquet")
print("gpio out:", crs_of("out.parquet"))  # None, and the key is absent
```

A GeoPackage source behaves the same way. The operations do not matter.
`add_bbox()` and `sort_hilbert()` change nothing here.

## Why Portolan cares

Issue #805 makes `add` rewrite a GeoParquet in place to give it a bbox covering
column. That rewrite replaces the operator's own file. Before #805 `add` copied
a `.parquet` source through untouched, so this upstream bug never touched it.

## What Portolan does

`preparation._assert_rewrite_kept_everything` compares the source and the
rewritten file before the swap. It compares the row count, the column set, and
the declared CRS. `preparation._rewrite_or_keep` catches the failure, keeps the
operator's file, and prints the reason:

```console
$ portolan add roads/data.parquet
→ Rewriting data.parquet (11.1KB): it carries no bbox covering column
⚠ Kept data.parquet as it is: it would lose the CRS it declared, because geoparquet-io writes none
✓ Added 1 file to 1 collection
```

The file stays non-conformant. `portolan check` reports `PTL-DAT-007` on it.
That is the honest outcome. Portolan does not write the CRS back into the
rewritten copy, because repairing an upstream writer's output in our layer is
the workaround this repo does not take.

## When the upstream fix lands

Remove nothing. The guard is a safety gate on a destructive in-place operation,
not a workaround for this one bug. It stays useful for any future writer
regression. The warning stops firing on its own once the CRS survives the write.
