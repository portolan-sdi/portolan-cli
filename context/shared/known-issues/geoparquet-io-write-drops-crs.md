# geoparquet-io drops the CRS on write

**Status:** resolved. Portolan runs geoparquet-io 1.4.0, which contains the fix.
**Upstream issue:** [geoparquet-io#625](https://github.com/geoparquet/geoparquet-io/issues/625), closed 2026-08-20 by
[`ff02db8`](https://github.com/geoparquet/geoparquet-io/commit/ff02db82) ("Fix Table API dropping the source CRS between convert() and write()", PR #644).
**Shipped in:** geoparquet-io 1.4.0, released 2026-08-30.
**Portolan issues:** #805 introduced the guard. #810 bumped the pin and closed this.

This note stays as the record. Do not file a new upstream issue.

## What used to happen

`gpio.convert(<geoparquet>).write(<out>)` wrote no `crs` key for the primary
geometry column. The source's `crs` value did not reach the output.

The key was absent, not `null`. The GeoParquet specification reads an absent
`crs` as OGC:CRS84. A projected file therefore came back labelled as longitude
and latitude. The coordinates did not change. Only the label did.

Only the Python Table API dropped it, which is what Portolan calls. The CLI
never lost the CRS.

## Why Portolan cared

Issue #805 makes `add` rewrite a GeoParquet in place to give it a bbox covering
column. That rewrite replaces the operator's own file. Before #805 `add` copied
a `.parquet` source through untouched, so the bug never touched it.

## What Portolan does now

`preparation._assert_rewrite_kept_everything` still compares the source and the
rewritten file before the swap. It compares the row count, the column set, and
the declared CRS. `preparation._rewrite_or_keep` catches a failure and keeps the
operator's file.

**Keep this gate.** It guards a destructive in-place operation against any
cause. It is not a workaround for one dependency bug. It stays silent while the
rewrite is faithful, which is the normal case on 1.4.0.

A projected file now completes the rewrite and keeps its CRS:

```console
$ portolan add roads/data.parquet
→ Rewriting data.parquet (11.1KB): it carries no bbox covering column
✓ Added 1 file to 1 collection

$ python -c "import geopandas as gpd; print(gpd.read_parquet('roads/data.parquet').crs.to_epsg())"
3857
```

## What the fix exposed

geoparquet-io now preserves the source CRS, so Portolan started to receive CRS
values it had never seen. An ESRI `.prj` such as POSGAR 1994 carries no
authority code, and pyproj renders it as a PROJJSON dict rather than an
`EPSG:NNNN` string. `preparation._extract_bbox_wgs84` raised
`PROJJSON CRS not supported. Convert to EPSG code or WKT string.` on that value.
That message was Portolan's own, not an upstream regression.

`pyproj.CRS.from_user_input` reads PROJJSON directly, so `portolan_cli/crs.py`
now accepts `str | dict | None` and passes the dict through. `crs.describe_crs`
gives a dict a short label, because a message must not print a whole coordinate
system definition.

## Tests that hold this

- `tests/integration/test_add_spatial_optimization.py::TestTheRewriteNeverLosesData` proves the rewrite keeps EPSG:3857 and adds the covering column.
- `tests/integration/test_add_spatial_optimization.py::TestAProjjsonCrsSurvivesAdd` proves a POSGAR Shapefile publishes a lon/lat extent.
- `tests/unit/test_preparation.py::TestAssertRewriteKeptEverything` drives the gate directly.
