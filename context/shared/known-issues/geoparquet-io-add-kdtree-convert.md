# geoparquet-io `Table.add_kdtree` fails on a converted table

**Status:** open upstream.
**Upstream issue:** [geoparquet-io#1222](https://github.com/geoparquet/geoparquet-io/issues/1222), filed 2026-10-12.
**Affected versions:** geoparquet-io 1.4.0 through 1.6.0.
**Portolan issue:** #953 found it.

## What happens

`Table.add_kdtree` raises a DuckDB binder error on a table that
`gpio.convert()` returns:

```text
_duckdb.BinderException: Binder Error: No function matches the given name and argument types 'ST_Centroid(BLOB)'.
```

The converted table stores the geometry as Arrow `large_binary`.
`core/add/kdtree.py` `_build_sampling_query` passes that WKB column to
`ST_Centroid` with no cast to GEOMETRY. The same call works on a table that
`gpio.read()` returns from a GeoParquet file.

## Where Portolan meets it

A non-partitioned kdtree index fails: `conversion.vector.spatial_index: kdtree`
with `partition: false`. `convert.apply_vector_settings` calls `add_kdtree`.
`portolan add` and `portolan check --fix` both reach that call.

A partitioned kdtree write works. `convert._write_partitioned` lets
`Table.partition_by_kdtree` add the column, and that method does not hit the bug.

## What Portolan does

Nothing. Do not cast the geometry or write a temporary file in Portolan to
avoid the bug. The fix belongs in geoparquet-io.

## Tests

`tests/unit/test_convert.py::TestPartitionedOutputWithGeoparquetIO::test_single_file_kdtree_index`
is marked `xfail(strict=True)`. It fails CI once a geoparquet-io release fixes
the bug. Then raise the pin, remove the marker, and delete this note.
