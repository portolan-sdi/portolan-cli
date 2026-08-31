# S2 spatial indexing is unavailable in geoparquet-io 1.4.0

**Status:** open upstream. Portolan needs no change when it clears.
**Upstream issue:** [geoparquet-io#778](https://github.com/geoparquet/geoparquet-io/issues/778).
**Portolan issue:** #810 bumped the pin that introduced this.

## What happens

`gpio add s2` and `gpio partition s2` stop with an explanation before they read
input. Through the Python Table API, `Table.add_s2()` and
`Table.partition_by_s2()` raise `ExtensionUnavailableError`.

geoparquet-io 1.4.0 raised its floor to `duckdb>=1.5.2`, which drops a segfault
in geometry repair. The DuckDB `geography` community extension, which S2 needs,
is not published for that DuckDB. The build hits a C++11/C++17 link error in
DuckDB's plan_serializer (duckdb/duckdb#22097). The fix is merged upstream
(paleolimbot/duckdb-geography#34) and republication is pending.

## What Portolan does

Nothing. `s2` stays in `conversion_config.VALID_SPATIAL_INDEXES`, and
`convert._add_spatial_index` and `convert._write_partitioned` still call the S2
methods. geoparquet-io raises before it reads input, so an operator who selects
`s2` gets the reason and loses no data.

S2 works again with no Portolan change once the extension is republished.

`a5` is the closest substitute. It gives the same kind of global cell index and
needs no `geography` extension.

## Tests

`tests/unit/test_convert.py::test_convert_file_builds_an_s2_index` is marked
`xfail(strict=False)`. It passes on its own once the extension returns, which
is the signal to delete this note.
