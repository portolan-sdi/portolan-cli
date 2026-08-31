# DuckDB 1.5.5 segfaults in ST_Read_Meta on malformed input

**Status:** open upstream. Portolan pins `duckdb>=1.5.2,<1.5.5`.
**Affected version:** duckdb 1.5.5 only. 1.5.1, 1.5.2, 1.5.3 and 1.5.4 are all fine.
**Found by:** the geoparquet-io 1.4.0 bump (Portolan issue #810), which raised the DuckDB floor to `>=1.5.2` and let the resolver reach 1.5.5.

## What happens

`ST_Read_Meta` on a malformed spatial file kills the process with `SIGSEGV`.
The process exits 139. No Python exception is raised, so no `except` clause can
catch it and no error reaches the operator.

This is a DuckDB spatial bug. geoparquet-io is not involved.

## Reproduction

```python
import duckdb

with open("bad.geojson", "w") as f:
    f.write('{"type": "FeatureCollection", "features": [INVALID')

con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")
con.execute("SELECT * FROM ST_Read_Meta('bad.geojson')").fetchone()
```

Result by version:

| duckdb | Result |
|--------|--------|
| 1.5.1 | returns without error |
| 1.5.2 | returns without error |
| 1.5.3 | returns without error |
| 1.5.4 | returns without error |
| 1.5.5 | `SIGSEGV`, exit 139 |

## Why Portolan cares

`convert.read_spatial_to_arrow` calls `crs_utils.detect_crs_from_spatial_file`,
which runs `ST_Read_Meta`. Every `portolan add` of a non-Parquet vector file
reaches it. One malformed file in a directory therefore kills the whole run,
with no message and no partial result.

`tests/unit/test_convert.py::test_conversion_exception_returns_failed` catches
this. It feeds truncated GeoJSON to `convert_file` and expects
`ConversionStatus.FAILED`. On 1.5.5 the test process dies instead.

## What Portolan does

`pyproject.toml` pins `duckdb>=1.5.2,<1.5.5`. The lower bound comes from
geoparquet-io 1.4.0. This follows the same pattern as the `pyarrow<22.0.0` pin.

Do not add a subprocess guard or a pre-parse check around the call. The crash
belongs to DuckDB. Raise the pin once a fixed release ships.

## Related

The existing note
`context/shared/known-issues/geoparquet-io-windows-segfault.md` records a
Windows-only crash on malformed input. This one is separate. It reproduces on
Linux and it comes from the DuckDB version, not the platform.
