# geoparquet-io macOS Abort on Multilayer Conversion

## Status
**Known issue** — Workaround applied (skip test on macOS)

## Description
geoparquet-io crashes with `Fatal Python error: Aborted` during multilayer file conversion on macOS. The crash occurs in `read_spatial_to_arrow` when processing FileGDB or GeoPackage files with multiple layers.

## Stack Trace
```
Fatal Python error: Aborted

Thread 0x00000001f080e240 (most recent call first):
  File ".../geoparquet_io/core/convert.py", line 1033 in read_spatial_to_arrow
  File ".../geoparquet_io/api/table.py", line 282 in convert
  File ".../portolan_cli/convert.py", line 669 in _convert_vector_layer
  File ".../portolan_cli/convert.py", line 615 in convert_multilayer_file
```

## Impact
- `test_add_multilayer_creates_stac_structure` crashes CI on macOS
- Does not affect single-layer file processing
- Does not affect Linux or Windows

## Workaround Applied
`tests/integration/test_add_multilayer_integration.py::test_add_multilayer_creates_stac_structure` is skipped on macOS:
```python
@pytest.mark.skipif(
    sys.platform == "darwin",
    reason="geoparquet-io aborts on multilayer conversion on macOS (upstream bug)",
)
```

## Upstream Bug
To be filed at: https://github.com/geoparquet/geoparquet-io/issues

## Related
- CI run: Python 3.12/macOS in PR #346
