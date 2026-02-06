# geoparquet-io Windows Segfault on Malformed Input

## Status
**Known issue** — Workaround applied (skip test on Windows)

## Description
geoparquet-io crashes with a Windows fatal exception (access violation) when processing malformed GeoJSON input on Windows. The crash occurs in the `detect_crs_from_spatial_file` function in the underlying C++ code.

## Stack Trace
```
Windows fatal exception: access violation

Current thread 0x000004ac (most recent call first):
  File "...\geoparquet_io\core\common.py", line 1230 in detect_crs_from_spatial_file
  File "...\geoparquet_io\api\table.py", line 278 in convert
```

## Impact
- Integration tests that pass malformed input to geoparquet-io crash on Windows
- Does not affect valid input processing
- Does not affect Linux/macOS

## Workaround Applied
`tests/integration/test_gpio_integration.py::test_malformed_geojson_raises` is skipped on Windows:
```python
@pytest.mark.skipif(
    sys.platform == "win32",
    reason="geoparquet-io segfaults on malformed input on Windows (upstream bug)",
)
```

## Upstream Bug
To be filed at: https://github.com/geoparquet/geoparquet-io/issues

## Related
- PR #36 (v0.3 format conversion) first encountered this issue
