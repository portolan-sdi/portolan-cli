# Feature: convert_file() Function

Converts a single file to cloud-native format (GeoParquet or COG).

## Function Signature

```python
def convert_file(
    source: Path,
    output_dir: Path | None = None,
) -> ConversionResult
```

## Happy Path

### Vector Conversion
- [ ] GeoJSON file -> GeoParquet: Returns SUCCESS, output path ends with .parquet
- [ ] Shapefile -> GeoParquet: Returns SUCCESS, wraps existing convert_vector()
- [ ] GeoPackage -> GeoParquet: Returns SUCCESS

### Raster Conversion
- [ ] Non-COG TIFF -> COG: Returns SUCCESS, output is valid COG
- [ ] JPEG2000 -> COG: Returns SUCCESS
- [ ] COG defaults applied: DEFLATE compression, predictor=2, 512x512 tiles, nearest resampling

## Skip Scenarios (Cloud-Native Input)

- [ ] GeoParquet input: Returns SKIPPED, output is None, format_to is None
- [ ] Valid COG input: Returns SKIPPED
- [ ] FlatGeobuf input: Returns SKIPPED
- [ ] PMTiles input: Returns SKIPPED

## Failure Scenarios

- [ ] Exception during conversion: Returns FAILED, error message captured, original file preserved
- [ ] File not found: Raises FileNotFoundError (not caught)
- [ ] Unsupported format: Returns FAILED with clear error message

## Validation Scenarios

- [ ] Validation fails after conversion: Returns INVALID, output file preserved for inspection
- [ ] COG validation fails: Returns INVALID with validation errors in error field

## Output Location

- [ ] Default output_dir=None: Output in same directory as source
- [ ] Custom output_dir: Output in specified directory
- [ ] Output filename: Same stem as source with appropriate extension (.parquet or .tif)

## Invariants

- [ ] Original file is NEVER deleted (side-by-side conversion)
- [ ] duration_ms is always >= 0
- [ ] format_from is always non-empty string
- [ ] status is always a valid ConversionStatus
- [ ] On SUCCESS: output is not None and file exists
- [ ] On SKIPPED: output is None, format_to is None, error is None
- [ ] On FAILED: error is not None
- [ ] On INVALID: output is not None (file kept for inspection), error is not None
