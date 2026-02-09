# PMTiles Validation Fixtures

Test fixtures for the `PMTilesRecommendedRule` validation rule.

## Files

| File | Purpose |
|------|---------|
| `sample.parquet` | Minimal valid GeoParquet file (1 point feature) |
| `sample.pmtiles` | Empty placeholder file (rule only checks existence, not content) |

## Design Note

The `PMTilesRecommendedRule` only checks for **file existence**, not validity:
- Validating PMTiles content would require the PMTiles plugin
- The rule's purpose is to *recommend* generating PMTiles, not validate existing ones
- An empty placeholder is sufficient for testing "file exists" logic

## Usage

These fixtures test the PMTiles recommendation logic:
- `sample.parquet` alone → should emit WARNING
- `sample.parquet` + `sample.pmtiles` → should PASS (no warning)

## Regenerating sample.parquet

```bash
uv run python -c "
import geoparquet_io as gpio
import json
from pathlib import Path

geojson = {
    'type': 'FeatureCollection',
    'features': [{
        'type': 'Feature',
        'properties': {'name': 'test'},
        'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]}
    }]
}

temp = Path('temp.geojson')
temp.write_text(json.dumps(geojson))
gpio.convert(str(temp)).write('sample.parquet')
temp.unlink()
"
```
