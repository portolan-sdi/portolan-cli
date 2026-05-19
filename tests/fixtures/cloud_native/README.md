# Cloud-Native Vector Format Fixtures

Test fixtures for cloud-native vector formats that don't require conversion.

## Files

| File | Format | Features | CRS | Notes |
|------|--------|----------|-----|-------|
| `sample.fgb` | FlatGeobuf | 3 Points | EPSG:4326 | SF, NYC, Chicago |
| `sample.pmtiles` | PMTiles (MVT) | 3 Points | EPSG:3857 (tiles), bbox in 4326 | z4-z8 |

## Generation

Generated from the same source GeoDataFrame:

```python
import geopandas as gpd
from shapely.geometry import Point

gdf = gpd.GeoDataFrame({
    'name': ['Feature A', 'Feature B', 'Feature C'],
    'value': [100, 200, 300],
    'geometry': [
        Point(-122.4, 37.8),  # San Francisco
        Point(-73.9, 40.7),   # New York
        Point(-87.6, 41.9),   # Chicago
    ]
}, crs='EPSG:4326')

# FlatGeobuf
gdf.to_file('sample.fgb', driver='FlatGeobuf')

# PMTiles (requires tippecanoe)
gdf.to_file('/tmp/sample.geojson', driver='GeoJSON')
# tippecanoe -o sample.pmtiles -z8 -Z4 --force /tmp/sample.geojson
```

## Usage

These fixtures test Issue #368: adding cloud-native vector formats without conversion.

- **PMTiles**: bbox extracted from header (WGS84), tiles are Web Mercator (3857)
- **FlatGeobuf**: CRS, bbox, schema, feature count extracted from header
