# Best Practices

These are recommended conventions, not requirements. Portolan linters will warn about deviations but will not fail validation.

## Scalability

### STAC-GeoParquet

For catalogs with many items, **SHOULD** include a [stac-geoparquet](https://github.com/stac-utils/stac-geoparquet) file alongside JSON metadata.

- **SHOULD** provide `items.parquet` when a collection contains > 100 items
- **MUST** provide `items.parquet` when a collection contains > 1000 items

STAC-GeoParquet enables efficient search and filtering without requiring a STAC API server:

```python
import geopandas as gpd

# Query items by bbox without loading all JSON
items = gpd.read_parquet(
    "s3://bucket/collection/items.parquet",
    bbox=(-122.5, 37.5, -122.0, 38.0)
)
```

**Location**: Place `items.parquet` in the collection directory alongside `collection.json`.

**When to use**:
- Image collections with many individual COGs
- Time-series data with frequent updates
- Any collection where users need to search/filter items

**Note**: This is currently a best practice while tooling matures. May be promoted to a core requirement in a future spec version.

### PMTiles

For vector datasets, **SHOULD** include PMTiles derivatives for web visualization.

- **SHOULD** provide `.pmtiles` when a GeoParquet file exceeds 10 MB
- **MUST** provide `.pmtiles` when a GeoParquet file exceeds 100 MB

PMTiles enable efficient web map rendering without server-side tile generation.

**Note**: PMTiles generation requires tippecanoe, which has platform-specific installation requirements. This is currently a best practice while tooling matures.

## Visualization

- **SHOULD** include a thumbnail image generated from default styling
- **SHOULD** provide default styling via `style.json`
  - Compatible with MapLibre GL JS and deck.gl
  - Enables immediate visualization without custom configuration

## Documentation

- **SHOULD** include collection-level READMEs for datasets with:
  - Multiple years of data
  - Multiple source agencies or methodologies
  - Complex versioning or update schedules

## Metadata

- **SHOULD** provide machine-readable metadata in Parquet format for datasets with:
  - Many coded/categorical variables
  - Complex classification schemes
  - Variables requiring detailed definitions

- **SHOULD** include column descriptions
  - May become a core requirement as tooling matures
  - Helps AI systems and users understand data structure

## Multi-file Relationships

### Join Relationships

When geometry and attribute data are in separate files:

- **SHOULD** document the join columns explicitly in the README
- **SHOULD** include a working code example showing how to join the files

Example:

```markdown
## Data Structure

Geometry and attribute data are stored separately:
- `departamentos.parquet` - polygon geometries with `codigo_depto` key
- `attributes.parquet` - demographic attributes with `codigo_depto` key

### Joining the data

```python
import geopandas as gpd
import pandas as pd

# Read files
geometry = gpd.read_parquet("departamentos.parquet")
attributes = pd.read_parquet("attributes.parquet")

# Join on codigo_depto
data = geometry.merge(attributes, on="codigo_depto")
```
```
