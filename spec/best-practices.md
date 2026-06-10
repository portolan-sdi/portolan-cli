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
- **SHOULD** provide visualization styles as standalone STAC assets (see [Visualization Styles](#visualization-styles))

### Visualization Styles

Collections with PMTiles assets **SHOULD** include one or more Mapbox GL v8 style files in a `styles/` subdirectory. Each style is a complete, self-contained JSON file that can be loaded directly by MapLibre GL JS.

#### Style File Format

```json
{
  "version": 8,
  "name": "Buildings by Construction Year",
  "sources": {
    "data": {
      "type": "vector",
      "url": "../data.pmtiles"
    }
  },
  "layers": [
    {
      "id": "buildings-fill",
      "type": "fill",
      "source": "data",
      "source-layer": "buildings",
      "paint": {
        "fill-color": ["interpolate", ["linear"], ["get", "year"],
          1900, "#8B4513", 1960, "#DAA520", 2020, "#FFFF00"
        ],
        "fill-opacity": 0.7
      }
    }
  ]
}
```

Key conventions:
- `version`: Always `8` (Mapbox GL spec version)
- `name`: Human-readable label (used in style pickers)
- `sources.data.url`: Relative path from `styles/` to the PMTiles file (typically `../filename.pmtiles`)
- `layers[].source`: Always `"data"` (matches the source key)

#### STAC Registration

Each style file is registered as a STAC asset on the collection:

```json
{
  "portolan:styles": ["styles/default", "styles/by-age"],
  "assets": {
    "styles/default": {
      "href": "./styles/default.json",
      "type": "application/json",
      "title": "Default",
      "description": "Blue building footprints with semi-transparent fill.",
      "roles": ["style"]
    },
    "styles/by-age": {
      "href": "./styles/by-age.json",
      "type": "application/json",
      "title": "Buildings by Age",
      "description": "Color ramp from brown (pre-1900) to yellow (post-2010).",
      "roles": ["style"]
    }
  }
}
```

- **Asset key**: `styles/{name}` (matches the filename stem)
- **Type**: `application/json`
- **Roles**: `["style"]`
- **Title**: Short label for style pickers
- **Description**: What the style shows and what the colors represent

#### `portolan:styles` Manifest

The `portolan:styles` property on the collection is a JSON array of asset keys in display order. The first entry is the default style.

- If only one style exists, it is the default
- Consumers **SHOULD** render the first style by default
- Consumers with multiple styles **SHOULD** offer a style picker

#### Best Practices

1. **Create multiple styles for rich datasets.** If a collection has interesting categorical or numeric attributes, create data-driven styles for each (e.g., buildings by age, by use, by height).

2. **Vary default styles across a catalog.** Each collection should have a visually distinct default color so the catalog is not monotone. Use subject matter to inform color choices — water in blues, vegetation in greens, built environment in warm tones.

3. **Use data-driven styling.** Leverage Mapbox GL expressions (`interpolate`, `match`, `case`, `step`) to reveal patterns. Include a description explaining what the colors represent.

4. **Consider label layers.** For datasets with names (roads, monuments, admin areas), include a label layer or a dedicated "with labels" style variant.

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
