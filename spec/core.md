# Core Requirements

These requirements apply to all Portolan catalogs, regardless of data format.

## Catalog Structure

A Portolan catalog is a directory with STAC metadata at the project root and internal tooling configuration in `.portolan/`. See [structure.md](structure.md) for the full directory layout.

```
project/
├── .portolan/
│   └── config.yaml
├── catalog.json
├── llms.txt
├── versions.json
└── {collection_id}/
    ├── collection.json
    ├── llms.txt
    ├── versions.json
    └── {item_id}/
        └── data.parquet
```

## STAC Compliance

- **MUST** be a valid STAC Catalog or Collection
- **MUST** follow STAC specification version 1.1.0
- **MUST** use `SELF_CONTAINED` catalog type (relative links, portable)

### Spatial Extent for Tabular Collections

STAC requires `extent.spatial.bbox` for Collections. For **tabular (non-geospatial) collections** (`portolan:geospatial: false`):

- `extent.spatial.bbox` represents the **area of interest** (AOI) the data pertains to, not a geometric footprint
- Portolan CLI **always provides** `extent.spatial` via automatic AOI inheritance from sibling collections (or global fallback)
- Portolan validators treat the bbox as informational metadata, not a constraint

See [formats/tabular.md](formats/tabular.md) for full tabular collection requirements.

## Data Storage

Portolan catalogs assume data is hosted in S3-compatible object storage. This is the ground truth for all assets.

## Asset URLs

Asset hrefs **MUST** be absolute S3 URLs:

```json
"assets": {
  "data": {
    "href": "https://bucket-name.s3.region.amazonaws.com/path/to/file.parquet",
    "type": "application/vnd.apache.parquet",
    "roles": ["data"]
  }
}
```

## Link Paths

STAC link relations (`root`, `self`, `child`, `parent`) **SHOULD** use relative paths within the catalog structure:

```json
"links": [
  {"rel": "root", "href": "./catalog.json", "type": "application/json"},
  {"rel": "self", "href": "./collection.json", "type": "application/json"},
  {"rel": "child", "href": "./2022/collection.json", "type": "application/json"}
]
```

This keeps the catalog portable if mirrored to a different bucket.

## Providers

**SHOULD** use STAC-standard `providers` array:

```json
"providers": [
  {
    "name": "Organization Name",
    "roles": ["producer"],
    "url": "https://example.com"
  }
]
```

## Source Provenance

When data is extracted from an external source that is the canonical location for the data, the collection **MUST** include a `rel: "via"` link pointing to the original source URL:

```json
{
  "rel": "via",
  "href": "https://services-eu1.arcgis.com/example/FeatureServer",
  "type": "text/html",
  "title": "Source ArcGIS Feature Service"
}
```

This is standard STAC practice for provenance and enables consumers to trace data back to its origin.

## Root Documentation

- **MUST** include a `README.md` at the catalog root
- README content requirements: Title, description, license, and data provenance at minimum

## AI & LLM Integration

- **MUST** include an `llms.txt` file at both the catalog root and each collection directory
- **MUST** link `llms.txt` in the STAC JSON `links` array with `rel: "llms"`

See [ai-integration.md](ai-integration.md) for full requirements and content recommendations.

## Versioning

- **MUST** include version tracking via `versions.json` manifest file (see [versions.md](versions.md))
- **SHOULD** include STAC link relations (`predecessor-version`, `successor-version`, `latest-version`) when multiple versions exist
- **SHOULD** include a link to versions.json in the collection:

```json
{
  "rel": "version-history",
  "href": "./versions.json",
  "type": "application/json"
}
```

The `versions.json` file tracks version history, asset checksums, and sync state per collection.

## Recognized File Extensions

See [extensions.md](extensions.md) for the complete list of file extensions recognized by Portolan tools, including:
- Primary geospatial formats (GeoParquet, GeoJSON, Shapefile, COG, etc.)
- Sidecar files (Shapefile components, aux.xml, etc.)
- Visualization formats (PMTiles, MBTiles)
- Files that are skipped during import

## Format-Specific Requirements

Additional requirements apply based on data type. See format addenda:

- [Vector data](formats/vector.md)
- [Raster data](formats/raster.md)
- [Point cloud data](formats/pointcloud.md)
- [Tabular (non-geospatial) data](formats/tabular.md)

Format addenda are normative and define **MUST** requirements, not suggestions.
