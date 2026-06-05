# Core Requirements

These requirements apply to all Portolan catalogs, regardless of data format.

## Catalog Structure

A Portolan catalog is a directory with STAC metadata at the project root and internal tooling state in `.portolan/`. See [structure.md](structure.md) for the full directory layout.

```
project/
├── .portolan/
│   ├── config.yaml
│   └── state.json
├── catalog.json
├── versions.json
└── {collection_id}/
    ├── collection.json
    ├── versions.json
    └── {item_id}/
        └── data.parquet
```

## STAC Compliance

- **MUST** be a valid STAC Catalog or Collection
- **MUST** follow STAC specification version 1.0.0 or later
- **MUST** use `SELF_CONTAINED` catalog type (relative links, portable)

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
- README content requirements: TBD (see [QUESTIONS.md](QUESTIONS.md))

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

Format addenda are normative and define **MUST** requirements, not suggestions.
