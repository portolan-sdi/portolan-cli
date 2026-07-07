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

## Human-Readable Titles

STAC Browser and other clients render `child`/`item` link titles directly; without them a client must fetch every child just to display its name. Portolan therefore requires human-readable titles throughout the catalog (see [ADR-0053](../context/shared/adr/0053-mandatory-human-readable-titles.md)):

- Every `catalog.json` and `collection.json` **MUST** have a non-empty `title` and `description`
- Titles **MUST** be human-readable — a raw slug (e.g. `snake_case`) or a technical namespace prefix (e.g. `ns:LayerName`) is not acceptable
- Every `child` and `item` link **MUST** include a `title`

`portolan check` enforces these at ERROR severity, and `portolan check --fix` auto-populates human-readable titles by humanizing slugs.

## Bounding Box Validity

Bounding boxes carry the spatial footprint that drives extent unions and map-UI browsing. Garbage coordinates poison the catalog-level extent and break viewers, so every `bbox` (catalog extent, collection extent, and item) **MUST**:

- Contain no `NaN` or infinite values (including 3D elevation coordinates)
- Contain no sentinel "effectively infinite" values (e.g. `±1.79e308`)
- Have WGS84 coordinates within range (longitude in `[-180, 180]`, latitude in `[-90, 90]`)
- Have `south <= north`

`portolan check` enforces bbox validity at ERROR severity.

### Spatial Extent for Tabular Collections

STAC requires `extent.spatial.bbox` for Collections. For **tabular (non-geospatial) collections** (`portolan:geospatial: false`):

- `extent.spatial.bbox` represents the **area of interest** (AOI) the data pertains to, not a geometric footprint
- Portolan CLI **always provides** `extent.spatial` via automatic AOI inheritance from sibling collections (or global fallback)
- Portolan validators treat the bbox as informational metadata, not a constraint

See [formats/tabular.md](formats/tabular.md) for full tabular collection requirements.

## Temporal Metadata

Items **SHOULD** carry an explicit `datetime` (or `start_datetime`/`end_datetime` interval) describing when the data applies. Items added without a datetime receive a null temporal extent (an open interval) and are marked `portolan:datetime_provisional: true` (see [ADR-0035](../context/shared/adr/0035-temporal-extent-handling.md)).

Provisional items are valid STAC but temporally incomplete:

- They are accepted so ingestion is never blocked on unknown dates
- `portolan check` flags them at WARNING severity
- Enriching the datetime enables time-based browsing and clears the provisional flag

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

STAC link relations (`root`, `self`, `child`, `parent`) **SHOULD** use relative paths within the catalog structure. Hierarchy is built from nested catalogs, so `child` links point from a catalog down to its intermediate catalogs or leaf collections, never from one collection to another (see [structure.md](structure.md#nested-catalogs-flat-collections)). This is an intermediate catalog linking to a leaf collection:

```json
"links": [
  {"rel": "root", "href": "../catalog.json", "type": "application/json"},
  {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
  {"rel": "self", "href": "./catalog.json", "type": "application/json"},
  {"rel": "child", "href": "./air-quality/collection.json", "type": "application/json", "title": "Air Quality"}
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
