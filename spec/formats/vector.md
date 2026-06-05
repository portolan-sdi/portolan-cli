# Vector Data Format Requirements

## Required Formats

- **MUST** provide data in GeoParquet format
  - Follows [GeoParquet specification](https://geoparquet.org/)
  - Enables efficient querying and analysis without a server
  - **SHOULD** follow the [Best Practices for Distributing GeoParquet](https://github.com/opengeospatial/geoparquet/blob/main/format-specs/distributing-geoparquet.md) recommendations (zstd compression, bbox covering, spatial ordering, appropriate row group sizes)

## Collection-Level Assets

When a vector dataset is represented as a single file (GeoParquet, FlatGeobuf, or any other supported format), it **MUST** be represented as a collection-level asset rather than wrapped in a STAC item. The item indirection adds no value for single-file datasets.

```json
{
  "type": "Collection",
  "id": "tunnels",
  "assets": {
    "data": {
      "href": "./tunnels.parquet",
      "type": "application/vnd.apache.parquet",
      "roles": ["data"]
    },
    "pmtiles": {
      "href": "./tunnels.pmtiles",
      "type": "application/vnd.pmtiles",
      "roles": ["visual"]
    }
  }
}
```

The directory layout for a single-file collection is flat:

```
tunnels/
  collection.json
  versions.json
  tunnels.parquet
  tunnels.pmtiles        (recommended)
  thumbnail.png          (recommended)
```

No item directory or item JSON is needed.

## Partitioned Datasets

GeoParquet files larger than approximately 2 GB **SHOULD** be spatially partitioned into multiple files, following the guidance in [Best Practices for Distributing GeoParquet](https://github.com/opengeospatial/geoparquet/blob/main/format-specs/distributing-geoparquet.md#spatial-partitioning).

When a dataset is partitioned:

- Each partition file **MUST** be represented as a STAC item within the collection, with its spatial extent (bbox) reflecting the data in that partition.
- The collection description **SHOULD** include the glob pattern that tools can use to access all partitions as a single dataset, since most users want a single URL they can pass to their analysis tools rather than enumerating STAC items. For example:

  > Access all partitions: `s3://bucket/buildings/*.parquet`

### Directory Layout for Partitioned Datasets

```
buildings/
  collection.json
  versions.json
  partition-001.parquet
  partition-002.parquet
  partition-003.parquet
  ...
```

Each partition file also has a corresponding STAC item linked from the collection, but the collection-level glob asset is the primary entry point for data access.

## Recommended Formats

- **SHOULD** provide PMTiles for visualization
  - Optimized for web map rendering
  - Cloud-native, range-request friendly
  - PMTiles **MUST** be represented as a collection-level asset, not only at the item level — this ensures the visualization derivative is always discoverable alongside the data without navigating into items
  - When PMTiles are provided, **MUST** add a `rel: "pmtiles"` link following the [web-map-links](https://github.com/stac-extensions/web-map-links) STAC extension

## Styling

- **MAY** include `style.json` for default visualization
  - Compatible with MapLibre GL JS and deck.gl
  - See [best practices](../best-practices.md) for styling recommendations
