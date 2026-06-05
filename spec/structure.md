# Catalog Structure

A Portolan catalog is a directory with STAC metadata and cloud-native geospatial data. Internal tooling state lives in `.portolan/`; all STAC-visible files live at the project root.

## Directory Layout

```
project/
├── .portolan/
│   ├── config.yaml                    # Internal: catalog configuration
│   └── state.json                     # Internal: local sync state
├── catalog.json                       # STAC Catalog (root metadata)
├── versions.json                      # Catalog-level versioning
└── {collection_id}/
    ├── collection.json                # STAC Collection metadata
    ├── versions.json                  # Collection-level versioning
    └── {item_id}/
        └── {filename}.parquet         # Asset file
```

## Root Level

| File | Required | Description |
|------|----------|-------------|
| `.portolan/` | **MUST** | Internal tooling directory (config, state) |
| `catalog.json` | **MUST** | STAC Catalog (root metadata) |
| `versions.json` | **MUST** | Catalog-level version tracking |

The `.portolan` directory **MUST** exist at the project root. Tools **SHOULD** create this directory via `portolan init`.

### `.portolan/` Contents

| File | Required | Description |
|------|----------|-------------|
| `config.yaml` | **MUST** | Catalog configuration (sentinel file) |
| `state.json` | **SHOULD** | Local sync state |

Only Portolan-internal tooling state lives in `.portolan/`. STAC metadata and version manifests live at the project root alongside the data they describe, making catalogs compatible with standard STAC tooling (STAC Browser, PySTAC, stac-validator).

## Collection Level

Each collection is a top-level subdirectory of the project root, named with the collection ID.

| File | Required | Description |
|------|----------|-------------|
| `collection.json` | **MUST** | STAC Collection metadata |
| `versions.json` | **MUST** | Version history and checksums (see [versions.md](versions.md)) |
| `{item_id}/` | — | One directory per dataset item |

Collection IDs **SHOULD**:
- Contain only lowercase letters, numbers, hyphens, and underscores
- Start with a letter
- Be unique within the catalog

Note: The CLI does not currently enforce these naming conventions. Validation may be added in a future release.

## Single-File Collections

When a collection contains a single data file (e.g., one GeoParquet file), the data **MUST** be represented as a collection-level asset. No item directory or item JSON is needed. See [vector format requirements](formats/vector.md#collection-level-assets) for details.

```
{collection_id}/
  collection.json
  versions.json
  {filename}.parquet
  {filename}.pmtiles          (recommended)
  thumbnail.png               (recommended)
```

## Item Level

Items are used when a collection contains multiple data files — for example, partitioned datasets or multi-file raster mosaics.

Each item is a subdirectory of the collection named with the item ID.

| File | Required | Description |
|------|----------|-------------|
| Primary data asset | **MUST** | One of: `.parquet` (vector), `.tif` (raster), `.copc.laz` (point cloud) |
| `{item_id}.pmtiles` | **SHOULD** | Vector tile derivative for web display (vector only) |
| `thumbnail.png` | **SHOULD** | Preview image (any format: `.png`, `.jpg`, `.webp`) |
| `style.json` | **MAY** | MapLibre-compatible styling |

Item IDs are derived from the item directory name. By convention, item directories **SHOULD** be named after the primary data file's stem (e.g., source file `census.shp` goes into item directory `census/`).

## Flat Hierarchy

Portolan catalogs use a **flat hierarchy**: collections contain items directly, with no nested sub-collections.

```
# Correct (flat)
census-2020/tracts/tracts.parquet
census-2020/blocks/blocks.parquet

# Incorrect (nested)
census/2020/tracts/tracts.parquet
```

This simplifies tooling and avoids ambiguity about where versioning boundaries lie.

## STAC Conventions

Portolan catalogs **MUST** be saved as `SELF_CONTAINED` (pystac terminology), meaning:
- All links use relative paths
- The catalog is portable across different hosting locations
- No absolute filesystem paths leak into metadata

### Defaults

| Property | Default Value |
|----------|---------------|
| STAC version | `1.0.0` |
| Catalog ID | `portolan-catalog` |
| Collection license | `proprietary` (SPDX identifier) |

These defaults can be overridden during catalog creation or dataset import.

## Examples

A catalog with a single-file vector collection:

```
project/
├── .portolan/
│   ├── config.yaml
│   └── state.json
├── catalog.json
├── versions.json
└── districts/
    ├── collection.json
    ├── versions.json
    ├── districts.parquet
    ├── districts.pmtiles
    └── thumbnail.png
```

A catalog with a partitioned vector collection (data > 2 GB):

```
project/
├── .portolan/
│   ├── config.yaml
│   └── state.json
├── catalog.json
├── versions.json
└── buildings/
    ├── collection.json
    ├── versions.json
    ├── buildings.pmtiles
    ├── partition-001.parquet
    ├── partition-002.parquet
    └── partition-003.parquet
```
