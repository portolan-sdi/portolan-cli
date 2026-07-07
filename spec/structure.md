# Catalog Structure

A Portolan catalog is a directory with STAC metadata and cloud-native geospatial data. Internal tooling configuration lives in `.portolan/`; all STAC-visible files live at the project root.

## Directory Layout

```
project/
├── .portolan/
│   └── config.yaml                    # Internal: catalog configuration
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
| `.portolan/` | **MUST** | Internal tooling directory (config) |
| `catalog.json` | **MUST** | STAC Catalog (root metadata) |
| `versions.json` | **MUST** | Catalog-level version tracking |

The `.portolan` directory **MUST** exist at the project root. Tools **SHOULD** create this directory via `portolan init`.

### `.portolan/` Contents

| File | Required | Description |
|------|----------|-------------|
| `config.yaml` | **MUST** | Catalog configuration (sentinel file) |

Only Portolan-internal tooling configuration lives in `.portolan/`. STAC metadata and version manifests live at the project root alongside the data they describe, making catalogs compatible with standard STAC tooling (STAC Browser, PySTAC, stac-validator).

## Collection Level

Each collection lives in a subdirectory named with its collection ID. For a nested collection the ID is a POSIX path (e.g. `environment/air-quality`) and the directory sits under one or more intermediate catalog directories (see [Nested Catalogs, Flat Collections](#nested-catalogs-flat-collections)).

| File | Required | Description |
|------|----------|-------------|
| `collection.json` | **MUST** | STAC Collection metadata |
| `versions.json` | **MUST** | Version history and checksums (see [versions.md](versions.md)) |
| `{item_id}/` | — | One directory per item |

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
  styles/                     (recommended for PMTiles collections)
    default.json
```

## Item Level

Items are used when a collection contains multiple data files — for example, partitioned collections or multi-file raster mosaics.

Each item is a subdirectory of the collection named with the item ID.

| File | Required | Description |
|------|----------|-------------|
| Primary data asset | **MUST** | One of: `.parquet` (vector), `.tif` (raster), `.copc.laz` (point cloud) |
| `{item_id}.pmtiles` | **SHOULD** | Vector tile derivative for web display (vector only) |
| `thumbnail.png` | **SHOULD** | Preview image (any format: `.png`, `.jpg`, `.webp`) |

Item IDs are derived from the item directory name. By convention, item directories **SHOULD** be named after the primary data file's stem (e.g., source file `census.shp` goes into item directory `census/`).

## Nested Catalogs, Flat Collections

Portolan organizes hierarchy with **nested catalogs**, not nested collections ([ADR-0032](../context/shared/adr/0032-nested-catalogs-with-flat-collections.md)). Intermediate levels are catalogs (`catalog.json`); collections are always leaves. A collection **MUST NOT** contain a child collection. This keeps collections flat for STAC API compatibility while still allowing thematic organization above them.

Directory mapping:

| Directory | File | Role |
|-----------|------|------|
| Root | `catalog.json` | Root catalog |
| Level above a collection | `catalog.json` | Intermediate (thematic) catalog |
| Data directory | `collection.json` | Leaf collection (holds vector assets or item subdirs) |
| Subdirectory within a collection | `catalog.json` | Organizes many items below a collection |

A nested collection's ID is its POSIX path from the catalog root (e.g. `environment/air-quality`). `portolan add` writes a `catalog.json` at each intermediate level and links parent → child down to the leaf `collection.json`.

```
# Correct: intermediate levels are catalogs, collections are leaves
environment/
├── catalog.json                 ← intermediate catalog
├── air-quality/
│   ├── collection.json          ← leaf collection (data)
│   └── pm25.parquet
└── water-quality/
    ├── collection.json          ← leaf collection (data)
    └── turbidity.parquet

# Incorrect: a collection containing a child collection
environment/
├── collection.json              ← collection with a child collection (not allowed)
└── air-quality/
    └── collection.json
```

Deep nesting is allowed; each level above the leaf is a catalog. Catalogs may also appear *below* a collection to organize its items (for example, a raster collection grouping items by year).

## STAC Conventions

Portolan catalogs **MUST** be saved as `SELF_CONTAINED` (pystac terminology), meaning:
- All links use relative paths
- The catalog is portable across different hosting locations
- No absolute filesystem paths leak into metadata

### Defaults

| Property | Default Value |
|----------|---------------|
| STAC version | `1.1.0` |
| Catalog ID | `portolan-catalog` |
| Collection license | `other` (STAC 1.1 keyword for a license not covered by SPDX; add a `rel="license"` link when the concrete license is known) |

These defaults can be overridden during catalog creation or import.

## Examples

A catalog with a single-file vector collection:

```
project/
├── .portolan/
│   └── config.yaml
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
│   └── config.yaml
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
