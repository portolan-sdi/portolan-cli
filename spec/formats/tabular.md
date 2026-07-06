# Tabular (Non-Geospatial) Data Format Requirements

Portolan supports **non-geospatial tabular data** as companion data alongside geospatial layers. This enables catalogs to include tables keyed by time, administrative code, or category rather than by location.

## Scope

Portolan is a **geospatial-first** catalog tool. Tabular support is scoped to companion data that relates to the same geographic area as the catalog's spatial layers — not general-purpose data cataloging.

Examples of appropriate tabular data:
- Census demographics linked by tract ID
- Permit records keyed by parcel number
- Budget allocations by administrative unit
- Time-series data (electricity load, sensor readings)

## Enabling Tabular Support

Tabular data support is **opt-in** via `.portolan/config.yaml`:

```yaml
tabular:
  enabled: true   # Track standalone tabular files as collection-level assets
  convert: true   # Convert CSV/TSV/Excel to Parquet (default)
```

When `tabular.enabled` is `false` (default):
- Tabular files **with** a companion geo file → tracked as companion assets
- Tabular files **without** a companion geo file → rejected with helpful error

When `tabular.enabled` is `true`:
- Standalone tabular files → tracked as collection-level assets
- A collection can be tabular-only (no geo data)

## Recognized Formats

| Extension | Format | Handling |
|-----------|--------|----------|
| `.parquet` | Plain Parquet | Content inspection — no `geo` metadata key |
| `.csv` | CSV | Converts to Parquet |
| `.tsv` | TSV | Converts to Parquet |
| `.xlsx`, `.xls` | Excel | Converts to Parquet |

### GeoParquet vs Plain Parquet Classification

`.parquet` files are classified by content inspection:

1. Read Parquet footer metadata (O(1) operation)
2. Check for `b"geo"` key in schema metadata
3. If present → GeoParquet (geo pipeline)
4. If absent → Plain Parquet (tabular pipeline)

## Marking Collections Non-Spatial

Tabular collections **MUST** include the `portolan:geospatial` property:

```json
{
  "type": "Collection",
  "id": "electricity-prices",
  "portolan:geospatial": false,
  ...
}
```

- `portolan:geospatial: false` → tabular collection, spatial extent relaxations apply
- `portolan:geospatial: true` or absent → geospatial collection, all spatial requirements apply

This explicit flag distinguishes *intentionally non-spatial* from *spatial but unmeasured*, enabling validators and federation agents to route queries correctly.

## Spatial Extent Handling

For tabular collections (`portolan:geospatial: false`):

- `extent.spatial.bbox` represents the **area of interest** (AOI) the data pertains to, not a geometric footprint
- Portolan CLI **always provides** `extent.spatial` via automatic AOI inheritance (see below)
- Portolan validators treat the bbox as informational for tabular collections

> **Note on STAC compliance**: The STAC Collection schema requires `extent.spatial`. While Portolan's semantic model considers spatial extent optional for tabular data, the CLI always provides it to maintain STAC schema compatibility. The `portolan:geospatial: false` flag signals that the bbox is an AOI, not a geometry footprint.

### AOI Inheritance

Portolan CLI computes `extent.spatial` automatically for tabular collections:

1. **Explicit bbox** in `metadata.yaml` (manual override)
2. **Inherit from sibling geo collections** — compute union bbox
3. **Global fallback** `[-180, -90, 180, 90]` when no siblings exist

This default is appropriate because companion tabular data typically pertains to the same geographic area as the catalog's spatial layers.

**Limitation**: Union bbox computation uses simple min/max aggregation, which does NOT correctly handle antimeridian-crossing bboxes (where west > east). For catalogs with such collections, use an explicit bbox in `metadata.yaml`.

## Temporal Extent

Tabular collections **SHOULD** populate `extent.temporal` when the data has a time dimension:

```json
"extent": {
  "temporal": {
    "interval": [["2007-01-01T00:00:00Z", "2024-12-31T23:59:59Z"]]
  }
}
```

## Schema Documentation

Tabular collections **SHOULD** describe their columns using the [STAC Table extension](https://github.com/stac-extensions/table):

```json
"stac_extensions": [
  "https://stac-extensions.github.io/table/v1.2.0/schema.json"
],
"table:columns": [
  {"name": "geo", "type": "string", "description": "Country code (ISO-3166-1 alpha-2)"},
  {"name": "period", "type": "string", "description": "Reporting half-year, e.g. 2024-S1"},
  {"name": "price_eur_kwh", "type": "double", "description": "Price in EUR per kWh"}
]
```

Because tabular data has no geometry to hint at its meaning, the schema is the primary semantic handle for consumers and agents.

## Collection-Level Assets

Tabular files become **collection-level assets** (not items), following the same pattern as single-file vector collections:

```json
{
  "type": "Collection",
  "id": "eurostat-electricity-prices",
  "portolan:geospatial": false,
  "assets": {
    "data": {
      "href": "./electricity-prices.parquet",
      "type": "application/vnd.apache.parquet",
      "roles": ["data"]
    }
  }
}
```

### Source File Tracking

When conversion is enabled (`tabular.convert: true`), both the source file and converted Parquet are tracked as assets:

```json
"assets": {
  "data": {
    "href": "./electricity-prices.parquet",
    "type": "application/vnd.apache.parquet",
    "roles": ["data"]
  },
  "source": {
    "href": "./electricity-prices.csv",
    "type": "text/csv",
    "roles": ["source"]
  }
}
```

## Directory Layout

```
eurostat-electricity-prices/
  collection.json
  versions.json
  llms.txt
  electricity-prices.parquet
  electricity-prices.csv        (source, if converted)
```

No item directory or item JSON is needed.

## Example: Complete Tabular Collection

```json
{
  "type": "Collection",
  "stac_version": "1.1.0",
  "id": "eurostat-electricity-prices",
  "title": "Industrial electricity prices by country",
  "description": "Half-yearly industrial electricity prices (EUR/kWh) by European country. Source: Eurostat nrg_pc_205.",
  "license": "CC-BY-4.0",
  "portolan:geospatial": false,
  "stac_extensions": [
    "https://stac-extensions.github.io/table/v1.2.0/schema.json"
  ],
  "extent": {
    "spatial": {
      "bbox": [[-25, 34, 45, 72]]
    },
    "temporal": {
      "interval": [["2007-01-01T00:00:00Z", "2024-12-31T23:59:59Z"]]
    }
  },
  "providers": [
    {
      "name": "Eurostat",
      "roles": ["producer", "licensor"],
      "url": "https://ec.europa.eu/eurostat"
    }
  ],
  "table:columns": [
    {"name": "geo", "type": "string", "description": "Country code (Eurostat / ISO-3166-1 alpha-2)"},
    {"name": "period", "type": "string", "description": "Reporting half-year, e.g. 2024-S1"},
    {"name": "price_eur_kwh", "type": "double", "description": "Industrial electricity price, EUR/kWh"}
  ],
  "assets": {
    "data": {
      "href": "https://example-bucket.s3.eu-north-1.amazonaws.com/eurostat-electricity-prices/electricity-prices.parquet",
      "type": "application/vnd.apache.parquet",
      "roles": ["data"]
    }
  },
  "links": [
    {"rel": "root", "href": "../catalog.json", "type": "application/json"},
    {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
    {"rel": "self", "href": "./collection.json", "type": "application/json"},
    {"rel": "version-history", "href": "./versions.json", "type": "application/json"},
    {"rel": "llms", "href": "./llms.txt", "type": "text/markdown", "title": "Agent/LLM usage guide"},
    {
      "rel": "via",
      "href": "https://ec.europa.eu/eurostat/databrowser/view/nrg_pc_205/default/table",
      "type": "text/html",
      "title": "Source: Eurostat — Electricity prices for non-household consumers"
    }
  ]
}
```

Note: The `extent.spatial.bbox` represents the European AOI (inherited or explicit), not geometry. No PMTiles or geometry validation apply to tabular collections.

## What Does NOT Apply

For tabular collections:

- **No GeoParquet metadata** — the Parquet file has no `geo` key
- **No PMTiles** — visualization derivatives are for spatial data
- **No spatial extent requirement** — `extent.spatial` is optional
- **No geometry validation** — there is no geometry to validate

All other Portolan core requirements apply unchanged: absolute S3 asset hrefs (when published), relative STAC links, `providers`, provenance via `rel: "via"`, `README.md`, `llms.txt`, and `versions.json` tracking.
