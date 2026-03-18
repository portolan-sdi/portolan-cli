# ADR-0032: Nested Catalogs with Flat Collections

## Status
Adopted

## Context

Portolan needs to support hierarchical organization of geospatial datasets (e.g., `environment → air-quality → pm25.parquet`), but STAC APIs are flat at the collection level. Instead of using nested _collections_ for thematic or other logical groupings (theme, region, etc.), we will use nested catalogs. This is consistent, e.g., with how Planet Labs organizes their public STAC catalog, and will make it easier to expose a STAC API.

```
catalog/
├── catalog.json                    ← Root catalog
├── north-america/
│   ├── catalog.json                ← Sub-catalog (organizational)
│   ├── canada/
│   │   ├── collection.json         ← Leaf collection (data)
│   │   └── data.parquet
│   └── usa/
│       ├── collection.json         ← Leaf collection (data)
│       └── data.parquet
```

([Source: STAC Best Practices](https://github.com/radiantearth/stac-spec/blob/master/best-practices.md))

Catalogs can also be both above and below collections, and can, for example, be used within a raster collection to organize items.

## Decision

Use nested catalogs, not collections, for hierarchy. Nested catalogs above collections are for thematic organization, while nested catalogs below collections are for organizing items.


### Structure Patterns

**Pattern 1: Catalogs above collections (thematic organization):**
```
catalog-root/
├── catalog.json                    ← Root catalog
├── environment/
│   ├── catalog.json                ← Theme catalog
│   ├── air-quality/
│   │   ├── collection.json         ← Collection (has data)
│   │   └── pm25.parquet
│   └── water-quality/
│       ├── collection.json         ← Collection (has data)
│       └── turbidity.parquet
```

**Pattern 2: Catalogs below collections (organizing items within raster collection):**
```
catalog-root/
├── catalog.json
└── landsat/
    ├── collection.json             ← Collection
    ├── 2024/
    │   ├── catalog.json            ← Sub-catalog organizing items by year
    │   ├── 01-15/
    │   │   ├── item.json
    │   │   └── scene.tif
    │   └── 01-16/
    │       ├── item.json
    │       └── scene.tif
    └── 2023/
        └── catalog.json            ← Sub-catalog organizing items by year
```

**Directory mapping:**
- **Root directory** → `catalog.json` (root catalog)
- **Theme/domain directories** → `catalog.json` (above collections)
- **Data directories** → `collection.json` (where vector files or raster items exist)
- **Organizational subdirectories within collections** → `catalog.json` (below collections, for organizing many items)

### Link Structure

**Root catalog:**
```json
{
  "type": "Catalog",
  "id": "portolan-catalog",
  "description": "Geospatial data catalog",
  "links": [
    {"rel": "self", "href": "./catalog.json"},
    {"rel": "root", "href": "./catalog.json"},
    {"rel": "child", "href": "./environment/catalog.json", "title": "Environment"},
    {"rel": "child", "href": "./infrastructure/catalog.json", "title": "Infrastructure"}
  ]
}
```

**Sub-catalog (intermediate level):**
```json
{
  "type": "Catalog",
  "id": "environment",
  "description": "Environmental datasets",
  "links": [
    {"rel": "self", "href": "./environment/catalog.json"},
    {"rel": "root", "href": "../catalog.json"},
    {"rel": "parent", "href": "../catalog.json"},
    {"rel": "child", "href": "./air-quality/collection.json", "title": "Air Quality"},
    {"rel": "child", "href": "./water-quality/collection.json", "title": "Water Quality"}
  ]
}
```

**Collection (leaf level):**
```json
{
  "type": "Collection",
  "id": "air-quality-pm25",
  "description": "PM2.5 air quality measurements",
  "extent": {...},
  "license": "CC-BY-4.0",
  "links": [
    {"rel": "self", "href": "./air-quality/collection.json"},
    {"rel": "root", "href": "../../catalog.json"},
    {"rel": "parent", "href": "../catalog.json"}
  ],
  "assets": {
    "data": {
      "href": "./pm25.parquet",
      "type": "application/vnd.apache.parquet"
    }
  }
}
```

## Consequences
This provides for more logical organization of Portolan catalogs and makes STAC API implementations easier, too. Catalog metadata is also simpler and easier to maintain.

## Alternatives Considered

### Alternative 1: Nested Collections

Allow `collection.json` at both intermediate and leaf levels.

**Example:**
```
environment/
├── collection.json          ← Parent collection
├── air-quality/
│   ├── collection.json      ← Child collection
│   └── pm25.parquet
```

**Rejected.**

**Pros:**
- Semantically consistent (everything is a "collection")
- Intermediate levels can have extent/license metadata

**Cons:**
- **STAC APIs don't support this** — most implementations index collections flat
- **Zero real-world usage** — no production catalogs use this pattern
- **Metadata duplication** — parent collection extent overlaps with child extents
- **Complex queries** — "find all collections" returns both parents and leaves (ambiguous)

### Alternative 2: Flat Collections Only (Status Quo)

Keep ADR-0012: no nesting at all, just `catalog.json → collection.json`.

**Rejected.**

**Pros:**
- Simplest possible structure
- No link maintenance complexity

**Cons:**
- **Scales poorly** — 100+ collections in one directory is unmanageable
- **No logical grouping** — can't organize by domain/theme/region
- **User friction** — users naturally create nested directories, Portolan flattens them
- **Discovery poor** — flat list of 100+ collections is hard to browse

### Alternative 3: Nested Collections with API Fallback

Use nested collections in static catalogs, flatten for STAC API indexing.

**Rejected.**

**Pros:**
- Rich hierarchy in static mode
- API compatibility via flattening

**Cons:**
- **Two representations** — static vs API have different structures (confusing)
- **Sync complexity** — must maintain both representations
- **Fragile** — changes to hierarchy break API flattening logic
- **Over-engineering** — solving a problem we don't have (Portolan uses static catalogs)

### Alternative 4: Nested Catalogs with Flat Collections

**Accepted (this is the decision).**

**Pros:**
- STAC API compatible (collections are flat)
- Hierarchical organization (catalogs provide structure)
- Real-world validated (Planet Labs uses this)
- Lightweight intermediate levels (catalogs are simple)
- Clear semantics (catalogs for organization, collections for data)

**Cons:**
- More files (catalogs at intermediate levels)
- Link maintenance (parent/child links)

## Related

- **ADR-0012** (Flat Catalog Hierarchy): **Superseded** — this ADR replaces the "no nesting" constraint
- **ADR-0031** (Collection-Level Assets for Vector Data): Defines what goes in collections
- **ADR-0023** (STAC Structure Separation): Catalogs/collections at root, internals in `.portolan/`
- **Issue #226**: Catalog structure patterns (parent issue)
- **Issue #231**: Collection-level assets and nested collections (research ticket)

## References

1. [STAC Best Practices - Catalog Layout](https://github.com/radiantearth/stac-spec/blob/master/best-practices.md#catalog-layout)
2. [STAC Catalog Specification](https://github.com/radiantearth/stac-spec/blob/master/catalog-spec/README.md)
3. [STAC Collection Specification](https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md)
4. [Planet Labs STAC Implementation](https://www.planet.com/data/stac/)
5. [STAC API Specification - Collection Endpoints](https://github.com/radiantearth/stac-api-spec)
