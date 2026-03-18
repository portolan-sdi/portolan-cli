# ADR-0032: Nested Catalogs with Flat Collections

## Status
Proposed

## Context

Portolan needs to support hierarchical organization of geospatial datasets (e.g., `environment → air-quality → pm25.parquet`), but the STAC specification has limitations around nested collections.

**Current constraints (ADR-0012: Flat Catalog Hierarchy):**
- Catalog has only one level: `catalog.json → collection.json → item.json`
- No support for logical groupings (domains, subdomains, themes)
- Users organize data in nested directories but Portolan flattens them

**STAC specification allows nested collections:**

> "A Collection can have parent Catalog and Collection objects, as well as child Item, Catalog, and Collection objects."

**BUT with critical limitation:**

> "STAC APIs are **flat at the collection level**" — nested collection hierarchies work in static file-based catalogs but have **limited support in STAC API implementations**.

([Source: STAC Best Practices](https://github.com/radiantearth/stac-spec/blob/master/best-practices.md))

**Real-world evidence:**

We analyzed three production STAC catalogs:

| Catalog | Collections | Nested Collections | Nested Catalogs |
|---------|-------------|--------------------|-----------------|
| **Canadian Government** | 42 | No (0%) | No (flat) |
| **Swiss Government** | 39 | No (0%) | No (flat) |
| **Planet Labs** | 31 | No (0%) | Yes (geographic hierarchy) |

**Key findings:**
- **Zero catalogs** use nested collections in production
- **Planet Labs** uses nested **catalogs** (not collections) for geographic organization
- **STAC APIs** (stac-fastapi, Franklin) don't index nested collections properly

**The pattern that works:**
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

**NOT this (nested collections):**
```
catalog/
├── catalog.json
└── north-america/
    ├── collection.json             ← ❌ Parent collection
    ├── canada/
    │   ├── collection.json         ← ❌ Child collection
    │   └── data.parquet
```

## Decision

**Use nested catalogs for hierarchy. Keep collections flat at leaf level.**

### Catalog vs Collection Usage

| STAC Entity | Purpose | Required Metadata | When to Use |
|-------------|---------|-------------------|-------------|
| **Catalog** | Organizational grouping | `id`, `description`, `links` (minimal) | Intermediate directories |
| **Collection** | Dataset with discoverable metadata | `extent`, `license`, `summaries`, `providers` (extensive) | Leaf directories with data |

### Structure Pattern

**Nested catalogs with leaf collections:**
```
catalog-root/
├── catalog.json                    ← Root catalog
├── environment/
│   ├── catalog.json                ← Sub-catalog (domain)
│   ├── air-quality/
│   │   ├── collection.json         ← Collection (has data)
│   │   └── pm25.parquet
│   └── water-quality/
│       ├── collection.json         ← Collection (has data)
│       └── turbidity.parquet
└── infrastructure/
    ├── catalog.json                ← Sub-catalog (domain)
    └── roads/
        ├── collection.json         ← Collection (has data)
        └── network.parquet
```

**Directory mapping:**
- **Root directory** → `catalog.json` (root catalog)
- **Intermediate directories** → `catalog.json` (sub-catalog for organization)
- **Leaf directories with data** → `collection.json` (full metadata)

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

### Portolan Commands

**Initialize catalog with hierarchy:**
```bash
portolan init data/

# Scans directory structure:
# data/
# ├── environment/
# │   ├── air-quality/pm25.parquet
# │   └── water-quality/turbidity.parquet

# Creates:
# - data/catalog.json (root)
# - data/environment/catalog.json (sub-catalog)
# - data/environment/air-quality/collection.json (leaf)
# - data/environment/water-quality/collection.json (leaf)

✓ Created 1 root catalog
✓ Created 1 sub-catalog
✓ Created 2 collections
```

**Add to nested catalog:**
```bash
cd data/environment/noise
portolan add traffic-noise.parquet

# Detects existing catalog hierarchy
# Creates collection.json in noise/ directory
# Updates parent catalog links
```

## Consequences

### Positive

1. **STAC API compatibility**: Flat collections work with all STAC API implementations (stac-fastapi, Franklin)
2. **Hierarchical organization**: Users can organize data logically (domains → subdomains → datasets)
3. **Lightweight intermediate levels**: Catalogs require minimal metadata (just `id`, `description`, `links`)
4. **Real-world validation**: Matches Planet Labs' production pattern
5. **Clear semantics**: Catalogs = organization, Collections = data
6. **Easier migration**: Existing flat catalogs just add intermediate `catalog.json` files (collections unchanged)
7. **Filesystem alignment**: Directory structure mirrors STAC structure naturally

### Negative

1. **More files**: Intermediate directories get `catalog.json` files (adds file count)
2. **Link maintenance**: Parent/child links must be kept in sync
3. **Depth complexity**: Unlimited nesting could create very deep hierarchies (mitigated by validation)
4. **Discovery UX**: STAC Browser may not highlight catalog hierarchy prominently (depends on tool)

### Neutral

1. **Versioning**: Both catalogs and collections can be versioned in `versions.json`
2. **Cloud sync**: S3/GCS sync works identically for both entity types
3. **Validation**: Catalogs have simpler validation (fewer required fields) than collections

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

## Implementation Notes

### Catalog vs Collection Detection

When `portolan init` scans a directory:

```python
def should_create_catalog_or_collection(directory: Path) -> str:
    """Determine whether directory should be catalog or collection."""

    # Check for data files in this directory
    has_data_files = any(
        f.suffix in ['.parquet', '.tif', '.shp', '.gpkg']
        for f in directory.iterdir()
        if f.is_file()
    )

    if has_data_files:
        return "collection"  # Leaf level: has data
    else:
        return "catalog"     # Intermediate level: organizational
```

### Link Generation

**Parent → Child links:**
```python
# In parent catalog.json
"links": [
    {
        "rel": "child",
        "href": "./air-quality/collection.json",
        "title": "Air Quality"
    }
]
```

**Child → Parent links:**
```python
# In child collection.json
"links": [
    {
        "rel": "parent",
        "href": "../catalog.json"
    }
]
```

**Root links (all entities):**
```python
# Point to root catalog
"links": [
    {
        "rel": "root",
        "href": "../../catalog.json"  # Relative to entity
    }
]
```

### Depth Limits

Prevent excessive nesting:
```python
MAX_CATALOG_DEPTH = 5  # e.g., root → domain → subdomain → theme → collection

def validate_catalog_depth(catalog_path: Path) -> None:
    """Ensure catalog hierarchy doesn't exceed maximum depth."""
    depth = len(catalog_path.relative_to(catalog_root).parts)
    if depth > MAX_CATALOG_DEPTH:
        raise ValueError(f"Catalog depth {depth} exceeds maximum {MAX_CATALOG_DEPTH}")
```

### Migration from Flat Catalogs

Existing flat catalogs can adopt nested structure incrementally:

**Before (flat):**
```
catalog/
├── catalog.json
├── air-quality/
│   ├── collection.json
│   └── pm25.parquet
└── water-quality/
    ├── collection.json
    └── turbidity.parquet
```

**After (nested):**
```
catalog/
├── catalog.json             ← Updated with child link
├── environment/
│   ├── catalog.json         ← New sub-catalog
│   ├── air-quality/
│   │   ├── collection.json  ← Unchanged
│   │   └── pm25.parquet
│   └── water-quality/
│       ├── collection.json  ← Unchanged
│       └── turbidity.parquet
```

**Migration steps:**
1. Create intermediate `catalog.json` files
2. Update parent catalog with `child` links
3. Update collections with new `parent` links (now point to sub-catalog)
4. Collections themselves unchanged (no breaking change)

### Testing

- **Unit tests**: Catalog vs collection detection logic
- **Integration tests**: `portolan init` with nested directories
- **Link validation**: Ensure parent/child/root links are correct
- **STAC validation**: Validate catalogs and collections with pystac
- **Real-world tests**: Den Haag datasets, multi-domain structures

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
