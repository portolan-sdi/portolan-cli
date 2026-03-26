# ADR-0039: Hierarchical .portolan/ Folders at Collection/Subcatalog Levels

## Status

Proposed

## Context

[ADR-0023](0023-stac-structure-separation.md) established `.portolan/` at the catalog root for internal tooling state. [ADR-0024](0024-hierarchical-config-system.md) defined config.yaml with collection-level overrides, but stored them in a `collections:` section within the root file.

With the addition of metadata.yaml ([ADR-0038](0038-metadata-yaml-enrichment.md)), we need per-collection metadata that can grow large (column descriptions, collection-specific citations). A single root file with 100+ collection sections becomes unwieldy.

### Forces

- Large catalogs may have 100+ collections, each with unique metadata
- Collections in different subcatalogs may have different maintainers/contacts
- The config.yaml `collections:` approach doesn't scale for metadata.yaml
- Consistency: config and metadata should follow the same pattern
- [ADR-0032](0032-nested-catalogs-with-flat-collections.md) already establishes nested catalog structure

## Decision

### .portolan/ at Any Level

`.portolan/` folders can exist at catalog, subcatalog, or collection levels:

```
catalog/
  .portolan/
    config.yaml           # Catalog-level defaults
    metadata.yaml         # Catalog-level metadata

  demographics/
    .portolan/
      config.yaml         # Collection-specific config (optional)
      metadata.yaml       # Collection-specific metadata
    collection.json

  historical/                # Subcatalog
    .portolan/
      config.yaml         # Subcatalog-level config
      metadata.yaml       # Subcatalog-level metadata
    census-1990/
      .portolan/
        metadata.yaml     # Collection in subcatalog
      collection.json
```

### Inheritance Rules

For both config.yaml and metadata.yaml:

1. Start at the current scope (collection, subcatalog, or catalog)
2. Walk up the directory tree to catalog root
3. Collect all `.portolan/*.yaml` files found
4. Merge with **child overriding parent** for each field

### Precedence (config.yaml)

Extended from [ADR-0024](0024-hierarchical-config-system.md):

```
CLI argument > Env var > Collection config > Subcatalog config > Catalog config > Default
```

### Precedence (metadata.yaml)

```
Collection metadata > Subcatalog metadata > Catalog metadata
```

For nested fields (like `columns:`), the merge is per-key:

```yaml
# Catalog .portolan/metadata.yaml
contact:
  name: "Data Team"
  email: "data@org.com"
columns:
  geoid:
    description: "Census GEOID"

# Collection .portolan/metadata.yaml
contact:
  email: "demographics@org.com"   # Overrides catalog email
columns:
  total_pop:                       # Adds new column doc
    description: "Total population"
```

Result:
```yaml
contact:
  name: "Data Team"              # Inherited from catalog
  email: "demographics@org.com"  # Overridden by collection
columns:
  geoid:
    description: "Census GEOID"  # Inherited
  total_pop:
    description: "Total population"  # Added
```

### Backwards Compatibility

The existing `collections:` section in root config.yaml continues to work. Collection-level `.portolan/config.yaml` takes precedence over root `collections.<name>` entries.

Migration path: users can gradually move `collections:` entries to per-collection `.portolan/` folders.

## Consequences

### Benefits

- **Scales to large catalogs**: 100 collections = 100 small files, not one giant file
- **Clear ownership**: Collection maintainers edit their own `.portolan/` folder
- **Consistent pattern**: config.yaml and metadata.yaml work identically
- **Git-friendly**: Changes to one collection don't touch the root folder

### Trade-offs

- **More folders**: Each collection with custom config/metadata gets a `.portolan/` folder
- **Discovery**: Must walk directory tree to find all config/metadata (but tools do this, not humans)

### Implementation Changes

1. **Config resolution**: `get_setting()` walks up directory tree, not just catalog root
2. **Metadata resolution**: New function follows same pattern as config
3. **README generation**: Merges metadata.yaml files before generating
4. **`portolan init`**: Supports `--scope=collection` to create collection-level `.portolan/`

### API Changes

```python
def find_portolan_files(
    start_path: Path,
    filename: str,  # "config.yaml" or "metadata.yaml"
    catalog_root: Path,
) -> list[Path]:
    """Find all .portolan/{filename} from start_path up to catalog_root.

    Returns paths in order from catalog root to start_path (for merging).
    """

def load_merged_config(
    path: Path,
    catalog_root: Path,
) -> dict[str, Any]:
    """Load config.yaml with full hierarchy merged."""

def load_merged_metadata(
    path: Path,
    catalog_root: Path,
) -> dict[str, Any]:
    """Load metadata.yaml with full hierarchy merged."""
```

## Alternatives Considered

### Keep collections: section in root file

**Rejected**: Doesn't scale. A root file with 100 `collections:` entries is hard to navigate and creates merge conflicts when multiple people edit different collections.

### metadata.yaml outside .portolan/ (visible in collection)

**Rejected**: Inconsistent with the principle that `.portolan/` contains Portolan internals. Mixes Portolan files with STAC files.

### Separate metadata.yaml naming per level

e.g., `catalog.metadata.yaml`, `collection.metadata.yaml`

**Rejected**: Adds complexity without benefit. The file's location (which `.portolan/` folder) already indicates its scope.

## References

- [ADR-0023: STAC Structure Separation](0023-stac-structure-separation.md)
- [ADR-0024: Hierarchical Config System](0024-hierarchical-config-system.md)
- [ADR-0032: Nested Catalogs with Flat Collections](0032-nested-catalogs-with-flat-collections.md)
- [ADR-0038: Metadata YAML as Enrichment Layer](0038-metadata-yaml-enrichment.md)
- [Design Document: Metadata + README](../plans/2026-03-26-metadata-readme-design.md)
