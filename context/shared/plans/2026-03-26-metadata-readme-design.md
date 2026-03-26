# Design: Metadata Enrichment + README Generation

**Date**: 2026-03-26
**Status**: Proposed
**GitHub Issues**: [#108](https://github.com/portolan-sdi/portolan-cli/issues/108), [#3](https://github.com/portolan-sdi/portolan-cli/issues/3)
**Related ADRs**: [ADR-0018](../adr/0018-metadata-generation-tiers.md), [ADR-0024](../adr/0024-hierarchical-config-system.md), [ADR-0038](../adr/0038-metadata-yaml-enrichment.md), [ADR-0039](../adr/0039-hierarchical-portolan-folders.md)

## Problem Statement

Portolan extracts comprehensive machine-oriented metadata into STAC (bbox, CRS, schema, statistics), but lacks:

1. **Human-enrichable metadata**: Descriptions, citations, contact info, DOIs
2. **README generation**: Auto-generated documentation for catalog discoverability
3. **Best practices specification**: What metadata is required vs optional for publishable catalogs

Users publishing to platforms like Source.coop need richer documentation than STAC alone provides.

## Architecture

```
┌─────────────────────────┐     ┌──────────────────────────┐
│   STAC JSON             │     │   .portolan/metadata.yaml │
│   (auto-extracted)      │     │   (human supplement)      │
│                         │     │                           │
│   - bbox, CRS           │     │   - title, description    │
│   - schema, stats       │     │   - citation, DOI         │
│   - geometry types      │     │   - contact info          │
│   - file checksums      │     │   - column docs           │
└───────────┬─────────────┘     └─────────────┬─────────────┘
            │                                  │
            └──────────────┬───────────────────┘
                           ▼
                  ┌─────────────────┐
                  │   README.md     │
                  │   (generated)   │
                  │                 │
                  │   Never edited  │
                  │   directly      │
                  └─────────────────┘
```

### Key Principles

1. **YAML supplements STAC**: No duplication. YAML only contains fields STAC doesn't capture well.
2. **README is a pure output**: Generated from STAC + YAML, never hand-edited.
3. **Schema is the spec**: The metadata.yaml template defines best practices. Required fields aren't marked optional.
4. **Hierarchical inheritance**: metadata.yaml at any catalog level; child overrides parent.

## metadata.yaml Schema

```yaml
# .portolan/metadata.yaml
#
# This file defines catalog metadata that supplements auto-extracted STAC.
# Fields without "# optional" comments are REQUIRED for publication.
# The schema itself is the best practices specification.

# ─────────────────────────────────────────────────────────────────────────────
# REQUIRED: Core identification
# ─────────────────────────────────────────────────────────────────────────────

title: ""                           # Human-readable catalog/collection title
description: |                      # Rich description (markdown supported)
  Describe the data: what it contains, coverage, methodology, limitations.

# ─────────────────────────────────────────────────────────────────────────────
# REQUIRED: Accountability
# ─────────────────────────────────────────────────────────────────────────────

contact:
  name: ""                          # Person or team name
  email: ""                         # Contact email

license: ""                         # SPDX identifier (e.g., "CC-BY-4.0", "MIT")

# ─────────────────────────────────────────────────────────────────────────────
# OPTIONAL: Discovery and citation
# ─────────────────────────────────────────────────────────────────────────────

license_url: ""                     # optional - URL to full license text
citation: ""                        # optional - Academic citation text
doi: ""                             # optional - Zenodo/DataCite DOI
keywords: []                        # optional - Discovery tags
attribution: ""                     # optional - Required attribution text for maps

# ─────────────────────────────────────────────────────────────────────────────
# OPTIONAL: Data lifecycle
# ─────────────────────────────────────────────────────────────────────────────

update_frequency: ""                # optional - "annual", "monthly", "one-time", etc.
source_url: ""                      # optional - Original data source URL
processing_notes: ""                # optional - How data was processed/transformed

# ─────────────────────────────────────────────────────────────────────────────
# OPTIONAL: Schema documentation (for GeoParquet/vector data)
# ─────────────────────────────────────────────────────────────────────────────

columns:                            # optional - Per-column documentation
  # column_name:
  #   description: ""               # What this column represents
  #   unit: ""                      # Unit of measurement (e.g., "meters", "USD")
  #   semantic_type: ""             # Semantic type (e.g., "identifier", "categorical")

# ─────────────────────────────────────────────────────────────────────────────
# OPTIONAL: Band documentation (for COG/raster data)
# ─────────────────────────────────────────────────────────────────────────────

bands:                              # optional - Per-band documentation
  # band_1:
  #   name: ""                      # Semantic name (e.g., "Red", "NDVI")
  #   description: ""               # What this band represents
  #   unit: ""                      # Unit of measurement
```

## README Template

The generated README combines STAC metadata with metadata.yaml:

```markdown
# {title}

{description}

## Spatial Coverage

- **Bounding Box**: [{bbox}]
- **CRS**: {proj:code}
- **Geometry Types**: {vector:geometry_types}

## Temporal Coverage

- **Interval**: {start_datetime} to {end_datetime}
- **Update Frequency**: {update_frequency}

## Schema

| Column | Type | Description | Unit |
|--------|------|-------------|------|
| {name} | {type} | {description} | {unit} |

## Files

| Collection | Items | Format | Size |
|------------|-------|--------|------|
| {collection_id} | {item_count} | {format} | {total_size} |

## Citation

{citation}

DOI: {doi}

## License

{license} ([Full text]({license_url}))

## Contact

{contact.name} <{contact.email}>

---

*Generated by [Portolan](https://github.com/portolan-sdi/portolan-cli) from STAC metadata and .portolan/metadata.yaml*
```

## Hierarchical Structure

metadata.yaml can exist at any level in the catalog hierarchy:

```
catalog/
  .portolan/
    config.yaml           # Catalog-level config
    metadata.yaml         # Catalog-level: title, default contact, license

  demographics/
    .portolan/
      metadata.yaml       # Collection-level: description, citation
    collection.json

  historical/
    .portolan/
      metadata.yaml       # Subcatalog-level metadata
    census-1990/
      .portolan/
        metadata.yaml     # Collection in subcatalog
      collection.json
```

### Inheritance Rules

1. Start at the current level (collection, subcatalog, or catalog)
2. Walk up to catalog root, collecting metadata.yaml files
3. Merge with child overriding parent for each field
4. Required fields must be present in the merged result

This mirrors the existing config.yaml precedence from [ADR-0024](../adr/0024-hierarchical-config-system.md).

## CLI Commands

### `portolan metadata init [PATH]`

Generate a metadata.yaml template at the specified level.

```bash
# At catalog root
portolan metadata init

# At collection level
portolan metadata init demographics/
```

Creates `.portolan/metadata.yaml` with the template schema.

### `portolan readme [PATH]`

Generate README.md from STAC + metadata.yaml.

```bash
# Generate catalog README
portolan readme

# Generate collection README
portolan readme demographics/
```

Options:
- `--check`: Verify README is up-to-date (exit 1 if stale)
- `--stdout`: Print to stdout instead of writing file

### `portolan metadata validate [PATH]`

Validate metadata.yaml against schema and check required fields.

```bash
portolan metadata validate
```

Checks:
- YAML syntax
- Required fields present and non-empty
- DOI format (if present)
- License is valid SPDX identifier
- Email format

## CI Integration

### Pre-commit Hook

```yaml
# .pre-commit-config.yaml
- repo: local
  hooks:
    - id: readme-freshness
      name: Check README freshness
      entry: portolan readme --check
      language: system
      pass_filenames: false
```

### GitHub Actions

```yaml
- name: Validate metadata
  run: portolan metadata validate

- name: Check README freshness
  run: portolan readme --check
```

## Implementation Tickets

### Ticket 1: Hierarchical .portolan/ Support

**Prerequisite for metadata work.**

- Enable `.portolan/` folders at collection/subcatalog levels
- Generalize config resolution to walk directory tree
- Apply same inheritance pattern for any `.portolan/*.yaml` file
- Update documentation

**Files**: `portolan_cli/config.py`, `portolan_cli/catalog.py`

### Ticket 2: metadata.yaml + README Generation

**Addresses #108 and #3.**

- Create metadata.yaml schema and validation
- Add `portolan metadata init` command
- Add `portolan metadata validate` command
- Add `portolan readme` command with README template
- Add `--check` mode for CI
- Update CLAUDE.md with new ADR references

**Files**:
- New: `portolan_cli/metadata_yaml.py`, `portolan_cli/readme.py`
- Modify: `portolan_cli/cli.py`

## Sources

- [GitHub Issue #108: Metadata Enrichment](https://github.com/portolan-sdi/portolan-cli/issues/108)
- [GitHub Issue #3: Auto README Generation](https://github.com/portolan-sdi/portolan-cli/issues/3)
- [ADR-0018: Metadata Generation Tiers](../adr/0018-metadata-generation-tiers.md) - Tiering philosophy
- [ADR-0024: Hierarchical Config System](../adr/0024-hierarchical-config-system.md) - Precedence pattern
- [ADR-0023: STAC Structure Separation](../adr/0023-stac-structure-separation.md) - `.portolan/` principle
- Research agent analysis of current STAC extraction (2026-03-26)
