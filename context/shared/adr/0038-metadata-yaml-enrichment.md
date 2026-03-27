# ADR-0038: Metadata YAML as Human Enrichment Layer

## Status

Accepted

## Context

Portolan's STAC generation ([ADR-0018](0018-metadata-generation-tiers.md)) extracts comprehensive machine-oriented metadata: bbox, CRS, schema, statistics, checksums. Title and description are set during `portolan init`. However, there's no mechanism for human-enrichable metadata that STAC doesn't capture:

- Academic citations and DOIs
- Contact information (accountability)
- License (SPDX identifier)
- Data quality caveats (known issues)
- Processing notes

GitHub Issues [#108](https://github.com/portolan-sdi/portolan-cli/issues/108) (metadata enrichment) and [#3](https://github.com/portolan-sdi/portolan-cli/issues/3) (README generation) both address this gap.

### Forces

- STAC already captures technical metadata well—don't duplicate
- Users need LLM-friendly, editable files for enrichment
- README.md is the primary human-readable interface but shouldn't be hand-edited (merge conflicts, sync drift)
- [ADR-0024](0024-hierarchical-config-system.md) established `.portolan/` as the location for Portolan internals
- The "best practices" spec should be machine-readable, not a separate document

## Decision

### Three-Layer Architecture

```
STAC JSON (auto-extracted)  +  .portolan/metadata.yaml (human supplement)
              ↓                            ↓
              └────────────┬───────────────┘
                           ↓
                      README.md (fully generated)
```

1. **STAC JSON**: Machine-oriented, auto-extracted from data files
2. **metadata.yaml**: Human/LLM-editable supplement for fields STAC doesn't capture
3. **README.md**: Fully generated output, never hand-edited

### Schema is the Spec

The metadata.yaml template itself defines best practices:

- **Required fields**: `contact` (name + email) and `license` (SPDX identifier)
- **Optional fields**: citation, DOI, known_issues, etc.
- Title and description come from STAC (set during `portolan init`)
- No separate natural language specification document

### Separation from config.yaml

metadata.yaml is separate from config.yaml because:

- **Different audiences**: config.yaml controls tooling behavior; metadata.yaml describes the data
- **LLM safety**: Enrichment agents should edit metadata without risking config changes
- **Single responsibility**: README generator only needs metadata, not config settings

### Location

`.portolan/metadata.yaml` at catalog root, with optional overrides at collection/subcatalog levels per [ADR-0039](0039-hierarchical-portolan-folders.md).

## Consequences

### Benefits

- **No duplication**: YAML only contains what STAC doesn't—bbox, CRS, schema stay in STAC
- **CI-verifiable**: YAML schema validation, DOI format checks, README freshness
- **LLM-friendly**: Structured YAML is easier for AI enrichment than free-form markdown
- **No merge conflicts**: README is generated, not hand-maintained
- **Self-documenting spec**: The template is the best practices document

### Trade-offs

- **Two files to manage**: metadata.yaml + config.yaml (mitigated by clear separation of concerns)
- **Learning curve**: Users must understand that README edits get overwritten

### README Generation

`portolan readme` combines STAC and metadata.yaml into a comprehensive document:

```bash
portolan readme           # Generate README.md
portolan readme --stdout  # Preview without writing
portolan readme --check   # CI freshness validation (exit 1 if stale)
```

README includes a footer indicating it's generated—users know not to edit it.

### What Goes Where

| Field | Location | Rationale |
|-------|----------|-----------|
| title, description | STAC catalog/collection | Set during `portolan init` |
| bbox, CRS, schema | STAC | Auto-extracted from data |
| columns (table:columns) | STAC summaries | Auto-extracted by scan |
| bands (eo:bands, raster:bands) | STAC summaries | Auto-extracted by scan |
| file size, checksum | STAC assets | Auto-computed |
| contact (name, email) | metadata.yaml | **Required** - accountability |
| license | metadata.yaml | **Required** - SPDX identifier |
| citation, DOI | metadata.yaml | Optional - academic attribution |
| known_issues | metadata.yaml | Optional - data quality caveats |

### README Content Sources

`portolan readme` generates comprehensive documentation:

**Auto-filled from STAC:**
- Title (from `title` or `id`)
- Description
- Spatial coverage (bbox, CRS)
- Temporal coverage (interval)
- Schema/columns (from `table:columns` extension)
- Bands (from `eo:bands` or `raster:bands`)
- Files with checksums
- Code examples (based on detected format: GeoParquet → geopandas, COG → rasterio)
- STAC links

**From metadata.yaml:**
- License
- Contact
- Citation and DOI
- Known issues

## Alternatives Considered

### Embed metadata in STAC JSON

**Rejected**: STAC has description fields, but they're designed for machine parsing, not rich documentation. Mixing human prose into JSON is awkward.

### Edit README directly with markers

**Rejected**: Marker-based "editable zones" in generated files are fragile. Users accidentally edit outside markers, generation logic is complex, merge conflicts still occur.

### Merge metadata into config.yaml

**Rejected**: Different audiences. LLM enrichment should target a focused file without risk of changing `remote` or `aws_profile`.

### YAML template as separate spec document

**Rejected**: Spec documents drift from implementation. The template itself being the spec ensures they stay in sync.

## References

- [GitHub Issue #108: Metadata Enrichment](https://github.com/portolan-sdi/portolan-cli/issues/108)
- [GitHub Issue #3: Auto README Generation](https://github.com/portolan-sdi/portolan-cli/issues/3)
- [ADR-0018: Metadata Generation Tiers](0018-metadata-generation-tiers.md)
- [ADR-0024: Hierarchical Config System](0024-hierarchical-config-system.md)
- [ADR-0039: Hierarchical .portolan/ Folders](0039-hierarchical-portolan-folders.md)
- [Design Document: Metadata + README](../plans/2026-03-26-metadata-readme-design.md)
