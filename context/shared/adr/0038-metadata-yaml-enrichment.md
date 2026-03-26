# ADR-0038: Metadata YAML as Human Enrichment Layer

## Status

Proposed

## Context

Portolan's STAC generation ([ADR-0018](0018-metadata-generation-tiers.md)) extracts comprehensive machine-oriented metadata: bbox, CRS, schema, statistics, checksums. However, there's no mechanism for human-enrichable metadata that STAC doesn't capture well:

- Rich descriptions (beyond generic defaults)
- Academic citations and DOIs
- Contact information
- Column/band documentation (units, semantic meaning)
- Data provenance and processing notes

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

- Fields present in the template = recommended fields
- Fields without `# optional` comments = required for publication
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

`portolan readme` combines:

1. STAC metadata (bbox, CRS, schema, file counts)
2. metadata.yaml (descriptions, citations, contact)

README includes a footer indicating it's generated—users know not to edit it.

### What Goes Where

| Field | Location | Rationale |
|-------|----------|-----------|
| bbox, CRS, schema | STAC | Auto-extracted from data |
| file size, checksum | STAC/versions.json | Auto-computed |
| title, description | metadata.yaml | Human-written |
| citation, DOI | metadata.yaml | Human-provided |
| contact info | metadata.yaml | Human-provided |
| column descriptions | metadata.yaml | Human/LLM enrichment |
| license | metadata.yaml | Human choice (SPDX identifier) |

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
