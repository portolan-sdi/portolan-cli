# ADR-0018: Metadata Generation Tiers

## Status
Accepted

## Context

STAC metadata has many fields. Some can be extracted automatically, others require human input. We need clarity on what `check --fix` regenerates vs what users must provide.

## Decision

Metadata is categorized into four tiers:

| Tier | Fields | Source |
|------|--------|--------|
| **1: Auto-extractable** | bbox, crs, geometry_type, feature_count, schema, sha256, size_bytes | File headers (O(1) read) |
| **2: Derivable** | datetime, title, extent.spatial, extent.temporal | Heuristics (mtime, filename, child aggregation) |
| **3: Auto with defaults** | description, license | Sensible defaults provided |
| **4: Human-enrichable** | semantic_type, unit, providers, custom titles | Optional; user adds if desired |

**Key decision: No human-required fields.** All metadata can be auto-generated with sensible defaults. Users may enrich but are never blocked.

## Consequences

### What `--fix` regenerates
- All Tier 1 fields (re-extracted from file)
- All Tier 2 fields (re-derived)
- Tier 3 fields only if missing (won't overwrite user customization)

### What `--fix` preserves
- Tier 4 fields (user enrichments)
- Tier 3 fields if already set

### Trade-offs
- Defaults may be generic ("A Portolan-managed dataset")
- Users wanting rich descriptions must add them manually or use LLM-assisted tooling (future)
