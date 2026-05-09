# ADR-0044: Consumption Guides Architecture

## Status
Proposed

## Context

Portolan's value proposition is "publish once, consume anywhere." Currently, users can publish data to cloud storage with rich STAC metadata, but there's no guidance on how to actually *consume* that data from analytics engines like DuckDB, Python/GeoPandas, or other tools.

Issue #121 tracks this gap. The question: what's the right mechanism for consumption guides?

### Forces at Play

1. **Two audiences**: Human users (need readable docs) and AI agents (need structured, parseable guidance)
2. **Self-describing catalogs**: Consumers often have just a URL—no Portolan installed
3. **Portolan's optimizations**: GeoParquet files are spatially ordered (Hilbert), row-grouped, ZSTD compressed, with bbox structs
4. **Protocol complexity**: S3 URLs need credential config; some endpoints (Source Coop) serve HTTPS directly
5. **User environment varies**: Some users have DuckDB installed, others don't; some are new to geospatial

### Options Evaluated

| Option | Pros | Cons |
|--------|------|------|
| **Static docs only** | Simple, no code | Generic, not adaptive |
| **README snippets** | Self-describing | Can't adapt to user's environment |
| **STAC extension** | Machine-readable | Snippets drift, overengineered |
| **metadata.yaml field** | Human-curated | Another schema to maintain |
| **Skill only** | Adaptive, environment-aware, contextual | Requires AI assistant |

## Decision

**Use a Claude Code skill as the primary consumption interface.** No mandatory README changes or metadata schema additions.

The skill:
1. **Detects user's environment** — checks for DuckDB, rioxarray, geopandas, etc.
2. **Understands Portolan's structure** — STAC catalogs, collection-level vector assets, item-level raster assets
3. **Knows Portolan's optimizations** — leverages Hilbert ordering, row groups, bbox structs for efficient queries
4. **Guides exploration** — dry runs, size checks, schema inspection before full queries
5. **Adapts to skill level** — explains options to naive users, suggests cloud-native approaches

### Why Skill-Only

1. **Adaptive**: Can check what's installed and guide accordingly
2. **Contextual**: Reads actual STAC metadata, understands schema, suggests appropriate queries
3. **No code changes**: README generation stays simple; no new metadata fields
4. **Iteratively improvable**: Skill instructions can evolve without code releases

### Portolan GeoParquet Optimizations

The skill must know that Portolan produces optimized GeoParquet:

| Optimization | What it enables |
|--------------|-----------------|
| **Hilbert spatial ordering** | Sequential reads for spatial queries |
| **Row groups** | Efficient range scans, predicate pushdown |
| **ZSTD compression** | Smaller files, fast decompression |
| **bbox struct column** | Fast spatial filtering without geometry parsing |

DuckDB and PyArrow can leverage these automatically, but the skill should explain why queries are fast.

### Recommended Tools

| Data Type | Primary Tool | Alternative |
|-----------|--------------|-------------|
| **Vectors (GeoParquet)** | DuckDB + spatial extension | GeoPandas, PyArrow |
| **Rasters (COG)** | rioxarray | rasterio |
| **STAC metadata** | Read JSON directly | pystac |

The skill checks what's installed and recommends accordingly. For users without tools installed, it explains options and suggests the most cloud-native approach.

## Consequences

### Easier

- **Minimal code changes** — just an optional metadata.yaml field
- **Environment-aware** guidance (checks what's installed)
- **Adaptive to complexity** — simple catalogs get simple queries, complex ones get contextual help
- **Leverages Portolan optimizations** — skill knows about Hilbert ordering, bbox structs

### Harder

- **Requires AI assistant** — users without Claude Code don't get this guidance
- **Non-deterministic** — skill output varies by context
- **Documentation gap** — need a simple docs page for non-AI users

### Mitigation

Add a basic consumption guide to `docs/` for users without AI assistants. This is static but covers the fundamentals.

## Optional: metadata.yaml Examples

For datasets with unusual structure (required joins, non-obvious columns, multiple related files), publishers can add custom examples to metadata.yaml:

```yaml
examples:
  - engine: duckdb
    description: "Join census data with geographic boundaries"
    code: |
      SELECT r.*, c.population
      FROM read_parquet('https://.../radios.parquet') r
      JOIN read_parquet('https://.../census-data.parquet') c
        ON r.cod_2022 = c.id_geo
  - engine: python
    description: "Load and merge with GeoPandas"
    code: |
      radios = gpd.read_parquet('https://.../radios.parquet')
      census = pd.read_parquet('https://.../census-data.parquet')
      merged = radios.merge(census, left_on='cod_2022', right_on='id_geo')
```

This is **optional** — the skill generates default examples if omitted. Use it when:
- Dataset requires joins across multiple files
- Column names are non-obvious
- Spatial structure is unusual (partitioned, multi-resolution)
- Domain-specific query patterns would help users

## Alternatives Considered

### README Snippets with Auto-Generated URLs

Enhancing `portolan readme` to include DuckDB/Python examples with full URLs.

Rejected because:
- Can't adapt to user's environment
- Asset selection is ambiguous for complex catalogs
- Joins and relationships need contextual understanding

### Mandatory metadata.yaml Examples

Requiring consumption examples in metadata.yaml for all catalogs.

Rejected because:
- Overhead for simple single-file catalogs
- Skill generates good defaults automatically
- Optional field serves complex cases without burdening simple ones
