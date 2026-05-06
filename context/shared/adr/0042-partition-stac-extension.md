# ADR-0042: Partition STAC Extension

## Status
Proposed

## Context

Portolan supports spatial partitioning of large GeoParquet files (PR #399) using Hive-style directory structures:

```
buildings/
├── kdtree_cell=0/data.parquet
├── kdtree_cell=1/data.parquet
└── kdtree_cell=2/data.parquet
```

However, the STAC output lacks machine-readable partition metadata. Consumers cannot programmatically discover:
- Which columns are partition keys
- What partitioning strategy was used (kdtree, h3, s2, quadkey)
- How to construct efficient queries

The STAC Table Extension (v1.2.0) does not define partition metadata—it describes table schemas, not physical organization.

We consulted m-mohr (table extension maintainer), who [commented](https://github.com/portolan-sdi/portolan-cli/issues/232#issuecomment-4129148114):

> "As long as partitioning strategies are format-specific, they should probably live in a separate extension."

This aligns with our existing `stac-iceberg-extension` pattern—format-specific metadata in dedicated extensions.

## Decision

**Create a standalone `stac-partition-extension`** with the `partition:` namespace.

### Extension Schema

```json
{
  "stac_extensions": [
    "https://stac-extensions.github.io/table/v1.2.0/schema.json",
    "https://portolan.org/stac-extensions/partition/v1.0.0/schema.json"
  ],
  "partition:scheme": "hive",
  "partition:strategy": "kdtree",
  "partition:keys": [
    {"name": "kdtree_cell", "type": "string"}
  ],
  "partition:file_count": 42,
  "assets": {
    "data": {
      "href": "./kdtree_cell=*/*.parquet",
      "partition:glob": "s3://bucket/buildings/kdtree_cell=*/*.parquet"
    }
  }
}
```

### Field Definitions

| Field | Scope | Type | Required | Description |
|-------|-------|------|----------|-------------|
| `partition:scheme` | Collection/Item | string | Yes | Partitioning scheme: `hive`, `directory` |
| `partition:strategy` | Collection/Item | string | No | Algorithm: `kdtree`, `h3`, `s2`, `quadkey`, `a5` |
| `partition:keys` | Collection/Item | array | Yes | Partition key definitions |
| `partition:file_count` | Collection/Item | integer | No | Number of partition files |
| `partition:glob` | Asset | string | No | Absolute glob URL for bulk access |

### Migration from `portolan:glob`

Current implementation uses `portolan:glob`. Migration path:

| Version | Behavior |
|---------|----------|
| v0.x | Both `portolan:glob` and `partition:glob` emitted |
| v1.0 | Deprecation warning for `portolan:glob` |
| v2.0 | Only `partition:glob` (breaking) |

## Consequences

### Positive

- **Machine-readable**: Consumers can programmatically discover partition structure
- **Standard pattern**: Follows existing `stac-iceberg-extension` approach
- **Composable**: Works alongside table extension (schema) and iceberg extension (lakehouse)
- **Upstream-friendly**: Can propose to STAC ecosystem if adoption grows

### Negative

- **Maintenance burden**: Another extension to maintain
- **Namespace proliferation**: `partition:` joins `portolan:`, `table:`, `iceberg:`
- **Not upstream**: Won't auto-validate in generic STAC tools without extension

### Neutral

- Partition pruning remains consumer responsibility (DuckDB, PyArrow)
- Per-partition statistics deferred (can derive from Parquet metadata)

## Alternatives Considered

### A. Extend STAC Table Extension upstream

Add `table:partitioning` to the table extension.

**Rejected because:**
- Table extension maintainer advised against it (format-specific)
- Would require upstream approval process
- Table extension is "Pilot" maturity (evolving)

### B. Use `portolan:` namespace

Keep partition metadata in `portolan:partition_*` fields.

**Rejected because:**
- `portolan:` is for Portolan-specific operational metadata (e.g., `datetime_provisional`)
- Partition metadata is semantically about data organization, not Portolan internals
- Harder for other tools to adopt

### C. Inline in table extension fields

Encode partitioning in `table:columns` descriptions or custom properties.

**Rejected because:**
- Not machine-readable
- Violates single-responsibility (columns describe schema, not storage)
- Would require parsing free-text

### D. No metadata, rely on directory conventions

Consumers detect Hive partitioning from directory structure.

**Rejected because:**
- Requires filesystem access (doesn't work for HTTP STAC catalogs)
- Can't distinguish intentional partitioning from coincidental directory structure
- No way to document partition strategy or validate consistency

## References

- Issue #232: Hive Partitioning Metadata Design for STAC
- PR #399: Spatial partitioning implementation
- m-mohr comment: https://github.com/portolan-sdi/portolan-cli/issues/232#issuecomment-4129148114
- stac-iceberg-extension: https://github.com/portolan-sdi/stac-iceberg-extension
- STAC Table Extension: https://github.com/stac-extensions/table
