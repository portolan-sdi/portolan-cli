# Partition STAC Extension Implementation Plan

**Date**: 2026-05-06
**Status**: Draft
**Issues**: #232 (Hive Partitioning Metadata Design)
**Depends On**: PR #399 (spatial partitioning), PR #404 (vector settings)

## Problem Statement

Portolan now supports spatial partitioning of large GeoParquet files (PR #399), but the STAC output lacks machine-readable partition metadata. Consumers cannot programmatically discover:

- Which columns are partition keys
- What partitioning strategy was used
- How to construct efficient queries

The STAC Table Extension does not define partition metadata. Per m-mohr (table extension maintainer): partitioning is format-specific and should live in a separate extension.

## Goals

1. **Create `stac-partition-extension`** — standalone STAC extension for partition metadata
2. **Emit partition metadata** in portolan's STAC output
3. **Detect partitions** in `portolan scan` for existing partitioned datasets
4. **Integrate auto-partitioning** into `portolan add` workflow
5. **Validate partition consistency** across files

## Non-Goals

- Partition pruning optimization (DuckDB/PyArrow handle this)
- Partition evolution (complex, defer to future)
- Iceberg integration (separate `stac-iceberg-extension` exists)

## Design

### Extension Schema

Repository: `github.com/portolan-sdi/stac-partition-extension`

```json
{
  "stac_extensions": [
    "https://stac-extensions.github.io/table/v1.2.0/schema.json",
    "https://portolan.org/stac-extensions/partition/v1.0.0/schema.json"
  ],
  "partition:scheme": "hive",
  "partition:strategy": "kdtree",
  "partition:keys": [
    {
      "name": "kdtree_cell",
      "type": "string",
      "description": "KD-tree spatial partition cell identifier"
    }
  ],
  "partition:file_count": 42,
  "assets": {
    "data": {
      "href": "./kdtree_cell=*/*.parquet",
      "partition:glob": "s3://bucket/collection/kdtree_cell=*/*.parquet",
      "type": "application/vnd.apache.parquet",
      "roles": ["data"]
    }
  }
}
```

#### Field Definitions

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `partition:scheme` | string | Yes | Partitioning scheme: `hive`, `directory` |
| `partition:strategy` | string | No | Algorithm used: `kdtree`, `h3`, `s2`, `quadkey`, `a5` |
| `partition:keys` | array | Yes | Partition key definitions |
| `partition:keys[].name` | string | Yes | Column name |
| `partition:keys[].type` | string | Yes | Data type |
| `partition:keys[].description` | string | No | Human description |
| `partition:file_count` | integer | No | Number of partition files |
| `partition:glob` | string | No | Asset-level: absolute glob URL for bulk access |

#### Relationship to `portolan:glob`

Current implementation uses `portolan:glob`. Migration path:
1. v1.0.0: Support both `portolan:glob` and `partition:glob`
2. v1.1.0: Deprecate `portolan:glob` for partitioned assets
3. v2.0.0: Remove `portolan:glob` (breaking)

### Module Changes

```
portolan_cli/
├── partitioning.py          # Existing: partition_geoparquet(), build_glob_pattern()
│                            # Add: get_partition_metadata() -> dict
├── stac.py                  # Add partition:* fields when partitioned
├── dataset.py               # Wire partition metadata through scan/add
├── validation/
│   └── rules.py             # Add: PartitionConsistencyRule
└── scan.py                  # Detect existing partitions in directories
```

---

## Implementation Phases

### Phase 1: Extension Repository (External)

**Deliverable**: `stac-partition-extension` repo with v1.0.0 release

- [x] Create repo from STAC extension template
- [x] Write JSON Schema for `partition:*` fields
- [x] Add examples (kdtree, h3, multi-key)
- [x] Add validation test fixtures
- [x] README with consumer code examples (DuckDB, PyArrow)
- [x] Tag v1.0.0 release

**Files to create**:
```
stac-partition-extension/
├── README.md
├── json-schema/
│   └── schema.json
├── examples/
│   ├── collection-kdtree.json
│   ├── collection-h3.json
│   └── collection-multikey.json
└── tests/
    └── test_validation.py
```

**Exit criteria**: Extension passes STAC validator, examples render in STAC Browser

---

### Phase 2: ADR Documentation

**Deliverable**: ADR-0042 documenting extension design decisions

- [x] Create `context/shared/adr/0042-partition-stac-extension.md`
- [x] Document: why separate extension (m-mohr feedback)
- [x] Document: relationship to table extension
- [x] Document: `portolan:glob` → `partition:glob` migration
- [x] Update CLAUDE.md ADR index

**Exit criteria**: ADR reviewed and merged

---

### Phase 3: Emit Partition Metadata

**Deliverable**: Portolan STAC output includes `partition:*` fields

- [x] Add `get_partition_metadata(output_dir, strategy)` to `partitioning.py`
  - Returns dict with scheme, strategy, keys, file_count
  - Parses Hive directory structure
- [x] Update `_create_collection_stac()` in `stac.py`
  - Accept optional partition_metadata parameter
  - Add `partition:*` fields to collection properties
  - Add extension URL to `stac_extensions`
- [ ] Wire through from `partition_geoparquet()` result
- [x] Update `portolan:glob` → `partition:glob` in `push.py`
- [x] Unit tests for metadata emission

**Key code changes**:

```python
# partitioning.py
def get_partition_metadata(output_dir: Path, strategy: str) -> dict:
    """Extract partition metadata from Hive-style output directory."""
    partition_col = PARTITION_COLUMNS.get(strategy, f"{strategy}_cell")
    partition_dirs = list(output_dir.glob(f"{partition_col}=*"))

    return {
        "partition:scheme": "hive",
        "partition:strategy": strategy,
        "partition:keys": [
            {"name": partition_col, "type": "string"}
        ],
        "partition:file_count": sum(
            len(list(d.glob("*.parquet"))) for d in partition_dirs
        ),
    }
```

**Exit criteria**: `portolan add` on partitioned data produces valid STAC with partition extension

---

### Phase 4: Partition Detection in Scan

**Deliverable**: `portolan scan` detects and reports existing partitions

- [x] Add `detect_partitioning(directory: Path)` to `partitioning.py`
  - Detect Hive-style directories (`column=value/`)
  - Extract partition keys from directory names
  - Return `PartitionInfo` dataclass or None
- [ ] Update `portolan scan` output to show partition info
  - "Detected: Hive-partitioned (keys: kdtree_cell, 42 partitions)"
- [ ] Handle edge cases:
  - Mixed partition depths
  - Non-Hive directory structures
  - Empty partition directories
- [x] Unit tests for detection

**Exit criteria**: `portolan scan` on partitioned directory shows partition summary

---

### Phase 5: Auto-Partition in Add Workflow

**Deliverable**: Large files auto-partition during `portolan add`

- [ ] Add `partitioning.auto_partition` config option (default: true)
- [ ] Add `partitioning.prompt` config option (default: true for interactive)
- [ ] In `portolan add`, check `should_partition()` after file analysis
- [ ] If interactive + prompt enabled: ask user
- [ ] If `--auto` or non-interactive: use config threshold
- [ ] Wire partitioned output through to STAC generation
- [ ] Update `--help` and docs

**UX flow**:
```
$ portolan add large-dataset.parquet

Analyzing: large-dataset.parquet (4.2 GB, 12M rows)
⚠ File exceeds 2.0 GB threshold

Partition into ~100 spatial chunks? [Y/n] y

Partitioning: large-dataset.parquet
  Strategy: kdtree (data-driven spatial)
  Target: ~120,000 rows per partition
  ████████████████████████████████████████ 100%

✓ Created 98 partitions in large-dataset/
✓ Added to collection: default
```

**Exit criteria**: `portolan add bigfile.parquet` partitions and catalogs in one command

---

### Phase 6: Partition Validation Rules

**Deliverable**: `portolan check` validates partition consistency

- [ ] Add `PartitionSchemaConsistencyRule` to `validation/rules.py`
  - All partition files have same Parquet schema
  - Partition key column exists in schema
- [ ] Add `PartitionRowCountRule`
  - `table:row_count` matches sum of partition row counts
- [ ] Add `PartitionStructureRule`
  - All partition directories follow same key pattern
  - No orphan files outside partition structure
- [ ] Wire into `portolan check` with `--thorough` flag (expensive)
- [ ] Unit tests for each rule

**Exit criteria**: `portolan check --thorough` catches partition inconsistencies

---

### Phase 7: Documentation

**Deliverable**: User-facing partitioning guide

- [ ] Add `docs/guides/partitioning.md`
  - When to partition (thresholds, use cases)
  - Configuration options
  - Consumer code examples
- [ ] Update `docs/reference/configuration.md` with partition settings
- [ ] Update `docs/reference/cli.md` with `portolan partition` command
- [ ] Add partitioning to "Getting Started" if appropriate

**Exit criteria**: User can learn partitioning from docs alone

---

## Dependency Graph

```
Phase 1 (Extension Repo)
    │
    ├──► Phase 2 (ADR) ──────────────────────────────────┐
    │                                                     │
    └──► Phase 3 (Emit Metadata) ◄────────────────────────┘
              │
              ├──► Phase 4 (Scan Detection)
              │         │
              │         └──► Phase 5 (Auto-Partition in Add)
              │
              └──► Phase 6 (Validation Rules)
                        │
                        └──► Phase 7 (Documentation)
```

**Critical path**: 1 → 3 → 5 (extension → emit → add integration)

---

## Testing Strategy

| Phase | Test Type | Coverage |
|-------|-----------|----------|
| 1 | STAC validation | Extension schema validity |
| 3 | Unit | Metadata extraction, STAC emission |
| 4 | Unit + Integration | Detection on various directory structures |
| 5 | Integration | End-to-end add workflow |
| 6 | Unit | Each validation rule |
| 7 | Manual | Docs accuracy |

**Real-world test data**: Use existing `tests/fixtures/realdata/` or create synthetic partitioned fixture.

---

## Migration Notes

### `portolan:glob` → `partition:glob`

Existing catalogs using `portolan:glob` remain valid. The extension supports both during transition:

```python
# push.py - during transition period
if "*" in href:
    # Support both for backwards compatibility
    asset_data["portolan:glob"] = remote_glob  # Legacy
    asset_data["partition:glob"] = remote_glob  # New
```

Deprecation timeline:
- **v0.x**: Both supported, no warning
- **v1.0**: Both supported, deprecation warning for `portolan:glob`
- **v2.0**: Only `partition:glob` (breaking change)

---

## Open Questions

1. **Multi-key partitioning**: Should we support `year=2020/state=CA/` (multiple keys)?
   - Current implementation: single key only (spatial)
   - Future consideration: temporal + spatial compound keys

2. **Partition statistics**: Should we store per-partition row counts?
   - Pro: Enables smarter query planning
   - Con: Maintenance burden, can derive from Parquet metadata

3. **Non-Hive schemes**: Should we support directory-based (no `=`) partitioning?
   - Some datasets use `2020/CA/data.parquet` without Hive syntax
   - Lower priority, can add later

---

## Success Criteria

Issue #232 is complete when:

- [ ] `stac-partition-extension` v1.0.0 released
- [ ] ADR-0042 merged
- [ ] `portolan add` emits `partition:*` metadata for partitioned datasets
- [ ] `portolan scan` detects and reports existing partitions
- [ ] `portolan check` validates partition consistency
- [ ] Documentation covers partitioning workflow
- [ ] Issue #232 closed with summary comment

---

## References

- Issue #232: Hive Partitioning Metadata Design for STAC
- PR #399: feat(partition): add spatial partitioning for large GeoParquet files
- PR #404: feat(convert): add configurable vector spatial optimization
- m-mohr comment: https://github.com/portolan-sdi/portolan-cli/issues/232#issuecomment-4129148114
- cayetanobv Iceberg analysis: https://github.com/portolan-sdi/portolan-cli/issues/232#issuecomment-4091450081
- Existing Iceberg extension: https://github.com/portolan-sdi/stac-iceberg-extension
