# ADR-0005: versions.json as Single Source of Truth

## Status
Accepted (amended 2025-02-11 to add schema fingerprints)

## Context

Portolan needs to track:
- What files exist in a dataset
- What changed between versions
- What's synced to remote vs local-only
- Whether files have been corrupted or tampered with
- Schema structure for breaking change detection

This could be done with multiple files (version history, sync manifest, checksums) or a single unified file.

## Decision

Each collection has a single `versions.json` that serves as:

1. **Version history** — All published versions with semantic versions and timestamps
2. **Sync manifest** — What's on remote, what needs pushing
3. **Integrity checksums** — SHA256 for every asset file
4. **Change tracking** — Which files changed between versions
5. **Schema fingerprints** — Structural metadata for breaking change detection
6. **Breaking change flags** — Manual or detected breaking change markers

### Versioning Hierarchy

`versions.json` exists at each level of the catalog hierarchy:

```
s3://bucket/
├── versions.json           # Catalog-level versioning
├── collection-a/
│   ├── versions.json       # Collection-level versioning
│   └── v1.0.0/
│       └── data.parquet
└── collection-b/
    ├── versions.json       # Collection-level versioning
    └── v1.0.0/
        └── data.parquet
```

### Structure

```json
{
  "spec_version": "1.0.0",
  "current_version": "2.1.0",
  "versions": [
    {
      "version": "2.1.0",
      "created": "2024-01-15T10:30:00Z",
      "breaking": false,
      "message": "Data update, no schema changes",
      "schema": {
        "type": "geoparquet",
        "fingerprint": {
          "columns": [
            {"name": "geometry", "type": "geometry", "geometry_type": "Polygon", "crs": "EPSG:4326"},
            {"name": "name", "type": "string"},
            {"name": "population", "type": "int64"}
          ]
        }
      },
      "assets": {
        "data.parquet": {
          "sha256": "abc123...",
          "size_bytes": 1048576,
          "href": "s3://bucket/collection/v2.1.0/data.parquet"
        }
      },
      "changes": ["data.parquet"]
    }
  ]
}
```

### Schema Fingerprints

Schema fingerprints capture structural metadata for change detection. Full metadata lives in STAC; fingerprints are summaries sufficient to detect breaking changes.

**GeoParquet fingerprint:**
```json
{
  "type": "geoparquet",
  "fingerprint": {
    "columns": [
      {"name": "geometry", "type": "geometry", "geometry_type": "Polygon", "crs": "EPSG:4326"},
      {"name": "name", "type": "string"},
      {"name": "population", "type": "int64"}
    ]
  }
}
```

**COG fingerprint:**
```json
{
  "type": "cog",
  "fingerprint": {
    "bands": [
      {"name": "red", "data_type": "uint8"},
      {"name": "green", "data_type": "uint8"},
      {"name": "blue", "data_type": "uint8"}
    ],
    "crs": "EPSG:32610",
    "nodata": 0,
    "resolution": [10.0, 10.0]
  }
}
```

### Breaking Change Detection

Changes to these fingerprint fields are considered breaking:

| Format | Breaking Fields |
|--------|-----------------|
| **GeoParquet** | Column removed, column type changed, column renamed, geometry type changed, CRS changed |
| **COG** | Band removed, band data_type changed, CRS changed, resolution changed, nodata changed |

Adding columns or bands is NOT breaking (additive change).

### Sync mechanism

1. Compare local `versions.json` against remote
2. Push files where local checksum ≠ remote (or remote missing)
3. Update remote `versions.json` after successful push

### Version archival

- Each version's files live in versioned paths: `/v{version}/asset.parquet`
- `portolan prune` removes old version files (with safety mechanisms)
- Pruned versions retain metadata in `versions.json` (marked with `pruned: true`) for audit trail

## Consequences

### What becomes easier
- **Single file to understand** — No reconciling multiple sources
- **Atomic updates** — One file write per version bump
- **Simple sync** — Diff one JSON file to know what to push
- **Corruption detection** — Checksums catch tampering or bit rot
- **Offline work** — Local versions.json is complete history

### What becomes harder
- **Large catalogs** — File grows with version count (mitigated by prune)
- **Concurrent writes** — Single file = single writer (see [ADR-0015](0015-two-tier-versioning-architecture.md) for multi-user via plugin)
- **Partial failures** — If push fails mid-way, versions.json may be inconsistent (need transaction-like semantics)

### Trade-offs
- We accept file growth for simplicity
- We accept single-writer constraint for consistency (multi-user deferred to Iceberg plugin)

## Alternatives Considered

### 1. Separate files (versions.json, manifest.json, checksums.txt)
**Rejected:** Multiple sources of truth, sync complexity, reconciliation bugs.

### 2. Git-based versioning
**Rejected:** Requires git knowledge, doesn't work well with large binary files, adds dependency.

### 3. Database (SQLite)
**Rejected:** Not human-readable, harder to sync to object storage, overkill for the problem size.

### 4. Rely on object storage versioning (S3 versioning)
**Rejected:** Vendor-specific, no cross-cloud portability, doesn't capture semantic version info.

## Related ADRs

- [ADR-0006: Remote Ownership Model](0006-remote-ownership-model.md) — Portolan owns bucket contents
- [ADR-0015: Two-Tier Versioning Architecture](0015-two-tier-versioning-architecture.md) — MVP vs Iceberg plugin

## References

- [MVP Versioning Strategy](../plans/mvp-versioning-strategy.md) — Detailed implementation plan
