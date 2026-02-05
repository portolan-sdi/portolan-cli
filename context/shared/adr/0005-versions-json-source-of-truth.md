# ADR-0005: versions.json as Single Source of Truth

## Status
Accepted

## Context

Portolan needs to track:
- What files exist in a dataset
- What changed between versions
- What's synced to remote vs local-only
- Whether files have been corrupted or tampered with

This could be done with multiple files (version history, sync manifest, checksums) or a single unified file.

## Decision

Each dataset has a single `versions.json` that serves as:

1. **Version history** — All published versions with timestamps
2. **Sync manifest** — What's on remote, what needs pushing
3. **Integrity checksums** — SHA256 for every asset file
4. **Change tracking** — Which files changed between versions
5. **Schema evolution flags** — Breaking change markers

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
      "assets": {
        "data.parquet": {
          "sha256": "abc123...",
          "size_bytes": 1048576,
          "href": "s3://bucket/dataset/data.parquet"
        }
      },
      "changes": ["data.parquet"]
    }
  ]
}
```

### Sync mechanism

1. Compare local `versions.json` against remote
2. Push files where local checksum ≠ remote (or remote missing)
3. Update remote `versions.json` after successful push

### Version archival

- Current version files live at dataset root
- Old versions archived to `/v{version}/` paths
- `portolan prune` removes old version files (with safety mechanisms)

## Consequences

### What becomes easier
- **Single file to understand** — No reconciling multiple sources
- **Atomic updates** — One file write per version bump
- **Simple sync** — Diff one JSON file to know what to push
- **Corruption detection** — Checksums catch tampering or bit rot
- **Offline work** — Local versions.json is complete history

### What becomes harder
- **Large catalogs** — File grows with version count (mitigated by prune)
- **Concurrent writes** — Single file = single writer (addressed by locking, see issue #18)
- **Partial failures** — If push fails mid-way, versions.json may be inconsistent (need transaction-like semantics)

### Trade-offs
- We accept file growth for simplicity
- We accept single-writer constraint for consistency

## Alternatives Considered

### 1. Separate files (versions.json, manifest.json, checksums.txt)
**Rejected:** Multiple sources of truth, sync complexity, reconciliation bugs.

### 2. Git-based versioning
**Rejected:** Requires git knowledge, doesn't work well with large binary files, adds dependency.

### 3. Database (SQLite)
**Rejected:** Not human-readable, harder to sync to object storage, overkill for the problem size.

### 4. Rely on object storage versioning (S3 versioning)
**Rejected:** Vendor-specific, no cross-cloud portability, doesn't capture semantic version info.
