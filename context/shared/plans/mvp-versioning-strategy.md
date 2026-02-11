# Portolan MVP Scope & Versioning Strategy

**Date:** 2025-02-11
**Status:** Accepted

## Executive Summary

Portolan will ship an MVP with simple, file-based versioning (`versions.json`) that assumes single-writer access. Advanced features (concurrency, ACID writes, multi-user collaboration) will be provided via an **Iceberg + Icechunk plugin** that Javier will develop in parallel.

This approach:
1. Gets a working product to entry-level users (municipalities, small orgs) quickly
2. Avoids implementing complex distributed systems from scratch
3. Leaves room for Iceberg/Icechunk to mature geospatially (~6 month horizon)
4. Defines a clear plugin interface for enterprise features

---

## Two-Tier Architecture

```
                     ┌─────────────────────────────────────────┐
                     │            portolan CLI                 │
                     └────────────────┬────────────────────────┘
                                      │
              ┌───────────────────────┴───────────────────────┐
              │                                               │
     ┌────────▼────────┐                          ┌──────────▼──────────┐
     │  MVP Backend    │                          │  Plugin: icechunk   │
     │  (versions.json)│                          │  + iceberg          │
     ├─────────────────┤                          ├─────────────────────┤
     │ • Single writer │                          │ • Multi-writer ACID │
     │ • File-based    │                          │ • Native versioning │
     │ • Simple prune  │                          │ • Schema evolution  │
     │ • Rollback      │                          │ • Time travel       │
     │ • Schema stored │                          │ • Branch/tag/merge  │
     │ • $5/month S3   │                          │ • Garbage collection│
     └─────────────────┘                          └─────────────────────┘
           │                                                │
           │  Target: Municipality, small team              │  Target: Carto, HDX, multi-user orgs
           │  Concurrency: None (documented)                │  Concurrency: Native ACID
           │  Versioning: Linear append-only                │  Versioning: Branches, tags, time travel
```

---

## What's IN the MVP

### Core Versioning (`versions.json`)

| Feature | Description |
|---------|-------------|
| **Linear version history** | Append-only version entries with semantic versions and timestamps |
| **Checksums (SHA256)** | Integrity verification for all assets |
| **Sync manifest** | Track what's on remote vs local |
| **Schema fingerprints** | Store schema summary at publish time for change detection |
| **Version rollback** | Create new version with old content (append-style) |
| **Manual breaking flag** | `portolan publish --breaking` |

### Prune Safety (Issue #15)

| Feature | Description |
|---------|-------------|
| **`--dry-run`** | Show what would be deleted |
| **Confirmation prompt** | Interactive "are you sure?" |
| **`--yes` flag** | Skip confirmation for automation |

### Conflict Handling (Simplified)

| Feature | Description |
|---------|-------------|
| **Drift detection** | Warn if remote doesn't match expected state |
| **Fail loudly** | Error if remote `versions.json` changed unexpectedly |
| **`--force` override** | Allow explicit override (user takes responsibility) |

### Schema Tracking (Minimal)

| Feature | Description |
|---------|-------------|
| **Store schema fingerprint in versions.json** | Capture structural info for change detection |
| **`portolan diff` command** | Show schema changes between versions (optional) |
| **Warn on schema change** | Prompt for `--breaking` if schema differs (optional) |

---

## What's OUT of MVP (Deferred to Plugin)

| Feature | Why Deferred | Issue |
|---------|--------------|-------|
| **Concurrent writes** | Requires ACID transactions | #18, #33 |
| **Distributed locking** | Complex, error-prone, Icechunk solves this | #18 |
| **Automatic breaking change detection** | Needs heuristics defined, schema diff analysis | #14 |
| **Soft-delete / trash** | Nice-to-have, not essential for single-user | #18 |
| **Branch/tag versioning** | Linear history sufficient for MVP | — |
| **Time travel queries** | Just "current" and "list versions" for MVP | — |
| **Snapshot expiration / GC** | Simple prune is enough for MVP | — |
| **Multi-user access model** | Documented as unsupported in MVP | #33 |

---

## Versioning Hierarchy

Portolan uses STAC terminology. Each level is independently versioned:

```
Catalog (versioned)
└── Collection (versioned) ─── "dataset" in Portolan terminology
    └── Assets (files within a version)
```

### Storage Layout

```
s3://bucket/
├── versions.json                    # Catalog versioning
├── catalog.json
├── buildings/                       # A Collection (dataset)
│   ├── versions.json                # Collection versioning
│   ├── collection.json
│   ├── v1.0.0/
│   │   └── buildings.parquet
│   └── v2.0.0/
│       └── buildings.parquet
└── administrative/                  # A Collection with sub-collections
    ├── versions.json                # Collection versioning
    ├── collection.json
    ├── boundaries/                  # Sub-collection
    │   ├── versions.json
    │   ├── collection.json
    │   └── v1.0.0/
    │       └── boundaries.parquet
    └── regions/                     # Sub-collection
        ├── versions.json
        ├── collection.json
        └── v1.0.0/
            └── regions.parquet
```

### What Each `versions.json` Tracks

| Level | Tracks |
|-------|--------|
| **Catalog** | Structure changes (collections added/removed), catalog metadata |
| **Collection** | Asset versions, schema changes, data updates |
| **Sub-collection** | Same as collection—recursive structure |

---

## `versions.json` Schema (MVP)

```json
{
  "spec_version": "1.0.0",
  "current_version": "2.1.0",
  "versions": [
    {
      "version": "1.0.0",
      "created": "2024-01-10T08:00:00Z",
      "breaking": false,
      "message": "Initial release",
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
          "href": "s3://bucket/dataset/v1.0.0/data.parquet"
        }
      }
    },
    {
      "version": "2.0.0",
      "created": "2024-01-12T10:00:00Z",
      "breaking": true,
      "message": "Added region column, removed deprecated fields",
      "schema": {
        "type": "geoparquet",
        "fingerprint": {
          "columns": [
            {"name": "geometry", "type": "geometry", "geometry_type": "Polygon", "crs": "EPSG:4326"},
            {"name": "name", "type": "string"},
            {"name": "population", "type": "int64"},
            {"name": "region", "type": "string"}
          ]
        }
      },
      "assets": {
        "data.parquet": {
          "sha256": "def456...",
          "size_bytes": 1152000,
          "href": "s3://bucket/dataset/v2.0.0/data.parquet"
        }
      },
      "changes": ["data.parquet"]
    },
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
            {"name": "population", "type": "int64"},
            {"name": "region", "type": "string"}
          ]
        }
      },
      "assets": {
        "data.parquet": {
          "sha256": "ghi789...",
          "size_bytes": 1200000,
          "href": "s3://bucket/dataset/v2.1.0/data.parquet"
        }
      },
      "changes": ["data.parquet"]
    }
  ]
}
```

### Version Entry Fields

| Field | Type | Description |
|-------|------|-------------|
| `version` | string | Semantic version (major.minor.patch) |
| `created` | string | ISO 8601 timestamp |
| `breaking` | boolean | Whether this version has breaking changes |
| `message` | string | Human-readable description |
| `schema` | object | Schema fingerprint for change detection |
| `assets` | object | Map of asset filename to metadata |
| `changes` | array | List of assets that changed from previous version |
| `pruned` | boolean | (Optional) True if assets have been deleted |
| `pruned_at` | string | (Optional) ISO 8601 timestamp of pruning |
| `rollback_from` | string | (Optional) Version this was rolled back from |
| `rollback_to` | string | (Optional) Version this was rolled back to |

---

## Schema Fingerprints

Schema fingerprints capture the structural elements needed for change detection. Full metadata lives in STAC; fingerprints are summaries.

### GeoParquet Fingerprint

```json
{
  "type": "geoparquet",
  "fingerprint": {
    "columns": [
      {"name": "geometry", "type": "geometry", "geometry_type": "Polygon", "crs": "EPSG:4326"},
      {"name": "name", "type": "string"},
      {"name": "population", "type": "int64"},
      {"name": "region", "type": "string"}
    ]
  }
}
```

### COG Fingerprint

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

### What Constitutes a Breaking Change

| Format | Field | Breaking if Changed? |
|--------|-------|---------------------|
| **GeoParquet** | Column removed | Yes |
| | Column added | No |
| | Column type changed | Yes |
| | Column renamed | Yes (treated as remove + add) |
| | Geometry type changed | Yes |
| | CRS changed | Yes |
| **COG** | Band removed | Yes |
| | Band added | No |
| | Band data_type changed | Yes |
| | CRS changed | Yes |
| | Resolution changed | Yes |
| | NoData changed | Yes |

---

## Version Numbering

**Decision:** Semantic versioning with auto-bump logic.

```bash
portolan publish                    # 1.0.0 → 1.0.1 (patch)
portolan publish --breaking         # 1.0.1 → 2.0.0 (major)
portolan publish --version 3.0.0    # explicit override
```

Each version entry includes both semantic version AND timestamp:
- Semantic version for human communication ("use v2.0.0")
- Timestamp for audit trail and machine sorting

---

## Version Rollback Design

**Principle:** Rollback creates a new version (append-only history), similar to `git revert`.

```bash
portolan rollback v1.0.0
```

**Behavior:**
1. Read `versions.json`, find `v1.0.0` entry
2. Create a new version entry (e.g., `v2.2.0`) with:
   - Same schema as `v1.0.0`
   - Same assets as `v1.0.0` (references same paths or copies)
   - Message: `"Rollback to v1.0.0"`
3. Update `current_version` to new version
4. Sync to remote

**Why append-only:**
- History is never rewritten
- Audit trail shows what happened and when
- Simpler mental model (no "where did my versions go?")
- Matches git behavior (`git revert` vs `git reset --hard`)

**Example resulting entry:**

```json
{
  "version": "2.2.0",
  "created": "2024-01-20T14:00:00Z",
  "breaking": false,
  "message": "Rollback to v1.0.0",
  "rollback_from": "2.1.0",
  "rollback_to": "1.0.0",
  "schema": { /* same as v1.0.0 */ },
  "assets": { /* same as v1.0.0 */ }
}
```

### Rollback to Pruned Version

If a user prunes v1.0.0 then tries to rollback to it:

```bash
portolan rollback v1.0.0
# Error: "Version v1.0.0 has been pruned. Assets are no longer available.
#         To see pruned versions: portolan versions --show-pruned"
```

Prune keeps metadata (for audit trail) but deletes files.

---

## Plugin Interface

The MVP leaves hooks for the Iceberg + Icechunk plugin to override:

```python
from typing import Protocol

class VersioningBackend(Protocol):
    """Backend for version storage and management."""

    def get_current_version(self, collection: str) -> Version:
        """Get the current version of a collection."""
        ...

    def list_versions(self, collection: str) -> list[Version]:
        """List all versions of a collection."""
        ...

    def publish(
        self,
        collection: str,
        assets: dict,
        schema: SchemaFingerprint,
        breaking: bool,
        message: str,
    ) -> Version:
        """Publish a new version."""
        ...

    def rollback(self, collection: str, target_version: str) -> Version:
        """Rollback to a previous version (creates new version)."""
        ...

    def prune(
        self,
        collection: str,
        keep: int,
        dry_run: bool,
    ) -> list[Version]:
        """Remove old versions."""
        ...

    def check_drift(self, collection: str) -> DriftReport:
        """Check if remote state matches expected state."""
        ...


class JsonFileBackend(VersioningBackend):
    """MVP implementation using versions.json."""
    ...


class IcebergBackend(VersioningBackend):
    """Plugin implementation using Iceberg + Icechunk."""
    ...
```

### Plugin Discovery

Plugins register via Python entry points:

```toml
# portolan-iceberg/pyproject.toml
[project.entry-points."portolan.backends"]
iceberg = "portolan_iceberg:IcebergBackend"
```

---

## Single-Writer Documentation

The MVP must clearly document the single-writer assumption:

### In CLI help:

```
Note: Portolan MVP assumes single-writer access. Do not run concurrent
publish, sync, or prune operations on the same catalog. For multi-user
support, see the portolan-iceberg plugin.
```

### In docs:

> **Concurrency Warning**
>
> The MVP versioning system (`versions.json`) does not support concurrent writes.
> If two users publish to the same catalog simultaneously, data corruption may occur.
>
> For multi-user environments, install the `portolan-iceberg` plugin which provides
> ACID transactions and optimistic concurrency control.

### In code (fail loudly):

```python
def sync(self, collection: str, force: bool = False) -> None:
    remote_versions = self.fetch_remote_versions(collection)
    local_versions = self.load_local_versions(collection)

    if remote_versions.current_version != local_versions.expected_remote_version:
        if force:
            warn("Remote changed unexpectedly. Proceeding with --force.")
        else:
            raise ConflictError(
                f"Remote versions.json has changed (expected {local_versions.expected_remote_version}, "
                f"found {remote_versions.current_version}). "
                "Another process may have modified the catalog. "
                "Use --force to override, or resolve manually."
            )
```

---

## Issue Mapping

| Issue | MVP Scope | Plugin Scope |
|-------|-----------|--------------|
| **#14** Schema evolution | Store schema fingerprint, manual `--breaking` flag | Automatic detection, heuristics |
| **#15** Prune safety | `--dry-run`, confirmation, `--yes` | Soft-delete, trash, auto-expiration |
| **#18** Concurrent access | Documented as unsupported, fail loudly | ACID writes, optimistic concurrency |
| **#33** Multi-user ADR | Decision: single-writer MVP, plugin for multi | Full multi-writer support |

---

## Implementation Order

1. **Schema fingerprint storage** — Foundation for change detection
2. **Prune safety** — `--dry-run`, confirmation, `--yes`
3. **Drift detection** — Fail loudly on remote mismatch
4. **Rollback** — Creates new version from old
5. **Plugin interface definition** — Protocol for future backends

---

## References

- [ADR-0004: Iceberg as Plugin](../adr/0004-iceberg-as-plugin.md)
- [ADR-0005: versions.json as Source of Truth](../adr/0005-versions-json-source-of-truth.md)
- [ADR-0015: Two-Tier Versioning Architecture](../adr/0015-two-tier-versioning-architecture.md)
- [Issue #14: Schema evolution](https://github.com/portolan-sdi/portolan-cli/issues/14)
- [Issue #15: Prune safety](https://github.com/portolan-sdi/portolan-cli/issues/15)
- [Issue #18: Concurrent access](https://github.com/portolan-sdi/portolan-cli/issues/18)
- [Issue #33: Multi-user ADR](https://github.com/portolan-sdi/portolan-cli/issues/33)
- [Icechunk](https://github.com/earth-mover/icechunk) — Versioned cloud-native array storage
- [Apache Iceberg](https://iceberg.apache.org/) — Table format for huge analytic datasets
- [STAC Spec](https://github.com/radiantearth/stac-spec) — SpatioTemporal Asset Catalog specification
