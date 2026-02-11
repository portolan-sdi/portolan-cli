# ADR-0015: Two-Tier Versioning Architecture

## Status
Accepted

## Context

Portolan needs versioning for catalogs and collections. The challenge: our target users span from small municipalities (single user, simple needs) to organizations like Carto and HDX (multiple concurrent users, enterprise requirements).

Building robust concurrent access, ACID transactions, and distributed locking from scratch is complex and error-prone. Meanwhile, the Iceberg + Icechunk ecosystem is maturing and already solves these problems for data lakes.

We need to decide: do we build sophisticated multi-user versioning into MVP, or keep MVP simple and defer advanced features to a plugin?

Related issues:
- #14: Schema evolution and breaking change detection
- #15: Prune safety mechanisms
- #18: Concurrent access safety
- #33: Multi-user access model decision

## Decision

**Two-tier architecture: Simple MVP backend + Iceberg plugin for enterprise.**

### Tier 1: MVP Backend (`versions.json`)

A file-based versioning system that:
- Uses `versions.json` as the single source of truth (see [ADR-0005](0005-versions-json-source-of-truth.md))
- Assumes single-writer access (documented limitation)
- Provides linear, append-only version history
- Stores schema fingerprints for change detection
- Supports rollback (creates new version from old)
- Includes prune with safety mechanisms

**Target users:** Municipalities, small teams, individual publishers. Anyone who doesn't need concurrent access.

### Tier 2: Portolake Plugin

[Portolake](https://github.com/portolan-sdi/portolake) is a plugin that replaces the versioning backend with:

- **Apache Iceberg** — For tabular/vector data (GeoParquet) with ACID transactions
- **Icechunk** — For array/raster data (COG, NetCDF, HDF, Zarr) via VirtualiZarr

Features:
- ACID transactions for concurrent writes
- Native time travel and version branching
- Schema evolution with automatic detection
- Garbage collection and snapshot management
- Optimistic concurrency control

**Target users:** Carto, HDX, multi-user organizations. Anyone needing concurrent access.

### Why This Split

| Concern | MVP Approach | Plugin Approach |
|---------|--------------|-----------------|
| **Complexity** | Simple file operations | Delegate to mature ecosystem |
| **Concurrency** | Documented as unsupported | Native ACID support |
| **Time to ship** | Weeks | Parallel development by Javier |
| **Infrastructure** | $5/month S3 | May require catalog server |
| **Geospatial support** | Full (GeoParquet, COG) | Maturing (Iceberg V3 geometry) |

### Plugin Interface

Both backends implement the same protocol:

```python
class VersioningBackend(Protocol):
    def get_current_version(self, collection: str) -> Version: ...
    def list_versions(self, collection: str) -> list[Version]: ...
    def publish(self, collection: str, assets: dict, schema: SchemaFingerprint, breaking: bool, message: str) -> Version: ...
    def rollback(self, collection: str, target_version: str) -> Version: ...
    def prune(self, collection: str, keep: int, dry_run: bool) -> list[Version]: ...
    def check_drift(self, collection: str) -> DriftReport: ...
```

This allows the CLI to remain unchanged regardless of backend.

### Plugin Discovery

Via Python entry points (consistent with [ADR-0003](0003-plugin-architecture.md)):

```toml
# portolake/pyproject.toml
[project.entry-points."portolan.backends"]
iceberg = "portolan_iceberg:IcebergBackend"
```

## Consequences

### What becomes easier

- **Fast MVP delivery** — No need to solve distributed systems problems
- **Simple deployment** — Single-user catalogs work with static file hosting
- **Clear upgrade path** — Users can migrate to plugin when they need concurrency
- **Reduced maintenance** — Complex concurrency logic lives in specialized plugin
- **Independent evolution** — Iceberg plugin can track upstream Iceberg releases

### What becomes harder

- **Multi-user out of box** — Organizations need the plugin for concurrent access
- **Two codepaths** — Must maintain interface compatibility across backends
- **Documentation burden** — Must clearly communicate when plugin is needed

### Trade-offs

- We accept delayed multi-user support for faster MVP delivery
- We accept plugin dependency for enterprise features in exchange for simplicity
- We accept interface maintenance burden for clean separation of concerns

## Alternatives Considered

### 1. Build multi-user into MVP
**Rejected:** Significantly increases complexity and time to ship. Distributed locking is hard to get right. Better to delegate to Icechunk which specializes in this.

### 2. Iceberg-first, no simple backend
**Rejected:** Iceberg requires more infrastructure (catalog server at minimum). Excludes small users who just want static file hosting.

### 3. Wait for Iceberg geospatial support to mature
**Considered:** Could revisit when Iceberg V3+ geometry is well-supported in tooling. Current decision doesn't preclude this—plugin can always be promoted to core.

### 4. Optimistic locking in MVP
**Rejected:** Partial solution. Handles simple conflicts but not true concurrent writes. Adds complexity without fully solving the problem.

## Implementation Notes

### Single-Writer Documentation

The single-writer assumption must be prominently documented:

```
Note: Portolan MVP assumes single-writer access. Do not run concurrent
publish, sync, or prune operations on the same catalog. For multi-user
support, see the portolake plugin.
```

### Drift Detection

MVP fails loudly if remote state doesn't match expectations:

```python
if remote_versions.current_version != local_versions.expected_remote_version:
    raise ConflictError(
        "Remote versions.json has changed unexpectedly. "
        "Another process may have modified the catalog. "
        "Use --force to override, or resolve manually."
    )
```

This catches accidental concurrent access even without true locking.

### Migration Path

When users outgrow MVP:

1. Install `portolake` plugin
2. Configure Iceberg catalog connection
3. Run `portolan migrate --to iceberg` (future command)
4. Existing version history preserved in Iceberg format

## Related ADRs

- [ADR-0003: Plugin Architecture](0003-plugin-architecture.md) — Entry point pattern for plugins
- [ADR-0004: Iceberg as Plugin](0004-iceberg-as-plugin.md) — Why Iceberg isn't core
- [ADR-0005: versions.json as Source of Truth](0005-versions-json-source-of-truth.md) — MVP backend design
- [ADR-0006: Remote Ownership Model](0006-remote-ownership-model.md) — Portolan owns bucket contents

## References

- [MVP Versioning Strategy](../plans/mvp-versioning-strategy.md) — Detailed implementation plan
- [Portolake](https://github.com/portolan-sdi/portolake) — Enterprise versioning plugin for Portolan
- [Apache Iceberg](https://iceberg.apache.org/) — Table format for huge analytic datasets
- [Icechunk](https://github.com/earth-mover/icechunk) — Versioned cloud-native array storage
- [VirtualiZarr](https://github.com/zarr-developers/VirtualiZarr) — Virtual Zarr stores for legacy formats
