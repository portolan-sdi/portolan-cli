# ADR-0017: MTIME + Heuristics for Change Detection

## Status
Accepted

## Context

Portolan needs to detect when local files have changed so metadata can be regenerated. Options considered:

1. **MTIME only** — Fast (µs) but unreliable (touch, git checkout, S3 sync all affect mtime)
2. **Content hash (SHA256)** — Reliable but slow (100GB file = 200s at 500MB/s)
3. **MTIME + heuristics** — Fast gate, then O(1) metadata check
4. **Git-style object store** — Overkill for MVP

## Decision

Use **MTIME + heuristic fallback**:

```
stat() → mtime changed?
    NO  → skip (fast path)
    YES → extract metadata → heuristics changed?
              NO  → warn "file touched, metadata unchanged"
              YES → flag for update
```

Heuristics checked (all O(1) reads from file headers):
- bbox
- feature_count (vector) / dimensions (raster)
- schema fingerprint

## Consequences

### Benefits
- **Fast**: 1µs for unchanged files, 5-50ms for changed files
- **Scales**: Works on 100GB+ files without reading content
- **Practical**: Catches real data changes in typical workflows

### Trade-offs
- **False negatives**: Attribute-only edits (same bbox/count) are missed
- **Escape hatch**: `--force-rehash` flag for full SHA256 verification when needed
- **S3 uncertainty**: ETags aren't always content hashes (multipart uploads differ)

## Alternatives Considered

**MTIME only**: Too unreliable for S3 sync scenarios.

**Pure hash**: Too slow for interactive use (hours for large catalogs).

**Git-style object store**: Major architectural change; deferred to portolake plugin.
