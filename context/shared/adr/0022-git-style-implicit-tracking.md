# ADR-0022: Git-Style Implicit Tracking

## Status
Accepted

## Context

Current `dataset add/remove` commands add friction. Users expect filesystem operations to be sufficient—copy file → tracked, delete file → untracked.

## Decision

**Portolan uses implicit tracking based on filesystem structure:**

1. **Top-level subdirectory = collection** (`demographics/` → collection ID "demographics")
2. **Files in collection dirs are tracked** (nested organization allowed)
3. **Delete file = untrack** (no explicit remove command)
4. **`check --fix` generates STAC metadata** (collection.json, item.json)

Example:
```
catalog/
├── catalog.json
├── demographics/           # Collection
│   ├── collection.json     # Generated
│   └── 2020/census.parquet # User's data
└── .portolan/
    └── collections/demographics/versions.json  # Tracking
```

## Consequences

- `dataset add/remove` deprecated (copy/delete files instead)
- `dataset list/info` kept (read from filesystem)
- Future: `.portolanignore` for excluding files

## Alternatives Considered

**Explicit registration**: Rejected—unnecessary friction, doesn't match user expectations.

**Glob patterns (Cargo-style)**: Not needed—top-level subdir rule is sufficient for Portolan's scope.
