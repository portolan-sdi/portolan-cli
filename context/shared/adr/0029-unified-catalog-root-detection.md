# ADR-0029: Unified Catalog Root Detection

## Status

Accepted

## Context

Portolan CLI has inconsistent catalog root detection across commands, causing confusing behavior where users can add files successfully but then can't see them with `list`.

### Current State (Pre-fix)

Two competing functions detect catalog roots using different criteria:

| Function | Location | Sentinel | Used By |
|----------|----------|----------|---------|
| `find_catalog_root()` | `dataset.py` | `catalog.json` | `status`, `add`, `rm` |
| `_find_catalog_root()` | `cli.py` | `.portolan/` directory | `config get/set/list/unset` |

Additionally, `check` only looks at the current directory without walking up.

### Problems

1. **Shadow catalogs:** `add` auto-creates structure, but `list` looks for parent catalog, so added items are invisible
2. **Inconsistent behavior:** `status` shows sibling directories from parent catalog when user is in subdirectory
3. **Trust erosion:** Commands report contradictory states

### Related Issues

- GitHub Issue #162: Inconsistent catalog root detection across CLI commands
- GitHub Issue #137: Multiple related bugs that may be symptoms of this root cause

## Decision

**Use `.portolan/config.yaml` as the single sentinel for catalog root detection.**

All commands will use a unified `find_catalog_root()` function in `catalog.py` that:

1. Walks up from the current directory (or specified path)
2. Looks for `.portolan/config.yaml` (not `catalog.json`, not bare `.portolan/`)
3. Returns the catalog root path, or `None` if not found
4. Enforces a depth limit (`MAX_CATALOG_SEARCH_DEPTH = 20`) for security

This aligns with ADR-0027, which established `config.yaml` as the unified sentinel file for managed catalogs.

### Behavior Changes

| Command | Old Behavior | New Behavior |
|---------|--------------|--------------|
| `add` | Auto-creates catalog structure if missing | Fails with "run `portolan init`" message |
| `list` | Uses explicit `--catalog` argument (default `.`) | *Unchanged* - uses `--catalog` arg, not `find_catalog_root()` |
| `status` | Finds parent catalog via `catalog.json` | Finds parent catalog via `.portolan/config.yaml` |
| `rm` | Finds parent catalog via `catalog.json` | Finds parent catalog via `.portolan/config.yaml` |
| `config *` | Finds catalog via `.portolan/` directory | Finds catalog via `.portolan/config.yaml` |

> **Note:** `list` is excluded from the unified `find_catalog_root()` treatment because it takes `--catalog` as an explicit argument. This is a separate issue to address in a future PR.

### UNMANAGED_STAC Impact

Directories with only `catalog.json` (no `.portolan/`) will no longer be detected as catalogs. Users must run `portolan init` or `portolan adopt` to manage existing STAC catalogs.

This is acceptable because:
- UNMANAGED_STAC support was always incomplete (some commands worked, others didn't)
- Clear error messages guide users to the fix
- Pre-v1.0, so breaking changes are acceptable

## Implementation

### Single Function Location

The unified `find_catalog_root()` lives in `portolan_cli/catalog.py` alongside `detect_state()`, since both deal with catalog detection logic.

```python
def find_catalog_root(start_path: Path | None = None) -> Path | None:
    """Find catalog root by walking up to find .portolan/config.yaml.

    Per ADR-0029, uses .portolan/config.yaml as the single sentinel,
    unifying detection across all CLI commands.

    Security: Limited to MAX_CATALOG_SEARCH_DEPTH (20) levels.
    """
```

### Deleted Code

- `find_catalog_root()` from `dataset.py` (looked for `catalog.json`)
- `_find_catalog_root()` from `cli.py` (looked for `.portolan/` directory)

## Consequences

### Benefits

- **Consistent behavior:** All commands agree on catalog root
- **No shadow catalogs:** `add` requires `init` first, eliminating invisible items
- **Simpler mental model:** One sentinel, one function, one behavior
- **Aligns with ADR-0027:** Uses the established `config.yaml` sentinel

### Trade-offs

- **UNMANAGED_STAC no longer auto-detected:** Users must explicitly init/adopt
- **`add` no longer auto-creates:** Requires explicit `init` first (git-style)

### Migration

None required. Pre-v1.0 with minimal users. Existing managed catalogs already have `.portolan/config.yaml`.

## Alternatives Considered

### Option A: Use `catalog.json` as sentinel everywhere

**Rejected:** Would still allow "unmanaged" catalogs that only partially work. The root cause is supporting two modes (managed/unmanaged) inconsistently.

### Option B: Current-directory-only (no walking up)

**Rejected:** Breaks git-style UX that users expect. Commands should work from subdirectories.

### Option C: Distinguish managed/unmanaged with two-tier behavior

**Rejected:** More complex, doesn't eliminate the inconsistency problem, and UNMANAGED_STAC was never fully supported anyway.

## References

- [ADR-0027: Unified config.yaml as Sentinel](0027-unified-config-yaml-sentinel.md)
- [ADR-0023: STAC Structure Separation](0023-stac-structure-separation.md)
- [GitHub Issue #162](https://github.com/portolan-sdi/portolan-cli/issues/162)
