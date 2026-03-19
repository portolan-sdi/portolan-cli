# ADR-0033: Git-Style Command Scoping

## Status

Accepted

## Context

Issue #239 originally proposed enforcing that hierarchy-wide commands (`push`, `pull`, `list`, `status`) must be run from the catalog root directory, similar to how some tools require being in a specific directory. However, this creates friction for users who expect git-style behavior where commands "just work" from any subdirectory.

Git provides excellent UX by:
1. Walking up to find `.git/` from any subdirectory
2. Operating on the whole repository regardless of current directory
3. Still allowing scoped operations (e.g., `git add .` stages only current directory)

Portolan already has `find_catalog_root()` (per ADR-0029) that walks up to find `.portolan/config.yaml`. The infrastructure exists; we just needed to wire it into the CLI commands.

## Decision

Implement **git-style command scoping** with two tiers:

### Catalog-Wide Commands
These commands find the catalog root automatically and operate on the entire catalog:
- `push` — Pushes all collections (or use `--collection` for scoped)
- `pull` — Pulls specified collection (currently requires `--collection`)
- `status` — (Merged into `list` per issue #210)

### Directory-Scoped Commands
These commands validate we're inside a catalog but operate on the current directory:
- `scan` — Already operates on PATH argument (default `.`)
- `list` — Operates on current directory scope

### Implementation

1. **Helper functions** in `cli.py`:
   - `require_catalog_root()` — For catalog-wide commands; exits with git-style error if not in catalog
   - `require_inside_catalog()` — For scoped commands; validates catalog exists, returns cwd

2. **Error message** follows git style:
   ```
   fatal: not a portolan catalog (or any parent up to mount point)
   ```

3. **Optional `--catalog` override** retained for testing and automation (like git's `-C` flag)

### State Tracking

No new state tracking is required. Each collection's `versions.json` already tracks its own sync state independently. Scoped or catalog-wide operations simply iterate over the appropriate set of `versions.json` files.

## Consequences

### Positive
- **Better UX** — Commands work from any subdirectory within a catalog
- **Git-familiar** — Users expect this behavior from git-like tools
- **Simple implementation** — Uses existing `find_catalog_root()` infrastructure
- **No state complexity** — Per-collection state in `versions.json` handles everything

### Negative
- **Potential confusion** — Running `push` from a deep subdirectory pushes the entire catalog, not just that subdirectory. This matches git behavior (`git push` pushes commits, not files) but may surprise users.

### Neutral
- **`--catalog` option** still available for explicit control, mainly useful in scripts/tests
- **`--collection` flag** provides explicit scoping when needed

## Alternatives Considered

1. **Require running from root** (original #239 proposal)
   - Rejected: Too much friction, not git-like

2. **Implicit scoping based on cwd**
   - Rejected: Complex to implement, confusing semantics

3. **Always require explicit `--collection`**
   - Rejected: Too verbose for common case

## Related

- ADR-0029: Unified catalog root detection via `.portolan/config.yaml`
- Issue #239: Original issue (direction changed during brainstorming)
- ADR-0022: Git-style implicit tracking
