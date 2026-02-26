# ADR-0016: Scan-Before-Import Architecture

**Status:** Accepted
**Date:** 2025-02-11
**Related:** [#12](https://github.com/portolan-sdi/portolan-cli/issues/12), [#17](https://github.com/portolan-sdi/portolan-cli/issues/17)

## Context

Users need to import directories of geospatial files into Portolan catalogs. These directories range from well-organized hierarchies to messy accumulations of data. The `dataset add` command needs to handle both cases gracefully.

Key challenges:
1. Directory structures vary wildly (flat, hierarchical, mixed)
2. Files may have naming issues (special characters, duplicates)
3. Multiple "primary" assets in one directory create ambiguity
4. Users shouldn't have to reorganize all their data before importing

## Decision

**Separate scanning/validation from import, following the ruff model:**

```bash
portolan scan /data/           # Analyze and report (like ruff check)
portolan scan /data/ --fix     # Safe fixes (like ruff check --fix)
portolan dataset add /data/    # Import (assumes reasonably clean input)
```

The `dataset add` command will internally call `scan` and warn about issues, but the logic lives in `scan`.

## Rationale

### Why Not One Command?

**Option A: Smart `dataset add` that handles everything**
- Pros: Single command, "just works"
- Cons: Complex, hard to test, unclear what it will do

**Option B: Separate `scan` and `add` (chosen)**
- Pros: Clear separation, testable, familiar pattern (ruff, eslint)
- Cons: Two commands to learn

We chose Option B because:
1. **Predictability** — Users know what each command does
2. **Safety** — `scan` is read-only; `--fix` is opt-in
3. **Testability** — Scan logic is isolated and testable
4. **Familiarity** — Follows established CLI patterns

### Why `--fix` and `--unsafe-fix`?

Following ruff's model:
- `--fix` — Safe operations (rename files with invalid characters)
- `--unsafe-fix` — Destructive operations (move files, split directories)

This gives users control over what modifications are acceptable.

### Why Call `scan` from `dataset add`?

Convenience. Users who just want to import can run one command and get warnings. Power users can run `scan` separately for more control.

## Consequences

### Positive

- Clear mental model for users
- `scan` can be used standalone for validation
- Easy to add new checks without touching import logic
- `--fix` provides path from messy to clean

### Negative

- Two commands instead of one (mitigated by `add` calling `scan` internally)
- Need to maintain consistency between `scan` output and `add` behavior

### Neutral

- Pattern is well-established in other tools (ruff, eslint, prettier)

## Implementation Notes

- `scan` should use `os.walk()` for performance (not `pathlib.rglob()`)
- Output should be human-readable by default, `--json` for scripting
- Progress indicator needed for large directories
- `scan` results should be cacheable (future optimization)
