# ADR-0027: Unified config.yaml as Sentinel and User Config

## Status
Accepted

## Context

Portolan has two configuration files in `.portolan/` with confusingly similar names:

- **`config.json`** - Empty sentinel file (`{}`) used only for MANAGED state detection
- **`config.yaml`** - User configuration (remote URLs, AWS profile, conversion settings per ADR-0024)

The `detect_state()` function checks for the existence of both `config.json` AND `state.json` to determine MANAGED state. It never reads `config.json` contents—just uses filesystem presence as a signal.

### Problems

1. **Naming confusion**: Both files are called "config" but serve entirely different purposes
2. **Undocumented pattern**: The sentinel pattern isn't documented in any ADR
3. **Redundant file**: `config.json` contains only `{}` and will never be written to
4. **Developer confusion**: New contributors must learn this non-obvious convention

### Forces

- ADR-0024 established `config.yaml` for user settings
- PR #128 expands `config.yaml` with conversion overrides, making it the clear "real" config file
- We're pre-v1.0 with minimal users, so breaking changes are acceptable

## Decision

**Eliminate `config.json`. Use `config.yaml` as both the sentinel file and user configuration.**

### New Behavior

| State | Condition |
|-------|-----------|
| MANAGED | `.portolan/config.yaml` AND `.portolan/state.json` both exist |
| UNMANAGED_STAC | `catalog.json` exists at root, but not MANAGED |
| FRESH | Everything else |

### Implementation

1. `detect_state()` checks for `config.yaml` instead of `config.json`
2. `init_catalog()` creates an empty `config.yaml` (with optional comment header)
3. All documentation updated to reference `config.yaml` only

### Migration

None required. Pre-v1.0 with minimal users. Existing catalogs with old-style `config.json` will be detected as FRESH and can re-init.

## Consequences

### Benefits

- **Clearer mental model**: One config file, two purposes (sentinel + settings)
- **Reduced confusion**: No more "why are there two config files?"
- **Simpler `.portolan/`**: One fewer file to explain

### Trade-offs

- **Existing catalogs break**: Old catalogs with `config.json` will no longer be recognized as MANAGED
  - Mitigation: Pre-v1.0, acceptable. Users can re-init.
- **Empty YAML looks different**: An empty `config.yaml` still needs to exist
  - Mitigation: Use a comment header like `# Portolan configuration` to indicate it's intentional

## Alternatives Considered

### Option A: Rename config.json to managed.json or sentinel.json
**Rejected**: Still requires two files. The sentinel pattern itself is the problem—it's an empty file that exists only to be checked for existence.

### Option B: Document current behavior in ADR-0024
**Rejected**: Documentation doesn't solve the underlying design issue. New contributors would still be confused by the pattern.

## References

- [ADR-0024: Hierarchical Config System](0024-hierarchical-config-system.md)
- [ADR-0023: STAC Structure Separation](0023-stac-structure-separation.md)
- [GitHub Issue #107](https://github.com/portolan-sdi/portolan-cli/issues/107)
