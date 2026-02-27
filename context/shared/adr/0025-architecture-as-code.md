# ADR-0025: Architecture as Code with import-linter

## Status
Accepted

## Context

Portolan CLI had architectural documentation in `context/architecture.md` that described system boundaries, dependencies, and module responsibilities in prose. At high development velocity (5-10k LoC/day with AI assistance), prose documentation inevitably drifts from implementation.

**Problems with prose architecture docs:**
1. No automated validation - drift is silent
2. Violations discovered during code review (if at all)
3. AI agents may generate code that violates boundaries
4. Manual maintenance burden

**Key architectural invariants we want to enforce:**
1. CLI layer should not directly access storage backends
2. Utility modules (output, errors, constants) should be foundational (no internal dependencies)
3. Future: strict layered architecture with clear boundaries

## Decision

Replace prose architecture documentation with **import-linter** contracts that are:
1. **Enforced automatically** via pre-commit hooks and CI
2. **Verified on every commit** - violations block the build
3. **Self-documenting** - contracts express the rules in code

### Contracts implemented

```toml
[tool.importlinter]
root_packages = ["portolan_cli"]

[[tool.importlinter.contracts]]
id = "cli-no-storage"
name = "CLI cannot import storage backends directly"
type = "forbidden"
source_modules = ["portolan_cli.cli"]
forbidden_modules = ["portolan_cli.backends"]

[[tool.importlinter.contracts]]
id = "utilities-are-foundational"
name = "Utilities must not import other portolan modules"
type = "independence"
modules = [
    "portolan_cli.output",
    "portolan_cli.errors",
    "portolan_cli.constants",
    "portolan_cli.json_output",
]
```

### What we discovered

During contract implementation, we found that stricter layered architecture contracts would fail due to existing coupling:
- `scan_*` modules have circular imports (they're one logical unit)
- `backends` imports `versions` and `dataset` (inverse dependency)
- `metadata.update` imports `collection`, `item` (cross-layer)

These are not bugs - they reflect deliberate design choices. However, they mean full layered enforcement requires refactoring work.

### Integration

- **Pre-commit**: `uv run lint-imports` runs on every commit
- **CI**: Added to the Tier 2 PR workflow
- **Documentation**: `context/architecture.md` archived to `context/shared/archive/`

## Consequences

### Positive
- Architecture rules are enforced automatically
- Violations are caught immediately, not in code review
- AI agents cannot generate code that violates boundaries
- Rules are explicit and discoverable in `pyproject.toml`

### Negative
- Some architectural patterns can't be easily expressed as contracts
- Initial setup revealed more coupling than expected
- Stricter contracts require future refactoring work

### Future work
- Refactor `scan_*` modules into a proper subsystem with clear internal structure
- Evaluate whether `backends` should depend on `versions` or vice versa
- Add stricter layered contracts after refactoring

## Alternatives considered

### 1. Keep prose documentation + manual review
**Rejected**: Doesn't scale. At high velocity, reviewers can't catch all violations.

### 2. Custom import analysis scripts
**Rejected**: import-linter is mature, well-documented, and handles edge cases (TYPE_CHECKING blocks, conditional imports, etc.).

### 3. Full layered architecture contracts immediately
**Rejected**: Would require significant refactoring to pass. Better to start with contracts that reflect reality and tighten over time.

### 4. ArchUnit (Java-style architecture tests)
**Rejected**: import-linter is Python-native and integrates cleanly with pre-commit/CI.
