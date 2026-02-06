# ADR-0011: MVP Validation Framework

## Status
Accepted

## Context

v0.4 introduces `portolan check` to validate local catalogs. However, the full validation requirements won't be clear until we build out `dataset add` (v0.5), remote sync (v0.6), and evolve the Portolan spec.

We face a choice:
1. **Wait**: Don't build validation until requirements are fully known
2. **Speculate**: Build comprehensive validation based on guesses
3. **MVP**: Build minimal validation now, expand as requirements emerge

## Decision

Build an **MVP validation framework** with these principles:

### 1. Start with structural validation only

v0.4 validates:
- `.portolan/` directory exists
- `catalog.json` exists and is valid JSON
- Required STAC fields present (`type`, `stac_version`, `id`)
- Links are syntactically valid (if present)

v0.4 does NOT yet validate:
- Dataset-specific rules (no datasets until v0.5)
- Remote sync state (no remotes until v0.6)
- Schema evolution / breaking changes (deferred)
- Asset checksums (requires versions.json population)

### 2. Design for extensibility

The validation framework should support:
```python
# Pseudo-code for extensible rule system
class ValidationRule:
    """Base class for validation rules."""
    name: str
    severity: Literal["error", "warning", "info"]

    def check(self, catalog: Catalog) -> ValidationResult

# Rules can be added incrementally
RULES = [
    CatalogExistsRule(),      # v0.4
    CatalogJsonValidRule(),   # v0.4
    StacFieldsRule(),         # v0.4
    DatasetMetadataRule(),    # v0.5
    ChecksumIntegrityRule(),  # v0.6
]
```

### 3. Support `--fix` from the start

Even in MVP, `portolan check --fix` should:
- Fix what it can automatically (e.g., add missing optional fields)
- Report what requires manual intervention
- Never silently modify data files (only metadata/catalog files)

### 4. Actionable output

Following ADR-0009, output should be:
- **Specific**: "Missing required field 'id' in catalog.json" not "Invalid catalog"
- **Actionable**: Include fix suggestions or `--fix` flag hint
- **Structured**: Machine-readable option (`--json`) for CI/CD integration

## Consequences

### What becomes easier
- Ship v0.4 without speculating on future requirements
- Add validation rules incrementally as spec evolves
- Test validation logic in isolation (each rule is unit-testable)
- Users get early feedback on catalog structure

### What becomes harder
- Must revisit validation framework as requirements emerge
- Risk of API churn if rule interface changes
- May need to deprecate/replace early rules

### Trade-offs accepted
- We accept incomplete validation for shipping velocity
- We accept potential refactoring as requirements clarify
- We bias toward "too few rules" over "wrong rules"

## Implementation Notes

### File structure
```
portolan_cli/
  validation/
    __init__.py      # Public API: check(), fix()
    rules.py         # ValidationRule base + built-in rules
    results.py       # ValidationResult, ValidationReport
```

### CLI integration
```bash
portolan check              # Validate current directory
portolan check /path        # Validate specific path
portolan check --fix        # Auto-fix what's possible
portolan check --json       # Machine-readable output
portolan check --verbose    # Show all checks, not just failures
```

## References

- ADR-0007: CLI wraps API (validation logic in library, not CLI)
- ADR-0009: Output modes (--dry-run, --verbose, --json)
- ADR-0010: Delegate to upstream (validation of file *contents* still delegated)
