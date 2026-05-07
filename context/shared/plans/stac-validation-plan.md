# STAC Validation Implementation Plan

**Issue:** #397 — feat(check): STAC schema validation rule
**Status:** Ready for Implementation
**Author:** Claude + Nissim
**Date:** 2026-05-07
**Last Updated:** 2026-05-07 (API corrections after research pass)

## Summary

Add STAC schema validation and best-practices linting to `portolan check` using stac-check as the validation engine. Two new rules (`StacSchemaRule`, `StacLintRule`) run by default, with configurable severity and a `--strict` flag for full geometry validation.

## Decisions Made

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Engine | stac-check | Provides schema validation + 20+ lint checks; uses stac-validator under the hood |
| Rule count | Two: `StacSchemaRule` + `StacLintRule` | Schema = objective (spec), Lint = opinionated (best practices) |
| Default behavior | Both in `DEFAULT_RULES` | Always validate STAC conformance |
| Default mode | `fast=True` | Schema + best practices, skip geometry (~0.5ms/item) |
| `--strict` flag | `fast=False` | Full validation including geometry (~2ms/item) |
| Severity | Configurable per lint check | Via `.portolan/config.yaml` |
| Config | Wrap in portolan config | Not raw stac-check YAML |
| `--strict` plumbing | Runner injection with builder | Backward compatible, explicit, extensible |

## API Research Findings (2026-05-07)

### stac-check Linter API

**Constructor:**
```python
@dataclass
class Linter:
    item: Union[str, Dict]      # Path to STAC file or dict
    config_file: Optional[str] = None
    assets: bool = False
    links: bool = False
    recursive: bool = False     # Traverse catalog → collection → item
    max_depth: Optional[int] = None
    assets_open_urls: bool = True
    headers: Dict = field(default_factory=dict)
    pydantic: bool = False
    verbose: bool = False
    fast: bool = False          # Skip geometry validation
    fast_linting: bool = False  # Enable BP checks even when fast=True
```

**Key behaviors:**
- **No `run()` method** — validation executes automatically in `__post_init__()`
- **`best_practices_dict` is NOT an attribute** — must call `create_best_practices_dict()` explicitly

**Attributes after instantiation:**
- `valid_stac: bool` — overall validation status
- `error_msg: str` — error message if failed
- `recommendation: str` — fix suggestion
- `best_practices_msg: list[str]` — formatted messages (NOT dict)
- `validate_all: dict` — recursive validation results

**`best_practices_dict` keys** (from `create_best_practices_dict()`):
- `searchable_identifiers` — ID contains non-searchable characters
- `percent_encoded` — ID contains `:` or `/`
- `check_item_id` — filename doesn't match ID
- `check_catalog_id` — catalog filename isn't catalog.json
- `check_summaries` — collection missing summaries
- `datetime_null` — datetime field is null
- `check_unlocated` — bbox but no geometry
- `null_geometry` — missing geometry
- `bbox_geometry_mismatch` — bbox doesn't match geometry
- `bloated_links` — exceeds max links
- `bloated_metadata` — exceeds max properties
- `check_thumbnail` — invalid thumbnail format
- `check_links_title` — links missing title
- `check_links_self` — missing self link
- `geometry_coordinates_order` — likely reversed coordinates
- `geometry_coordinates_definite_errors` — invalid coordinate values
- `check_bbox_antimeridian` — antimeridian bbox formatting

## Dependencies

Add to `pyproject.toml`:

```toml
[project]
dependencies = [
    # ... existing ...
    "stac-check>=1.4.0",  # STAC schema validation + best practices linting
]
```

stac-check pulls in stac-validator transitively.

## Implementation

### 1. New File: `portolan_cli/validation/stac_rules.py`

```python
"""STAC schema validation and best-practices linting rules.

Uses stac-check as the validation engine. Two rules:
- StacSchemaRule: JSON Schema validation (ERROR severity)
- StacLintRule: Best practices checks (configurable severity)
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from stac_check.lint import Linter

from portolan_cli.validation.results import Severity, ValidationResult
from portolan_cli.validation.rules import ValidationRule

if TYPE_CHECKING:
    pass


class StacSchemaRule(ValidationRule):
    """Validate STAC objects against JSON Schema spec.

    Uses stac-check's schema validation (via stac-validator).
    Validates catalog.json and follows STAC link relations to
    validate collections and items.
    """

    name = "stac_schema"
    severity = Severity.ERROR
    description = "Validate STAC JSON against official schemas"

    def __init__(self, *, strict: bool = False) -> None:
        """Initialize rule.

        Args:
            strict: If True, enable full geometry validation (fast=False).
                    If False, skip geometry checks (fast=True).
        """
        self.strict = strict

    def check(self, catalog_path: Path) -> ValidationResult:
        catalog_json = catalog_path / "catalog.json"
        if not catalog_json.exists():
            return self._pass("No catalog.json found")

        try:
            # Validation runs automatically in __post_init__
            linter = Linter(
                item=str(catalog_json),
                recursive=True,
                fast=not self.strict,
            )
        except Exception as e:
            return self._fail(
                f"STAC validation failed: {e}",
                fix_hint="Check that all STAC files have valid JSON syntax",
            )

        if linter.valid_stac:
            return self._pass("All STAC objects pass schema validation")

        return self._fail(
            linter.error_msg or "STAC schema validation failed",
            fix_hint=linter.recommendation if hasattr(linter, 'recommendation') else None,
        )


class StacLintRule(ValidationRule):
    """Check STAC objects against best practices.

    Uses stac-check's best practices checks. Each check can have
    configurable severity via .portolan/config.yaml.
    """

    name = "stac_lint"
    severity = Severity.WARNING
    description = "Check STAC against best practices"

    # Checks to skip (handled by other portolan rules)
    SKIP_CHECKS: frozenset[str] = frozenset({
        "datetime_null",  # ProvisionalDatetimeRule handles this
    })

    # Default severity for each check (can be overridden in config)
    DEFAULT_SEVERITIES: dict[str, Severity] = {
        "searchable_identifiers": Severity.ERROR,
        "percent_encoded": Severity.ERROR,
        "check_catalog_id": Severity.WARNING,
        "check_item_id": Severity.WARNING,
        "check_thumbnail": Severity.WARNING,
        "check_links_title": Severity.INFO,
        "check_links_self": Severity.WARNING,
        "null_geometry": Severity.WARNING,
        "check_summaries": Severity.WARNING,
        "bloated_metadata": Severity.INFO,
        "bloated_links": Severity.INFO,
    }

    def __init__(
        self,
        *,
        strict: bool = False,
        config: dict[str, Any] | None = None,
    ) -> None:
        self.strict = strict
        self.config = config or {}

    def check(self, catalog_path: Path) -> ValidationResult:
        catalog_json = catalog_path / "catalog.json"
        if not catalog_json.exists():
            return self._pass("No catalog.json found")

        try:
            linter = Linter(
                item=str(catalog_json),
                recursive=True,
                fast=not self.strict,
                fast_linting=True,  # Always run BP checks
            )
        except Exception as e:
            return self._fail(f"STAC lint failed: {e}")

        # Get best practices dict (not an attribute, must call method)
        bp_dict = linter.create_best_practices_dict()

        # Collect violations by severity
        errors: list[str] = []
        warnings: list[str] = []
        infos: list[str] = []

        severity_map = self._get_severity_map()

        for check_name, messages in bp_dict.items():
            if check_name in self.SKIP_CHECKS:
                continue
            if not messages:
                continue

            severity = severity_map.get(check_name, Severity.WARNING)
            message = messages[0] if isinstance(messages, list) else str(messages)

            if severity == Severity.ERROR:
                errors.append(f"{check_name}: {message}")
            elif severity == Severity.WARNING:
                warnings.append(f"{check_name}: {message}")
            else:
                infos.append(f"{check_name}: {message}")

        if not errors and not warnings:
            return self._pass("All best practice checks passed")

        # Build summary message
        parts = []
        if errors:
            parts.append(f"{len(errors)} error(s)")
        if warnings:
            parts.append(f"{len(warnings)} warning(s)")

        all_issues = errors + warnings
        detail = "; ".join(all_issues[:3])
        if len(all_issues) > 3:
            detail += f" (+{len(all_issues) - 3} more)"

        return ValidationResult(
            rule_name=self.name,
            passed=len(errors) == 0,
            severity=Severity.ERROR if errors else Severity.WARNING,
            message=f"Best practice issues: {', '.join(parts)}. {detail}",
            fix_hint="Run with --verbose to see all issues",
        )

    def _get_severity_map(self) -> dict[str, Severity]:
        """Get severity map, merging defaults with config overrides."""
        result = dict(self.DEFAULT_SEVERITIES)

        overrides = self.config.get("stac_lint", {}).get("severity", {})
        for check_name, level in overrides.items():
            if isinstance(level, str):
                level_lower = level.lower()
                if level_lower == "skip":
                    self.SKIP_CHECKS = self.SKIP_CHECKS | {check_name}
                else:
                    try:
                        result[check_name] = Severity(level_lower)
                    except ValueError:
                        pass  # Invalid severity, keep default

        return result
```

### 2. Update `portolan_cli/validation/runner.py`

```python
"""Validation runner that executes all rules against a catalog."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from portolan_cli.validation.results import ValidationReport, ValidationResult
from portolan_cli.validation.rules import (
    CatalogExistsRule,
    CatalogJsonValidRule,
    MetadataFreshRule,
    PartitionSchemaConsistencyRule,
    PartitionStructureRule,
    PMTilesRecommendedRule,
    ProvisionalDatetimeRule,
    StacFieldsRule,
    ValidationRule,
)
from portolan_cli.validation.stac_rules import StacLintRule, StacSchemaRule

# Default rules for simple cases (no options)
# Immutable tuple to prevent accidental mutation
DEFAULT_RULES: tuple[ValidationRule, ...] = (
    CatalogExistsRule(),
    CatalogJsonValidRule(),
    StacFieldsRule(),
    StacSchemaRule(),
    StacLintRule(),
    PMTilesRecommendedRule(),
    MetadataFreshRule(),
    ProvisionalDatetimeRule(),
    PartitionStructureRule(),
    PartitionSchemaConsistencyRule(),
)


def _build_rules(
    *,
    strict: bool = False,
    config: dict[str, Any] | None = None,
) -> tuple[ValidationRule, ...]:
    """Build rule tuple with configuration options.

    Args:
        strict: Enable strict STAC validation (geometry checks).
        config: Portolan config dict for severity overrides.

    Returns:
        Tuple of configured validation rules.
    """
    return (
        CatalogExistsRule(),
        CatalogJsonValidRule(),
        StacFieldsRule(),
        StacSchemaRule(strict=strict),
        StacLintRule(strict=strict, config=config),
        PMTilesRecommendedRule(),
        MetadataFreshRule(),
        ProvisionalDatetimeRule(),
        PartitionStructureRule(),
        PartitionSchemaConsistencyRule(),
    )


def check(
    catalog_path: Path,
    *,
    rules: Sequence[ValidationRule] | None = None,
    strict: bool = False,
    config: dict[str, Any] | None = None,
) -> ValidationReport:
    """Run validation rules against a catalog.

    Args:
        catalog_path: Path to the directory containing .portolan.
        rules: Optional sequence of rules to run. If provided, strict/config ignored.
        strict: Enable strict STAC validation (geometry checks).
        config: Portolan config dict for severity overrides.

    Returns:
        ValidationReport with results from all rules.
    """
    if rules is None:
        if strict or config:
            rules = _build_rules(strict=strict, config=config)
        else:
            rules = DEFAULT_RULES

    results: list[ValidationResult] = []

    for rule in rules:
        result = rule.check(catalog_path)
        results.append(result)

    return ValidationReport(results=results)
```

### 3. Update CLI: Add `--strict` flag

In `portolan_cli/cli.py`, update the `check` command decorator and function:

```python
@cli.command("check")
@click.argument("path", type=click.Path(exists=False, path_type=Path), default=".")
@click.option("--json", "json_output", is_flag=True, help="Output as JSON")
@click.option("-v", "--verbose", is_flag=True, help="Show detailed output")
@click.option("--fix", is_flag=True, help="Apply automatic fixes")
@click.option("--dry-run", is_flag=True, help="Preview fixes without applying")
@click.option("--remove-legacy", is_flag=True, help="Remove legacy files during fix")
@click.option("--metadata", is_flag=True, help="Only check/fix metadata")
@click.option("--geo-assets", is_flag=True, help="Only check/fix geo-assets")
@click.option(
    "--strict",
    is_flag=True,
    help="Enable strict STAC validation (includes geometry checks)",
)
@click.pass_context
def check(
    ctx: click.Context,
    path: Path,
    json_output: bool,
    verbose: bool,
    fix: bool,
    dry_run: bool,
    remove_legacy: bool,
    metadata: bool,
    geo_assets: bool,
    strict: bool,
) -> None:
    ...
```

Pass `strict` through `_execute_check_workflow` to the validation runner.

### 4. Config Schema

Add to `.portolan/config.yaml` schema:

```yaml
stac_lint:
  # Override severity for specific checks
  # Valid values: error, warning, info, skip
  severity:
    searchable_identifiers: error
    check_thumbnail: warning
    bloated_metadata: skip  # disable this check
    check_links_title: info
```

### 5. Tests

#### Unit Tests: `tests/unit/validation/test_stac_rules.py`

```python
"""Tests for STAC schema and lint validation rules."""

import json
import pytest
from pathlib import Path

from portolan_cli.validation.stac_rules import StacSchemaRule, StacLintRule
from portolan_cli.validation.results import Severity


@pytest.fixture
def valid_catalog(tmp_path: Path) -> Path:
    """Create a minimal valid STAC catalog."""
    catalog = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "test-catalog",
        "description": "Test catalog",
        "links": [],
    }
    (tmp_path / "catalog.json").write_text(json.dumps(catalog))
    (tmp_path / ".portolan").mkdir()
    return tmp_path


@pytest.fixture
def catalog_missing_id(tmp_path: Path) -> Path:
    """Create catalog missing required 'id' field."""
    catalog = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "description": "Test catalog",
        "links": [],
    }
    (tmp_path / "catalog.json").write_text(json.dumps(catalog))
    (tmp_path / ".portolan").mkdir()
    return tmp_path


@pytest.fixture
def catalog_uppercase_id(tmp_path: Path) -> Path:
    """Create catalog with uppercase ID (fails searchable check)."""
    catalog = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "TEST-CATALOG",  # Uppercase
        "description": "Test catalog",
        "links": [],
    }
    (tmp_path / "catalog.json").write_text(json.dumps(catalog))
    (tmp_path / ".portolan").mkdir()
    return tmp_path


@pytest.fixture
def empty_dir(tmp_path: Path) -> Path:
    """Create empty directory (no catalog.json)."""
    return tmp_path


class TestStacSchemaRule:
    """Tests for StacSchemaRule."""

    def test_valid_catalog_passes(self, valid_catalog: Path) -> None:
        """Valid STAC catalog passes schema validation."""
        rule = StacSchemaRule()
        result = rule.check(valid_catalog)
        assert result.passed
        assert result.severity == Severity.ERROR

    def test_missing_required_field_fails(self, catalog_missing_id: Path) -> None:
        """Catalog missing required 'id' field fails."""
        rule = StacSchemaRule()
        result = rule.check(catalog_missing_id)
        assert not result.passed

    def test_no_catalog_passes(self, empty_dir: Path) -> None:
        """Missing catalog.json is a pass (nothing to validate)."""
        rule = StacSchemaRule()
        result = rule.check(empty_dir)
        assert result.passed

    def test_strict_mode_flag(self, valid_catalog: Path) -> None:
        """Strict mode can be enabled."""
        rule = StacSchemaRule(strict=True)
        assert rule.strict is True
        result = rule.check(valid_catalog)
        assert result.passed


class TestStacLintRule:
    """Tests for StacLintRule."""

    def test_valid_catalog_passes(self, valid_catalog: Path) -> None:
        """Valid catalog passes lint checks."""
        rule = StacLintRule()
        result = rule.check(valid_catalog)
        # May have warnings but no errors
        assert result.severity != Severity.ERROR or result.passed

    def test_severity_configurable(self, valid_catalog: Path) -> None:
        """Severity can be overridden via config."""
        config = {
            "stac_lint": {
                "severity": {
                    "check_thumbnail": "error",
                }
            }
        }
        rule = StacLintRule(config=config)
        severity_map = rule._get_severity_map()
        assert severity_map["check_thumbnail"] == Severity.ERROR

    def test_check_can_be_skipped(self, valid_catalog: Path) -> None:
        """Checks can be skipped via config."""
        config = {
            "stac_lint": {
                "severity": {
                    "bloated_metadata": "skip",
                }
            }
        }
        rule = StacLintRule(config=config)
        rule._get_severity_map()  # Triggers skip processing
        assert "bloated_metadata" in rule.SKIP_CHECKS

    def test_no_catalog_passes(self, empty_dir: Path) -> None:
        """Missing catalog.json is a pass."""
        rule = StacLintRule()
        result = rule.check(empty_dir)
        assert result.passed
```

#### Integration Test: `tests/integration/test_check_stac.py`

```python
"""Integration tests for portolan check with STAC validation."""

import json
import subprocess
from pathlib import Path

import pytest


@pytest.fixture
def valid_catalog(tmp_path: Path) -> Path:
    """Create a minimal valid STAC catalog."""
    catalog = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "test-catalog",
        "description": "Test catalog",
        "links": [],
    }
    (tmp_path / "catalog.json").write_text(json.dumps(catalog))
    (tmp_path / ".portolan").mkdir()
    (tmp_path / ".portolan" / "config.yaml").write_text("")
    return tmp_path


def test_check_stac_valid_catalog(valid_catalog: Path) -> None:
    """portolan check passes on valid STAC catalog."""
    result = subprocess.run(
        ["portolan", "check", "--json", str(valid_catalog)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    data = json.loads(result.stdout)
    assert data["success"]


def test_check_strict_flag_accepted(valid_catalog: Path) -> None:
    """--strict flag is accepted."""
    result = subprocess.run(
        ["portolan", "check", "--strict", "--json", str(valid_catalog)],
        capture_output=True,
        text=True,
    )
    # Should complete (pass or fail based on validation)
    assert result.returncode in (0, 1)


def test_check_stac_rules_in_output(valid_catalog: Path) -> None:
    """STAC rules appear in check output."""
    result = subprocess.run(
        ["portolan", "check", "--json", str(valid_catalog)],
        capture_output=True,
        text=True,
    )
    data = json.loads(result.stdout)
    rule_names = [r["rule_name"] for r in data["data"]["results"]]
    assert "stac_schema" in rule_names
    assert "stac_lint" in rule_names
```

### 6. Fixtures Needed

Add to `tests/fixtures/stac/`:

```
tests/fixtures/stac/
├── README.md                     # Document each fixture's purpose
├── valid/                        # Valid STAC following best practices
│   ├── catalog.json
│   └── test-collection/
│       └── collection.json
├── missing-id/                   # catalog.json missing 'id' field
│   └── catalog.json
├── uppercase-id/                 # ID with uppercase (fails searchable check)
│   └── catalog.json
└── invalid-json/                 # Malformed JSON
    └── catalog.json
```

## File Changes Summary

| File | Change |
|------|--------|
| `pyproject.toml` | Add `stac-check>=1.4.0` dependency |
| `portolan_cli/validation/stac_rules.py` | **NEW** — StacSchemaRule, StacLintRule |
| `portolan_cli/validation/runner.py` | Add `_build_rules()`, update `check()` signature |
| `portolan_cli/cli.py` | Add `--strict` flag to check command |
| `tests/unit/validation/test_stac_rules.py` | **NEW** — Unit tests |
| `tests/integration/test_check_stac.py` | **NEW** — Integration tests |
| `tests/fixtures/stac/*` | **NEW** — Test fixtures |

## Open Questions Resolved

| Question | Resolution |
|----------|------------|
| STAC version pinning | Use version in each object's `stac_version` field (stac-check default) |
| Extension handling | Schemas hosted on GitHub Pages; stac-check fetches automatically |
| Schema fetching | HTTP fetch acceptable (stac-check default); can add offline cache later |
| Severity model | Per-check configurable; defaults in code, overrides in config.yaml |
| Error UX | Summary message with first 3 issues; `--verbose` for full list |
| Auto-fix scope | Read-only for now; no auto-fix (schema violations require manual fix) |
| `--strict` plumbing | Runner injection with `_build_rules()` builder function |

## Sequence

1. Add stac-check dependency to pyproject.toml
2. Write test fixtures (TDD)
3. Write unit tests for StacSchemaRule (TDD: tests first)
4. Implement StacSchemaRule
5. Write unit tests for StacLintRule (TDD: tests first)
6. Implement StacLintRule
7. Update runner.py with `_build_rules()` and new signature
8. Add `--strict` flag to CLI
9. Write integration tests
10. Update CLAUDE.md with new rule names
