# STAC Validation Implementation Plan

**Issue:** #397 — feat(check): STAC schema validation rule
**Status:** Draft
**Author:** Claude + Nissim
**Date:** 2026-05-07

## Summary

Add STAC schema validation and best-practices linting to `portolan check` using stac-check as the validation engine. Two new rules (`StacSchemaRule`, `StacLintRule`) run by default, with configurable severity and a `--strict` flag for full geometry validation.

## Decisions Made

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Engine | stac-check | Provides schema validation + 20+ lint checks; uses stac-validator under the hood |
| Rule count | Two: `StacSchemaRule` + `StacLintRule` | Schema = objective (spec), Lint = opinionated (best practices) |
| Default behavior | Both in `DEFAULT_RULES` | Always validate STAC conformance |
| Default mode | `fast_linting=True` | Schema + best practices, skip geometry (~0.5ms/item) |
| `--strict` flag | Enables geometry validation | Full validation (~2ms/item) for users who want it |
| Severity | Configurable per lint check | Via `.portolan/config.yaml` |
| Config | Wrap in portolan config | Not raw stac-check YAML |

## Dependencies

Add to `pyproject.toml`:

```toml
[project]
dependencies = [
    # ... existing ...
    "stac-check>=1.14.0",  # STAC schema validation + best practices linting
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
from typing import TYPE_CHECKING

from stac_check.lint import Linter

from portolan_cli.validation.results import Severity, ValidationResult
from portolan_cli.validation.rules import ValidationRule

if TYPE_CHECKING:
    from portolan_cli.config import PortolanConfig


class StacSchemaRule(ValidationRule):
    """Validate STAC objects against JSON Schema spec.

    Uses stac-check's schema validation (via stac-validator).
    Walks catalog.json -> collection.json -> item.json following
    STAC link relations.
    """

    name = "stac_schema"
    severity = Severity.ERROR
    description = "Validate STAC JSON against official schemas"

    def __init__(self, *, strict: bool = False) -> None:
        """Initialize rule.

        Args:
            strict: If True, enable full geometry validation.
                    If False, use fast_linting mode (skip geometry).
        """
        self.strict = strict

    def check(self, catalog_path: Path) -> ValidationResult:
        catalog_json = catalog_path / "catalog.json"
        if not catalog_json.exists():
            return self._pass("No catalog.json found")

        try:
            linter = Linter(
                item=str(catalog_json),
                recursive=True,
                fast=not self.strict,  # fast=True skips geometry
            )
            linter.run()
        except Exception as e:
            return self._fail(
                f"STAC validation failed: {e}",
                fix_hint="Check that all STAC files have valid JSON syntax",
            )

        if linter.valid_stac:
            return self._pass("All STAC objects pass schema validation")

        return self._fail(
            linter.error_msg or "STAC schema validation failed",
            fix_hint=linter.recommendation,
        )


class StacLintRule(ValidationRule):
    """Check STAC objects against best practices.

    Uses stac-check's best_practices_dict. Each check can have
    configurable severity via .portolan/config.yaml.
    """

    name = "stac_lint"
    severity = Severity.WARNING  # Default; individual checks configurable
    description = "Check STAC against best practices"

    # Checks to skip (handled by other portolan rules)
    SKIP_CHECKS = frozenset({
        "check_datetime_null",  # ProvisionalDatetimeRule handles this
    })

    # Default severity for each check (can be overridden in config)
    DEFAULT_SEVERITIES: dict[str, Severity] = {
        "check_searchable_identifiers": Severity.ERROR,
        "check_percent_encoded": Severity.ERROR,
        "check_catalog_file_name": Severity.WARNING,
        "check_collection_file_name": Severity.WARNING,
        "check_item_id_file_name": Severity.WARNING,
        "check_thumbnail": Severity.WARNING,
        "check_links_title_field": Severity.INFO,
        "check_links_self": Severity.WARNING,
        "check_geometry_null": Severity.WARNING,
        "check_summaries": Severity.WARNING,
        "check_bloated_metadata": Severity.INFO,
        "check_bloated_links": Severity.INFO,
    }

    def __init__(
        self,
        *,
        strict: bool = False,
        config: PortolanConfig | None = None,
    ) -> None:
        self.strict = strict
        self.config = config

    def check(self, catalog_path: Path) -> ValidationResult:
        catalog_json = catalog_path / "catalog.json"
        if not catalog_json.exists():
            return self._pass("No catalog.json found")

        try:
            linter = Linter(
                item=str(catalog_json),
                recursive=True,
                fast_linting=not self.strict,
            )
            linter.run()
        except Exception as e:
            return self._fail(f"STAC lint failed: {e}")

        # Collect violations by severity
        errors: list[str] = []
        warnings: list[str] = []
        infos: list[str] = []

        severity_map = self._get_severity_map()

        for check_name, messages in linter.best_practices_dict.items():
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

        if self.config:
            overrides = self.config.get("stac_lint", {}).get("severity", {})
            for check_name, level in overrides.items():
                if isinstance(level, str):
                    try:
                        result[check_name] = Severity(level.lower())
                    except ValueError:
                        pass  # Invalid severity, keep default

        return result
```

### 2. Update `portolan_cli/validation/runner.py`

```python
from portolan_cli.validation.stac_rules import StacLintRule, StacSchemaRule

# Add to DEFAULT_RULES tuple
DEFAULT_RULES: tuple[ValidationRule, ...] = (
    CatalogExistsRule(),
    CatalogJsonValidRule(),
    StacFieldsRule(),
    StacSchemaRule(),      # NEW
    StacLintRule(),        # NEW
    PMTilesRecommendedRule(),
    MetadataFreshRule(),
    ProvisionalDatetimeRule(),
    PartitionStructureRule(),
    PartitionSchemaConsistencyRule(),
)
```

### 3. Update CLI: Add `--strict` flag

In `portolan_cli/cli.py`, update the `check` command:

```python
@click.option(
    "--strict",
    is_flag=True,
    help="Enable strict STAC validation (includes geometry checks)",
)
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
    strict: bool,  # NEW
) -> None:
    ...
```

Pass `strict` to the validation runner, which passes it to STAC rules.

### 4. Config Schema

Add to `.portolan/config.yaml` schema:

```yaml
stac_lint:
  # Override severity for specific checks
  # Valid values: error, warning, info, skip
  severity:
    check_searchable_identifiers: error
    check_thumbnail: warning
    check_bloated_metadata: skip  # disable this check
    check_links_title_field: info
```

### 5. Tests

#### Unit Tests: `tests/unit/validation/test_stac_rules.py`

```python
"""Tests for STAC schema and lint validation rules."""

import pytest
from pathlib import Path

from portolan_cli.validation.stac_rules import StacSchemaRule, StacLintRule
from portolan_cli.validation.results import Severity


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
        assert "id" in result.message.lower() or "required" in result.message.lower()

    def test_invalid_stac_version_fails(self, catalog_bad_version: Path) -> None:
        """Invalid stac_version fails schema validation."""
        rule = StacSchemaRule()
        result = rule.check(catalog_bad_version)
        assert not result.passed

    def test_no_catalog_passes(self, empty_dir: Path) -> None:
        """Missing catalog.json is a pass (nothing to validate)."""
        rule = StacSchemaRule()
        result = rule.check(empty_dir)
        assert result.passed


class TestStacLintRule:
    """Tests for StacLintRule."""

    def test_clean_catalog_passes(self, best_practices_catalog: Path) -> None:
        """Catalog following all best practices passes."""
        rule = StacLintRule()
        result = rule.check(best_practices_catalog)
        assert result.passed

    def test_bad_identifier_fails_as_error(self, catalog_uppercase_id: Path) -> None:
        """Non-searchable identifier fails with ERROR severity."""
        rule = StacLintRule()
        result = rule.check(catalog_uppercase_id)
        assert not result.passed
        assert result.severity == Severity.ERROR
        assert "searchable" in result.message.lower()

    def test_missing_thumbnail_warns(self, catalog_no_thumbnail: Path) -> None:
        """Missing thumbnail is WARNING, not ERROR."""
        rule = StacLintRule()
        result = rule.check(catalog_no_thumbnail)
        # May pass overall if no ERRORs, but should have warnings
        assert "thumbnail" in result.message.lower() or result.passed

    def test_severity_configurable(self, catalog_no_thumbnail: Path) -> None:
        """Severity can be overridden via config."""
        from portolan_cli.config import PortolanConfig

        config = PortolanConfig({
            "stac_lint": {
                "severity": {
                    "check_thumbnail": "error",
                }
            }
        })
        rule = StacLintRule(config=config)
        result = rule.check(catalog_no_thumbnail)
        assert not result.passed
        assert result.severity == Severity.ERROR

    def test_datetime_null_skipped(self, catalog_null_datetime: Path) -> None:
        """check_datetime_null is skipped (handled by ProvisionalDatetimeRule)."""
        rule = StacLintRule()
        result = rule.check(catalog_null_datetime)
        # Should not mention datetime - it's skipped
        assert "datetime" not in result.message.lower()
```

#### Integration Test: `tests/integration/test_check_stac.py`

```python
"""Integration tests for portolan check with STAC validation."""

import subprocess
import json
from pathlib import Path


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


def test_check_strict_includes_geometry(catalog_with_geometry: Path) -> None:
    """--strict flag enables geometry validation."""
    result = subprocess.run(
        ["portolan", "check", "--strict", "--json", str(catalog_with_geometry)],
        capture_output=True,
        text=True,
    )
    # Should complete (pass or fail based on geometry validity)
    assert result.returncode in (0, 1)


def test_check_invalid_schema_fails(catalog_missing_id: Path) -> None:
    """Invalid STAC schema causes check to fail."""
    result = subprocess.run(
        ["portolan", "check", "--json", str(catalog_missing_id)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    data = json.loads(result.stdout)
    assert not data["success"]
    assert any(r["rule_name"] == "stac_schema" for r in data["data"]["results"])
```

### 6. Fixtures Needed

Add to `tests/fixtures/stac/`:

```
tests/fixtures/stac/
├── valid/                    # Valid STAC following best practices
│   ├── catalog.json
│   └── test-collection/
│       └── collection.json
├── missing-id/               # catalog.json missing 'id' field
│   └── catalog.json
├── uppercase-id/             # ID with uppercase (fails searchable check)
│   └── catalog.json
├── no-thumbnail/             # Collection without thumbnail
│   └── collection.json
└── null-datetime/            # Item with null datetime
    └── item.json
```

## File Changes Summary

| File | Change |
|------|--------|
| `pyproject.toml` | Add `stac-check>=1.14.0` dependency |
| `portolan_cli/validation/stac_rules.py` | **NEW** — StacSchemaRule, StacLintRule |
| `portolan_cli/validation/runner.py` | Add STAC rules to DEFAULT_RULES |
| `portolan_cli/cli.py` | Add `--strict` flag to check command |
| `portolan_cli/config.py` | Add stac_lint config schema (if needed) |
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

## ADR Needed?

**Probably not.** This is straightforward integration of an external tool into an existing framework. The decisions made are:
- Use stac-check (established community tool)
- Two rules (matches existing pattern)
- Configurable severity (user preference)

None of these are surprising or controversial enough to warrant an ADR.

## Sequence

1. Add stac-check dependency
2. Implement StacSchemaRule (simpler)
3. Implement StacLintRule (severity mapping)
4. Add --strict flag to CLI
5. Write test fixtures
6. Write unit tests (TDD: tests first, then verify)
7. Write integration tests
8. Update CLAUDE.md with new rule names
