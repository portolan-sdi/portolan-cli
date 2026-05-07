"""STAC schema validation and best-practices linting rules.

Uses stac-check as the validation engine. Two rules:
- StacSchemaRule: JSON Schema validation (ERROR severity)
- StacLintRule: Best practices checks (configurable severity)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from stac_check.lint import Linter  # type: ignore[import-untyped]

from portolan_cli.validation.results import Severity, ValidationResult
from portolan_cli.validation.rules import ValidationRule


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

        # Check root-level validation
        if not linter.valid_stac:
            recommendation = getattr(linter, "recommendation", None)
            return self._fail(
                linter.error_msg or "STAC schema validation failed",
                fix_hint=recommendation,
            )

        # Check recursive validation results (stac-check stores these separately)
        validate_all = getattr(linter, "validate_all", [])
        failed = [r for r in validate_all if not r.get("valid_stac", True)]
        if failed:
            first_error = failed[0]
            msg = first_error.get("error_message", "Schema validation failed")
            path = first_error.get("path", "unknown")
            hint = first_error.get("recommendation")
            return self._fail(
                f"{msg} in {path}",
                fix_hint=hint,
            )

        return self._pass("All STAC objects pass schema validation")


class StacLintRule(ValidationRule):
    """Check STAC objects against best practices.

    Uses stac-check's best practices checks. Each check can have
    configurable severity via .portolan/config.yaml.
    """

    name = "stac_lint"
    severity = Severity.WARNING
    description = "Check STAC against best practices"

    # Checks to skip (handled by other portolan rules)
    SKIP_CHECKS: frozenset[str] = frozenset(
        {
            "datetime_null",  # ProvisionalDatetimeRule handles this
        }
    )

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
        # Compute skip checks once at init time (not as side effect of _get_severity_map)
        self._runtime_skip_checks: frozenset[str] = self._compute_skip_checks()

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
        skip_checks = self.SKIP_CHECKS | self._runtime_skip_checks

        for check_name, messages in bp_dict.items():
            if check_name in skip_checks:
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

    def _compute_skip_checks(self) -> frozenset[str]:
        """Compute checks to skip based on config (called once at init)."""
        skip = set()
        overrides = self.config.get("stac_lint", {}).get("severity", {})
        for check_name, level in overrides.items():
            if isinstance(level, str) and level.lower() == "skip":
                skip.add(check_name)
        return frozenset(skip)

    def _get_severity_map(self) -> dict[str, Severity]:
        """Get severity map, merging defaults with config overrides."""
        result = dict(self.DEFAULT_SEVERITIES)

        overrides = self.config.get("stac_lint", {}).get("severity", {})
        for check_name, level in overrides.items():
            if isinstance(level, str):
                level_lower = level.lower()
                if level_lower != "skip":
                    try:
                        result[check_name] = Severity(level_lower)
                    except ValueError:
                        pass  # Invalid severity, keep default

        return result
