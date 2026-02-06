"""Validation rule base class and built-in rules.

Each rule checks one aspect of catalog validity. Rules are designed
to be unit-testable in isolation and composable into a validation pipeline.

Per ADR-0011, v0.4 rules only check catalog structure, not dataset contents.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

from portolan_cli.validation.results import Severity, ValidationResult


class ValidationRule(ABC):
    """Base class for all validation rules.

    Subclasses must define:
        name: Unique identifier for the rule
        severity: ERROR (blocking) or WARNING (non-blocking)
        description: Human-readable explanation for --verbose

    Subclasses must implement:
        check(): Run the validation and return a result
    """

    name: str
    severity: Severity
    description: str

    @abstractmethod
    def check(self, catalog_path: Path) -> ValidationResult:
        """Run this validation rule against a catalog.

        Args:
            catalog_path: Path to the directory containing .portolan.

        Returns:
            ValidationResult indicating pass/fail with message.
        """
        ...

    def _pass(self, message: str) -> ValidationResult:
        """Helper to create a passing result."""
        return ValidationResult(
            rule_name=self.name,
            passed=True,
            severity=self.severity,
            message=message,
        )

    def _fail(self, message: str, *, fix_hint: str | None = None) -> ValidationResult:
        """Helper to create a failing result."""
        return ValidationResult(
            rule_name=self.name,
            passed=False,
            severity=self.severity,
            message=message,
            fix_hint=fix_hint,
        )


class CatalogExistsRule(ValidationRule):
    """Check that .portolan directory exists.

    This is the most fundamental check - without the catalog directory,
    no other validation can proceed.
    """

    name = "catalog_exists"
    severity = Severity.ERROR
    description = "Verify .portolan directory exists"

    def check(self, catalog_path: Path) -> ValidationResult:
        """Check for .portolan directory."""
        portolan_dir = catalog_path / ".portolan"

        if not portolan_dir.exists():
            return self._fail(
                f"Catalog not found: {portolan_dir} does not exist",
                fix_hint="Run 'portolan init' to create a catalog",
            )

        if not portolan_dir.is_dir():
            return self._fail(
                f"Invalid catalog: {portolan_dir} exists but is not a directory",
                fix_hint="Remove the file and run 'portolan init'",
            )

        return self._pass(f"Catalog directory exists: {portolan_dir}")
