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
