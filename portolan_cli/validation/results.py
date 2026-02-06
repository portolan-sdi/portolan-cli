"""Validation result data structures.

These classes capture the output of validation rules and aggregate
them into reports for CLI display and JSON export.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Severity(Enum):
    """Severity level for validation results.

    ERROR: Blocks catalog operations (validation fails)
    WARNING: Non-blocking issue (validation passes with warnings)
    INFO: Suggestion for improvement (always passes)
    """

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass(frozen=True)
class ValidationResult:
    """Result from a single validation rule.

    Attributes:
        rule_name: Identifier for the rule that produced this result.
        passed: Whether the validation passed.
        severity: How serious a failure is (ERROR blocks, WARNING doesn't).
        message: Human-readable description of the result.
        fix_hint: Optional suggestion for fixing the issue.
    """

    rule_name: str
    passed: bool
    severity: Severity
    message: str
    fix_hint: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        d: dict[str, Any] = {
            "rule_name": self.rule_name,
            "passed": self.passed,
            "severity": self.severity.value,
            "message": self.message,
        }
        if self.fix_hint is not None:
            d["fix_hint"] = self.fix_hint
        return d


@dataclass
class ValidationReport:
    """Aggregate of all validation results.

    Attributes:
        results: List of individual validation results.
    """

    results: list[ValidationResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """True if no ERROR-severity rules failed."""
        return not any(not r.passed and r.severity == Severity.ERROR for r in self.results)

    @property
    def errors(self) -> list[ValidationResult]:
        """Return only failed ERROR-severity results."""
        return [r for r in self.results if not r.passed and r.severity == Severity.ERROR]

    @property
    def warnings(self) -> list[ValidationResult]:
        """Return only failed WARNING-severity results."""
        return [r for r in self.results if not r.passed and r.severity == Severity.WARNING]

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict for --json output."""
        return {
            "passed": self.passed,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "results": [r.to_dict() for r in self.results],
        }
