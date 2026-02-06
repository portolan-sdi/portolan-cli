"""Validation framework for Portolan catalogs.

This module provides the public API for validating catalogs:
- check(): Run validation rules against a catalog
- ValidationReport: Aggregate validation results
- ValidationRule: Base class for custom rules

Per ADR-0011, this is an MVP that validates catalog structure only.
Dataset-specific and remote validation comes in later versions.
"""

from portolan_cli.validation.results import (
    Severity,
    ValidationReport,
    ValidationResult,
)
from portolan_cli.validation.rules import ValidationRule

__all__ = [
    "Severity",
    "ValidationReport",
    "ValidationResult",
    "ValidationRule",
]
