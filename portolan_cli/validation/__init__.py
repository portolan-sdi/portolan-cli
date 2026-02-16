"""Validation framework for Portolan catalogs.

.. deprecated::
    This module is deprecated per ADR-0017. Catalog validation will be
    redesigned when the `sync` command is implemented. The `.portolan/`
    convention is being replaced with standard STAC catalog layout.

    The module remains for backward compatibility but is not used by
    the current CLI. It may be removed in a future version.

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
from portolan_cli.validation.runner import check

__all__ = [
    "Severity",
    "ValidationReport",
    "ValidationResult",
    "ValidationRule",
    "check",
]
