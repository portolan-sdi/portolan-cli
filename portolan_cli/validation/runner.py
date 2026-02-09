"""Validation runner that executes all rules against a catalog."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from portolan_cli.validation.results import ValidationReport, ValidationResult
from portolan_cli.validation.rules import (
    CatalogExistsRule,
    CatalogJsonValidRule,
    PMTilesRecommendedRule,
    StacFieldsRule,
    ValidationRule,
)

# Default rules for v0.4 (catalog structure only)
# Immutable tuple to prevent accidental mutation
DEFAULT_RULES: tuple[ValidationRule, ...] = (
    CatalogExistsRule(),
    CatalogJsonValidRule(),
    StacFieldsRule(),
    PMTilesRecommendedRule(),
)


def check(
    catalog_path: Path,
    *,
    rules: Sequence[ValidationRule] | None = None,
) -> ValidationReport:
    """Run validation rules against a catalog.

    Args:
        catalog_path: Path to the directory containing .portolan.
        rules: Optional sequence of rules to run. Defaults to DEFAULT_RULES.

    Returns:
        ValidationReport with results from all rules.
    """
    if rules is None:
        rules = DEFAULT_RULES

    results: list[ValidationResult] = []

    for rule in rules:
        result = rule.check(catalog_path)
        results.append(result)

    return ValidationReport(results=results)
