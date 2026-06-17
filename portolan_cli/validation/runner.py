"""Validation runner that executes all rules against a catalog."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from portolan_cli.validation.results import ValidationReport, ValidationResult
from portolan_cli.validation.rules import (
    BboxValidRule,
    CatalogExistsRule,
    CatalogJsonValidRule,
    MetadataFreshRule,
    PartitionSchemaConsistencyRule,
    PartitionStructureRule,
    PMTilesRecommendedRule,
    ProvisionalDatetimeRule,
    StacFieldsRule,
    TabularCollectionLevelAssetsRule,
    TabularGeospatialFlagRule,
    TabularTableExtensionRule,
    TabularTemporalExtentRule,
    ValidationRule,
)
from portolan_cli.validation.stac_rules import (
    MandatoryTitlesRule,
    StacLintRule,
    StacSchemaRule,
)

# Default rules (no configuration options)
# Immutable tuple to prevent accidental mutation
DEFAULT_RULES: tuple[ValidationRule, ...] = (
    CatalogExistsRule(),
    CatalogJsonValidRule(),
    StacFieldsRule(),
    StacSchemaRule(),
    StacLintRule(),
    MandatoryTitlesRule(),
    BboxValidRule(),
    PMTilesRecommendedRule(),
    MetadataFreshRule(),
    ProvisionalDatetimeRule(),
    PartitionStructureRule(),
    PartitionSchemaConsistencyRule(),
    TabularGeospatialFlagRule(),
    TabularTableExtensionRule(),
    TabularTemporalExtentRule(),
    TabularCollectionLevelAssetsRule(),
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
        MandatoryTitlesRule(),
        BboxValidRule(),
        PMTilesRecommendedRule(),
        MetadataFreshRule(),
        ProvisionalDatetimeRule(),
        PartitionStructureRule(),
        PartitionSchemaConsistencyRule(),
        TabularGeospatialFlagRule(),
        TabularTableExtensionRule(),
        TabularTemporalExtentRule(),
        TabularCollectionLevelAssetsRule(),
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
