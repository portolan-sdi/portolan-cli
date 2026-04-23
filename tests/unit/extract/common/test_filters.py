"""Tests for common extraction filtering.

These tests verify the shared filtering logic used across all extraction
backends (ArcGIS, WFS, etc.). Both services and layers use glob patterns
(fnmatch) to include or exclude items from extraction.
"""

from __future__ import annotations

import pytest

from portolan_cli.extract.common.filters import (
    apply_unified_filter,
    filter_layers,
    filter_services,
)

pytestmark = pytest.mark.unit


# =============================================================================
# Service Filtering Tests
# =============================================================================


class TestFilterServicesInclude:
    """Tests for include-only filtering."""

    def test_single_include_pattern(self) -> None:
        """Single include pattern should match services."""
        services = ["Census_2020", "Census_2010", "Transportation", "Legacy_Data"]
        result = filter_services(services, include=["Census*"])

        assert result == ["Census_2020", "Census_2010"]

    def test_multiple_include_patterns(self) -> None:
        """Multiple include patterns should match all."""
        services = ["Census_2020", "Census_2010", "Transportation", "Legacy_Data"]
        result = filter_services(services, include=["Census*", "Transport*"])

        assert result == ["Census_2020", "Census_2010", "Transportation"]

    def test_include_no_match(self) -> None:
        """No matching patterns should return empty list."""
        services = ["Census_2020", "Transportation"]
        result = filter_services(services, include=["NonExistent*"])

        assert result == []


class TestFilterServicesExclude:
    """Tests for exclude-only filtering."""

    def test_single_exclude_pattern(self) -> None:
        """Single exclude pattern should remove services."""
        services = ["Census_2020", "Census_2010", "Transportation", "Legacy_Data"]
        result = filter_services(services, exclude=["Legacy*"])

        assert result == ["Census_2020", "Census_2010", "Transportation"]

    def test_exclude_no_match(self) -> None:
        """Exclude pattern with no matches should return all services."""
        services = ["Census_2020", "Transportation"]
        result = filter_services(services, exclude=["NonExistent*"])

        assert result == ["Census_2020", "Transportation"]


class TestFilterServicesCombined:
    """Tests for combined include + exclude filtering."""

    def test_include_then_exclude(self) -> None:
        """Include patterns should apply first, then exclude."""
        services = [
            "Census_2020",
            "Census_2010",
            "Census_2000",
            "Transportation",
        ]
        result = filter_services(services, include=["Census*"], exclude=["*2000"])

        assert result == ["Census_2020", "Census_2010"]


class TestFilterServicesNoFilters:
    """Tests for no filtering (passthrough)."""

    def test_no_filters_returns_all(self) -> None:
        """No include or exclude should return all services."""
        services = ["Census_2020", "Transportation", "Legacy_Data"]
        result = filter_services(services)

        assert result == services

    def test_empty_services_list(self) -> None:
        """Empty services list should return empty."""
        result = filter_services([], include=["Census*"])

        assert result == []


class TestFilterServicesCaseSensitivity:
    """Tests for case sensitivity handling."""

    def test_case_sensitive_by_default(self) -> None:
        """Pattern matching should be case-sensitive by default."""
        services = ["Census_2020", "CENSUS_2021", "census_2022"]
        result = filter_services(services, include=["Census*"])

        assert result == ["Census_2020"]

    def test_case_insensitive_option(self) -> None:
        """Case-insensitive matching when specified."""
        services = ["Census_2020", "CENSUS_2021", "census_2022"]
        result = filter_services(services, include=["census*"], case_sensitive=False)

        assert result == ["Census_2020", "CENSUS_2021", "census_2022"]


# =============================================================================
# Layer Filtering Tests
# =============================================================================


class TestFilterLayersBasic:
    """Tests for basic layer filtering."""

    @pytest.fixture
    def sample_layers(self) -> list[dict[str, int | str]]:
        """Sample layer list for testing."""
        return [
            {"id": 0, "name": "Census_Block_Groups"},
            {"id": 1, "name": "Census_Tracts"},
            {"id": 2, "name": "Transportation"},
            {"id": 3, "name": "Boundaries"},
        ]

    def test_no_filters_returns_all_layers(self, sample_layers: list[dict[str, int | str]]) -> None:
        """When no include/exclude specified, return all layers."""
        result = filter_layers(sample_layers, include=None, exclude=None)

        assert result == sample_layers


class TestFilterLayersByGlobPattern:
    """Tests for glob pattern matching on layer names."""

    @pytest.fixture
    def country_layers(self) -> list[dict[str, int | str]]:
        """Layers with country code prefixes."""
        return [
            {"id": 0, "name": "sdn_admin_boundaries"},
            {"id": 1, "name": "sdn_health_facilities"},
            {"id": 2, "name": "ukr_admin_boundaries"},
        ]

    def test_glob_country_prefix(self, country_layers: list[dict[str, int | str]]) -> None:
        """Filter by country code prefix glob pattern."""
        result = filter_layers(country_layers, include=["sdn_*"], exclude=None)

        assert len(result) == 2
        assert all("sdn_" in str(layer["name"]) for layer in result)

    def test_glob_combined_with_exclude(self, country_layers: list[dict[str, int | str]]) -> None:
        """Glob include with glob exclude."""
        result = filter_layers(country_layers, include=["sdn_*"], exclude=["*health*"])

        assert len(result) == 1
        assert result[0]["name"] == "sdn_admin_boundaries"


class TestFilterLayersById:
    """Tests for filtering layers by numeric ID."""

    @pytest.fixture
    def sample_layers(self) -> list[dict[str, int | str]]:
        return [
            {"id": 0, "name": "Layer_A"},
            {"id": 1, "name": "Layer_B"},
            {"id": 2, "name": "Layer_C"},
        ]

    def test_include_by_id_string(self, sample_layers: list[dict[str, int | str]]) -> None:
        """Include filter matches layer IDs passed as strings."""
        result = filter_layers(sample_layers, include=["0", "1"], exclude=None)

        assert len(result) == 2
        assert result[0]["id"] == 0
        assert result[1]["id"] == 1


# =============================================================================
# Unified Filter Tests
# =============================================================================


class TestApplyUnifiedFilter:
    """Tests for apply_unified_filter function."""

    def test_applies_to_both_services_and_layers(self) -> None:
        """Unified filter applies same pattern to both."""
        services = ["sdn_census", "ukr_census", "eth_census"]
        layers = [
            {"id": 0, "name": "sdn_boundaries"},
            {"id": 1, "name": "ukr_boundaries"},
        ]

        filtered_services, filtered_layers = apply_unified_filter(
            services, layers, filter_pattern=["sdn_*"], exclude_pattern=None
        )

        assert filtered_services == ["sdn_census"]
        assert len(filtered_layers) == 1
        assert filtered_layers[0]["name"] == "sdn_boundaries"

    def test_none_services_handled(self) -> None:
        """When services is None, only layers are filtered."""
        layers = [
            {"id": 0, "name": "sdn_boundaries"},
            {"id": 1, "name": "ukr_boundaries"},
        ]

        filtered_services, filtered_layers = apply_unified_filter(
            None, layers, filter_pattern=["sdn_*"], exclude_pattern=None
        )

        assert filtered_services is None
        assert len(filtered_layers) == 1
