"""Tests for ArcGIS service and layer filtering.

Both services and layers use glob patterns (fnmatch) to include or exclude
items from extraction. This is particularly useful for:
- Services root URLs where many services may exist
- Large services with many layers
- Pattern-based filtering (country codes, years, etc.)
"""

from __future__ import annotations

import pytest

from portolan_cli.extract.arcgis.filters import (
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

    def test_include_exact_match(self) -> None:
        """Exact pattern should match exactly."""
        services = ["Census_2020", "Census_2010", "Transportation"]
        result = filter_services(services, include=["Census_2020"])

        assert result == ["Census_2020"]

    def test_include_question_mark_wildcard(self) -> None:
        """Question mark should match single character."""
        services = ["Census_2020", "Census_2021", "Census_2010"]
        result = filter_services(services, include=["Census_202?"])

        assert result == ["Census_2020", "Census_2021"]

    def test_include_no_match(self) -> None:
        """No matching patterns should return empty list."""
        services = ["Census_2020", "Transportation"]
        result = filter_services(services, include=["NonExistent*"])

        assert result == []

    def test_include_preserves_order(self) -> None:
        """Filtered results should preserve original order."""
        services = ["Z_Service", "A_Service", "M_Service"]
        result = filter_services(services, include=["*_Service"])

        assert result == ["Z_Service", "A_Service", "M_Service"]


class TestFilterServicesExclude:
    """Tests for exclude-only filtering."""

    def test_single_exclude_pattern(self) -> None:
        """Single exclude pattern should remove services."""
        services = ["Census_2020", "Census_2010", "Transportation", "Legacy_Data"]
        result = filter_services(services, exclude=["Legacy*"])

        assert result == ["Census_2020", "Census_2010", "Transportation"]

    def test_multiple_exclude_patterns(self) -> None:
        """Multiple exclude patterns should remove all matches."""
        services = ["Census_2020", "Census_2010", "Transportation", "Legacy_Data", "Test_Service"]
        result = filter_services(services, exclude=["Legacy*", "Test*"])

        assert result == ["Census_2020", "Census_2010", "Transportation"]

    def test_exclude_all(self) -> None:
        """Excluding all services should return empty list."""
        services = ["Census_2020", "Census_2010"]
        result = filter_services(services, exclude=["Census*"])

        assert result == []

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
            "Legacy_Data",
        ]
        result = filter_services(services, include=["Census*"], exclude=["*2000"])

        assert result == ["Census_2020", "Census_2010"]

    def test_exclude_from_include_subset(self) -> None:
        """Exclude should only apply to included services."""
        services = [
            "Public_Census",
            "Public_Transport",
            "Private_Census",
            "Private_Transport",
        ]
        result = filter_services(services, include=["Public*"], exclude=["*Transport"])

        assert result == ["Public_Census"]


class TestFilterServicesNoFilters:
    """Tests for no filtering (passthrough)."""

    def test_no_filters_returns_all(self) -> None:
        """No include or exclude should return all services."""
        services = ["Census_2020", "Transportation", "Legacy_Data"]
        result = filter_services(services)

        assert result == services

    def test_empty_include_returns_all(self) -> None:
        """Empty include list should return all services."""
        services = ["Census_2020", "Transportation"]
        result = filter_services(services, include=[])

        assert result == services

    def test_empty_services_list(self) -> None:
        """Empty services list should return empty."""
        result = filter_services([], include=["Census*"])

        assert result == []


class TestFilterServicesEdgeCases:
    """Tests for edge cases and special patterns."""

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

    def test_slash_in_service_name(self) -> None:
        """Services with folder paths (slashes) should work."""
        services = ["Public/Census", "Public/Transport", "Private/Census"]
        result = filter_services(services, include=["Public/*"])

        assert result == ["Public/Census", "Public/Transport"]


class TestFilterServicesRealWorld:
    """Tests with real-world service name patterns."""

    def test_filter_by_year(self) -> None:
        """Filter services by year suffix."""
        services = [
            "Census_2020",
            "Census_2019",
            "Transportation_2020",
            "Transportation_2019",
        ]
        result = filter_services(services, include=["*_2020"])

        assert result == ["Census_2020", "Transportation_2020"]

    def test_filter_by_country_code(self) -> None:
        """Filter services by ISO3 country code prefix."""
        services = [
            "sdn_admin_boundaries",
            "sdn_health_facilities",
            "ukr_admin_boundaries",
            "ukr_roads",
            "eth_population",
        ]
        result = filter_services(services, include=["sdn_*"], case_sensitive=False)

        assert result == ["sdn_admin_boundaries", "sdn_health_facilities"]

    def test_exclude_archive_and_test(self) -> None:
        """Common pattern: exclude Archive and Test services."""
        services = [
            "Census_2020",
            "Transportation",
            "Archive_Census",
            "Test_Service",
        ]
        result = filter_services(services, exclude=["Archive*", "Test*"])

        assert result == ["Census_2020", "Transportation"]


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

    def test_empty_include_list_returns_all(
        self, sample_layers: list[dict[str, int | str]]
    ) -> None:
        """Empty include list should return all layers."""
        result = filter_layers(sample_layers, include=[], exclude=None)

        assert result == sample_layers


class TestFilterLayersByID:
    """Tests for filtering layers by numeric ID."""

    @pytest.fixture
    def sample_layers(self) -> list[dict[str, int | str]]:
        return [
            {"id": 0, "name": "Census_Block_Groups"},
            {"id": 1, "name": "Census_Tracts"},
            {"id": 2, "name": "Transportation"},
        ]

    def test_include_by_id_string(self, sample_layers: list[dict[str, int | str]]) -> None:
        """Include filter matches layer IDs passed as strings."""
        result = filter_layers(sample_layers, include=["0", "1"], exclude=None)

        assert len(result) == 2
        assert result[0]["id"] == 0
        assert result[1]["id"] == 1

    def test_exclude_by_id(self, sample_layers: list[dict[str, int | str]]) -> None:
        """Exclude filter removes layers by ID."""
        result = filter_layers(sample_layers, include=None, exclude=["2"])

        assert len(result) == 2
        assert all(layer["id"] != 2 for layer in result)

    def test_nonexistent_id_ignored(self, sample_layers: list[dict[str, int | str]]) -> None:
        """Nonexistent IDs in include list are silently ignored."""
        result = filter_layers(sample_layers, include=["0", "999"], exclude=None)

        assert len(result) == 1
        assert result[0]["id"] == 0


class TestFilterLayersByGlobPattern:
    """Tests for glob pattern matching on layer names."""

    @pytest.fixture
    def country_layers(self) -> list[dict[str, int | str]]:
        """Layers with country code prefixes (common real-world pattern)."""
        return [
            {"id": 0, "name": "sdn_admin_boundaries"},
            {"id": 1, "name": "sdn_health_facilities"},
            {"id": 2, "name": "sdn_roads"},
            {"id": 3, "name": "ukr_admin_boundaries"},
            {"id": 4, "name": "ukr_population"},
            {"id": 5, "name": "eth_admin_boundaries"},
        ]

    def test_glob_country_prefix(self, country_layers: list[dict[str, int | str]]) -> None:
        """Filter by country code prefix glob pattern."""
        result = filter_layers(country_layers, include=["sdn_*"], exclude=None)

        assert len(result) == 3
        assert all("sdn_" in str(layer["name"]) for layer in result)

    def test_glob_suffix_pattern(self, country_layers: list[dict[str, int | str]]) -> None:
        """Filter by suffix glob pattern."""
        result = filter_layers(country_layers, include=["*_admin_boundaries"], exclude=None)

        assert len(result) == 3
        names = [layer["name"] for layer in result]
        assert "sdn_admin_boundaries" in names
        assert "ukr_admin_boundaries" in names
        assert "eth_admin_boundaries" in names

    def test_glob_combined_with_exclude(self, country_layers: list[dict[str, int | str]]) -> None:
        """Glob include with glob exclude."""
        result = filter_layers(country_layers, include=["sdn_*"], exclude=["*_roads"])

        assert len(result) == 2
        names = [layer["name"] for layer in result]
        assert "sdn_admin_boundaries" in names
        assert "sdn_health_facilities" in names
        assert "sdn_roads" not in names

    def test_glob_multiple_patterns(self, country_layers: list[dict[str, int | str]]) -> None:
        """Multiple glob patterns in include list."""
        result = filter_layers(country_layers, include=["sdn_*", "ukr_*"], exclude=None)

        assert len(result) == 5
        assert all(str(layer["name"]).startswith(("sdn_", "ukr_")) for layer in result)

    def test_glob_case_insensitive(self, country_layers: list[dict[str, int | str]]) -> None:
        """Glob patterns are case-insensitive by default."""
        result = filter_layers(country_layers, include=["SDN_*"], exclude=None)

        assert len(result) == 3  # Should match sdn_ layers


class TestFilterLayersYearPatterns:
    """Tests for year-based filtering patterns."""

    @pytest.fixture
    def yearly_layers(self) -> list[dict[str, int | str]]:
        """Layers with year suffixes."""
        return [
            {"id": 0, "name": "census_2020"},
            {"id": 1, "name": "census_2021"},
            {"id": 2, "name": "census_2022"},
            {"id": 3, "name": "population_2020"},
            {"id": 4, "name": "roads_v2"},
        ]

    def test_filter_by_year(self, yearly_layers: list[dict[str, int | str]]) -> None:
        """Filter layers by year suffix."""
        result = filter_layers(yearly_layers, include=["*_2020"], exclude=None)

        assert len(result) == 2
        names = [layer["name"] for layer in result]
        assert "census_2020" in names
        assert "population_2020" in names

    def test_filter_recent_years(self, yearly_layers: list[dict[str, int | str]]) -> None:
        """Filter for multiple recent years."""
        result = filter_layers(yearly_layers, include=["*_2021", "*_2022"], exclude=None)

        assert len(result) == 2

    def test_exclude_old_years(self, yearly_layers: list[dict[str, int | str]]) -> None:
        """Exclude old year data."""
        result = filter_layers(yearly_layers, include=None, exclude=["*_2020"])

        assert len(result) == 3
        assert all("_2020" not in str(layer["name"]) for layer in result)


class TestFilterLayersEdgeCases:
    """Edge cases for layer filtering."""

    def test_empty_layers_list(self) -> None:
        """Empty input returns empty output."""
        result = filter_layers([], include=["anything"], exclude=None)

        assert result == []

    def test_preserves_order(self) -> None:
        """Filtered result preserves original layer order."""
        layers = [
            {"id": 3, "name": "layer_c"},
            {"id": 1, "name": "layer_a"},
            {"id": 2, "name": "layer_b"},
        ]
        result = filter_layers(layers, include=["layer_*"], exclude=None)

        assert [layer["id"] for layer in result] == [3, 1, 2]

    def test_whitespace_trimmed(self) -> None:
        """Filter values with whitespace are trimmed."""
        layers = [{"id": 0, "name": "test_layer"}]
        result = filter_layers(layers, include=["  test_layer  "], exclude=None)

        assert len(result) == 1

    def test_mixed_id_and_glob(self) -> None:
        """Can mix ID filters with glob patterns."""
        layers = [
            {"id": 0, "name": "admin_boundaries"},
            {"id": 1, "name": "health_facilities"},
            {"id": 2, "name": "roads"},
        ]
        result = filter_layers(layers, include=["0", "*_facilities"], exclude=None)

        assert len(result) == 2
        ids = [layer["id"] for layer in result]
        assert 0 in ids
        assert 1 in ids


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
            {"id": 2, "name": "eth_boundaries"},
        ]

        filtered_services, filtered_layers = apply_unified_filter(
            services, layers, filter_pattern=["sdn_*"], exclude_pattern=None
        )

        assert filtered_services == ["sdn_census"]
        assert len(filtered_layers) == 1
        assert filtered_layers[0]["name"] == "sdn_boundaries"

    def test_exclude_pattern_applies_to_both(self) -> None:
        """Exclude pattern applies to both services and layers."""
        services = ["sdn_census", "sdn_test", "ukr_census"]
        layers = [
            {"id": 0, "name": "sdn_boundaries"},
            {"id": 1, "name": "sdn_test_layer"},
        ]

        filtered_services, filtered_layers = apply_unified_filter(
            services, layers, filter_pattern=["sdn_*"], exclude_pattern=["*test*"]
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

    def test_none_layers_handled(self) -> None:
        """When layers is None, only services are filtered."""
        services = ["sdn_census", "ukr_census"]

        filtered_services, filtered_layers = apply_unified_filter(
            services, None, filter_pattern=["sdn_*"], exclude_pattern=None
        )

        assert filtered_services == ["sdn_census"]
        assert filtered_layers is None

    def test_no_filter_returns_all(self) -> None:
        """No filter pattern returns all items."""
        services = ["a", "b"]
        layers = [{"id": 0, "name": "x"}, {"id": 1, "name": "y"}]

        filtered_services, filtered_layers = apply_unified_filter(
            services, layers, filter_pattern=None, exclude_pattern=None
        )

        assert filtered_services == services
        assert filtered_layers == layers
