"""Tests for common extraction filtering.

These tests verify the shared filtering logic used across all extraction
backends (ArcGIS, WFS, etc.). Both services and layers use glob patterns
(fnmatch) to include or exclude items from extraction.
"""

from __future__ import annotations

import pytest

from portolan_cli.extract.common.filters import filter_services, is_glob, layer_matches

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
# Layer Matching Tests
# =============================================================================


class TestLayerMatches:
    """``layer_matches`` decides one pattern against one layer."""

    @pytest.mark.parametrize(
        ("pattern", "expected"),
        [("sdn_*", True), ("SDN_ADMIN*", True), ("ukr_*", False), ("*health*", False)],
    )
    def test_glob_matches_the_name_without_case(self, pattern: str, expected: bool) -> None:
        assert layer_matches(0, "sdn_admin_boundaries", pattern) is expected

    def test_numeric_pattern_matches_the_id_only(self) -> None:
        assert layer_matches(5, "Layer_C", "5") is True
        assert layer_matches(2, "5", "5") is False

    def test_plain_pattern_matches_the_leaf(self) -> None:
        """#768: a WFS name is workspace:layer, and the leaf alone matches."""
        assert layer_matches(0, "cadastru_data:terenuri", "terenuri") is True
        assert layer_matches(0, "cadastru_data:terenuri", "cadastru_data:terenuri") is True
        assert layer_matches(0, "cadastru_data:terenuri", "cadastru") is False

    def test_whitespace_around_a_pattern_is_ignored(self) -> None:
        assert layer_matches(1, "roads", " roads ") is True

    @pytest.mark.parametrize(("pattern", "expected"), [("a*", True), ("a?", True), ("[ab]", True)])
    def test_is_glob(self, pattern: str, expected: bool) -> None:
        assert is_glob(pattern) is expected
        assert is_glob("roads") is False


# =============================================================================
# Folder-Qualified Name Tests
# =============================================================================


@pytest.mark.unit
def test_filter_matches_folder_qualified_names() -> None:
    """fnmatch * spans / so folder-qualified names like ecml/active_faults are matched by ecml/*."""
    names = ["ecml/active_faults", "ecml/airports_v2", "water/rivers", "Top"]
    assert filter_services(names, include=["ecml/*"]) == ["ecml/active_faults", "ecml/airports_v2"]
    assert filter_services(names, exclude=["water/*"]) == [
        "ecml/active_faults",
        "ecml/airports_v2",
        "Top",
    ]
