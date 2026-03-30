"""Tests for ArcGIS metadata extraction.

These tests verify the metadata mapper for `portolan extract arcgis`:
- Map ArcGIS REST API fields to metadata.yaml structure
- Return a dataclass with extracted fields (None for missing)
- Handle edge cases (empty strings, missing fields, nested objects)
"""

from __future__ import annotations

import pytest

from portolan_cli.extract.arcgis.metadata import ArcGISMetadata, extract_arcgis_metadata


class TestArcGISMetadataDataclass:
    """Tests for ArcGISMetadata dataclass."""

    @pytest.mark.unit
    def test_dataclass_has_expected_fields(self) -> None:
        """ArcGISMetadata has all expected fields."""
        metadata = ArcGISMetadata(
            source_url="https://example.com/FeatureServer",
            attribution="Test Attribution",
            description="Test Description",
            processing_notes="Test Notes",
            contact_name="Test Author",
            keywords=["test", "keywords"],
            known_issues="Test Issues",
            license_info_raw="Test License",
        )

        assert metadata.source_url == "https://example.com/FeatureServer"
        assert metadata.attribution == "Test Attribution"
        assert metadata.description == "Test Description"
        assert metadata.processing_notes == "Test Notes"
        assert metadata.contact_name == "Test Author"
        assert metadata.keywords == ["test", "keywords"]
        assert metadata.known_issues == "Test Issues"
        assert metadata.license_info_raw == "Test License"

    @pytest.mark.unit
    def test_dataclass_allows_none_values(self) -> None:
        """ArcGISMetadata fields can be None."""
        metadata = ArcGISMetadata(
            source_url="https://example.com/FeatureServer",
            attribution=None,
            description=None,
            processing_notes=None,
            contact_name=None,
            keywords=None,
            known_issues=None,
            license_info_raw=None,
        )

        assert metadata.source_url == "https://example.com/FeatureServer"
        assert metadata.attribution is None
        assert metadata.description is None
        assert metadata.processing_notes is None
        assert metadata.contact_name is None
        assert metadata.keywords is None
        assert metadata.known_issues is None
        assert metadata.license_info_raw is None


class TestExtractArcGISMetadata:
    """Tests for extract_arcgis_metadata function."""

    # =========================================================================
    # Basic field extraction
    # =========================================================================

    @pytest.mark.unit
    def test_source_url_required(self) -> None:
        """source_url is set from the function parameter."""
        service_info: dict[str, object] = {}
        metadata = extract_arcgis_metadata(
            service_info, source_url="https://example.com/FeatureServer"
        )

        assert metadata.source_url == "https://example.com/FeatureServer"

    @pytest.mark.unit
    def test_copyright_text_maps_to_attribution(self) -> None:
        """copyrightText maps to attribution."""
        service_info = {"copyrightText": "City of Philadelphia"}
        metadata = extract_arcgis_metadata(
            service_info, source_url="https://example.com/FeatureServer"
        )

        assert metadata.attribution == "City of Philadelphia"

    @pytest.mark.unit
    def test_description_maps_to_description(self) -> None:
        """description maps to description."""
        service_info = {"description": "Census data for the city"}
        metadata = extract_arcgis_metadata(
            service_info, source_url="https://example.com/FeatureServer"
        )

        assert metadata.description == "Census data for the city"

    @pytest.mark.unit
    def test_service_description_maps_to_processing_notes(self) -> None:
        """serviceDescription maps to processing_notes."""
        service_info = {"serviceDescription": "Updated quarterly from Census API"}
        metadata = extract_arcgis_metadata(
            service_info, source_url="https://example.com/FeatureServer"
        )

        assert metadata.processing_notes == "Updated quarterly from Census API"

    @pytest.mark.unit
    def test_access_information_maps_to_known_issues(self) -> None:
        """accessInformation maps to known_issues."""
        service_info = {"accessInformation": "Limited to non-commercial use"}
        metadata = extract_arcgis_metadata(
            service_info, source_url="https://example.com/FeatureServer"
        )

        assert metadata.known_issues == "Limited to non-commercial use"

    @pytest.mark.unit
    def test_license_info_maps_to_license_info_raw(self) -> None:
        """licenseInfo maps to license_info_raw (not SPDX)."""
        service_info = {"licenseInfo": "Public domain - no restrictions apply"}
        metadata = extract_arcgis_metadata(
            service_info, source_url="https://example.com/FeatureServer"
        )

        assert metadata.license_info_raw == "Public domain - no restrictions apply"

    # =========================================================================
    # Nested documentInfo extraction
    # =========================================================================

    @pytest.mark.unit
    def test_document_info_author_maps_to_contact_name(self) -> None:
        """documentInfo.Author maps to contact_name."""
        service_info = {
            "documentInfo": {
                "Author": "GIS Department",
            }
        }
        metadata = extract_arcgis_metadata(
            service_info, source_url="https://example.com/FeatureServer"
        )

        assert metadata.contact_name == "GIS Department"

    @pytest.mark.unit
    def test_document_info_keywords_split_to_list(self) -> None:
        """documentInfo.Keywords (comma-separated) maps to keywords list."""
        service_info = {
            "documentInfo": {
                "Keywords": "census, demographics, population",
            }
        }
        metadata = extract_arcgis_metadata(
            service_info, source_url="https://example.com/FeatureServer"
        )

        assert metadata.keywords == ["census", "demographics", "population"]

    @pytest.mark.unit
    def test_document_info_keywords_strips_whitespace(self) -> None:
        """Keywords are trimmed of whitespace."""
        service_info = {
            "documentInfo": {
                "Keywords": "  census  ,  demographics  ,  population  ",
            }
        }
        metadata = extract_arcgis_metadata(
            service_info, source_url="https://example.com/FeatureServer"
        )

        assert metadata.keywords == ["census", "demographics", "population"]

    @pytest.mark.unit
    def test_document_info_keywords_removes_empty_entries(self) -> None:
        """Empty keywords after splitting are removed."""
        service_info = {
            "documentInfo": {
                "Keywords": "census,,demographics,  ,population",
            }
        }
        metadata = extract_arcgis_metadata(
            service_info, source_url="https://example.com/FeatureServer"
        )

        assert metadata.keywords == ["census", "demographics", "population"]

    @pytest.mark.unit
    def test_document_info_single_keyword(self) -> None:
        """Single keyword (no comma) still becomes a list."""
        service_info = {
            "documentInfo": {
                "Keywords": "census",
            }
        }
        metadata = extract_arcgis_metadata(
            service_info, source_url="https://example.com/FeatureServer"
        )

        assert metadata.keywords == ["census"]

    # =========================================================================
    # Missing fields
    # =========================================================================

    @pytest.mark.unit
    def test_missing_fields_return_none(self) -> None:
        """Missing fields return None."""
        service_info: dict[str, object] = {}
        metadata = extract_arcgis_metadata(
            service_info, source_url="https://example.com/FeatureServer"
        )

        assert metadata.attribution is None
        assert metadata.description is None
        assert metadata.processing_notes is None
        assert metadata.contact_name is None
        assert metadata.keywords is None
        assert metadata.known_issues is None
        assert metadata.license_info_raw is None

    @pytest.mark.unit
    def test_empty_document_info_returns_none(self) -> None:
        """Empty documentInfo dict returns None for nested fields."""
        service_info: dict[str, object] = {"documentInfo": {}}
        metadata = extract_arcgis_metadata(
            service_info, source_url="https://example.com/FeatureServer"
        )

        assert metadata.contact_name is None
        assert metadata.keywords is None

    @pytest.mark.unit
    def test_missing_document_info_returns_none(self) -> None:
        """Missing documentInfo returns None for nested fields."""
        service_info: dict[str, object] = {"copyrightText": "Test"}
        metadata = extract_arcgis_metadata(
            service_info, source_url="https://example.com/FeatureServer"
        )

        assert metadata.contact_name is None
        assert metadata.keywords is None

    # =========================================================================
    # Empty string handling
    # =========================================================================

    @pytest.mark.unit
    def test_empty_string_treated_as_none(self) -> None:
        """Empty strings are converted to None."""
        service_info = {
            "copyrightText": "",
            "description": "",
            "serviceDescription": "",
            "accessInformation": "",
            "licenseInfo": "",
        }
        metadata = extract_arcgis_metadata(
            service_info, source_url="https://example.com/FeatureServer"
        )

        assert metadata.attribution is None
        assert metadata.description is None
        assert metadata.processing_notes is None
        assert metadata.known_issues is None
        assert metadata.license_info_raw is None

    @pytest.mark.unit
    def test_whitespace_only_string_treated_as_none(self) -> None:
        """Whitespace-only strings are converted to None."""
        service_info = {
            "copyrightText": "   ",
            "description": "\t\n",
        }
        metadata = extract_arcgis_metadata(
            service_info, source_url="https://example.com/FeatureServer"
        )

        assert metadata.attribution is None
        assert metadata.description is None

    @pytest.mark.unit
    def test_empty_keywords_string_returns_none(self) -> None:
        """Empty Keywords string returns None (not empty list)."""
        service_info = {
            "documentInfo": {
                "Keywords": "",
            }
        }
        metadata = extract_arcgis_metadata(
            service_info, source_url="https://example.com/FeatureServer"
        )

        assert metadata.keywords is None

    @pytest.mark.unit
    def test_whitespace_only_keywords_returns_none(self) -> None:
        """Whitespace-only Keywords returns None."""
        service_info = {
            "documentInfo": {
                "Keywords": "   ,  ,  ",
            }
        }
        metadata = extract_arcgis_metadata(
            service_info, source_url="https://example.com/FeatureServer"
        )

        assert metadata.keywords is None

    # =========================================================================
    # Full service info example
    # =========================================================================

    @pytest.mark.unit
    def test_full_service_info_extraction(self) -> None:
        """Extract all fields from a realistic service info dict."""
        service_info = {
            "copyrightText": "City of Philadelphia",
            "description": "Census data including demographics and boundaries",
            "serviceDescription": "Updated quarterly. Source: US Census Bureau.",
            "accessInformation": "Data may be incomplete for recent additions",
            "licenseInfo": "Public domain - freely redistributable",
            "documentInfo": {
                "Author": "Philadelphia GIS Team",
                "Keywords": "census, demographics, boundaries, philly",
            },
        }

        metadata = extract_arcgis_metadata(
            service_info, source_url="https://services.arcgis.com/test/FeatureServer"
        )

        assert metadata.source_url == "https://services.arcgis.com/test/FeatureServer"
        assert metadata.attribution == "City of Philadelphia"
        assert metadata.description == "Census data including demographics and boundaries"
        assert metadata.processing_notes == "Updated quarterly. Source: US Census Bureau."
        assert metadata.known_issues == "Data may be incomplete for recent additions"
        assert metadata.license_info_raw == "Public domain - freely redistributable"
        assert metadata.contact_name == "Philadelphia GIS Team"
        assert metadata.keywords == ["census", "demographics", "boundaries", "philly"]

    # =========================================================================
    # Edge cases
    # =========================================================================

    @pytest.mark.unit
    def test_html_in_description_preserved(self) -> None:
        """HTML content in description is preserved (not stripped)."""
        service_info = {
            "description": "<p>Census <b>data</b> for the city</p>",
        }
        metadata = extract_arcgis_metadata(
            service_info, source_url="https://example.com/FeatureServer"
        )

        assert metadata.description == "<p>Census <b>data</b> for the city</p>"

    @pytest.mark.unit
    def test_non_string_values_ignored(self) -> None:
        """Non-string values (numbers, lists) return None."""
        service_info: dict[str, object] = {
            "copyrightText": 12345,  # Number instead of string
            "description": ["not", "a", "string"],  # List instead of string
        }
        metadata = extract_arcgis_metadata(
            service_info, source_url="https://example.com/FeatureServer"
        )

        assert metadata.attribution is None
        assert metadata.description is None

    @pytest.mark.unit
    def test_document_info_not_dict_returns_none(self) -> None:
        """documentInfo as non-dict value returns None for nested fields."""
        service_info: dict[str, object] = {
            "documentInfo": "not a dict",
        }
        metadata = extract_arcgis_metadata(
            service_info, source_url="https://example.com/FeatureServer"
        )

        assert metadata.contact_name is None
        assert metadata.keywords is None

    @pytest.mark.unit
    def test_preserves_unicode_characters(self) -> None:
        """Unicode characters are preserved."""
        service_info = {
            "copyrightText": "Stadt Munchen",
            "description": "Donnees de recensement",
        }
        metadata = extract_arcgis_metadata(
            service_info, source_url="https://example.com/FeatureServer"
        )

        assert metadata.attribution == "Stadt Munchen"
        assert metadata.description == "Donnees de recensement"

    @pytest.mark.unit
    def test_newlines_in_description_preserved(self) -> None:
        """Newlines in description are preserved."""
        service_info = {
            "description": "Line 1\nLine 2\nLine 3",
        }
        metadata = extract_arcgis_metadata(
            service_info, source_url="https://example.com/FeatureServer"
        )

        assert metadata.description == "Line 1\nLine 2\nLine 3"
