"""Tests for ArcGISMetadata.to_extracted() method.

These tests verify that ArcGISMetadata correctly converts to the canonical
ExtractedMetadata type for metadata.yaml seeding.
"""

from __future__ import annotations

import pytest

from portolan_cli.extract.arcgis.metadata import ArcGISMetadata
from portolan_cli.metadata_extraction import ExtractedMetadata


class TestToExtracted:
    """Tests for ArcGISMetadata.to_extracted() method."""

    @pytest.mark.unit
    def test_returns_extracted_metadata_instance(self) -> None:
        """to_extracted() returns an ExtractedMetadata instance."""
        arcgis = ArcGISMetadata(
            source_url="https://example.com/FeatureServer",
            attribution=None,
            description=None,
            processing_notes=None,
            contact_name=None,
            keywords=None,
            known_issues=None,
            license_info_raw=None,
        )

        result = arcgis.to_extracted()

        assert isinstance(result, ExtractedMetadata)

    @pytest.mark.unit
    def test_source_url_mapped(self) -> None:
        """source_url is mapped to ExtractedMetadata.source_url."""
        arcgis = ArcGISMetadata(
            source_url="https://services.arcgis.com/test/FeatureServer",
            attribution=None,
            description=None,
            processing_notes=None,
            contact_name=None,
            keywords=None,
            known_issues=None,
            license_info_raw=None,
        )

        result = arcgis.to_extracted()

        assert result.source_url == "https://services.arcgis.com/test/FeatureServer"

    @pytest.mark.unit
    def test_source_type_set_to_arcgis_featureserver(self) -> None:
        """source_type is set to 'arcgis_featureserver'."""
        arcgis = ArcGISMetadata(
            source_url="https://example.com/FeatureServer",
            attribution=None,
            description=None,
            processing_notes=None,
            contact_name=None,
            keywords=None,
            known_issues=None,
            license_info_raw=None,
        )

        result = arcgis.to_extracted()

        assert result.source_type == "arcgis_featureserver"

    @pytest.mark.unit
    def test_attribution_mapped(self) -> None:
        """attribution is mapped to ExtractedMetadata.attribution."""
        arcgis = ArcGISMetadata(
            source_url="https://example.com/FeatureServer",
            attribution="City of Philadelphia",
            description=None,
            processing_notes=None,
            contact_name=None,
            keywords=None,
            known_issues=None,
            license_info_raw=None,
        )

        result = arcgis.to_extracted()

        assert result.attribution == "City of Philadelphia"

    @pytest.mark.unit
    def test_keywords_mapped(self) -> None:
        """keywords list is mapped to ExtractedMetadata.keywords."""
        arcgis = ArcGISMetadata(
            source_url="https://example.com/FeatureServer",
            attribution=None,
            description=None,
            processing_notes=None,
            contact_name=None,
            keywords=["census", "demographics", "population"],
            known_issues=None,
            license_info_raw=None,
        )

        result = arcgis.to_extracted()

        assert result.keywords == ["census", "demographics", "population"]

    @pytest.mark.unit
    def test_contact_name_mapped(self) -> None:
        """contact_name is mapped to ExtractedMetadata.contact_name."""
        arcgis = ArcGISMetadata(
            source_url="https://example.com/FeatureServer",
            attribution=None,
            description=None,
            processing_notes=None,
            contact_name="GIS Department",
            keywords=None,
            known_issues=None,
            license_info_raw=None,
        )

        result = arcgis.to_extracted()

        assert result.contact_name == "GIS Department"

    @pytest.mark.unit
    def test_processing_notes_mapped(self) -> None:
        """processing_notes is mapped to ExtractedMetadata.processing_notes."""
        arcgis = ArcGISMetadata(
            source_url="https://example.com/FeatureServer",
            attribution=None,
            description=None,
            processing_notes="Updated quarterly from Census API",
            contact_name=None,
            keywords=None,
            known_issues=None,
            license_info_raw=None,
        )

        result = arcgis.to_extracted()

        assert result.processing_notes == "Updated quarterly from Census API"

    @pytest.mark.unit
    def test_known_issues_mapped(self) -> None:
        """known_issues is mapped to ExtractedMetadata.known_issues."""
        arcgis = ArcGISMetadata(
            source_url="https://example.com/FeatureServer",
            attribution=None,
            description=None,
            processing_notes=None,
            contact_name=None,
            keywords=None,
            known_issues="Limited to non-commercial use",
            license_info_raw=None,
        )

        result = arcgis.to_extracted()

        assert result.known_issues == "Limited to non-commercial use"

    @pytest.mark.unit
    def test_license_info_raw_mapped_to_license_hint(self) -> None:
        """license_info_raw is mapped to ExtractedMetadata.license_hint."""
        arcgis = ArcGISMetadata(
            source_url="https://example.com/FeatureServer",
            attribution=None,
            description=None,
            processing_notes=None,
            contact_name=None,
            keywords=None,
            known_issues=None,
            license_info_raw="Public domain - no restrictions",
        )

        result = arcgis.to_extracted()

        assert result.license_hint == "Public domain - no restrictions"

    @pytest.mark.unit
    def test_none_fields_preserved(self) -> None:
        """None values in ArcGISMetadata are preserved as None in ExtractedMetadata."""
        arcgis = ArcGISMetadata(
            source_url="https://example.com/FeatureServer",
            attribution=None,
            description=None,
            processing_notes=None,
            contact_name=None,
            keywords=None,
            known_issues=None,
            license_info_raw=None,
        )

        result = arcgis.to_extracted()

        assert result.attribution is None
        assert result.keywords is None
        assert result.contact_name is None
        assert result.processing_notes is None
        assert result.known_issues is None
        assert result.license_hint is None

    @pytest.mark.unit
    def test_full_mapping(self) -> None:
        """All fields are correctly mapped in a full extraction."""
        arcgis = ArcGISMetadata(
            source_url="https://services.arcgis.com/philly/FeatureServer",
            attribution="City of Philadelphia",
            description="Census data for the city",  # Note: description is NOT mapped
            processing_notes="Updated quarterly",
            contact_name="GIS Team",
            keywords=["census", "demographics"],
            known_issues="May be incomplete",
            license_info_raw="CC-BY-4.0",
        )

        result = arcgis.to_extracted()

        assert result.source_url == "https://services.arcgis.com/philly/FeatureServer"
        assert result.source_type == "arcgis_featureserver"
        assert result.attribution == "City of Philadelphia"
        assert result.keywords == ["census", "demographics"]
        assert result.contact_name == "GIS Team"
        assert result.processing_notes == "Updated quarterly"
        assert result.known_issues == "May be incomplete"
        assert result.license_hint == "CC-BY-4.0"

    @pytest.mark.unit
    def test_description_not_mapped(self) -> None:
        """description field from ArcGIS is NOT mapped (goes to STAC, not metadata.yaml).

        Per the design, ArcGIS description maps to STAC description, not
        to ExtractedMetadata (which seeds metadata.yaml).
        """
        arcgis = ArcGISMetadata(
            source_url="https://example.com/FeatureServer",
            attribution=None,
            description="This should not appear in ExtractedMetadata",
            processing_notes=None,
            contact_name=None,
            keywords=None,
            known_issues=None,
            license_info_raw=None,
        )

        result = arcgis.to_extracted()

        # ExtractedMetadata doesn't have a description field
        # This test documents the intentional design decision
        assert not hasattr(result, "description")
