"""Tests for ImageServerMetadataExtracted.to_extracted() conversion.

These tests verify the mapping from ImageServer-specific metadata to the
canonical ExtractedMetadata format used for metadata.yaml seeding.
"""

from __future__ import annotations

import pytest

from portolan_cli.extract.arcgis.imageserver.report import ImageServerMetadataExtracted
from portolan_cli.metadata_extraction import ExtractedMetadata


@pytest.mark.unit
class TestImageServerMetadataExtractedToExtracted:
    """Tests for ImageServerMetadataExtracted.to_extracted() method."""

    def test_returns_extracted_metadata_instance(self) -> None:
        """to_extracted() should return an ExtractedMetadata instance."""
        metadata = ImageServerMetadataExtracted(
            source_url="https://example.com/arcgis/rest/services/Test/ImageServer",
            service_name="Test",
            description=None,
            copyright_text=None,
            pixel_type="U8",
            band_count=3,
            spatial_reference_wkid=4326,
            pixel_size_x=0.5,
            pixel_size_y=0.5,
            extent_bbox=[-180, -90, 180, 90],
        )

        result = metadata.to_extracted()

        assert isinstance(result, ExtractedMetadata)

    def test_maps_source_url(self) -> None:
        """source_url should be passed through."""
        metadata = ImageServerMetadataExtracted(
            source_url="https://example.com/arcgis/rest/services/Imagery/ImageServer",
            service_name="Imagery",
            description=None,
            copyright_text=None,
            pixel_type="F32",
            band_count=1,
            spatial_reference_wkid=3857,
            pixel_size_x=1.0,
            pixel_size_y=1.0,
            extent_bbox=[0, 0, 100, 100],
        )

        result = metadata.to_extracted()

        assert result.source_url == "https://example.com/arcgis/rest/services/Imagery/ImageServer"

    def test_source_type_is_arcgis_imageserver(self) -> None:
        """source_type should be 'arcgis_imageserver'."""
        metadata = ImageServerMetadataExtracted(
            source_url="https://example.com/ImageServer",
            service_name="Test",
            description=None,
            copyright_text=None,
            pixel_type="U8",
            band_count=1,
            spatial_reference_wkid=4326,
            pixel_size_x=1.0,
            pixel_size_y=1.0,
            extent_bbox=[0, 0, 1, 1],
        )

        result = metadata.to_extracted()

        assert result.source_type == "arcgis_imageserver"

    def test_maps_copyright_text_to_attribution(self) -> None:
        """copyright_text should map to attribution."""
        metadata = ImageServerMetadataExtracted(
            source_url="https://example.com/ImageServer",
            service_name="Test",
            description=None,
            copyright_text="(c) 2024 City of Philadelphia",
            pixel_type="U8",
            band_count=3,
            spatial_reference_wkid=4326,
            pixel_size_x=0.5,
            pixel_size_y=0.5,
            extent_bbox=[0, 0, 1, 1],
        )

        result = metadata.to_extracted()

        assert result.attribution == "(c) 2024 City of Philadelphia"

    def test_maps_author_to_contact_name(self) -> None:
        """author should map to contact_name."""
        metadata = ImageServerMetadataExtracted(
            source_url="https://example.com/ImageServer",
            service_name="Test",
            description=None,
            copyright_text=None,
            pixel_type="U8",
            band_count=1,
            spatial_reference_wkid=4326,
            pixel_size_x=1.0,
            pixel_size_y=1.0,
            extent_bbox=[0, 0, 1, 1],
            author="Jane Doe",
        )

        result = metadata.to_extracted()

        assert result.contact_name == "Jane Doe"

    def test_maps_keywords(self) -> None:
        """keywords list should be passed through."""
        metadata = ImageServerMetadataExtracted(
            source_url="https://example.com/ImageServer",
            service_name="Test",
            description=None,
            copyright_text=None,
            pixel_type="U8",
            band_count=1,
            spatial_reference_wkid=4326,
            pixel_size_x=1.0,
            pixel_size_y=1.0,
            extent_bbox=[0, 0, 1, 1],
            keywords=["imagery", "satellite", "landcover"],
        )

        result = metadata.to_extracted()

        assert result.keywords == ["imagery", "satellite", "landcover"]

    def test_keyword_fallback_to_service_name(self) -> None:
        """When keywords is None, fall back to lowercase service_name."""
        metadata = ImageServerMetadataExtracted(
            source_url="https://example.com/ImageServer",
            service_name="OrthoimageryPHL",
            description=None,
            copyright_text=None,
            pixel_type="U8",
            band_count=3,
            spatial_reference_wkid=4326,
            pixel_size_x=0.1,
            pixel_size_y=0.1,
            extent_bbox=[0, 0, 1, 1],
            keywords=None,
        )

        result = metadata.to_extracted()

        assert result.keywords == ["orthoimageryphl"]

    def test_keyword_fallback_with_empty_list(self) -> None:
        """Empty keywords list should use fallback."""
        metadata = ImageServerMetadataExtracted(
            source_url="https://example.com/ImageServer",
            service_name="TestService",
            description=None,
            copyright_text=None,
            pixel_type="U8",
            band_count=1,
            spatial_reference_wkid=4326,
            pixel_size_x=1.0,
            pixel_size_y=1.0,
            extent_bbox=[0, 0, 1, 1],
            keywords=[],  # Empty list
        )

        result = metadata.to_extracted()

        # Empty list is falsy, so fallback should trigger
        assert result.keywords == ["testservice"]

    def test_keyword_fallback_with_no_service_name(self) -> None:
        """When keywords is None and service_name is empty, keywords should be None."""
        metadata = ImageServerMetadataExtracted(
            source_url="https://example.com/ImageServer",
            service_name="",  # Empty service name
            description=None,
            copyright_text=None,
            pixel_type="U8",
            band_count=1,
            spatial_reference_wkid=4326,
            pixel_size_x=1.0,
            pixel_size_y=1.0,
            extent_bbox=[0, 0, 1, 1],
            keywords=None,
        )

        result = metadata.to_extracted()

        assert result.keywords is None

    def test_maps_license_info_to_license_hint(self) -> None:
        """license_info should map to license_hint."""
        metadata = ImageServerMetadataExtracted(
            source_url="https://example.com/ImageServer",
            service_name="Test",
            description=None,
            copyright_text=None,
            pixel_type="U8",
            band_count=1,
            spatial_reference_wkid=4326,
            pixel_size_x=1.0,
            pixel_size_y=1.0,
            extent_bbox=[0, 0, 1, 1],
            license_info="Creative Commons BY 4.0",
        )

        result = metadata.to_extracted()

        assert result.license_hint == "Creative Commons BY 4.0"

    def test_maps_access_information_to_known_issues(self) -> None:
        """access_information should map to known_issues."""
        metadata = ImageServerMetadataExtracted(
            source_url="https://example.com/ImageServer",
            service_name="Test",
            description=None,
            copyright_text=None,
            pixel_type="U8",
            band_count=1,
            spatial_reference_wkid=4326,
            pixel_size_x=1.0,
            pixel_size_y=1.0,
            extent_bbox=[0, 0, 1, 1],
            access_information="Restricted to government use only",
        )

        result = metadata.to_extracted()

        assert result.known_issues == "Restricted to government use only"

    def test_processing_notes_with_description_only(self) -> None:
        """When only description is set, processing_notes should include it and specs."""
        metadata = ImageServerMetadataExtracted(
            source_url="https://example.com/ImageServer",
            service_name="Test",
            description="High-resolution aerial imagery",
            copyright_text=None,
            pixel_type="U16",
            band_count=4,
            spatial_reference_wkid=4326,
            pixel_size_x=0.5,
            pixel_size_y=0.5,
            extent_bbox=[0, 0, 1, 1],
        )

        result = metadata.to_extracted()

        assert "High-resolution aerial imagery" in result.processing_notes
        assert "4 bands" in result.processing_notes
        assert "U16 pixel type" in result.processing_notes
        assert "EPSG:4326" in result.processing_notes

    def test_processing_notes_with_service_description_only(self) -> None:
        """When only service_description is set, processing_notes should include it and specs."""
        metadata = ImageServerMetadataExtracted(
            source_url="https://example.com/ImageServer",
            service_name="Test",
            description=None,
            copyright_text=None,
            pixel_type="F32",
            band_count=1,
            spatial_reference_wkid=3857,
            pixel_size_x=10.0,
            pixel_size_y=10.0,
            extent_bbox=[0, 0, 1, 1],
            service_description="DEM elevation data",
        )

        result = metadata.to_extracted()

        assert "DEM elevation data" in result.processing_notes
        assert "1 bands" in result.processing_notes
        assert "F32 pixel type" in result.processing_notes
        assert "EPSG:3857" in result.processing_notes

    def test_processing_notes_combines_description_and_service_description(self) -> None:
        """processing_notes should combine description and service_description."""
        metadata = ImageServerMetadataExtracted(
            source_url="https://example.com/ImageServer",
            service_name="Test",
            description="Primary description",
            copyright_text=None,
            pixel_type="U8",
            band_count=3,
            spatial_reference_wkid=4326,
            pixel_size_x=1.0,
            pixel_size_y=1.0,
            extent_bbox=[0, 0, 1, 1],
            service_description="Extended service description",
        )

        result = metadata.to_extracted()

        # Both should be present, with description first
        assert "Primary description" in result.processing_notes
        assert "Extended service description" in result.processing_notes
        # Description should come before service_description
        desc_idx = result.processing_notes.index("Primary description")
        svc_idx = result.processing_notes.index("Extended service description")
        assert desc_idx < svc_idx

    def test_processing_notes_includes_technical_specs(self) -> None:
        """processing_notes should always include technical specs."""
        metadata = ImageServerMetadataExtracted(
            source_url="https://example.com/ImageServer",
            service_name="Test",
            description=None,
            copyright_text=None,
            pixel_type="U8",
            band_count=3,
            spatial_reference_wkid=4326,
            pixel_size_x=0.5,
            pixel_size_y=0.5,
            extent_bbox=[0, 0, 1, 1],
        )

        result = metadata.to_extracted()

        assert "Technical specs:" in result.processing_notes
        assert "3 bands" in result.processing_notes
        assert "U8 pixel type" in result.processing_notes
        assert "EPSG:4326" in result.processing_notes

    def test_processing_notes_without_spatial_reference(self) -> None:
        """processing_notes should not include EPSG when wkid is None."""
        metadata = ImageServerMetadataExtracted(
            source_url="https://example.com/ImageServer",
            service_name="Test",
            description=None,
            copyright_text=None,
            pixel_type="U8",
            band_count=3,
            spatial_reference_wkid=None,  # No spatial reference
            pixel_size_x=1.0,
            pixel_size_y=1.0,
            extent_bbox=[0, 0, 1, 1],
        )

        result = metadata.to_extracted()

        assert "EPSG:" not in result.processing_notes
        assert "3 bands" in result.processing_notes
        assert "U8 pixel type" in result.processing_notes

    def test_all_none_optional_fields(self) -> None:
        """When all optional fields are None, result should have minimal data."""
        metadata = ImageServerMetadataExtracted(
            source_url="https://example.com/ImageServer",
            service_name="MinimalTest",
            description=None,
            copyright_text=None,
            pixel_type="U8",
            band_count=1,
            spatial_reference_wkid=4326,
            pixel_size_x=1.0,
            pixel_size_y=1.0,
            extent_bbox=[0, 0, 1, 1],
            service_description=None,
            author=None,
            keywords=None,
            license_info=None,
            access_information=None,
        )

        result = metadata.to_extracted()

        assert result.source_url == "https://example.com/ImageServer"
        assert result.source_type == "arcgis_imageserver"
        assert result.attribution is None
        assert result.contact_name is None
        assert result.known_issues is None
        assert result.license_hint is None
        # Keywords should fallback to service_name
        assert result.keywords == ["minimaltest"]
        # processing_notes should have technical specs
        assert "Technical specs:" in result.processing_notes

    def test_full_metadata_mapping(self) -> None:
        """Test complete field mapping with all fields populated."""
        metadata = ImageServerMetadataExtracted(
            source_url="https://gis.cityofphiladelphia.gov/arcgis/rest/services/Imagery/ImageServer",
            service_name="PhillyOrtho2024",
            description="2024 orthoimagery for Philadelphia",
            copyright_text="City of Philadelphia",
            pixel_type="U8",
            band_count=4,
            spatial_reference_wkid=2272,
            pixel_size_x=0.25,
            pixel_size_y=0.25,
            extent_bbox=[-75.3, 39.8, -74.9, 40.2],
            service_description="6-inch resolution RGBI imagery",
            author="GIS Division",
            keywords=["imagery", "ortho", "2024"],
            license_info="PDDL",
            access_information="Public access, no restrictions",
        )

        result = metadata.to_extracted()

        assert (
            result.source_url
            == "https://gis.cityofphiladelphia.gov/arcgis/rest/services/Imagery/ImageServer"
        )
        assert result.source_type == "arcgis_imageserver"
        assert result.attribution == "City of Philadelphia"
        assert result.keywords == ["imagery", "ortho", "2024"]
        assert result.contact_name == "GIS Division"
        assert result.license_hint == "PDDL"
        assert result.known_issues == "Public access, no restrictions"
        # processing_notes should have all content
        assert "2024 orthoimagery for Philadelphia" in result.processing_notes
        assert "6-inch resolution RGBI imagery" in result.processing_notes
        assert "4 bands" in result.processing_notes
        assert "U8 pixel type" in result.processing_notes
        assert "EPSG:2272" in result.processing_notes
