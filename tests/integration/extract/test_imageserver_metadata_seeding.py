"""Integration tests for ImageServer metadata seeding.

Tests that ImageServer extraction automatically seeds metadata.yaml
with harvested service metadata (Wave 3B).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
import yaml

from portolan_cli.extract.arcgis.imageserver.report import (
    ImageServerExtractionReport,
    ImageServerExtractionSummary,
    ImageServerMetadataExtracted,
    TileResult,
)

pytestmark = [pytest.mark.integration]


def make_imageserver_metadata_extracted(
    source_url: str = "https://services.arcgis.com/test/ImageServer",
    service_name: str = "TestImageServer",
    description: str | None = "Test imagery service",
    copyright_text: str | None = "City of Test",
    pixel_type: str = "U8",
    band_count: int = 3,
    spatial_reference_wkid: int | None = 4326,
    pixel_size_x: float = 0.5,
    pixel_size_y: float = 0.5,
    service_description: str | None = "Extracted from municipal GIS",
    author: str | None = "Test Author",
    keywords: list[str] | None = None,
    license_info: str | None = None,
    access_information: str | None = None,
) -> ImageServerMetadataExtracted:
    """Create ImageServerMetadataExtracted with sensible defaults."""
    return ImageServerMetadataExtracted(
        source_url=source_url,
        service_name=service_name,
        description=description,
        copyright_text=copyright_text,
        pixel_type=pixel_type,
        band_count=band_count,
        spatial_reference_wkid=spatial_reference_wkid,
        pixel_size_x=pixel_size_x,
        pixel_size_y=pixel_size_y,
        extent_bbox=[-122.5, 37.5, -122.0, 38.0],
        service_description=service_description,
        author=author,
        keywords=keywords,
        license_info=license_info,
        access_information=access_information,
    )


def make_imageserver_report(
    metadata_extracted: ImageServerMetadataExtracted,
    tiles: list[TileResult] | None = None,
) -> ImageServerExtractionReport:
    """Create an ImageServerExtractionReport with sensible defaults."""
    if tiles is None:
        tiles = [
            TileResult(
                tile_id="tile_0_0",
                status="success",
                size_bytes=10000,
                duration_seconds=1.0,
                output_path="tiles/tile_0_0/tile_0_0.tif",
                error=None,
                attempts=1,
            ),
        ]

    succeeded = sum(1 for t in tiles if t.status == "success")
    failed = sum(1 for t in tiles if t.status == "failed")
    skipped = sum(1 for t in tiles if t.status == "skipped")

    return ImageServerExtractionReport(
        extraction_type="imageserver",
        extraction_date="2026-04-07T12:00:00Z",
        source_url=metadata_extracted.source_url,
        portolan_version="0.1.0",
        riocogeo_version="1.4.0",
        metadata_extracted=metadata_extracted,
        tiles=tiles,
        summary=ImageServerExtractionSummary(
            total_tiles=len(tiles),
            succeeded=succeeded,
            failed=failed,
            skipped=skipped,
            total_size_bytes=sum(t.size_bytes or 0 for t in tiles),
            total_duration_seconds=sum(t.duration_seconds or 0 for t in tiles),
        ),
    )


class TestImageServerMetadataExtractedToExtracted:
    """Tests for ImageServerMetadataExtracted.to_extracted() method (Wave 2B)."""

    def test_to_extracted_includes_source_url(self) -> None:
        """to_extracted() maps source_url correctly."""
        metadata = make_imageserver_metadata_extracted(
            source_url="https://services.arcgis.com/imagery/ImageServer"
        )
        extracted = metadata.to_extracted()
        assert extracted.source_url == "https://services.arcgis.com/imagery/ImageServer"

    def test_to_extracted_includes_attribution(self) -> None:
        """to_extracted() maps copyright_text to attribution."""
        metadata = make_imageserver_metadata_extracted(copyright_text="City of Philadelphia")
        extracted = metadata.to_extracted()
        assert extracted.attribution == "City of Philadelphia"

    def test_to_extracted_includes_processing_notes_with_raster_specs(self) -> None:
        """to_extracted() includes raster technical specs in processing_notes."""
        metadata = make_imageserver_metadata_extracted(
            pixel_type="U16",
            band_count=4,
            pixel_size_x=0.25,
            pixel_size_y=0.25,
            service_description="High-res imagery",
        )
        extracted = metadata.to_extracted()

        # Should include both service_description and technical specs
        assert "High-res imagery" in extracted.processing_notes
        assert "pixel_type: U16" in extracted.processing_notes
        assert "band_count: 4" in extracted.processing_notes
        assert "pixel_size" in extracted.processing_notes

    def test_to_extracted_includes_contact_name(self) -> None:
        """to_extracted() maps author to contact_name."""
        metadata = make_imageserver_metadata_extracted(author="Test Author")
        extracted = metadata.to_extracted()
        assert extracted.contact_name == "Test Author"

    def test_to_extracted_includes_keywords(self) -> None:
        """to_extracted() maps keywords correctly."""
        metadata = make_imageserver_metadata_extracted(keywords=["imagery", "aerial"])
        extracted = metadata.to_extracted()
        assert extracted.keywords == ["imagery", "aerial"]

    def test_to_extracted_handles_none_values(self) -> None:
        """to_extracted() handles None values gracefully."""
        metadata = make_imageserver_metadata_extracted(
            description=None,
            copyright_text=None,
            author=None,
            keywords=None,
            service_description=None,
        )
        extracted = metadata.to_extracted()

        # Should still have source_url
        assert extracted.source_url is not None
        # Optional fields should be None or have minimal processing_notes
        assert extracted.attribution is None
        assert extracted.contact_name is None
        assert extracted.keywords is None

    def test_to_extracted_source_type_is_arcgis_imageserver(self) -> None:
        """to_extracted() includes source_type as arcgis_imageserver (consistent with featureserver)."""
        metadata = make_imageserver_metadata_extracted()
        extracted = metadata.to_extracted()
        assert extracted.source_type == "arcgis_imageserver"


class TestSeedMetadataYaml:
    """Tests for seed_metadata_yaml function (Wave 1 infrastructure)."""

    def test_seed_creates_metadata_yaml(self, tmp_path: Path) -> None:
        """seed_metadata_yaml creates .portolan/metadata.yaml file."""
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        metadata = make_imageserver_metadata_extracted()
        extracted = metadata.to_extracted()

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        metadata_path = portolan_dir / "metadata.yaml"

        result = seed_metadata_yaml(extracted, metadata_path)

        assert result is True
        assert metadata_path.exists()

    def test_seed_includes_source_url(self, tmp_path: Path) -> None:
        """Seeded metadata.yaml contains source_url from service."""
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        metadata = make_imageserver_metadata_extracted(
            source_url="https://services.arcgis.com/test/ImageServer"
        )
        extracted = metadata.to_extracted()

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        metadata_path = portolan_dir / "metadata.yaml"

        seed_metadata_yaml(extracted, metadata_path)

        content = yaml.safe_load(metadata_path.read_text())
        assert content["source_url"] == "https://services.arcgis.com/test/ImageServer"

    def test_seed_includes_processing_notes(self, tmp_path: Path) -> None:
        """Seeded metadata.yaml contains processing_notes with technical specs."""
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        metadata = make_imageserver_metadata_extracted(
            pixel_type="F32",
            band_count=1,
            service_description="Elevation data",
        )
        extracted = metadata.to_extracted()

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        metadata_path = portolan_dir / "metadata.yaml"

        seed_metadata_yaml(extracted, metadata_path)

        content = yaml.safe_load(metadata_path.read_text())
        processing_notes = content.get("processing_notes", "")
        assert "Elevation data" in processing_notes
        assert "F32" in processing_notes

    def test_seed_does_not_overwrite_existing(self, tmp_path: Path) -> None:
        """seed_metadata_yaml does NOT overwrite existing metadata.yaml."""
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        metadata = make_imageserver_metadata_extracted(
            source_url="https://new-service.com/ImageServer"
        )
        extracted = metadata.to_extracted()

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        metadata_path = portolan_dir / "metadata.yaml"

        # Pre-create with different content
        existing_content = {
            "source_url": "https://existing-service.com/ImageServer",
            "contact": {"name": "Existing User", "email": "existing@example.com"},
            "license": "CC-BY-4.0",
        }
        metadata_path.write_text(yaml.dump(existing_content))

        # Attempt to seed - should return False
        result = seed_metadata_yaml(extracted, metadata_path)

        assert result is False
        # Content should be unchanged
        content = yaml.safe_load(metadata_path.read_text())
        assert content["source_url"] == "https://existing-service.com/ImageServer"

    def test_seed_includes_attribution_from_copyright(self, tmp_path: Path) -> None:
        """Seeded metadata.yaml contains attribution from copyright_text."""
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        metadata = make_imageserver_metadata_extracted(copyright_text="City of Philadelphia, 2026")
        extracted = metadata.to_extracted()

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        metadata_path = portolan_dir / "metadata.yaml"

        seed_metadata_yaml(extracted, metadata_path)

        content = yaml.safe_load(metadata_path.read_text())
        assert content.get("attribution") == "City of Philadelphia, 2026"

    def test_seed_includes_keywords(self, tmp_path: Path) -> None:
        """Seeded metadata.yaml contains keywords from service."""
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        metadata = make_imageserver_metadata_extracted(keywords=["aerial", "imagery", "ortho"])
        extracted = metadata.to_extracted()

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        metadata_path = portolan_dir / "metadata.yaml"

        seed_metadata_yaml(extracted, metadata_path)

        content = yaml.safe_load(metadata_path.read_text())
        assert content.get("keywords") == ["aerial", "imagery", "ortho"]

    def test_seed_includes_placeholder_contact_and_license(self, tmp_path: Path) -> None:
        """Seeded metadata.yaml includes placeholder for required contact/license fields."""
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        metadata = make_imageserver_metadata_extracted()
        extracted = metadata.to_extracted()

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        metadata_path = portolan_dir / "metadata.yaml"

        seed_metadata_yaml(extracted, metadata_path)

        content = yaml.safe_load(metadata_path.read_text())
        # Should have placeholder structure for required fields
        assert "contact" in content
        assert "license" in content


class TestImageServerExtractionSeeding:
    """Tests for end-to-end ImageServer extraction with metadata seeding."""

    def test_extraction_creates_metadata_yaml(self, tmp_path: Path) -> None:
        """extract_imageserver creates .portolan/metadata.yaml after extraction."""
        import asyncio
        from unittest.mock import MagicMock

        from portolan_cli.extract.arcgis.imageserver.extractor import (
            ExtractionConfig,
            extract_imageserver,
        )

        output_dir = tmp_path / "imageserver_output"

        # Mock the discovery and extraction
        mock_metadata = MagicMock()
        mock_metadata.name = "TestImagery"
        mock_metadata.pixel_type = "U8"
        mock_metadata.band_count = 3
        mock_metadata.pixel_size_x = 0.5
        mock_metadata.pixel_size_y = 0.5
        mock_metadata.full_extent = {"xmin": -122.5, "ymin": 37.5, "xmax": -122.0, "ymax": 38.0}
        mock_metadata.description = "Test imagery"
        mock_metadata.copyright_text = "City of Test"
        mock_metadata.service_description = "Municipal aerial imagery"
        mock_metadata.author = "GIS Dept"
        mock_metadata.keywords = ["aerial", "imagery"]
        mock_metadata.license_info = None
        mock_metadata.access_information = None

        with patch(
            "portolan_cli.extract.arcgis.imageserver.extractor.discover_imageserver",
            new_callable=AsyncMock,
            return_value=mock_metadata,
        ):
            with patch(
                "portolan_cli.extract.arcgis.imageserver.extractor._extract_all_tiles",
                new_callable=AsyncMock,
            ) as mock_extract:
                # Mock successful extraction with minimal results
                from portolan_cli.extract.arcgis.imageserver.extractor import _ProcessingStats

                mock_stats = _ProcessingStats()
                mock_stats.tiles_downloaded = 0
                mock_stats.tile_results = []
                mock_extract.return_value = mock_stats

                # Skip catalog init for this test
                config = ExtractionConfig(dry_run=False, raw=True)

                asyncio.run(
                    extract_imageserver(
                        url="https://services.arcgis.com/test/ImageServer",
                        output_dir=output_dir,
                        config=config,
                    )
                )

        # Verify metadata.yaml was created
        metadata_path = output_dir / ".portolan" / "metadata.yaml"
        assert metadata_path.exists(), "metadata.yaml should be created after extraction"

    def test_extraction_does_not_overwrite_existing_metadata_yaml(self, tmp_path: Path) -> None:
        """extract_imageserver does NOT overwrite existing metadata.yaml."""
        import asyncio
        from unittest.mock import MagicMock

        from portolan_cli.extract.arcgis.imageserver.extractor import (
            ExtractionConfig,
            extract_imageserver,
        )

        output_dir = tmp_path / "imageserver_output"
        portolan_dir = output_dir / ".portolan"
        portolan_dir.mkdir(parents=True)

        # Pre-create metadata.yaml with custom content
        metadata_path = portolan_dir / "metadata.yaml"
        existing_content = {
            "source_url": "https://existing-service.com/ImageServer",
            "contact": {"name": "Custom User", "email": "custom@example.com"},
            "license": "MIT",
        }
        metadata_path.write_text(yaml.dump(existing_content))

        # Mock the discovery and extraction
        mock_metadata = MagicMock()
        mock_metadata.name = "NewImagery"
        mock_metadata.pixel_type = "U8"
        mock_metadata.band_count = 3
        mock_metadata.pixel_size_x = 0.5
        mock_metadata.pixel_size_y = 0.5
        mock_metadata.full_extent = {"xmin": -122.5, "ymin": 37.5, "xmax": -122.0, "ymax": 38.0}
        mock_metadata.description = "New imagery"
        mock_metadata.copyright_text = "New Copyright"
        mock_metadata.service_description = None
        mock_metadata.author = None
        mock_metadata.keywords = None
        mock_metadata.license_info = None
        mock_metadata.access_information = None

        with patch(
            "portolan_cli.extract.arcgis.imageserver.extractor.discover_imageserver",
            new_callable=AsyncMock,
            return_value=mock_metadata,
        ):
            with patch(
                "portolan_cli.extract.arcgis.imageserver.extractor._extract_all_tiles",
                new_callable=AsyncMock,
            ) as mock_extract:
                from portolan_cli.extract.arcgis.imageserver.extractor import _ProcessingStats

                mock_stats = _ProcessingStats()
                mock_stats.tiles_downloaded = 0
                mock_stats.tile_results = []
                mock_extract.return_value = mock_stats

                config = ExtractionConfig(dry_run=False, raw=True)

                asyncio.run(
                    extract_imageserver(
                        url="https://services.arcgis.com/new/ImageServer",
                        output_dir=output_dir,
                        config=config,
                    )
                )

        # metadata.yaml should be unchanged
        content = yaml.safe_load(metadata_path.read_text())
        assert content["source_url"] == "https://existing-service.com/ImageServer"
        assert content["contact"]["name"] == "Custom User"

    def test_extraction_metadata_contains_service_url(self, tmp_path: Path) -> None:
        """Seeded metadata.yaml contains the source ImageServer URL."""
        import asyncio
        from unittest.mock import MagicMock

        from portolan_cli.extract.arcgis.imageserver.extractor import (
            ExtractionConfig,
            extract_imageserver,
        )

        output_dir = tmp_path / "imageserver_output"

        mock_metadata = MagicMock()
        mock_metadata.name = "TestImagery"
        mock_metadata.pixel_type = "F32"
        mock_metadata.band_count = 1
        mock_metadata.pixel_size_x = 1.0
        mock_metadata.pixel_size_y = 1.0
        mock_metadata.full_extent = {"xmin": 0, "ymin": 0, "xmax": 100, "ymax": 100}
        mock_metadata.description = "Elevation"
        mock_metadata.copyright_text = None
        mock_metadata.service_description = "DEM data"
        mock_metadata.author = None
        mock_metadata.keywords = None
        mock_metadata.license_info = None
        mock_metadata.access_information = None

        with patch(
            "portolan_cli.extract.arcgis.imageserver.extractor.discover_imageserver",
            new_callable=AsyncMock,
            return_value=mock_metadata,
        ):
            with patch(
                "portolan_cli.extract.arcgis.imageserver.extractor._extract_all_tiles",
                new_callable=AsyncMock,
            ) as mock_extract:
                from portolan_cli.extract.arcgis.imageserver.extractor import _ProcessingStats

                mock_stats = _ProcessingStats()
                mock_stats.tiles_downloaded = 0
                mock_stats.tile_results = []
                mock_extract.return_value = mock_stats

                config = ExtractionConfig(dry_run=False, raw=True)

                asyncio.run(
                    extract_imageserver(
                        url="https://elevation.arcgis.com/DEM/ImageServer",
                        output_dir=output_dir,
                        config=config,
                    )
                )

        metadata_path = output_dir / ".portolan" / "metadata.yaml"
        content = yaml.safe_load(metadata_path.read_text())
        assert content["source_url"] == "https://elevation.arcgis.com/DEM/ImageServer"
