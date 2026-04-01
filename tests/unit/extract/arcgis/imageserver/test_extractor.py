"""Tests for ImageServer extraction orchestrator.

Tests verify the full extraction pipeline using Wave 1 data models.
Uses mocking for HTTP and COG conversion to keep tests fast and isolated.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from portolan_cli.extract.arcgis.imageserver.discovery import ImageServerMetadata
from portolan_cli.extract.arcgis.imageserver.extractor import (
    ExtractionConfig,
    ExtractionResult,
    ImageServerExtractionError,
    download_tile,
    extract_imageserver,
)
from portolan_cli.extract.arcgis.imageserver.tiling import TileSpec

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_metadata() -> ImageServerMetadata:
    """Standard ImageServer metadata for testing."""
    return ImageServerMetadata(
        name="TestImageServer",
        band_count=1,
        pixel_type="F32",
        pixel_size_x=10.0,
        pixel_size_y=10.0,
        full_extent={
            "xmin": 0,
            "ymin": 0,
            "xmax": 10000,
            "ymax": 10000,
            "spatialReference": {"wkid": 4326},
        },
        max_image_width=4096,
        max_image_height=4096,
        capabilities=["Image", "Metadata"],
        description="Test service",
    )


@pytest.fixture
def small_extent_metadata() -> ImageServerMetadata:
    """Metadata with small extent (single tile)."""
    return ImageServerMetadata(
        name="SmallService",
        band_count=1,
        pixel_type="U8",
        pixel_size_x=1.0,
        pixel_size_y=1.0,
        full_extent={
            "xmin": 0,
            "ymin": 0,
            "xmax": 100,
            "ymax": 100,
            "spatialReference": {"wkid": 4326},
        },
        max_image_width=4096,
        max_image_height=4096,
        capabilities=["Image"],
    )


@pytest.fixture
def sample_tile() -> TileSpec:
    """Sample tile for download tests."""
    return TileSpec(
        x=0,
        y=0,
        bbox=(0.0, 0.0, 4096.0, 4096.0),
        width_px=4096,
        height_px=4096,
    )


# =============================================================================
# ExtractionConfig Tests
# =============================================================================


@pytest.mark.unit
class TestExtractionConfig:
    """Tests for ExtractionConfig dataclass."""

    def test_default_tile_size(self) -> None:
        """Default tile size is 4096."""
        config = ExtractionConfig()
        assert config.tile_size == 4096

    def test_default_compression(self) -> None:
        """Default compression is DEFLATE (via cog_settings)."""
        config = ExtractionConfig()
        # compression is now in cog_settings (per ADR-0019)
        assert config.cog_settings.compression == "DEFLATE"
        # Legacy field is None when using cog_settings
        assert config.compression is None

    def test_default_max_retries(self) -> None:
        """Default max retries is 3."""
        config = ExtractionConfig()
        assert config.max_retries == 3

    def test_default_dry_run_false(self) -> None:
        """Default dry_run is False."""
        config = ExtractionConfig()
        assert config.dry_run is False

    def test_custom_values(self) -> None:
        """Custom config values are preserved."""
        from portolan_cli.conversion_config import CogSettings

        config = ExtractionConfig(
            tile_size=2048,
            cog_settings=CogSettings(compression="JPEG", quality=85),
            max_retries=5,
            dry_run=True,
        )
        assert config.tile_size == 2048
        assert config.cog_settings.compression == "JPEG"
        assert config.cog_settings.quality == 85
        assert config.max_retries == 5
        assert config.dry_run is True


# =============================================================================
# ExtractionResult Tests
# =============================================================================


@pytest.mark.unit
class TestExtractionResult:
    """Tests for ExtractionResult dataclass."""

    def test_result_attributes(self) -> None:
        """ExtractionResult has expected attributes."""
        result = ExtractionResult(
            collection_path=Path("/output/collection.json"),
            items_created=10,
            tiles_downloaded=10,
            tiles_skipped=0,
            total_bytes=1024000,
        )
        assert result.collection_path == Path("/output/collection.json")
        assert result.items_created == 10
        assert result.tiles_downloaded == 10
        assert result.tiles_skipped == 0
        assert result.total_bytes == 1024000


# =============================================================================
# Error Handling Tests
# =============================================================================


@pytest.mark.unit
class TestErrorHandling:
    """Tests for error handling in extraction."""

    def test_extraction_error_is_exception(self) -> None:
        """ImageServerExtractionError is an Exception."""
        error = ImageServerExtractionError("Test error")
        assert isinstance(error, Exception)
        assert str(error) == "Test error"

    def test_extraction_error_with_cause(self) -> None:
        """ImageServerExtractionError can wrap another exception."""
        cause = ValueError("Original error")
        error = ImageServerExtractionError("Wrapped error")
        error.__cause__ = cause
        assert error.__cause__ is cause


# =============================================================================
# Async Function Tests (proper pytest-asyncio)
# =============================================================================


@pytest.mark.unit
class TestDownloadTile:
    """Tests for download_tile async function."""

    # Valid TIFF header (little-endian) for mock responses
    # Magic bytes II (0x4949) + version 42 (0x002A) + offset to first IFD
    VALID_TIFF_HEADER = b"II\x2a\x00" + b"\x08\x00\x00\x00" + b"\x00" * 100

    @pytest.mark.asyncio
    async def test_download_builds_correct_url(self, sample_tile: TileSpec, tmp_path: Path) -> None:
        """Download constructs correct exportImage URL."""
        mock_client = AsyncMock()
        mock_response = AsyncMock()
        # Use valid TIFF header to pass validation
        mock_response.content = self.VALID_TIFF_HEADER
        mock_response.raise_for_status = MagicMock()
        mock_client.get.return_value = mock_response

        output_path = tmp_path / "out.tif"
        url = "https://example.com/ImageServer"
        await download_tile(url, sample_tile, output_path, mock_client)

        # Verify URL was called
        mock_client.get.assert_called_once()
        call_args = mock_client.get.call_args
        called_url = call_args[0][0] if call_args[0] else str(call_args)
        # URL should contain exportImage
        assert "exportImage" in called_url

    @pytest.mark.asyncio
    async def test_download_returns_bytes_count(
        self, sample_tile: TileSpec, tmp_path: Path
    ) -> None:
        """Download returns number of bytes downloaded."""
        mock_client = AsyncMock()
        mock_response = AsyncMock()
        # Use valid TIFF header + padding to get 1000 bytes
        content = self.VALID_TIFF_HEADER + b"x" * (1000 - len(self.VALID_TIFF_HEADER))
        mock_response.content = content
        mock_response.raise_for_status = MagicMock()
        mock_client.get.return_value = mock_response

        output_path = tmp_path / "out.tif"
        result = await download_tile(
            "https://example.com/ImageServer",
            sample_tile,
            output_path,
            mock_client,
        )

        assert result == 1000
        # Verify file was actually written
        assert output_path.exists()
        assert output_path.read_bytes() == content


@pytest.mark.unit
class TestExtractImageserver:
    """Tests for extract_imageserver async function."""

    @pytest.mark.asyncio
    async def test_dry_run_returns_result(
        self, tmp_path: Path, small_extent_metadata: ImageServerMetadata
    ) -> None:
        """Dry run returns ExtractionResult without downloads."""
        with patch(
            "portolan_cli.extract.arcgis.imageserver.extractor.discover_imageserver",
            new_callable=AsyncMock,
        ) as mock_discover:
            mock_discover.return_value = small_extent_metadata

            config = ExtractionConfig(dry_run=True)
            result = await extract_imageserver(
                "https://example.com/ImageServer",
                tmp_path,
                config=config,
            )

        assert isinstance(result, ExtractionResult)
        assert result.tiles_downloaded == 0

    @pytest.mark.asyncio
    async def test_extraction_with_bbox_filter(
        self, tmp_path: Path, sample_metadata: ImageServerMetadata
    ) -> None:
        """Extraction accepts bbox filter parameter."""
        with patch(
            "portolan_cli.extract.arcgis.imageserver.extractor.discover_imageserver",
            new_callable=AsyncMock,
        ) as mock_discover:
            mock_discover.return_value = sample_metadata

            config = ExtractionConfig(dry_run=True)
            result = await extract_imageserver(
                "https://example.com/ImageServer",
                tmp_path,
                config=config,
                bbox=(0, 0, 100, 100),
            )

        assert isinstance(result, ExtractionResult)

    @pytest.mark.asyncio
    async def test_discovery_error_propagates(self, tmp_path: Path) -> None:
        """Discovery errors propagate correctly."""
        from portolan_cli.extract.arcgis.imageserver.discovery import (
            ImageServerDiscoveryError,
        )

        with patch(
            "portolan_cli.extract.arcgis.imageserver.extractor.discover_imageserver",
            new_callable=AsyncMock,
        ) as mock_discover:
            mock_discover.side_effect = ImageServerDiscoveryError("Connection failed")

            with pytest.raises((ImageServerExtractionError, ImageServerDiscoveryError)):
                await extract_imageserver(
                    "https://invalid.example.com/ImageServer",
                    tmp_path,
                )


# =============================================================================
# Integration-style Tests (still unit, but test module interactions)
# =============================================================================


@pytest.mark.unit
class TestModuleImports:
    """Tests verifying module structure and imports."""

    def test_all_exports_importable(self) -> None:
        """All __all__ exports are importable."""
        from portolan_cli.extract.arcgis.imageserver import (
            ExtractionConfig,
            ExtractionResult,
            ImageServerExtractionError,
            download_tile,
            extract_imageserver,
        )

        # Just verify they're the right types
        assert ExtractionConfig is not None
        assert ExtractionResult is not None
        assert ImageServerExtractionError is not None
        assert callable(download_tile)
        assert callable(extract_imageserver)

    def test_extraction_config_is_dataclass(self) -> None:
        """ExtractionConfig is a proper dataclass."""
        from dataclasses import is_dataclass

        assert is_dataclass(ExtractionConfig)

    def test_extraction_result_is_dataclass(self) -> None:
        """ExtractionResult is a proper dataclass."""
        from dataclasses import is_dataclass

        assert is_dataclass(ExtractionResult)
