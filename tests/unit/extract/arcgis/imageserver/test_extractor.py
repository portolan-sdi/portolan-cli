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

    def test_default_raw_false(self) -> None:
        """Default raw is False (auto-init catalog by default)."""
        config = ExtractionConfig()
        assert config.raw is False

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
            output_dir=Path("/output"),
            tiles_downloaded=10,
            tiles_skipped=0,
            total_bytes=1024000,
            catalog_initialized=True,
        )
        assert result.output_dir == Path("/output")
        assert result.tiles_downloaded == 10
        assert result.tiles_skipped == 0
        assert result.total_bytes == 1024000
        assert result.catalog_initialized is True

    def test_result_defaults(self) -> None:
        """ExtractionResult has sensible defaults."""
        result = ExtractionResult(
            output_dir=Path("/output"),
            tiles_downloaded=5,
            tiles_skipped=0,
        )
        assert result.tiles_failed == 0
        assert result.total_bytes == 0
        assert result.catalog_initialized is False


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


# =============================================================================
# Tests for Issue #335 Fixes
# =============================================================================


@pytest.mark.unit
class TestBboxCrsDetection:
    """Tests for WGS84 bbox detection and reprojection (issue #335 fix)."""

    def test_is_likely_wgs84_with_lat_lon_coords(self) -> None:
        """Bbox with WGS84-range coordinates is detected."""
        from portolan_cli.extract.arcgis.imageserver.extractor import _is_likely_wgs84

        # Philadelphia area in WGS84
        bbox = (-75.17, 39.95, -75.15, 39.97)
        assert _is_likely_wgs84(bbox) is True

    def test_is_likely_wgs84_with_web_mercator_coords(self) -> None:
        """Bbox with Web Mercator coordinates is NOT detected as WGS84."""
        from portolan_cli.extract.arcgis.imageserver.extractor import _is_likely_wgs84

        # Philadelphia area in Web Mercator (large numbers)
        bbox = (-8367886, 4858679, -8365659, 4861583)
        assert _is_likely_wgs84(bbox) is False

    def test_is_likely_wgs84_edge_case_poles(self) -> None:
        """Bbox at edge of WGS84 range is detected."""
        from portolan_cli.extract.arcgis.imageserver.extractor import _is_likely_wgs84

        # Global extent
        bbox = (-180, -90, 180, 90)
        assert _is_likely_wgs84(bbox) is True

    def test_reproject_bbox_wgs84_to_web_mercator(self) -> None:
        """Bbox is correctly reprojected from WGS84 to Web Mercator."""
        from portolan_cli.extract.arcgis.imageserver.extractor import _reproject_bbox

        # Philadelphia area: known coordinates for verification
        # WGS84: (-75.17, 39.95, -75.15, 39.97)
        # Expected Web Mercator (approximately):
        # minx: -8367886, miny: 4858679, maxx: -8365659, maxy: 4861583
        bbox = (-75.17, 39.95, -75.15, 39.97)
        result = _reproject_bbox(bbox, "EPSG:4326", "EPSG:3857")

        # Verify against known correct values (within 100m tolerance)
        assert -8368000 < result[0] < -8367000  # minx ~ -8367886
        assert 4858000 < result[1] < 4859000  # miny ~ 4858679
        assert -8366000 < result[2] < -8365000  # maxx ~ -8365659
        assert 4861000 < result[3] < 4862000  # maxy ~ 4861583

    def test_reproject_bbox_if_needed_passthrough_for_wgs84_service(self) -> None:
        """Bbox is not reprojected if service is already WGS84."""
        from portolan_cli.extract.arcgis.imageserver.extractor import reproject_bbox_if_needed

        bbox = (-75.17, 39.95, -75.15, 39.97)
        result = reproject_bbox_if_needed(bbox, "EPSG:4326")

        # Should be unchanged
        assert result == bbox

    def test_reproject_bbox_if_needed_converts_wgs84_to_service_crs(self) -> None:
        """WGS84 bbox is auto-reprojected to service CRS."""
        from portolan_cli.extract.arcgis.imageserver.extractor import reproject_bbox_if_needed

        # WGS84 coords (Philadelphia)
        bbox = (-75.17, 39.95, -75.15, 39.97)
        result = reproject_bbox_if_needed(bbox, "EPSG:3857")

        # Verify against known correct Web Mercator values
        assert -8368000 < result[0] < -8367000  # minx ~ -8367886
        assert 4858000 < result[1] < 4859000  # miny ~ 4858679
        assert -8366000 < result[2] < -8365000  # maxx ~ -8365659
        assert 4861000 < result[3] < 4862000  # maxy ~ 4861583

    def test_reproject_bbox_if_needed_explicit_bbox_crs(self) -> None:
        """Explicit bbox_crs parameter overrides auto-detection."""
        from portolan_cli.extract.arcgis.imageserver.extractor import reproject_bbox_if_needed

        # State Plane coords that happen to be in WGS84 range (would trigger false positive)
        bbox = (100.0, 50.0, 150.0, 80.0)

        # Without explicit bbox_crs, this would be detected as WGS84 and reprojected
        # With explicit bbox_crs matching service CRS, no reprojection happens
        result = reproject_bbox_if_needed(bbox, "EPSG:3857", bbox_crs="EPSG:3857")

        # Should be unchanged (same CRS)
        assert result == bbox

    def test_reproject_bbox_if_needed_explicit_bbox_crs_different(self) -> None:
        """Explicit bbox_crs triggers reprojection when different from service CRS."""
        from portolan_cli.extract.arcgis.imageserver.extractor import reproject_bbox_if_needed

        # Explicit WGS84 bbox
        bbox = (-75.17, 39.95, -75.15, 39.97)
        result = reproject_bbox_if_needed(bbox, "EPSG:3857", bbox_crs="EPSG:4326")

        # Should be reprojected to Web Mercator
        assert -8368000 < result[0] < -8367000


@pytest.mark.unit
class TestCollectionNameValidation:
    """Tests for collection name validation (path traversal prevention)."""

    def test_validate_collection_name_simple(self) -> None:
        """Simple collection names pass validation."""
        from portolan_cli.extract.arcgis.imageserver.extractor import _validate_collection_name

        assert _validate_collection_name("tiles") == "tiles"
        assert _validate_collection_name("naip-2024") == "naip-2024"
        assert _validate_collection_name("my_collection") == "my_collection"

    def test_validate_collection_name_strips_path_components(self) -> None:
        """Path traversal attempts are sanitized."""
        from portolan_cli.extract.arcgis.imageserver.extractor import _validate_collection_name

        # Path traversal attempts get stripped to just the base name
        assert _validate_collection_name("../../../etc") == "etc"
        assert _validate_collection_name("/etc/passwd") == "passwd"
        assert _validate_collection_name("foo/bar/baz") == "baz"

    def test_validate_collection_name_rejects_empty(self) -> None:
        """Empty names are rejected."""
        from portolan_cli.extract.arcgis.imageserver.extractor import _validate_collection_name

        with pytest.raises(ValueError, match="cannot be empty"):
            _validate_collection_name("")

        with pytest.raises(ValueError, match="cannot be empty"):
            _validate_collection_name(".")

        with pytest.raises(ValueError, match="cannot be empty"):
            _validate_collection_name("..")

    def test_validate_collection_name_rejects_invalid_chars(self) -> None:
        """Names with invalid characters are rejected."""
        from portolan_cli.extract.arcgis.imageserver.extractor import _validate_collection_name

        with pytest.raises(ValueError, match="cannot contain"):
            _validate_collection_name("foo<bar")

        with pytest.raises(ValueError, match="cannot contain"):
            _validate_collection_name("foo|bar")

        with pytest.raises(ValueError, match="cannot contain"):
            _validate_collection_name("foo?bar")


@pytest.mark.unit
class TestJsonErrorParsing:
    """Tests for JSON error response parsing (issue #335 fix)."""

    @pytest.mark.asyncio
    async def test_download_tile_parses_arcgis_json_error(
        self, sample_tile: TileSpec, tmp_path: Path
    ) -> None:
        """JSON error responses from ArcGIS are parsed correctly."""
        from portolan_cli.extract.arcgis.imageserver.extractor import (
            ImageServerExtractionError,
            download_tile,
        )

        mock_client = AsyncMock()
        mock_response = AsyncMock()
        # Simulate ArcGIS JSON error response (not a TIFF)
        error_json = (
            b'{"error":{"code":400,"message":"The requested image exceeds the size limit."}}'
        )
        mock_response.content = error_json
        mock_response.status_code = 200  # ArcGIS returns 200 with error in body
        mock_response.raise_for_status = MagicMock()
        mock_client.get.return_value = mock_response

        output_path = tmp_path / "out.tif"
        with pytest.raises(ImageServerExtractionError) as exc_info:
            await download_tile(
                "https://example.com/ImageServer",
                sample_tile,
                output_path,
                mock_client,
            )

        # Error message should contain the ArcGIS error details
        assert "400" in str(exc_info.value)
        assert "exceeds the size limit" in str(exc_info.value)
