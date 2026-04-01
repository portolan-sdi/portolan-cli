"""Tests for ArcGIS ImageServer discovery.

ImageServer discovery fetches raster service metadata from ArcGIS REST API
endpoints. This enables Portolan to understand raster data capabilities
before extraction (band count, pixel type, extent, resolution).

TDD: These tests are written FIRST, before implementation.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

# These imports will fail until we implement the module
from portolan_cli.extract.arcgis.imageserver.discovery import (
    ImageServerDiscoveryError,
    ImageServerMetadata,
    discover_imageserver,
)

pytestmark = pytest.mark.unit


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def imageserver_response() -> dict[str, Any]:
    """Typical ImageServer response with raster metadata."""
    return {
        "name": "NAIP_2020",
        "description": "National Agriculture Imagery Program 2020",
        "copyrightText": "USDA Farm Service Agency",
        "bandCount": 4,
        "pixelType": "U8",
        "pixelSizeX": 0.6,
        "pixelSizeY": 0.6,
        "extent": {
            "xmin": -124.848974,
            "ymin": 24.396308,
            "xmax": -66.885444,
            "ymax": 49.384358,
            "spatialReference": {"wkid": 4326, "latestWkid": 4326},
        },
        "maxImageHeight": 4096,
        "maxImageWidth": 4096,
        "capabilities": "Image,Metadata,Catalog,Mensuration",
        "serviceDataType": "esriImageServiceDataTypeProcessed",
        "minScale": 0,
        "maxScale": 0,
        "defaultCompressionQuality": 75,
    }


@pytest.fixture
def minimal_imageserver_response() -> dict[str, Any]:
    """Minimal ImageServer response with only required fields."""
    return {
        "name": "Minimal_Raster",
        "bandCount": 1,
        "pixelType": "F32",
        "pixelSizeX": 10.0,
        "pixelSizeY": 10.0,
        "extent": {
            "xmin": 0.0,
            "ymin": 0.0,
            "xmax": 100.0,
            "ymax": 100.0,
            "spatialReference": {"wkid": 32618},
        },
        "maxImageHeight": 2048,
        "maxImageWidth": 2048,
    }


@pytest.fixture
def multispectral_response() -> dict[str, Any]:
    """ImageServer response for multispectral imagery."""
    return {
        "name": "Landsat_8",
        "description": "Landsat 8 multispectral imagery",
        "bandCount": 11,
        "pixelType": "U16",
        "pixelSizeX": 30.0,
        "pixelSizeY": 30.0,
        "extent": {
            "xmin": -180.0,
            "ymin": -90.0,
            "xmax": 180.0,
            "ymax": 90.0,
            "spatialReference": {"wkid": 4326},
        },
        "maxImageHeight": 8192,
        "maxImageWidth": 8192,
        "capabilities": "Image,Metadata,Catalog,Download",
    }


def _mock_httpx_response(data: dict[str, Any]) -> MagicMock:
    """Create a mock httpx response with given JSON data."""
    mock_response = MagicMock()
    mock_response.json.return_value = data
    mock_response.raise_for_status = MagicMock()
    return mock_response


# =============================================================================
# ImageServerMetadata dataclass tests
# =============================================================================


class TestImageServerMetadata:
    """Tests for ImageServerMetadata dataclass."""

    def test_creates_with_required_fields(self) -> None:
        """Should create metadata with all required fields."""
        metadata = ImageServerMetadata(
            name="Test_Raster",
            band_count=3,
            pixel_type="U8",
            pixel_size_x=1.0,
            pixel_size_y=1.0,
            full_extent={
                "xmin": 0.0,
                "ymin": 0.0,
                "xmax": 100.0,
                "ymax": 100.0,
                "spatialReference": {"wkid": 4326},
            },
            max_image_width=4096,
            max_image_height=4096,
            capabilities=["Image", "Metadata"],
        )

        assert metadata.name == "Test_Raster"
        assert metadata.band_count == 3
        assert metadata.pixel_type == "U8"
        assert metadata.pixel_size_x == 1.0
        assert metadata.pixel_size_y == 1.0
        assert metadata.max_image_width == 4096
        assert metadata.max_image_height == 4096

    def test_full_extent_contains_spatial_reference(self) -> None:
        """Full extent should include spatial reference."""
        metadata = ImageServerMetadata(
            name="Test",
            band_count=1,
            pixel_type="F32",
            pixel_size_x=10.0,
            pixel_size_y=10.0,
            full_extent={
                "xmin": -180.0,
                "ymin": -90.0,
                "xmax": 180.0,
                "ymax": 90.0,
                "spatialReference": {"wkid": 4326, "latestWkid": 4326},
            },
            max_image_width=2048,
            max_image_height=2048,
            capabilities=[],
        )

        assert metadata.full_extent["spatialReference"]["wkid"] == 4326

    def test_capabilities_as_list(self) -> None:
        """Capabilities should be stored as a list."""
        metadata = ImageServerMetadata(
            name="Test",
            band_count=1,
            pixel_type="U8",
            pixel_size_x=1.0,
            pixel_size_y=1.0,
            full_extent={"xmin": 0, "ymin": 0, "xmax": 1, "ymax": 1},
            max_image_width=1024,
            max_image_height=1024,
            capabilities=["Image", "Metadata", "Catalog"],
        )

        assert "Image" in metadata.capabilities
        assert "Metadata" in metadata.capabilities
        assert len(metadata.capabilities) == 3

    def test_optional_fields_default_to_none(self) -> None:
        """Optional fields should default to None."""
        metadata = ImageServerMetadata(
            name="Test",
            band_count=1,
            pixel_type="U8",
            pixel_size_x=1.0,
            pixel_size_y=1.0,
            full_extent={"xmin": 0, "ymin": 0, "xmax": 1, "ymax": 1},
            max_image_width=1024,
            max_image_height=1024,
            capabilities=[],
        )

        assert metadata.description is None
        assert metadata.copyright_text is None
        assert metadata.service_data_type is None

    def test_with_optional_fields(self) -> None:
        """Should store optional fields when provided."""
        metadata = ImageServerMetadata(
            name="Test",
            band_count=3,
            pixel_type="U8",
            pixel_size_x=1.0,
            pixel_size_y=1.0,
            full_extent={"xmin": 0, "ymin": 0, "xmax": 1, "ymax": 1},
            max_image_width=1024,
            max_image_height=1024,
            capabilities=["Image"],
            description="Test raster description",
            copyright_text="Test copyright",
            service_data_type="esriImageServiceDataTypeGeneric",
        )

        assert metadata.description == "Test raster description"
        assert metadata.copyright_text == "Test copyright"
        assert metadata.service_data_type == "esriImageServiceDataTypeGeneric"


# =============================================================================
# discover_imageserver tests
# =============================================================================


class TestDiscoverImageserver:
    """Tests for discover_imageserver function."""

    def test_discovers_basic_metadata(self, imageserver_response: dict[str, Any]) -> None:
        """Should discover basic metadata from ImageServer response."""
        with patch("portolan_cli.extract.arcgis.imageserver.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _mock_httpx_response(
                imageserver_response
            )

            result = discover_imageserver("https://services.arcgis.com/test/ImageServer")

            assert result.name == "NAIP_2020"
            assert result.band_count == 4
            assert result.pixel_type == "U8"
            assert result.pixel_size_x == 0.6
            assert result.pixel_size_y == 0.6

    def test_discovers_extent_with_spatial_reference(
        self, imageserver_response: dict[str, Any]
    ) -> None:
        """Should extract full extent including spatial reference."""
        with patch("portolan_cli.extract.arcgis.imageserver.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _mock_httpx_response(
                imageserver_response
            )

            result = discover_imageserver("https://services.arcgis.com/test/ImageServer")

            assert result.full_extent["xmin"] == pytest.approx(-124.848974)
            assert result.full_extent["ymax"] == pytest.approx(49.384358)
            assert result.full_extent["spatialReference"]["wkid"] == 4326

    def test_discovers_image_limits(self, imageserver_response: dict[str, Any]) -> None:
        """Should extract max image dimensions."""
        with patch("portolan_cli.extract.arcgis.imageserver.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _mock_httpx_response(
                imageserver_response
            )

            result = discover_imageserver("https://services.arcgis.com/test/ImageServer")

            assert result.max_image_width == 4096
            assert result.max_image_height == 4096

    def test_parses_capabilities_string(self, imageserver_response: dict[str, Any]) -> None:
        """Should parse comma-separated capabilities into list."""
        with patch("portolan_cli.extract.arcgis.imageserver.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _mock_httpx_response(
                imageserver_response
            )

            result = discover_imageserver("https://services.arcgis.com/test/ImageServer")

            assert "Image" in result.capabilities
            assert "Metadata" in result.capabilities
            assert "Catalog" in result.capabilities
            assert "Mensuration" in result.capabilities

    def test_handles_minimal_response(self, minimal_imageserver_response: dict[str, Any]) -> None:
        """Should handle response with only required fields."""
        with patch("portolan_cli.extract.arcgis.imageserver.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _mock_httpx_response(
                minimal_imageserver_response
            )

            result = discover_imageserver("https://services.arcgis.com/test/ImageServer")

            assert result.name == "Minimal_Raster"
            assert result.band_count == 1
            assert result.pixel_type == "F32"
            assert result.description is None
            assert result.copyright_text is None

    def test_handles_multispectral_imagery(self, multispectral_response: dict[str, Any]) -> None:
        """Should correctly parse high band count imagery."""
        with patch("portolan_cli.extract.arcgis.imageserver.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _mock_httpx_response(
                multispectral_response
            )

            result = discover_imageserver("https://services.arcgis.com/test/ImageServer")

            assert result.band_count == 11
            assert result.pixel_type == "U16"
            assert "Download" in result.capabilities

    def test_extracts_optional_metadata(self, imageserver_response: dict[str, Any]) -> None:
        """Should extract optional description and copyright."""
        with patch("portolan_cli.extract.arcgis.imageserver.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _mock_httpx_response(
                imageserver_response
            )

            result = discover_imageserver("https://services.arcgis.com/test/ImageServer")

            assert result.description == "National Agriculture Imagery Program 2020"
            assert result.copyright_text == "USDA Farm Service Agency"
            assert result.service_data_type == "esriImageServiceDataTypeProcessed"

    def test_adds_f_json_to_url(self) -> None:
        """Should add f=json parameter to URL."""
        with patch("portolan_cli.extract.arcgis.imageserver.discovery.httpx.Client") as mock_client:
            mock_response = _mock_httpx_response(
                {
                    "name": "Test",
                    "bandCount": 1,
                    "pixelType": "U8",
                    "pixelSizeX": 1.0,
                    "pixelSizeY": 1.0,
                    "extent": {"xmin": 0, "ymin": 0, "xmax": 1, "ymax": 1},
                    "maxImageHeight": 1024,
                    "maxImageWidth": 1024,
                }
            )
            mock_client.return_value.__enter__.return_value.get.return_value = mock_response

            discover_imageserver("https://services.arcgis.com/test/ImageServer")

            # Verify the URL used includes f=json
            call_args = mock_client.return_value.__enter__.return_value.get.call_args
            url_called = call_args[0][0]
            assert "f=json" in url_called


# =============================================================================
# Error handling tests
# =============================================================================


class TestDiscoverImageserverErrors:
    """Tests for error handling in discover_imageserver."""

    def test_raises_on_http_error(self) -> None:
        """Should raise ImageServerDiscoveryError on HTTP errors."""
        with patch("portolan_cli.extract.arcgis.imageserver.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.side_effect = httpx.HTTPStatusError(
                "Not Found",
                request=httpx.Request("GET", "https://example.com"),
                response=httpx.Response(404),
            )

            with pytest.raises(ImageServerDiscoveryError, match="Failed to fetch"):
                discover_imageserver("https://services.arcgis.com/test/ImageServer")

    def test_raises_on_connection_error(self) -> None:
        """Should raise ImageServerDiscoveryError on connection errors."""
        with patch("portolan_cli.extract.arcgis.imageserver.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.side_effect = httpx.ConnectError(
                "Connection refused"
            )

            with pytest.raises(ImageServerDiscoveryError, match="Failed to connect"):
                discover_imageserver("https://services.arcgis.com/test/ImageServer")

    def test_raises_on_timeout(self) -> None:
        """Should raise ImageServerDiscoveryError on timeout."""
        with patch("portolan_cli.extract.arcgis.imageserver.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.side_effect = (
                httpx.TimeoutException("Request timed out")
            )

            with pytest.raises(ImageServerDiscoveryError, match="timed out"):
                discover_imageserver("https://services.arcgis.com/test/ImageServer")

    def test_raises_on_invalid_json(self) -> None:
        """Should raise ImageServerDiscoveryError on invalid JSON response."""
        with patch("portolan_cli.extract.arcgis.imageserver.discovery.httpx.Client") as mock_client:
            mock_response = MagicMock()
            mock_response.json.side_effect = ValueError("Invalid JSON")
            mock_response.raise_for_status = MagicMock()
            mock_client.return_value.__enter__.return_value.get.return_value = mock_response

            with pytest.raises(ImageServerDiscoveryError, match="Invalid JSON"):
                discover_imageserver("https://services.arcgis.com/test/ImageServer")

    def test_raises_on_missing_required_field(self) -> None:
        """Should raise ImageServerDiscoveryError when required field is missing."""
        incomplete_response = {
            "name": "Test",
            # Missing bandCount, pixelType, etc.
        }

        with patch("portolan_cli.extract.arcgis.imageserver.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _mock_httpx_response(
                incomplete_response
            )

            with pytest.raises(ImageServerDiscoveryError, match="Missing required field"):
                discover_imageserver("https://services.arcgis.com/test/ImageServer")

    def test_raises_on_arcgis_error_response(self) -> None:
        """Should raise ImageServerDiscoveryError on ArcGIS error response."""
        error_response = {
            "error": {
                "code": 403,
                "message": "Token Required",
                "details": ["Token Required"],
            }
        }

        with patch("portolan_cli.extract.arcgis.imageserver.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _mock_httpx_response(
                error_response
            )

            with pytest.raises(ImageServerDiscoveryError, match="Token Required"):
                discover_imageserver("https://services.arcgis.com/test/ImageServer")

    def test_handles_401_unauthorized(self) -> None:
        """Should provide clear message for authentication errors."""
        with patch("portolan_cli.extract.arcgis.imageserver.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.side_effect = httpx.HTTPStatusError(
                "Unauthorized",
                request=httpx.Request("GET", "https://example.com"),
                response=httpx.Response(401),
            )

            with pytest.raises(ImageServerDiscoveryError, match="401"):
                discover_imageserver("https://services.arcgis.com/test/ImageServer")


# =============================================================================
# Edge cases
# =============================================================================


class TestDiscoverImageserverEdgeCases:
    """Tests for edge cases in discover_imageserver."""

    def test_handles_empty_capabilities(self) -> None:
        """Should handle missing or empty capabilities."""
        response = {
            "name": "Test",
            "bandCount": 1,
            "pixelType": "U8",
            "pixelSizeX": 1.0,
            "pixelSizeY": 1.0,
            "extent": {"xmin": 0, "ymin": 0, "xmax": 1, "ymax": 1},
            "maxImageHeight": 1024,
            "maxImageWidth": 1024,
            # No capabilities field
        }

        with patch("portolan_cli.extract.arcgis.imageserver.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _mock_httpx_response(
                response
            )

            result = discover_imageserver("https://services.arcgis.com/test/ImageServer")

            assert result.capabilities == []

    def test_handles_empty_string_capabilities(self) -> None:
        """Should handle empty string capabilities."""
        response = {
            "name": "Test",
            "bandCount": 1,
            "pixelType": "U8",
            "pixelSizeX": 1.0,
            "pixelSizeY": 1.0,
            "extent": {"xmin": 0, "ymin": 0, "xmax": 1, "ymax": 1},
            "maxImageHeight": 1024,
            "maxImageWidth": 1024,
            "capabilities": "",
        }

        with patch("portolan_cli.extract.arcgis.imageserver.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _mock_httpx_response(
                response
            )

            result = discover_imageserver("https://services.arcgis.com/test/ImageServer")

            assert result.capabilities == []

    def test_handles_url_with_existing_params(self) -> None:
        """Should preserve existing URL parameters."""
        with patch("portolan_cli.extract.arcgis.imageserver.discovery.httpx.Client") as mock_client:
            mock_response = _mock_httpx_response(
                {
                    "name": "Test",
                    "bandCount": 1,
                    "pixelType": "U8",
                    "pixelSizeX": 1.0,
                    "pixelSizeY": 1.0,
                    "extent": {"xmin": 0, "ymin": 0, "xmax": 1, "ymax": 1},
                    "maxImageHeight": 1024,
                    "maxImageWidth": 1024,
                }
            )
            mock_client.return_value.__enter__.return_value.get.return_value = mock_response

            discover_imageserver("https://services.arcgis.com/test/ImageServer?token=abc123")

            call_args = mock_client.return_value.__enter__.return_value.get.call_args
            url_called = call_args[0][0]
            assert "token=abc123" in url_called
            assert "f=json" in url_called

    def test_handles_extent_without_latest_wkid(self) -> None:
        """Should handle spatial reference without latestWkid."""
        response = {
            "name": "Test",
            "bandCount": 1,
            "pixelType": "U8",
            "pixelSizeX": 1.0,
            "pixelSizeY": 1.0,
            "extent": {
                "xmin": 0,
                "ymin": 0,
                "xmax": 1,
                "ymax": 1,
                "spatialReference": {"wkid": 32618},  # No latestWkid
            },
            "maxImageHeight": 1024,
            "maxImageWidth": 1024,
        }

        with patch("portolan_cli.extract.arcgis.imageserver.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _mock_httpx_response(
                response
            )

            result = discover_imageserver("https://services.arcgis.com/test/ImageServer")

            assert result.full_extent["spatialReference"]["wkid"] == 32618

    def test_timeout_parameter_is_used(self) -> None:
        """Should pass timeout parameter to httpx client."""
        with patch("portolan_cli.extract.arcgis.imageserver.discovery.httpx.Client") as mock_client:
            mock_response = _mock_httpx_response(
                {
                    "name": "Test",
                    "bandCount": 1,
                    "pixelType": "U8",
                    "pixelSizeX": 1.0,
                    "pixelSizeY": 1.0,
                    "extent": {"xmin": 0, "ymin": 0, "xmax": 1, "ymax": 1},
                    "maxImageHeight": 1024,
                    "maxImageWidth": 1024,
                }
            )
            mock_client.return_value.__enter__.return_value.get.return_value = mock_response

            discover_imageserver(
                "https://services.arcgis.com/test/ImageServer",
                timeout=30.0,
            )

            mock_client.assert_called_once_with(timeout=30.0)

    def test_handles_float_pixel_size(self) -> None:
        """Should handle various pixel size formats."""
        response = {
            "name": "Test",
            "bandCount": 1,
            "pixelType": "F64",
            "pixelSizeX": 0.00027777777778,  # ~30m in degrees
            "pixelSizeY": 0.00027777777778,
            "extent": {"xmin": 0, "ymin": 0, "xmax": 1, "ymax": 1},
            "maxImageHeight": 1024,
            "maxImageWidth": 1024,
        }

        with patch("portolan_cli.extract.arcgis.imageserver.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _mock_httpx_response(
                response
            )

            result = discover_imageserver("https://services.arcgis.com/test/ImageServer")

            assert result.pixel_size_x == pytest.approx(0.00027777777778)
            assert result.pixel_type == "F64"
