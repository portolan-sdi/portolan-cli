"""Tests for ImageServer URL parsing.

TDD tests for Wave 3: URL parser should detect ImageServer URLs.
"""

from __future__ import annotations

import pytest

from portolan_cli.extract.arcgis.url_parser import (
    ArcGISURLType,
    ParsedArcGISURL,
    parse_arcgis_url,
)

pytestmark = pytest.mark.unit


class TestParseArcGISURLImageServer:
    """Tests for ImageServer URL detection."""

    def test_basic_image_server(self) -> None:
        """Basic ImageServer URL should be detected."""
        url = "https://services.arcgis.com/abc123/ArcGIS/rest/services/Imagery/ImageServer"
        result = parse_arcgis_url(url)

        assert result.url_type == ArcGISURLType.IMAGE_SERVER
        assert result.service_name == "Imagery"
        assert result.base_url == url
        assert result.layer_id is None

    def test_image_server_with_trailing_slash(self) -> None:
        """ImageServer URL with trailing slash should work."""
        url = "https://example.com/rest/services/Imagery/ImageServer/"
        result = parse_arcgis_url(url)

        assert result.url_type == ArcGISURLType.IMAGE_SERVER
        assert result.service_name == "Imagery"

    def test_image_server_case_insensitive(self) -> None:
        """ImageServer detection should be case-insensitive."""
        url = "https://example.com/rest/services/Imagery/imageserver"
        result = parse_arcgis_url(url)

        assert result.url_type == ArcGISURLType.IMAGE_SERVER

    def test_image_server_with_query_params(self) -> None:
        """ImageServer URL with query parameters should work."""
        url = "https://example.com/rest/services/Imagery/ImageServer?f=json"
        result = parse_arcgis_url(url)

        assert result.url_type == ArcGISURLType.IMAGE_SERVER
        assert result.service_name == "Imagery"

    def test_image_server_in_folder(self) -> None:
        """ImageServer in a folder should extract full path as name."""
        url = "https://example.com/rest/services/Public/Imagery/ImageServer"
        result = parse_arcgis_url(url)

        assert result.url_type == ArcGISURLType.IMAGE_SERVER
        assert result.service_name == "Public/Imagery"

    def test_image_server_deeply_nested(self) -> None:
        """Deeply nested ImageServer should extract full path."""
        url = "https://example.com/rest/services/Hosted/Satellite/2024/ImageServer"
        result = parse_arcgis_url(url)

        assert result.url_type == ArcGISURLType.IMAGE_SERVER
        assert result.service_name == "Hosted/Satellite/2024"


class TestImageServerServiceEndpointName:
    """Tests for service_endpoint_name property with ImageServer."""

    def test_service_endpoint_name_simple(self) -> None:
        """service_endpoint_name should work for simple ImageServer names."""
        result = ParsedArcGISURL(
            url_type=ArcGISURLType.IMAGE_SERVER,
            base_url="https://example.com/ImageServer",
            service_name="Imagery",
            layer_id=None,
        )
        assert result.service_endpoint_name == "Imagery"

    def test_service_endpoint_name_nested(self) -> None:
        """service_endpoint_name should return last segment for nested paths."""
        result = ParsedArcGISURL(
            url_type=ArcGISURLType.IMAGE_SERVER,
            base_url="https://example.com/ImageServer",
            service_name="Hosted/Satellite/Landsat8",
            layer_id=None,
        )
        assert result.service_endpoint_name == "Landsat8"


class TestImageServerIsSingleService:
    """Tests for is_single_service property with ImageServer."""

    def test_image_server_is_single_service(self) -> None:
        """ImageServer should be a single service."""
        result = ParsedArcGISURL(
            url_type=ArcGISURLType.IMAGE_SERVER,
            base_url="https://example.com/ImageServer",
            service_name="Imagery",
            layer_id=None,
        )
        assert result.is_single_service is True


class TestRealWorldImageServerURLs:
    """Tests with real-world ImageServer URL patterns."""

    def test_esri_world_imagery(self) -> None:
        """ESRI World Imagery URL pattern."""
        url = "https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/ImageServer"
        result = parse_arcgis_url(url)

        assert result.url_type == ArcGISURLType.IMAGE_SERVER
        assert result.service_name == "World_Imagery"

    def test_naip_imagery(self) -> None:
        """NAIP Imagery URL pattern."""
        url = "https://imagery.arcgisonline.com/arcgis/rest/services/USA_NAIP_Imagery/ImageServer"
        result = parse_arcgis_url(url)

        assert result.url_type == ArcGISURLType.IMAGE_SERVER
        assert result.service_name == "USA_NAIP_Imagery"
