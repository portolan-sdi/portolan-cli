"""Tests for ArcGIS URL parsing.

The URL parser detects the type of ArcGIS REST endpoint:
- FeatureServer: `*/FeatureServer` or `*/FeatureServer/0` (layer endpoint)
- MapServer: `*/MapServer` or `*/MapServer/0`
- Services Root: `*/rest/services` (lists all available services)

It also extracts the service name for default output directory naming.
"""

from __future__ import annotations

import pytest

from portolan_cli.extract.arcgis.url_parser import (
    ArcGISURLType,
    InvalidArcGISURLError,
    ParsedArcGISURL,
    parse_arcgis_url,
)

pytestmark = pytest.mark.unit


class TestParseArcGISURLFeatureServer:
    """Tests for FeatureServer URL detection."""

    def test_basic_feature_server(self) -> None:
        """Basic FeatureServer URL should be detected."""
        url = "https://services.arcgis.com/abc123/ArcGIS/rest/services/Census/FeatureServer"
        result = parse_arcgis_url(url)

        assert result.url_type == ArcGISURLType.FEATURE_SERVER
        assert result.service_name == "Census"
        assert result.base_url == url
        assert result.layer_id is None

    def test_feature_server_with_layer_id(self) -> None:
        """FeatureServer URL with layer ID should extract layer."""
        url = "https://services.arcgis.com/abc123/ArcGIS/rest/services/Demographics/FeatureServer/0"
        result = parse_arcgis_url(url)

        assert result.url_type == ArcGISURLType.FEATURE_SERVER
        assert result.service_name == "Demographics"
        assert result.layer_id == 0
        # base_url should strip the layer ID
        assert (
            result.base_url
            == "https://services.arcgis.com/abc123/ArcGIS/rest/services/Demographics/FeatureServer"
        )

    def test_feature_server_with_multi_digit_layer_id(self) -> None:
        """FeatureServer URL with multi-digit layer ID."""
        url = "https://example.com/rest/services/Transportation/FeatureServer/123"
        result = parse_arcgis_url(url)

        assert result.url_type == ArcGISURLType.FEATURE_SERVER
        assert result.layer_id == 123

    def test_feature_server_case_insensitive(self) -> None:
        """FeatureServer detection should be case-insensitive."""
        url = "https://example.com/rest/services/Census/featureserver"
        result = parse_arcgis_url(url)

        assert result.url_type == ArcGISURLType.FEATURE_SERVER
        assert result.service_name == "Census"

    def test_feature_server_with_trailing_slash(self) -> None:
        """FeatureServer URL with trailing slash should work."""
        url = "https://example.com/rest/services/Census/FeatureServer/"
        result = parse_arcgis_url(url)

        assert result.url_type == ArcGISURLType.FEATURE_SERVER
        assert result.service_name == "Census"

    def test_feature_server_with_query_params(self) -> None:
        """FeatureServer URL with query parameters should work."""
        url = "https://example.com/rest/services/Census/FeatureServer?f=json"
        result = parse_arcgis_url(url)

        assert result.url_type == ArcGISURLType.FEATURE_SERVER
        assert result.service_name == "Census"


class TestParseArcGISURLMapServer:
    """Tests for MapServer URL detection."""

    def test_basic_map_server(self) -> None:
        """Basic MapServer URL should be detected."""
        url = "https://services.arcgis.com/abc123/ArcGIS/rest/services/Basemap/MapServer"
        result = parse_arcgis_url(url)

        assert result.url_type == ArcGISURLType.MAP_SERVER
        assert result.service_name == "Basemap"
        assert result.layer_id is None

    def test_map_server_with_layer_id(self) -> None:
        """MapServer URL with layer ID should extract layer."""
        url = "https://services.arcgis.com/abc123/ArcGIS/rest/services/Imagery/MapServer/2"
        result = parse_arcgis_url(url)

        assert result.url_type == ArcGISURLType.MAP_SERVER
        assert result.service_name == "Imagery"
        assert result.layer_id == 2

    def test_map_server_case_insensitive(self) -> None:
        """MapServer detection should be case-insensitive."""
        url = "https://example.com/rest/services/Maps/mapserver"
        result = parse_arcgis_url(url)

        assert result.url_type == ArcGISURLType.MAP_SERVER


class TestParseArcGISURLServicesRoot:
    """Tests for services root URL detection."""

    def test_services_root(self) -> None:
        """Services root URL should be detected."""
        url = "https://services.arcgis.com/abc123/ArcGIS/rest/services"
        result = parse_arcgis_url(url)

        assert result.url_type == ArcGISURLType.SERVICES_ROOT
        assert result.service_name is None
        assert result.layer_id is None
        assert result.base_url == url

    def test_services_root_with_trailing_slash(self) -> None:
        """Services root with trailing slash should work."""
        url = "https://services.arcgis.com/abc123/ArcGIS/rest/services/"
        result = parse_arcgis_url(url)

        assert result.url_type == ArcGISURLType.SERVICES_ROOT

    def test_services_root_case_insensitive(self) -> None:
        """Services root detection should be case-insensitive."""
        url = "https://example.com/REST/SERVICES"
        result = parse_arcgis_url(url)

        assert result.url_type == ArcGISURLType.SERVICES_ROOT

    def test_services_root_with_query_params(self) -> None:
        """Services root with query parameters should work."""
        url = "https://example.com/rest/services?f=json"
        result = parse_arcgis_url(url)

        assert result.url_type == ArcGISURLType.SERVICES_ROOT


class TestParseArcGISURLServiceName:
    """Tests for service name extraction."""

    def test_simple_service_name(self) -> None:
        """Simple service name should be extracted."""
        url = "https://example.com/rest/services/Census/FeatureServer"
        result = parse_arcgis_url(url)

        assert result.service_name == "Census"

    def test_service_name_with_underscores(self) -> None:
        """Service name with underscores should be preserved."""
        url = "https://example.com/rest/services/Census_Block_Groups/FeatureServer"
        result = parse_arcgis_url(url)

        assert result.service_name == "Census_Block_Groups"

    def test_service_name_in_folder(self) -> None:
        """Service in a folder should extract full path as name."""
        url = "https://example.com/rest/services/Demographics/Census2020/FeatureServer"
        result = parse_arcgis_url(url)

        # Service name includes the folder path
        assert result.service_name == "Demographics/Census2020"

    def test_service_name_deeply_nested(self) -> None:
        """Deeply nested service should extract full path."""
        url = "https://example.com/rest/services/Public/Data/Census/2020/FeatureServer"
        result = parse_arcgis_url(url)

        assert result.service_name == "Public/Data/Census/2020"


class TestParseArcGISURLInvalid:
    """Tests for invalid URL handling."""

    def test_invalid_not_arcgis_url(self) -> None:
        """Non-ArcGIS URL should raise error."""
        with pytest.raises(InvalidArcGISURLError) as exc_info:
            parse_arcgis_url("https://example.com/some/other/api")

        assert "not a recognized ArcGIS REST URL" in str(exc_info.value)

    def test_invalid_empty_url(self) -> None:
        """Empty URL should raise error."""
        with pytest.raises(InvalidArcGISURLError):
            parse_arcgis_url("")

    def test_invalid_malformed_url(self) -> None:
        """Malformed URL should raise error."""
        with pytest.raises(InvalidArcGISURLError):
            parse_arcgis_url("not-a-url")

    def test_invalid_partial_services_path(self) -> None:
        """Partial services path without server type should raise error."""
        with pytest.raises(InvalidArcGISURLError):
            parse_arcgis_url("https://example.com/rest/services/Census")

    def test_invalid_image_server(self) -> None:
        """ImageServer is not supported (raster out of scope)."""
        with pytest.raises(InvalidArcGISURLError) as exc_info:
            parse_arcgis_url("https://example.com/rest/services/Imagery/ImageServer")

        assert "ImageServer" in str(exc_info.value)


class TestParsedArcGISURL:
    """Tests for ParsedArcGISURL dataclass."""

    def test_is_single_service_feature_server(self) -> None:
        """FeatureServer should be a single service."""
        result = ParsedArcGISURL(
            url_type=ArcGISURLType.FEATURE_SERVER,
            base_url="https://example.com/FeatureServer",
            service_name="Census",
            layer_id=None,
        )
        assert result.is_single_service is True

    def test_is_single_service_services_root(self) -> None:
        """Services root should NOT be a single service."""
        result = ParsedArcGISURL(
            url_type=ArcGISURLType.SERVICES_ROOT,
            base_url="https://example.com/rest/services",
            service_name=None,
            layer_id=None,
        )
        assert result.is_single_service is False

    def test_service_endpoint_name(self) -> None:
        """service_endpoint_name should return last segment of service_name."""
        result = ParsedArcGISURL(
            url_type=ArcGISURLType.FEATURE_SERVER,
            base_url="https://example.com/FeatureServer",
            service_name="Demographics/Census2020",
            layer_id=None,
        )
        assert result.service_endpoint_name == "Census2020"

    def test_service_endpoint_name_simple(self) -> None:
        """service_endpoint_name should work for simple names."""
        result = ParsedArcGISURL(
            url_type=ArcGISURLType.FEATURE_SERVER,
            base_url="https://example.com/FeatureServer",
            service_name="Census",
            layer_id=None,
        )
        assert result.service_endpoint_name == "Census"

    def test_service_endpoint_name_none(self) -> None:
        """service_endpoint_name should return None when service_name is None."""
        result = ParsedArcGISURL(
            url_type=ArcGISURLType.SERVICES_ROOT,
            base_url="https://example.com/rest/services",
            service_name=None,
            layer_id=None,
        )
        assert result.service_endpoint_name is None


class TestRealWorldURLs:
    """Tests with real-world ArcGIS URL patterns."""

    def test_arcgis_online_feature_server(self) -> None:
        """ArcGIS Online FeatureServer URL."""
        url = "https://services.arcgis.com/fLeGjb7u4uXqeF9q/ArcGIS/rest/services/Philadelphia_Crime/FeatureServer"
        result = parse_arcgis_url(url)

        assert result.url_type == ArcGISURLType.FEATURE_SERVER
        assert result.service_name == "Philadelphia_Crime"

    def test_arcgis_online_services_root(self) -> None:
        """ArcGIS Online services root URL."""
        url = "https://services.arcgis.com/fLeGjb7u4uXqeF9q/ArcGIS/rest/services"
        result = parse_arcgis_url(url)

        assert result.url_type == ArcGISURLType.SERVICES_ROOT

    def test_enterprise_server_url(self) -> None:
        """ArcGIS Enterprise server URL."""
        url = "https://gis.city.gov/arcgis/rest/services/Public/Parcels/FeatureServer"
        result = parse_arcgis_url(url)

        assert result.url_type == ArcGISURLType.FEATURE_SERVER
        assert result.service_name == "Public/Parcels"

    def test_den_haag_services_root(self) -> None:
        """Den Haag services root (from test data)."""
        url = "https://geodata.denhaag.nl/arcgis/rest/services"
        result = parse_arcgis_url(url)

        assert result.url_type == ArcGISURLType.SERVICES_ROOT
