"""Tests for ArcGIS service/layer discovery.

Discovery client fetches service and layer information from ArcGIS REST API
endpoints using httpx. This is the first step in the extraction flow:
URL → Discovery → Filtering → Extraction.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from portolan_cli.extract.arcgis.discovery import (
    ArcGISDiscoveryError,
    FolderTraversal,
    LayerInfo,
    ServiceDiscoveryResult,
    ServiceInfo,
    _ensure_json_format,
    _fetch_json,
    discover_layers,
    discover_services,
    discover_services_recursive,
)

pytestmark = pytest.mark.unit


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def feature_server_response() -> dict[str, Any]:
    """Typical FeatureServer response with layers."""
    return {
        "serviceDescription": "Census data for Philadelphia",
        "description": "Block groups and tracts",
        "copyrightText": "City of Philadelphia",
        "layers": [
            {"id": 0, "name": "Census_Block_Groups", "type": "Feature Layer"},
            {"id": 1, "name": "Census_Tracts", "type": "Feature Layer"},
            {"id": 2, "name": "Boundaries", "type": "Feature Layer"},
        ],
        "tables": [
            {"id": 3, "name": "Metadata_Table", "type": "Table"},
        ],
        "documentInfo": {
            "Author": "GIS Team",
            "Keywords": "census, demographics, Philadelphia",
        },
    }


@pytest.fixture
def services_root_response() -> dict[str, Any]:
    """Typical services root response."""
    return {
        "services": [
            {"name": "Census_2020", "type": "FeatureServer"},
            {"name": "Census_2010", "type": "FeatureServer"},
            {"name": "Transportation", "type": "MapServer"},
            {"name": "Basemap", "type": "MapServer"},
        ],
        "folders": ["Archived", "Internal"],
    }


def _mock_httpx_response(data: dict[str, Any]) -> MagicMock:
    """Create a mock httpx response with given JSON data."""
    mock_response = MagicMock()
    mock_response.json.return_value = data
    mock_response.raise_for_status = MagicMock()
    mock_response.status_code = 200
    return mock_response


# =============================================================================
# _ensure_json_format tests
# =============================================================================


class TestEnsureJsonFormat:
    """Tests for _ensure_json_format helper."""

    def test_adds_f_json_to_url_without_params(self) -> None:
        """Should add f=json to URL without query params."""
        url = "https://services.arcgis.com/test/FeatureServer"
        result = _ensure_json_format(url)

        assert "f=json" in result

    def test_preserves_existing_f_param(self) -> None:
        """Should not duplicate f param if already present."""
        url = "https://services.arcgis.com/test/FeatureServer?f=json"
        result = _ensure_json_format(url)

        # Should not have f=json twice
        assert result.count("f=json") == 1

    def test_preserves_other_params(self) -> None:
        """Should preserve existing query parameters."""
        url = "https://services.arcgis.com/test/FeatureServer?token=abc123"
        result = _ensure_json_format(url)

        assert "token=abc123" in result
        assert "f=json" in result


# =============================================================================
# discover_layers tests
# =============================================================================


class TestDiscoverLayers:
    """Tests for discover_layers function."""

    def test_discovers_layers_from_feature_server(
        self, feature_server_response: dict[str, Any]
    ) -> None:
        """Should discover all layers from FeatureServer response."""
        with patch("portolan_cli.extract.arcgis.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _mock_httpx_response(
                feature_server_response
            )

            result = discover_layers("https://services.arcgis.com/test/FeatureServer")

            assert len(result.layers) == 3
            assert result.layers[0].id == 0
            assert result.layers[0].name == "Census_Block_Groups"
            assert result.layers[1].id == 1
            assert result.layers[1].name == "Census_Tracts"

    def test_includes_tables_when_requested(self, feature_server_response: dict[str, Any]) -> None:
        """Should include tables when include_tables=True."""
        with patch("portolan_cli.extract.arcgis.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _mock_httpx_response(
                feature_server_response
            )

            result = discover_layers(
                "https://services.arcgis.com/test/FeatureServer",
                include_tables=True,
            )

            assert len(result.layers) == 4  # 3 layers + 1 table
            assert any(layer.name == "Metadata_Table" for layer in result.layers)

    def test_extracts_service_metadata(self, feature_server_response: dict[str, Any]) -> None:
        """Should extract service-level metadata."""
        with patch("portolan_cli.extract.arcgis.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _mock_httpx_response(
                feature_server_response
            )

            result = discover_layers("https://services.arcgis.com/test/FeatureServer")

            assert result.service_description == "Census data for Philadelphia"
            assert result.description == "Block groups and tracts"
            assert result.copyright_text == "City of Philadelphia"
            assert result.author == "GIS Team"
            assert result.keywords == "census, demographics, Philadelphia"

    def test_handles_missing_metadata_gracefully(self) -> None:
        """Should handle missing metadata fields without error."""
        minimal_response = {"layers": [{"id": 0, "name": "Layer_0", "type": "Feature Layer"}]}

        with patch("portolan_cli.extract.arcgis.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _mock_httpx_response(
                minimal_response
            )

            result = discover_layers("https://services.arcgis.com/test/FeatureServer")

            assert len(result.layers) == 1
            assert result.service_description is None
            assert result.copyright_text is None

    def test_raises_on_invalid_json(self) -> None:
        """Should raise ArcGISDiscoveryError on invalid JSON response."""
        with patch("portolan_cli.extract.arcgis.discovery.httpx.Client") as mock_client:
            mock_response = MagicMock()
            mock_response.json.side_effect = ValueError("Invalid JSON")
            mock_response.raise_for_status = MagicMock()
            mock_response.status_code = 200
            mock_client.return_value.__enter__.return_value.get.return_value = mock_response

            with pytest.raises(ArcGISDiscoveryError, match="Invalid JSON"):
                discover_layers("https://services.arcgis.com/test/FeatureServer")


# =============================================================================
# discover_services tests
# =============================================================================


class TestDiscoverServices:
    """Tests for discover_services function."""

    def test_discovers_services_from_root(self, services_root_response: dict[str, Any]) -> None:
        """Should discover all services from services root."""
        with patch("portolan_cli.extract.arcgis.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _mock_httpx_response(
                services_root_response
            )

            result = discover_services("https://services.arcgis.com/org/rest/services")

            assert len(result) == 4
            assert result[0].name == "Census_2020"
            assert result[0].service_type == "FeatureServer"
            assert result[2].name == "Transportation"
            assert result[2].service_type == "MapServer"

    def test_filters_by_service_type(self, services_root_response: dict[str, Any]) -> None:
        """Should filter services by type when specified."""
        with patch("portolan_cli.extract.arcgis.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _mock_httpx_response(
                services_root_response
            )

            result = discover_services(
                "https://services.arcgis.com/org/rest/services",
                service_types=["FeatureServer"],
            )

            assert len(result) == 2
            assert all(s.service_type == "FeatureServer" for s in result)

    def test_returns_folders_list(self, services_root_response: dict[str, Any]) -> None:
        """Should return list of folders in response."""
        with patch("portolan_cli.extract.arcgis.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _mock_httpx_response(
                services_root_response
            )

            result = discover_services(
                "https://services.arcgis.com/org/rest/services",
                return_folders=True,
            )

            # When return_folders=True, returns tuple (services, folders)
            services, folders = result
            assert len(folders) == 2
            assert "Archived" in folders
            assert "Internal" in folders

    def test_handles_empty_services_list(self) -> None:
        """Should handle empty services list."""
        empty_response: dict[str, Any] = {"services": [], "folders": []}

        with patch("portolan_cli.extract.arcgis.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _mock_httpx_response(
                empty_response
            )

            result = discover_services("https://services.arcgis.com/org/rest/services")

            assert result == []


# =============================================================================
# Data class tests
# =============================================================================


class TestLayerInfo:
    """Tests for LayerInfo dataclass."""

    def test_creates_layer_info(self) -> None:
        """Should create LayerInfo with required fields."""
        layer = LayerInfo(id=0, name="Test_Layer", layer_type="Feature Layer")

        assert layer.id == 0
        assert layer.name == "Test_Layer"
        assert layer.layer_type == "Feature Layer"

    def test_layer_info_equality(self) -> None:
        """LayerInfo instances with same values should be equal."""
        layer1 = LayerInfo(id=0, name="Test", layer_type="Feature Layer")
        layer2 = LayerInfo(id=0, name="Test", layer_type="Feature Layer")

        assert layer1 == layer2


class TestServiceInfo:
    """Tests for ServiceInfo dataclass."""

    def test_creates_service_info(self) -> None:
        """Should create ServiceInfo with required fields."""
        service = ServiceInfo(name="Test_Service", service_type="FeatureServer")

        assert service.name == "Test_Service"
        assert service.service_type == "FeatureServer"

    def test_service_url_generation(self) -> None:
        """Should generate full service URL from base URL."""
        service = ServiceInfo(name="Census_2020", service_type="FeatureServer")
        base_url = "https://services.arcgis.com/org/rest/services"

        url = service.get_url(base_url)

        assert url == "https://services.arcgis.com/org/rest/services/Census_2020/FeatureServer"


class TestServiceDiscoveryResult:
    """Tests for ServiceDiscoveryResult dataclass."""

    def test_creates_with_minimal_data(self) -> None:
        """Should create result with just layers."""
        result = ServiceDiscoveryResult(layers=[])

        assert result.layers == []
        assert result.service_description is None
        assert result.copyright_text is None

    def test_creates_with_full_metadata(self) -> None:
        """Should create result with all metadata fields."""
        layer = LayerInfo(id=0, name="Test", layer_type="Feature Layer")
        result = ServiceDiscoveryResult(
            layers=[layer],
            service_description="Test service",
            description="Description",
            copyright_text="Copyright",
            author="Author",
            keywords="key1, key2",
            license_info="License",
            access_information="Access info",
        )

        assert len(result.layers) == 1
        assert result.service_description == "Test service"
        assert result.author == "Author"


# =============================================================================
# _fetch_json token and embedded-error tests
# =============================================================================


@pytest.mark.unit
def test_fetch_json_raises_on_embedded_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Should raise ArcGISDiscoveryError when ArcGIS returns an embedded error body."""

    def fake_get(self: object, url: str) -> httpx.Response:
        return httpx.Response(200, json={"error": {"code": 499, "message": "Token Required"}})

    monkeypatch.setattr(httpx.Client, "get", fake_get)
    with pytest.raises(ArcGISDiscoveryError, match="499"):
        _fetch_json("https://x/rest/services/Secret")


@pytest.mark.unit
def test_fetch_json_appends_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """Should append token=<token> to the request URL when token is provided."""
    seen: dict[str, str] = {}

    def fake_get(self: object, url: str) -> httpx.Response:
        seen["url"] = url
        return httpx.Response(200, json={"services": [], "folders": []})

    monkeypatch.setattr(httpx.Client, "get", fake_get)
    _fetch_json("https://x/rest/services", token="ABC123")
    assert "token=ABC123" in seen["url"]


@pytest.mark.unit
def test_fetch_json_raises_on_http_error_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """Should raise ArcGISDiscoveryError when the server returns a 4xx/5xx status."""

    def fake_get(self: object, url: str) -> httpx.Response:
        return httpx.Response(404, json={})

    monkeypatch.setattr(httpx.Client, "get", fake_get)
    with pytest.raises(ArcGISDiscoveryError, match="HTTP 404"):
        _fetch_json("https://x/rest/services/Missing")


@pytest.mark.unit
def test_discover_services_forwards_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """Should forward token to the request so --no-recurse discovery stays authenticated."""
    seen: dict[str, str] = {}

    def fake_get(self: object, url: str) -> httpx.Response:
        seen["url"] = url
        return httpx.Response(200, json={"services": [], "folders": []})

    monkeypatch.setattr(httpx.Client, "get", fake_get)
    discover_services("https://x/rest/services", token="TKN789")
    assert "token=TKN789" in seen["url"]


# =============================================================================
# discover_services_recursive tests
# =============================================================================


def _mock_endpoints(monkeypatch: pytest.MonkeyPatch, responses: dict[str, dict]) -> None:  # type: ignore[type-arg]
    """Map base URL (without query) -> JSON body."""

    def fake_fetch(url: str, timeout: float = 60.0, token: str | None = None) -> dict[str, Any]:
        base = url.split("?")[0]
        if base not in responses:
            raise ArcGISDiscoveryError(f"ArcGIS error from {url}: 499 Token Required")
        return responses[base]

    monkeypatch.setattr("portolan_cli.extract.arcgis.discovery._fetch_json", fake_fetch)


@pytest.mark.unit
def test_recursive_merges_folder_services(monkeypatch: pytest.MonkeyPatch) -> None:
    """Should merge root services with services from all folders."""
    root = "https://x/rest/services"
    _mock_endpoints(
        monkeypatch,
        {
            root: {
                "services": [{"name": "Top", "type": "MapServer"}],
                "folders": ["NationalDatasets"],
            },
            f"{root}/NationalDatasets": {
                "services": [
                    {"name": "NationalDatasets/Property", "type": "MapServer"},
                    {"name": "NationalDatasets/Agriculture", "type": "MapServer"},
                ],
                "folders": [],
            },
        },
    )
    services, traversal = discover_services_recursive(root)
    names = sorted(s.name for s in services)
    assert names == ["NationalDatasets/Agriculture", "NationalDatasets/Property", "Top"]
    assert traversal.visited == ["NationalDatasets"]
    assert traversal.skipped == []
    assert traversal.service_count == 3
    assert isinstance(traversal, FolderTraversal)


@pytest.mark.unit
def test_recursive_skips_secured_folder(monkeypatch: pytest.MonkeyPatch) -> None:
    """Should skip secured (erroring) folders and record them in skipped."""
    root = "https://x/rest/services"
    _mock_endpoints(
        monkeypatch,
        {
            root: {"services": [], "folders": ["Open", "Locked"]},
            f"{root}/Open": {
                "services": [{"name": "Open/A", "type": "MapServer"}],
                "folders": [],
            },
            # Locked intentionally absent -> fake_fetch raises -> skipped
        },
    )
    services, traversal = discover_services_recursive(root)
    assert [s.name for s in services] == ["Open/A"]
    assert traversal.visited == ["Open"]
    assert [f for f, _ in traversal.skipped] == ["Locked"]


@pytest.mark.unit
def test_recursive_respects_max_depth(monkeypatch: pytest.MonkeyPatch) -> None:
    """Should not fetch folders beyond max_depth."""
    root = "https://x/rest/services"
    _mock_endpoints(
        monkeypatch,
        {
            root: {"services": [], "folders": ["L1"]},
            f"{root}/L1": {
                "services": [{"name": "L1/A", "type": "MapServer"}],
                "folders": ["L2"],
            },
            f"{root}/L2": {
                "services": [{"name": "L2/B", "type": "MapServer"}],
                "folders": [],
            },
        },
    )
    services, _ = discover_services_recursive(root, max_depth=1)
    # depth 1 fetches root (depth 0) + L1 (depth 1), not L2
    assert [s.name for s in services] == ["L1/A"]


@pytest.mark.unit
def test_recursive_filters_by_service_type(monkeypatch: pytest.MonkeyPatch) -> None:
    """Should only return services matching the given service_types."""
    root = "https://x/rest/services"
    _mock_endpoints(
        monkeypatch,
        {
            root: {
                "services": [
                    {"name": "Tool", "type": "GPServer"},
                    {"name": "Map", "type": "MapServer"},
                ],
                "folders": [],
            },
        },
    )
    services, _ = discover_services_recursive(root, service_types=["FeatureServer", "MapServer"])
    assert [s.name for s in services] == ["Map"]


@pytest.mark.unit
def test_recursive_builds_full_path_for_nested_folders(monkeypatch: pytest.MonkeyPatch) -> None:
    """Should build full root-relative paths for sub-folders, not bare names."""
    root = "https://x/rest/services"
    _mock_endpoints(
        monkeypatch,
        {
            root: {"services": [], "folders": ["L1"]},
            f"{root}/L1": {"services": [{"name": "L1/A", "type": "MapServer"}], "folders": ["L2"]},
            f"{root}/L1/L2": {
                "services": [{"name": "L1/L2/B", "type": "MapServer"}],
                "folders": [],
            },
        },
    )
    services, traversal = discover_services_recursive(root, max_depth=2)
    assert sorted(s.name for s in services) == ["L1/A", "L1/L2/B"]
    assert traversal.visited == ["L1", "L1/L2"]
