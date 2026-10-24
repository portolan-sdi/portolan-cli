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
    ArcGISClient,
    ArcGISDiscoveryError,
    FolderTraversal,
    LayerInfo,
    ServiceDiscoveryResult,
    ServiceInfo,
    discover_layers,
    discover_services,
    discover_services_recursive,
    ensure_json_format,
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
# ensure_json_format tests
# =============================================================================


class TestEnsureJsonFormat:
    """Tests for ensure_json_format helper."""

    def test_adds_f_json_to_url_without_params(self) -> None:
        """Should add f=json to URL without query params."""
        url = "https://services.arcgis.com/test/FeatureServer"
        result = ensure_json_format(url)

        assert "f=json" in result

    def test_preserves_existing_f_param(self) -> None:
        """Should not duplicate f param if already present."""
        url = "https://services.arcgis.com/test/FeatureServer?f=json"
        result = ensure_json_format(url)

        # Should not have f=json twice
        assert result.count("f=json") == 1

    def test_preserves_other_params(self) -> None:
        """Should preserve existing query parameters."""
        url = "https://services.arcgis.com/test/FeatureServer?token=abc123"
        result = ensure_json_format(url)

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

    def test_returns_tables_apart_from_layers(
        self, feature_server_response: dict[str, Any]
    ) -> None:
        """#812: discovery reports the tables; the caller decides to extract them."""
        with patch("portolan_cli.extract.arcgis.discovery.httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _mock_httpx_response(
                feature_server_response
            )

            result = discover_layers("https://services.arcgis.com/test/FeatureServer")

            assert [layer.name for layer in result.layers] == [
                "Census_Block_Groups",
                "Census_Tracts",
                "Boundaries",
            ]
            assert result.tables == [LayerInfo(id=3, name="Metadata_Table", layer_type="Table")]

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
# ArcGISClient token, retry, and embedded-error tests
# =============================================================================


@pytest.mark.unit
def test_get_json_raises_on_embedded_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Should raise ArcGISDiscoveryError when ArcGIS returns an embedded error body."""

    def fake_get(self: object, url: str) -> httpx.Response:
        return httpx.Response(200, json={"error": {"code": 499, "message": "Token Required"}})

    monkeypatch.setattr(httpx.Client, "get", fake_get)
    with pytest.raises(ArcGISDiscoveryError, match="499"):
        ArcGISClient().get_json("https://x/rest/services/Secret")


@pytest.mark.unit
def test_get_json_appends_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """Should append token=<token> to the request URL when token is provided."""
    seen: dict[str, str] = {}

    def fake_get(self: object, url: str) -> httpx.Response:
        seen["url"] = url
        return httpx.Response(200, json={"services": [], "folders": []})

    monkeypatch.setattr(httpx.Client, "get", fake_get)
    ArcGISClient(token="ABC123").get_json("https://x/rest/services")
    assert "token=ABC123" in seen["url"]


@pytest.mark.unit
def test_get_json_raises_on_http_error_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """Should raise ArcGISDiscoveryError when the server returns a 4xx/5xx status."""

    def fake_get(self: object, url: str) -> httpx.Response:
        return httpx.Response(404, json={})

    monkeypatch.setattr(httpx.Client, "get", fake_get)
    with pytest.raises(ArcGISDiscoveryError, match="HTTP 404"):
        ArcGISClient().get_json("https://x/rest/services/Missing")


@pytest.mark.unit
def test_get_json_raises_on_non_object_json(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-object JSON must raise ArcGISDiscoveryError, not AttributeError on .get()."""

    def fake_get(self: object, url: str) -> httpx.Response:
        return httpx.Response(200, json=["not", "an", "object"])

    monkeypatch.setattr(httpx.Client, "get", fake_get)
    with pytest.raises(ArcGISDiscoveryError, match="Expected JSON object"):
        ArcGISClient().get_json("https://x/rest/services")


@pytest.mark.unit
def test_discover_services_forwards_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """Should forward token to the request so --no-recurse discovery stays authenticated."""
    seen: dict[str, str] = {}

    def fake_get(self: object, url: str) -> httpx.Response:
        seen["url"] = url
        return httpx.Response(200, json={"services": [], "folders": []})

    monkeypatch.setattr(httpx.Client, "get", fake_get)
    discover_services("https://x/rest/services", client=ArcGISClient(token="TKN789"))
    assert "token=TKN789" in seen["url"]


# =============================================================================
# discover_services_recursive tests
# =============================================================================


def _mock_endpoints(monkeypatch: pytest.MonkeyPatch, responses: dict[str, dict]) -> None:  # type: ignore[type-arg]
    """Map base URL (without query) -> JSON body."""

    def fake_fetch(self: ArcGISClient, url: str) -> dict[str, Any]:
        base = url.split("?")[0]
        if base not in responses:
            raise ArcGISDiscoveryError(f"ArcGIS error from {url}: 499 Token Required")
        return responses[base]

    monkeypatch.setattr(ArcGISClient, "get_json", fake_fetch)


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


# =============================================================================
# Query capability detection (issue #1001)
# =============================================================================


def _discover_with(monkeypatch: pytest.MonkeyPatch, body: dict[str, Any]) -> ServiceDiscoveryResult:
    monkeypatch.setattr(ArcGISClient, "get_json", lambda self, url, **_: body)
    return discover_layers("https://x/rest/services/S/MapServer")


@pytest.mark.unit
def test_discover_layers_skips_tiles_only_service(monkeypatch: pytest.MonkeyPatch) -> None:
    result = _discover_with(
        monkeypatch,
        {
            "capabilities": "Map,ChangeTracking,TilesOnly,Tilemap",
            "layers": [{"id": 0, "name": "punten", "type": "Feature Layer"}],
        },
    )
    assert result.layers == []
    assert [layer.name for layer in result.skipped_layers] == ["punten"]
    assert result.skip_reason == "skipped: no Query capability (tiles-only)"


@pytest.mark.unit
def test_discover_layers_reads_every_field(monkeypatch: pytest.MonkeyPatch) -> None:
    """A layer or table without a type gets the default type. The license fields are kept."""
    result = _discover_with(
        monkeypatch,
        {
            "layers": [{"id": 4, "name": "roads"}, {"id": 5, "name": "rail", "type": "Raster"}],
            "tables": [{"id": 6, "name": "codes"}],
            "licenseInfo": "CC-BY 4.0",
            "accessInformation": "City GIS",
        },
    )

    assert (result.layers, result.tables) == (
        [LayerInfo(4, "roads", "Feature Layer"), LayerInfo(5, "rail", "Raster")],
        [LayerInfo(6, "codes", "Table")],
    )
    assert (result.license_info, result.access_information) == ("CC-BY 4.0", "City GIS")


@pytest.mark.unit
@pytest.mark.parametrize("capabilities", ["Map,Query,Data", "Query", " map , QUERY ", None, "", 5])
def test_discover_layers_keeps_layers_when_query_or_unknown(
    monkeypatch: pytest.MonkeyPatch, capabilities: Any
) -> None:
    body: dict[str, Any] = {"layers": [{"id": 0, "name": "roads", "type": "Feature Layer"}]}
    if capabilities is not None:
        body["capabilities"] = capabilities
    result = _discover_with(monkeypatch, body)
    assert [layer.name for layer in result.layers] == ["roads"]
    assert result.skipped_layers == []
    assert result.skip_reason is None


# =============================================================================
# One client for every discovery request: token (#855) and retry (#811)
# =============================================================================


def _transport(answers: list[httpx.Response], seen: list[str]) -> httpx.MockTransport:
    """Answer each request with the next response, and record each URL."""

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return answers.pop(0)

    return httpx.MockTransport(handler)


_SERVICE = {"layers": [{"id": 0, "name": "Parcels", "type": "Feature Layer"}]}


@pytest.mark.unit
def test_discover_layers_sends_the_token() -> None:
    """#855: a token-gated server needs the token on the layer-list request."""
    seen: list[str] = []
    client = ArcGISClient(
        token="TKN", transport=_transport([httpx.Response(200, json=_SERVICE)], seen)
    )

    found = discover_layers("https://x/rest/services/S/FeatureServer", client)

    assert [layer.name for layer in found.layers] == ["Parcels"]
    assert seen == ["https://x/rest/services/S/FeatureServer?f=json&token=TKN"]


@pytest.mark.unit
def test_transient_failure_is_retried_once_and_discovery_completes() -> None:
    """#811: one HTTP 503 during discovery does not end the harvest."""
    seen: list[str] = []
    answers = [httpx.Response(503, json={}), httpx.Response(200, json=_SERVICE)]
    client = ArcGISClient(retries=3, retry_delay=0.0, transport=_transport(answers, seen))

    found = discover_layers("https://x/rest/services/S/FeatureServer", client)

    assert [layer.name for layer in found.layers] == ["Parcels"]
    assert len(seen) == 2


@pytest.mark.unit
def test_embedded_server_error_is_retried() -> None:
    seen: list[str] = []
    answers = [
        httpx.Response(200, json={"error": {"code": 500, "message": "Server busy"}}),
        httpx.Response(200, json=_SERVICE),
    ]
    client = ArcGISClient(retry_delay=0.0, transport=_transport(answers, seen))

    assert discover_layers("https://x/S/FeatureServer", client).layers[0].name == "Parcels"
    assert len(seen) == 2


@pytest.mark.unit
@pytest.mark.parametrize(
    "answer",
    [
        httpx.Response(404, json={}),
        httpx.Response(200, json={"error": {"code": 499, "message": "Token Required"}}),
    ],
)
def test_permanent_failure_is_not_retried(answer: httpx.Response) -> None:
    """#811: a bad URL or a missing token fails at once, with no repeat request."""
    seen: list[str] = []
    client = ArcGISClient(retries=3, retry_delay=0.0, transport=_transport([answer], seen))

    with pytest.raises(ArcGISDiscoveryError):
        discover_layers("https://x/S/FeatureServer", client)
    assert len(seen) == 1


_EMPTY_SERVICE = {"currentVersion": 12, "layers": [], "tables": []}


@pytest.mark.unit
def test_empty_layer_list_is_asked_again() -> None:
    """ArcGIS Online answers about half the requests for one live service with no layers.

    The live suite saw it on _Sharswood_Choice_Neighborhood_Boundary: 21 of 40
    requests in a row listed ``"layers": []`` in an otherwise equal body.
    """
    seen: list[str] = []
    answers = [httpx.Response(200, json=_EMPTY_SERVICE), httpx.Response(200, json=_SERVICE)]
    client = ArcGISClient(retries=3, retry_delay=0.0, transport=_transport(answers, seen))

    found = discover_layers("https://x/S/FeatureServer", client)

    assert [layer.name for layer in found.layers] == ["Parcels"]
    assert len(seen) == 2


@pytest.mark.unit
def test_service_that_stays_empty_returns_no_layers() -> None:
    seen: list[str] = []
    answers = [httpx.Response(200, json=_EMPTY_SERVICE) for _ in range(3)]
    client = ArcGISClient(retries=3, retry_delay=0.0, transport=_transport(answers, seen))

    found = discover_layers("https://x/S/FeatureServer", client)

    assert (found.layers, found.tables, len(seen)) == ([], [], 3)


def test_fetch_error_names_the_service_without_its_userinfo(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """#981: the error and the retry log name the service, not the password in its URL."""

    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    client = ArcGISClient(retries=2, retry_delay=0.0, transport=httpx.MockTransport(refuse))
    url = "https://user:SECRET@h.example/arcgis/rest/services/X/MapServer"

    with (
        caplog.at_level("DEBUG", logger="portolan_cli.extract"),
        pytest.raises(ArcGISDiscoveryError) as caught,
    ):
        client.get_json(url)

    clean = "https://h.example/arcgis/rest/services/X/MapServer"
    assert str(caught.value) == f"Failed to fetch from {clean}: refused"
    assert caplog.messages == [f"Retry 1 for {clean}: Failed to fetch from {clean}: refused"]
