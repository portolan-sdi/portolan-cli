"""Tests for WFS layer discovery.

These tests verify the WFS discovery module which wraps geoparquet-io's
WFS capabilities and normalizes terminology (typename → layer).
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from portolan_cli.extract.wfs.discovery import (
    LayerInfo,
    WFSDiscoveryError,
    discover_layers,
    list_layers,
)

pytestmark = pytest.mark.unit


class TestListLayers:
    """Tests for list_layers function."""

    def test_list_layers_returns_layer_info(self) -> None:
        """list_layers returns LayerInfo objects."""
        mock_layers = [
            {
                "typename": "buildings",
                "title": "Building Footprints",
                "abstract": "Building polygons",
                "bbox": (-122.5, 37.5, -122.0, 38.0),
            },
            {
                "typename": "roads",
                "title": "Road Network",
                "abstract": None,
                "bbox": None,
            },
        ]

        with patch("portolan_cli.extract.wfs.discovery.gpio_list_layers") as mock:
            mock.return_value = mock_layers
            layers = list_layers("https://example.com/wfs")

        assert len(layers) == 2
        assert isinstance(layers[0], LayerInfo)
        assert layers[0].name == "buildings"
        assert layers[0].title == "Building Footprints"
        assert layers[1].name == "roads"

    def test_list_layers_normalizes_typename_to_name(self) -> None:
        """typename field is exposed as 'name' for consistency."""
        mock_layers = [{"typename": "ns:featuretype", "title": "Test"}]

        with patch("portolan_cli.extract.wfs.discovery.gpio_list_layers") as mock:
            mock.return_value = mock_layers
            layers = list_layers("https://example.com/wfs")

        assert layers[0].name == "ns:featuretype"
        assert layers[0].typename == "ns:featuretype"  # Original also available

    def test_list_layers_handles_empty_service(self) -> None:
        """Empty WFS service returns empty list."""
        with patch("portolan_cli.extract.wfs.discovery.gpio_list_layers") as mock:
            mock.return_value = []
            layers = list_layers("https://example.com/wfs")

        assert layers == []


class TestDiscoverLayers:
    """Tests for discover_layers function (full discovery with metadata)."""

    def test_discover_layers_returns_discovery_result(self) -> None:
        """discover_layers returns WFSDiscoveryResult with service metadata."""
        # Create a mock WFS capabilities object
        from unittest.mock import MagicMock

        mock_wfs = MagicMock()

        # Mock identification
        mock_wfs.identification = MagicMock()
        mock_wfs.identification.title = "Test WFS Service"
        mock_wfs.identification.abstract = "A test WFS service"
        mock_wfs.identification.keywords = ["test", "wfs"]
        mock_wfs.identification.accessconstraints = None
        mock_wfs.identification.fees = None

        # Mock provider
        mock_wfs.provider = MagicMock()
        mock_wfs.provider.name = "Test Provider"
        mock_wfs.provider.contact = MagicMock()
        mock_wfs.provider.contact.name = "Test Contact"
        mock_wfs.provider.contact.organization = "Test Org"

        # Mock layer contents
        mock_layer = MagicMock()
        mock_layer.title = "Buildings"
        mock_layer.abstract = "Building footprints"
        mock_layer.keywords = ["Buildings", "Bâtiments"]
        mock_layer.boundingBoxWGS84 = (-122.5, 37.5, -122.0, 38.0)

        mock_wfs.contents = {"ns:buildings": mock_layer}

        with patch("geoparquet_io.core.wfs.get_wfs_capabilities") as mock_get:
            mock_get.return_value = mock_wfs
            result = discover_layers("https://example.com/wfs")

        assert len(result.layers) == 1
        assert result.layers[0].name == "ns:buildings"
        assert result.layers[0].keywords == ["Buildings", "Bâtiments"]
        assert result.service_url == "https://example.com/wfs"
        assert result.service_title == "Test WFS Service"
        assert result.service_abstract == "A test WFS service"
        assert result.provider == "Test Provider"
        assert result.contact_name == "Test Contact, Test Org"
        assert result.keywords == ["test", "wfs"]

    def test_discover_layers_handles_missing_metadata(self) -> None:
        """discover_layers handles services with missing metadata gracefully."""
        from unittest.mock import MagicMock

        mock_wfs = MagicMock()
        mock_wfs.identification = None
        mock_wfs.provider = None

        mock_layer = MagicMock()
        mock_layer.title = "Roads"
        mock_layer.abstract = None
        mock_layer.keywords = None
        mock_layer.boundingBoxWGS84 = None

        mock_wfs.contents = {"roads": mock_layer}

        with patch("geoparquet_io.core.wfs.get_wfs_capabilities") as mock_get:
            mock_get.return_value = mock_wfs
            result = discover_layers("https://example.com/wfs")

        assert len(result.layers) == 1
        assert result.layers[0].keywords is None
        assert result.service_title is None
        assert result.provider is None

    def test_discover_layers_extracts_layer_keywords(self) -> None:
        """discover_layers extracts layer-specific keywords."""
        from unittest.mock import MagicMock

        mock_wfs = MagicMock()
        mock_wfs.identification = None
        mock_wfs.provider = None

        # Layer with keywords
        mock_layer1 = MagicMock()
        mock_layer1.title = "Buildings"
        mock_layer1.abstract = "Building footprints"
        mock_layer1.keywords = ["Buildings", "Structures", "Bâtiments"]
        mock_layer1.boundingBoxWGS84 = None

        # Layer without keywords
        mock_layer2 = MagicMock()
        mock_layer2.title = "Roads"
        mock_layer2.abstract = None
        mock_layer2.keywords = []
        mock_layer2.boundingBoxWGS84 = None

        mock_wfs.contents = {"buildings": mock_layer1, "roads": mock_layer2}

        with patch("geoparquet_io.core.wfs.get_wfs_capabilities") as mock_get:
            mock_get.return_value = mock_wfs
            result = discover_layers("https://example.com/wfs")

        assert len(result.layers) == 2
        buildings_layer = next(lyr for lyr in result.layers if lyr.name == "buildings")
        roads_layer = next(lyr for lyr in result.layers if lyr.name == "roads")

        assert buildings_layer.keywords == ["Buildings", "Structures", "Bâtiments"]
        assert roads_layer.keywords is None  # Empty list -> None


class TestLayerInfo:
    """Tests for LayerInfo dataclass."""

    def test_layer_info_to_dict(self) -> None:
        """LayerInfo converts to dict for filtering."""
        layer = LayerInfo(
            name="buildings",
            typename="ns:buildings",
            title="Building Footprints",
            abstract="Polygons",
            bbox=(-122.5, 37.5, -122.0, 38.0),
        )

        d = layer.to_filter_dict()

        assert d["id"] == 0  # Default ID
        assert d["name"] == "buildings"

    def test_layer_info_with_id(self) -> None:
        """LayerInfo can have explicit ID for filtering."""
        layer = LayerInfo(
            name="roads",
            typename="roads",
            title="Roads",
            abstract=None,
            bbox=None,
            id=5,
        )

        d = layer.to_filter_dict()
        assert d["id"] == 5

    def test_layer_info_with_keywords(self) -> None:
        """LayerInfo stores layer-specific keywords."""
        layer = LayerInfo(
            name="buildings",
            typename="inspire_bu:BU.Building",
            title="Buildings",
            abstract="Building footprints",
            keywords=["Buildings", "Bâtiments"],
            bbox=(-122.5, 37.5, -122.0, 38.0),
            id=0,
        )

        assert layer.keywords == ["Buildings", "Bâtiments"]

    def test_layer_info_keywords_default_none(self) -> None:
        """LayerInfo keywords default to None."""
        layer = LayerInfo(
            name="roads",
            typename="roads",
            title="Roads",
        )

        assert layer.keywords is None


class TestWFSDiscoveryError:
    """Tests for WFSDiscoveryError."""

    def test_error_message(self) -> None:
        """Error includes message."""
        error = WFSDiscoveryError("Connection failed")
        assert str(error) == "Connection failed"
