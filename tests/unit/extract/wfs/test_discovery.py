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
        mock_layers = [
            {"typename": "buildings", "title": "Buildings", "abstract": None, "bbox": None}
        ]

        with patch("portolan_cli.extract.wfs.discovery.gpio_list_layers") as mock_list:
            mock_list.return_value = mock_layers
            result = discover_layers("https://example.com/wfs")

        assert len(result.layers) == 1
        assert result.layers[0].name == "buildings"
        assert result.service_url == "https://example.com/wfs"


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


class TestWFSDiscoveryError:
    """Tests for WFSDiscoveryError."""

    def test_error_message(self) -> None:
        """Error includes message."""
        error = WFSDiscoveryError("Connection failed")
        assert str(error) == "Connection failed"
