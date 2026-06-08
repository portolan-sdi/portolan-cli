"""Tests for style extraction orchestration.

Tests the high-level API for extracting styles from WMS and ESRI endpoints.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from portolan_cli.extract.common.styles import (
    StyleExtractionError,
    _build_wms_getstyles_url,
    extract_esri_style,
    extract_wms_style,
)

pytestmark = pytest.mark.unit


class TestBuildWMSGetStylesURL:
    """Tests for WMS URL construction."""

    def test_geoserver_wfs_to_wms(self) -> None:
        """GeoServer WFS URL converts to WMS GetStyles."""
        wfs_url = "https://geonode.example.com/geoserver/wfs"
        result = _build_wms_getstyles_url(wfs_url, "geonode:layer")

        assert "/wms?" in result
        assert "service=WMS" in result
        assert "request=GetStyles" in result
        assert "layers=geonode%3Alayer" in result or "layers=geonode:layer" in result

    def test_geoserver_ows_to_wms(self) -> None:
        """GeoServer OWS URL converts to WMS."""
        wfs_url = "https://example.com/geoserver/ows?service=WFS"
        result = _build_wms_getstyles_url(wfs_url, "layer")

        assert "/wms?" in result
        assert "request=GetStyles" in result

    def test_preserves_host_and_path(self) -> None:
        """Host and base path are preserved."""
        wfs_url = "https://geonode.pergamino.gob.ar/geoserver/wfs"
        result = _build_wms_getstyles_url(wfs_url, "test")

        assert "geonode.pergamino.gob.ar" in result
        assert "/geoserver/wms" in result


class TestExtractWMSStyle:
    """Tests for WMS style extraction."""

    @pytest.fixture
    def sample_sld(self) -> str:
        """Simple SLD for testing."""
        return """<?xml version="1.0" encoding="UTF-8"?>
        <sld:StyledLayerDescriptor xmlns:sld="http://www.opengis.net/sld">
            <sld:NamedLayer>
                <sld:Name>test</sld:Name>
                <sld:UserStyle>
                    <sld:Name>test</sld:Name>
                    <sld:FeatureTypeStyle>
                        <sld:Rule>
                            <sld:PolygonSymbolizer>
                                <sld:Fill>
                                    <sld:CssParameter name="fill">#ff0000</sld:CssParameter>
                                </sld:Fill>
                            </sld:PolygonSymbolizer>
                        </sld:Rule>
                    </sld:FeatureTypeStyle>
                </sld:UserStyle>
            </sld:NamedLayer>
        </sld:StyledLayerDescriptor>
        """

    def test_extracts_and_writes_style(self, tmp_path: Path, sample_sld: str) -> None:
        """Successfully extracts SLD and writes Mapbox GL JSON."""
        collection_path = tmp_path / "test-collection"
        collection_path.mkdir()

        with patch("portolan_cli.extract.common.styles._fetch_wms_style") as mock_fetch:
            mock_fetch.return_value = sample_sld

            result = extract_wms_style(
                wfs_url="https://example.com/geoserver/wfs",
                layer_name="test",
                collection_path=collection_path,
            )

        assert result is not None
        assert result.path.exists()
        assert result.source_format == "sld"

        # Verify written JSON is valid Mapbox GL
        style = json.loads(result.path.read_text())
        assert style["version"] == 8
        assert len(style["layers"]) >= 1

    def test_returns_none_on_fetch_failure(self, tmp_path: Path) -> None:
        """Returns None when WMS request fails."""
        collection_path = tmp_path / "test-collection"
        collection_path.mkdir()

        with patch("portolan_cli.extract.common.styles._fetch_wms_style") as mock_fetch:
            mock_fetch.side_effect = StyleExtractionError("Connection failed")

            result = extract_wms_style(
                wfs_url="https://example.com/geoserver/wfs",
                layer_name="test",
                collection_path=collection_path,
            )

        assert result is None

    def test_strips_workspace_prefix(self, tmp_path: Path, sample_sld: str) -> None:
        """Workspace prefix like 'geonode:' is stripped for source-layer."""
        collection_path = tmp_path / "test-collection"
        collection_path.mkdir()

        with patch("portolan_cli.extract.common.styles._fetch_wms_style") as mock_fetch:
            mock_fetch.return_value = sample_sld

            result = extract_wms_style(
                wfs_url="https://example.com/geoserver/wfs",
                layer_name="geonode:mylayer",
                collection_path=collection_path,
            )

        assert result is not None
        style = json.loads(result.path.read_text())
        # source-layer should be "mylayer" (stripped prefix), not "geonode:mylayer"
        assert style["layers"][0]["source-layer"] == "mylayer"


class TestExtractESRIStyle:
    """Tests for ESRI style extraction."""

    @pytest.fixture
    def sample_layer_json(self) -> dict[str, Any]:
        """Simple ESRI layer JSON with renderer."""
        return {
            "name": "TestLayer",
            "drawingInfo": {
                "renderer": {
                    "type": "simple",
                    "symbol": {
                        "type": "esriSFS",
                        "style": "esriSFSSolid",
                        "color": [255, 0, 0, 255],
                    },
                }
            },
        }

    def test_extracts_and_writes_style(
        self, tmp_path: Path, sample_layer_json: dict[str, Any]
    ) -> None:
        """Successfully extracts renderer and writes Mapbox GL JSON."""
        collection_path = tmp_path / "test-collection"
        collection_path.mkdir()

        with patch("portolan_cli.extract.common.styles._fetch_esri_layer_json") as mock_fetch:
            mock_fetch.return_value = sample_layer_json

            result = extract_esri_style(
                layer_url="https://example.com/FeatureServer/0",
                collection_path=collection_path,
            )

        assert result is not None
        assert result.path.exists()
        assert result.source_format == "esri"

        # Verify written JSON is valid Mapbox GL
        style = json.loads(result.path.read_text())
        assert style["version"] == 8
        assert len(style["layers"]) >= 1
        assert style["layers"][0]["type"] == "fill"

    def test_returns_none_when_no_renderer(self, tmp_path: Path) -> None:
        """Returns None when layer has no drawingInfo."""
        collection_path = tmp_path / "test-collection"
        collection_path.mkdir()

        with patch("portolan_cli.extract.common.styles._fetch_esri_layer_json") as mock_fetch:
            mock_fetch.return_value = {"name": "TestLayer"}

            result = extract_esri_style(
                layer_url="https://example.com/FeatureServer/0",
                collection_path=collection_path,
            )

        assert result is None

    def test_returns_none_on_fetch_failure(self, tmp_path: Path) -> None:
        """Returns None when ESRI request fails."""
        collection_path = tmp_path / "test-collection"
        collection_path.mkdir()

        with patch("portolan_cli.extract.common.styles._fetch_esri_layer_json") as mock_fetch:
            mock_fetch.side_effect = StyleExtractionError("Connection failed")

            result = extract_esri_style(
                layer_url="https://example.com/FeatureServer/0",
                collection_path=collection_path,
            )

        assert result is None

    def test_uses_layer_name_as_source_layer(
        self, tmp_path: Path, sample_layer_json: dict[str, Any]
    ) -> None:
        """Layer name from JSON is used as source-layer."""
        collection_path = tmp_path / "test-collection"
        collection_path.mkdir()

        with patch("portolan_cli.extract.common.styles._fetch_esri_layer_json") as mock_fetch:
            mock_fetch.return_value = sample_layer_json

            result = extract_esri_style(
                layer_url="https://example.com/FeatureServer/0",
                collection_path=collection_path,
            )

        assert result is not None
        style = json.loads(result.path.read_text())
        assert style["layers"][0]["source-layer"] == "TestLayer"


class TestStyleFileOutput:
    """Tests for style file writing."""

    def test_creates_styles_directory(self, tmp_path: Path) -> None:
        """styles/ directory is created if it doesn't exist."""
        collection_path = tmp_path / "test-collection"
        collection_path.mkdir()

        sample_json = {
            "name": "Test",
            "drawingInfo": {
                "renderer": {
                    "type": "simple",
                    "symbol": {"type": "esriSFS", "color": [0, 0, 255, 255]},
                }
            },
        }

        with patch("portolan_cli.extract.common.styles._fetch_esri_layer_json") as mock_fetch:
            mock_fetch.return_value = sample_json

            result = extract_esri_style(
                layer_url="https://example.com/FeatureServer/0",
                collection_path=collection_path,
            )

        assert result is not None
        assert (collection_path / "styles").is_dir()
        assert result.path == collection_path / "styles" / "source.json"

    def test_custom_style_name(self, tmp_path: Path) -> None:
        """Custom style name is used for output file."""
        collection_path = tmp_path / "test-collection"
        collection_path.mkdir()

        sample_json = {
            "name": "Test",
            "drawingInfo": {
                "renderer": {
                    "type": "simple",
                    "symbol": {"type": "esriSFS", "color": [0, 0, 255, 255]},
                }
            },
        }

        with patch("portolan_cli.extract.common.styles._fetch_esri_layer_json") as mock_fetch:
            mock_fetch.return_value = sample_json

            result = extract_esri_style(
                layer_url="https://example.com/FeatureServer/0",
                collection_path=collection_path,
                style_name="original",
            )

        assert result is not None
        assert result.path.name == "original.json"
