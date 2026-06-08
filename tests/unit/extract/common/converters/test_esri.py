"""Tests for ESRI renderer to Mapbox GL converter.

Uses real fixture data from ESRI REST endpoints:
- esri_classbreaks.json: Census MapServer graduated symbol sizes
- esri_uniquevalue.json: PAD-US categorical fill colors
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from portolan_cli.extract.common.converters.esri import (
    ESRIConverterError,
    convert_esri_renderer,
    parse_classbreaks_renderer,
    parse_simple_renderer,
    parse_uniquevalue_renderer,
)

pytestmark = pytest.mark.unit

FIXTURES_DIR = Path(__file__).parent.parent.parent.parent.parent / "fixtures" / "styles"


@pytest.fixture
def classbreaks_renderer() -> dict[str, Any]:
    """Load Census ClassBreaks renderer fixture."""
    data = json.loads((FIXTURES_DIR / "esri_classbreaks.json").read_text())
    renderer: dict[str, Any] = data["renderer"]
    return renderer


@pytest.fixture
def uniquevalue_renderer() -> dict[str, Any]:
    """Load PAD-US UniqueValue renderer fixture."""
    data = json.loads((FIXTURES_DIR / "esri_uniquevalue.json").read_text())
    renderer: dict[str, Any] = data["renderer"]
    return renderer


class TestSimpleRenderer:
    """Tests for simple (single symbol) renderer conversion."""

    def test_simple_fill_renderer(self) -> None:
        """Simple fill symbol converts to static fill layer."""
        renderer = {
            "type": "simple",
            "symbol": {
                "type": "esriSFS",
                "style": "esriSFSSolid",
                "color": [255, 0, 0, 255],
                "outline": {
                    "type": "esriSLS",
                    "style": "esriSLSSolid",
                    "color": [0, 0, 0, 255],
                    "width": 1,
                },
            },
        }
        style = parse_simple_renderer(renderer, source_layer="data")

        assert style["version"] == 8
        assert len(style["layers"]) >= 1
        fill_layer = style["layers"][0]
        assert fill_layer["type"] == "fill"
        assert fill_layer["paint"]["fill-color"] == "#ff0000"

    def test_simple_circle_renderer(self) -> None:
        """Simple marker symbol converts to circle layer."""
        renderer = {
            "type": "simple",
            "symbol": {
                "type": "esriSMS",
                "style": "esriSMSCircle",
                "color": [0, 128, 255, 255],
                "size": 10,
                "outline": {
                    "color": [0, 0, 0, 255],
                    "width": 1,
                },
            },
        }
        style = parse_simple_renderer(renderer, source_layer="data")

        circle_layer = style["layers"][0]
        assert circle_layer["type"] == "circle"
        assert circle_layer["paint"]["circle-color"] == "#0080ff"
        assert circle_layer["paint"]["circle-radius"] == 5  # ESRI size / 2


class TestUniqueValueRenderer:
    """Tests for unique value (categorical) renderer conversion."""

    def test_uniquevalue_fill_renderer(self, uniquevalue_renderer: dict[str, Any]) -> None:
        """PAD-US categorical fill converts to match expression."""
        style = parse_uniquevalue_renderer(uniquevalue_renderer, source_layer="padus")

        assert style["version"] == 8
        assert len(style["layers"]) >= 1

        fill_layer = style["layers"][0]
        assert fill_layer["type"] == "fill"

        # Color should be a match expression
        fill_color = fill_layer["paint"]["fill-color"]
        assert isinstance(fill_color, list)
        assert fill_color[0] == "match"
        assert fill_color[1] == ["get", "Pub_Access"]

        # Check that values are present
        # Format: ["match", ["get", "field"], val1, color1, val2, color2, ..., default]
        assert "RA" in fill_color  # Restricted Access
        assert "OA" in fill_color  # Open Access
        assert "XA" in fill_color  # Closed Access

    def test_uniquevalue_extracts_correct_colors(
        self, uniquevalue_renderer: dict[str, Any]
    ) -> None:
        """Verify exact color values from PAD-US fixture."""
        style = parse_uniquevalue_renderer(uniquevalue_renderer, source_layer="data")
        fill_color = style["layers"][0]["paint"]["fill-color"]

        # Find the color for "OA" (Open Access) - should be #81c435 (green)
        # The match expression is: ["match", ["get", "field"], v1, c1, v2, c2, ..., default]
        oa_idx = fill_color.index("OA")
        oa_color = fill_color[oa_idx + 1]
        assert oa_color == "#81c435"

        # "RA" (Restricted Access) should be #64b383
        ra_idx = fill_color.index("RA")
        ra_color = fill_color[ra_idx + 1]
        assert ra_color == "#64b383"


class TestClassBreaksRenderer:
    """Tests for class breaks (graduated) renderer conversion."""

    def test_classbreaks_circle_renderer(self, classbreaks_renderer: dict[str, Any]) -> None:
        """Census graduated circles convert to step expression."""
        style = parse_classbreaks_renderer(classbreaks_renderer, source_layer="census")

        assert style["version"] == 8
        assert len(style["layers"]) >= 1

        circle_layer = style["layers"][0]
        assert circle_layer["type"] == "circle"

        # Radius should be a step expression
        radius = circle_layer["paint"]["circle-radius"]
        assert isinstance(radius, list)
        assert radius[0] == "step"
        assert radius[1] == ["get", "POP2000"]

    def test_classbreaks_extracts_break_values(self, classbreaks_renderer: dict[str, Any]) -> None:
        """Verify break values from Census fixture."""
        style = parse_classbreaks_renderer(classbreaks_renderer, source_layer="data")
        radius = style["layers"][0]["paint"]["circle-radius"]

        # Step expression: ["step", ["get", "field"], initial, break1, val1, break2, val2, ...]
        # From fixture: breaks at 61, 264, 759, 1900
        # Sizes: 4, 7.5, 11, 14.5, 18 (divided by 2 for Mapbox radius)
        assert 61 in radius
        assert 264 in radius
        assert 759 in radius

    def test_classbreaks_preserves_color(self, classbreaks_renderer: dict[str, Any]) -> None:
        """All break classes have same fill color in this fixture."""
        style = parse_classbreaks_renderer(classbreaks_renderer, source_layer="data")
        circle_layer = style["layers"][0]

        # Census fixture uses same blue for all classes
        assert circle_layer["paint"]["circle-color"] == "#73b2ff"


class TestConvertESRIRenderer:
    """Tests for the main conversion entry point."""

    def test_convert_detects_simple_type(self) -> None:
        """Dispatcher routes simple renderer correctly."""
        renderer = {
            "type": "simple",
            "symbol": {
                "type": "esriSFS",
                "style": "esriSFSSolid",
                "color": [255, 0, 0, 255],
            },
        }
        style = convert_esri_renderer(renderer, source_layer="data")
        assert style["version"] == 8

    def test_convert_detects_uniquevalue_type(self, uniquevalue_renderer: dict[str, Any]) -> None:
        """Dispatcher routes uniqueValue renderer correctly."""
        style = convert_esri_renderer(uniquevalue_renderer, source_layer="data")
        fill_color = style["layers"][0]["paint"]["fill-color"]
        assert fill_color[0] == "match"

    def test_convert_detects_classbreaks_type(self, classbreaks_renderer: dict[str, Any]) -> None:
        """Dispatcher routes classBreaks renderer correctly."""
        style = convert_esri_renderer(classbreaks_renderer, source_layer="data")
        radius = style["layers"][0]["paint"]["circle-radius"]
        assert radius[0] == "step"

    def test_convert_unknown_type_raises(self) -> None:
        """Unknown renderer type raises ESRIConverterError."""
        renderer = {"type": "unknownRenderer"}
        with pytest.raises(ESRIConverterError, match="Unsupported.*unknownRenderer"):
            convert_esri_renderer(renderer, source_layer="data")


class TestWarningsAndPartialConversion:
    """Tests for warn-and-continue behavior on unsupported features."""

    def test_picture_marker_warns_but_continues(self) -> None:
        """esriPMS (picture marker) emits warning but still produces style."""
        renderer = {
            "type": "simple",
            "symbol": {
                "type": "esriPMS",
                "url": "https://example.com/icon.png",
                "width": 24,
                "height": 24,
            },
        }
        style, warnings = convert_esri_renderer(renderer, source_layer="data", return_warnings=True)

        # Should still return a style (fallback to circle)
        assert style["version"] == 8
        assert len(warnings) > 0
        assert any("picture marker" in w.lower() for w in warnings)

    def test_unsupported_symbol_style_warns(self) -> None:
        """Unsupported esriSMS style (e.g., cross) warns but continues."""
        renderer = {
            "type": "simple",
            "symbol": {
                "type": "esriSMS",
                "style": "esriSMSCross",  # Not directly supported in Mapbox GL
                "color": [255, 0, 0, 255],
                "size": 10,
            },
        }
        style, warnings = convert_esri_renderer(renderer, source_layer="data", return_warnings=True)

        # Falls back to circle
        assert style["layers"][0]["type"] == "circle"
        assert len(warnings) > 0
