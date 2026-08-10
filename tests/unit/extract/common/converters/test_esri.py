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
    _parse_circle_symbol,
    _parse_fill_symbol,
    _parse_line_symbol,
    _parse_symbol,
    _symbol_to_layer_type,
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


# ESRI's default symbol color when a symbol omits "color", and the values it
# converts to. Several parsers share this literal, so pin it once.
DEFAULT_ESRI_COLOR = [128, 128, 128, 255]
DEFAULT_HEX = "#808080"


class TestSymbolToLayerType:
    """Symbol-type to Mapbox GL layer-type routing (`_symbol_to_layer_type`)."""

    @pytest.mark.parametrize(
        ("symbol_type", "expected"),
        [
            ("esriSFS", "fill"),
            ("esriSLS", "line"),
            ("esriSMS", "circle"),
            ("esriPMS", "circle"),
        ],
    )
    def test_known_symbol_types(self, symbol_type: str, expected: str) -> None:
        """Each supported ESRI symbol type maps to its Mapbox GL layer type."""
        assert _symbol_to_layer_type({"type": symbol_type}) == expected

    def test_unknown_symbol_type_defaults_to_fill(self) -> None:
        """An unrecognized symbol type falls back to fill."""
        assert _symbol_to_layer_type({"type": "esriTextSymbol"}) == "fill"

    def test_missing_symbol_type_defaults_to_fill(self) -> None:
        """A symbol with no "type" key falls back to fill."""
        assert _symbol_to_layer_type({}) == "fill"


class TestParseFillSymbol:
    """esriSFS parsing (`_parse_fill_symbol`)."""

    def test_full_symbol_produces_exact_layer(self) -> None:
        """Color, alpha, and outline all land in the paint dict."""
        symbol = {
            "type": "esriSFS",
            "color": [255, 0, 0, 128],
            "outline": {"color": [0, 0, 255, 255], "width": 2},
        }
        layer = _parse_fill_symbol(symbol, "my-layer", "src")

        assert layer == {
            "id": "my-layer",
            "type": "fill",
            "source": "data",
            "source-layer": "src",
            "paint": {
                "fill-color": "#ff0000",
                "fill-opacity": 128 / 255.0,
                "fill-outline-color": "#0000ff",
            },
        }

    def test_missing_color_uses_esri_default_gray(self) -> None:
        """A symbol with no color renders opaque #808080."""
        layer = _parse_fill_symbol({"type": "esriSFS"}, "layer-0", "src")

        assert layer["paint"]["fill-color"] == DEFAULT_HEX
        assert layer["paint"]["fill-opacity"] == 1.0

    def test_outline_without_color_omits_outline(self) -> None:
        """An outline carrying only a width adds no fill-outline-color.

        Guards the `outline and outline.get("color")` conjunction: reading
        ``outline["color"]`` on this symbol would raise KeyError.
        """
        symbol = {"type": "esriSFS", "color": [1, 2, 3, 255], "outline": {"width": 3}}
        layer = _parse_fill_symbol(symbol, "layer-0", "src")

        assert "fill-outline-color" not in layer["paint"]

    def test_empty_outline_omits_outline(self) -> None:
        """An empty outline dict adds no fill-outline-color."""
        symbol = {"type": "esriSFS", "color": [1, 2, 3, 255], "outline": {}}
        layer = _parse_fill_symbol(symbol, "layer-0", "src")

        assert "fill-outline-color" not in layer["paint"]

    def test_missing_outline_omits_outline(self) -> None:
        """A symbol with no outline key adds no fill-outline-color."""
        layer = _parse_fill_symbol({"type": "esriSFS"}, "layer-0", "src")

        assert "fill-outline-color" not in layer["paint"]


class TestParseCircleSymbol:
    """esriSMS/esriPMS parsing (`_parse_circle_symbol`)."""

    def test_picture_marker_uses_fixed_placeholder(self) -> None:
        """esriPMS ignores its own paint and renders a fixed grey circle."""
        symbol = {
            "type": "esriPMS",
            "url": "https://example.com/icon.png",
            "color": [255, 0, 0, 255],
        }
        warnings: list[str] = []
        layer = _parse_circle_symbol(symbol, "pms", "src", warnings)

        assert layer["paint"]["circle-color"] == "#888888"
        assert layer["paint"]["circle-radius"] == 8
        assert warnings == [
            "Picture marker symbol (esriPMS) not fully supported; "
            "falling back to circle. URL: https://example.com/icon.png"
        ]

    def test_picture_marker_without_url_reports_unknown(self) -> None:
        """A picture marker with no url names the URL "unknown" in the warning."""
        warnings: list[str] = []
        _parse_circle_symbol({"type": "esriPMS"}, "pms", "src", warnings)

        assert warnings == [
            "Picture marker symbol (esriPMS) not fully supported; "
            "falling back to circle. URL: unknown"
        ]

    def test_picture_marker_without_warning_sink(self) -> None:
        """warnings=None drops the warning instead of raising."""
        layer = _parse_circle_symbol({"type": "esriPMS"}, "pms", "src", None)

        assert layer["paint"]["circle-color"] == "#888888"

    def test_default_marker_style_is_silent(self) -> None:
        """A marker with no style key is treated as a circle and warns nothing."""
        warnings: list[str] = []
        _parse_circle_symbol({"type": "esriSMS", "size": 10}, "sms", "src", warnings)

        assert warnings == []

    def test_explicit_circle_style_is_silent(self) -> None:
        """An explicit esriSMSCircle style warns nothing."""
        warnings: list[str] = []
        _parse_circle_symbol(
            {"type": "esriSMS", "style": "esriSMSCircle", "size": 10}, "sms", "src", warnings
        )

        assert warnings == []

    def test_non_circle_style_warns_with_style_name(self) -> None:
        """A non-circle marker style names itself in the warning."""
        warnings: list[str] = []
        _parse_circle_symbol(
            {"type": "esriSMS", "style": "esriSMSCross", "size": 10}, "sms", "src", warnings
        )

        assert warnings == [
            "Marker style 'esriSMSCross' not directly supported; falling back to circle."
        ]

    def test_non_circle_style_without_warning_sink(self) -> None:
        """warnings=None on an unsupported style still produces a layer."""
        layer = _parse_circle_symbol({"type": "esriSMS", "style": "esriSMSX"}, "sms", "src", None)

        assert layer["type"] == "circle"

    def test_missing_color_uses_esri_default_gray(self) -> None:
        """A marker with no color renders opaque #808080."""
        layer = _parse_circle_symbol({"type": "esriSMS", "size": 10}, "sms", "src")

        assert layer["paint"]["circle-color"] == DEFAULT_HEX
        assert layer["paint"]["circle-opacity"] == 1.0

    def test_missing_size_uses_default_diameter_of_ten(self) -> None:
        """A marker with no size gets ESRI's default diameter 10, radius 5."""
        layer = _parse_circle_symbol({"type": "esriSMS"}, "sms", "src")

        assert layer["paint"]["circle-radius"] == 5.0

    def test_size_is_halved_into_radius(self) -> None:
        """ESRI size is a diameter; Mapbox circle-radius is half of it."""
        layer = _parse_circle_symbol({"type": "esriSMS", "size": 18}, "sms", "src")

        assert layer["paint"]["circle-radius"] == 9.0

    def test_alpha_becomes_circle_opacity(self) -> None:
        """The color's alpha channel drives circle-opacity."""
        layer = _parse_circle_symbol({"type": "esriSMS", "color": [0, 0, 0, 51]}, "sms", "src")

        assert layer["paint"]["circle-opacity"] == 51 / 255.0

    def test_outline_sets_stroke_color_and_width(self) -> None:
        """A full outline populates both stroke paint properties."""
        symbol = {"type": "esriSMS", "size": 10, "outline": {"color": [0, 255, 0, 255], "width": 3}}
        layer = _parse_circle_symbol(symbol, "sms", "src")

        assert layer["paint"]["circle-stroke-color"] == "#00ff00"
        assert layer["paint"]["circle-stroke-width"] == 3

    def test_outline_width_only_sets_width_without_color(self) -> None:
        """An outline with only a width still contributes circle-stroke-width."""
        symbol = {"type": "esriSMS", "size": 10, "outline": {"width": 4}}
        layer = _parse_circle_symbol(symbol, "sms", "src")

        assert "circle-stroke-color" not in layer["paint"]
        assert layer["paint"]["circle-stroke-width"] == 4

    def test_outline_color_only_omits_width(self) -> None:
        """An outline with only a color contributes no circle-stroke-width."""
        symbol = {"type": "esriSMS", "size": 10, "outline": {"color": [0, 0, 0, 255]}}
        layer = _parse_circle_symbol(symbol, "sms", "src")

        assert layer["paint"]["circle-stroke-color"] == "#000000"
        assert "circle-stroke-width" not in layer["paint"]

    def test_no_outline_omits_both_stroke_properties(self) -> None:
        """A marker with no outline gets neither stroke property."""
        layer = _parse_circle_symbol({"type": "esriSMS", "size": 10}, "sms", "src")

        assert "circle-stroke-color" not in layer["paint"]
        assert "circle-stroke-width" not in layer["paint"]


class TestParseLineSymbol:
    """esriSLS parsing (`_parse_line_symbol`)."""

    def test_full_symbol_produces_exact_layer(self) -> None:
        """Color, alpha, and width all land in the paint dict."""
        symbol = {"type": "esriSLS", "color": [0, 128, 255, 204], "width": 2.5}
        layer = _parse_line_symbol(symbol, "my-line", "src")

        assert layer == {
            "id": "my-line",
            "type": "line",
            "source": "data",
            "source-layer": "src",
            "paint": {
                "line-color": "#0080ff",
                "line-width": 2.5,
                "line-opacity": 204 / 255.0,
            },
        }

    def test_defaults_when_color_and_width_missing(self) -> None:
        """A bare esriSLS renders opaque #808080 at width 1."""
        layer = _parse_line_symbol({"type": "esriSLS"}, "my-line", "src")

        assert layer["paint"]["line-color"] == DEFAULT_HEX
        assert layer["paint"]["line-width"] == 1
        assert layer["paint"]["line-opacity"] == 1.0


class TestParseSymbolRouting:
    """Symbol dispatch (`_parse_symbol`)."""

    def test_routes_fill_symbol(self) -> None:
        """esriSFS produces a fill layer carrying the symbol's color."""
        layer = _parse_symbol({"type": "esriSFS", "color": [255, 0, 0, 255]}, "l", "src")

        assert layer["type"] == "fill"
        assert layer["paint"]["fill-color"] == "#ff0000"

    def test_routes_line_symbol(self) -> None:
        """esriSLS produces a line layer carrying the symbol's color."""
        layer = _parse_symbol({"type": "esriSLS", "color": [255, 0, 0, 255]}, "l", "src")

        assert layer["type"] == "line"
        assert layer["paint"]["line-color"] == "#ff0000"

    def test_routes_marker_symbol(self) -> None:
        """esriSMS produces a circle layer carrying the symbol's color."""
        layer = _parse_symbol({"type": "esriSMS", "color": [255, 0, 0, 255]}, "l", "src")

        assert layer["type"] == "circle"
        assert layer["paint"]["circle-color"] == "#ff0000"

    def test_routes_picture_marker_symbol(self) -> None:
        """esriPMS reaches the circle parser and warns."""
        warnings: list[str] = []
        layer = _parse_symbol({"type": "esriPMS"}, "l", "src", warnings)

        assert layer["type"] == "circle"
        assert len(warnings) == 1

    def test_unknown_type_falls_back_to_grey_fill(self) -> None:
        """An unrecognized symbol type produces a half-opaque grey fill."""
        warnings: list[str] = []
        layer = _parse_symbol({"type": "esriTS"}, "l", "src", warnings)

        assert layer["type"] == "fill"
        assert layer["paint"]["fill-color"] == "#888888"
        assert layer["paint"]["fill-opacity"] == 0.5
        assert warnings == ["Unknown symbol type 'esriTS'; defaulting to fill."]

    def test_missing_type_names_empty_string_in_warning(self) -> None:
        """A symbol with no type reports an empty type in the warning."""
        warnings: list[str] = []
        _parse_symbol({}, "l", "src", warnings)

        assert warnings == ["Unknown symbol type ''; defaulting to fill."]

    def test_unknown_type_without_warning_sink(self) -> None:
        """warnings=None on an unknown symbol type still produces a layer."""
        layer = _parse_symbol({"type": "esriTS"}, "l", "src", None)

        assert layer["paint"]["fill-color"] == "#888888"


class TestSimpleRendererDetails:
    """Structural detail of `parse_simple_renderer`."""

    def test_layer_id_and_style_name(self) -> None:
        """The single layer is id "layer-0" under the "Simple Style" name."""
        renderer = {"type": "simple", "symbol": {"type": "esriSFS", "color": [1, 2, 3, 255]}}
        style = parse_simple_renderer(renderer, source_layer="src")

        assert style["name"] == "Simple Style"
        assert [layer["id"] for layer in style["layers"]] == ["layer-0"]
        assert style["layers"][0]["source-layer"] == "src"

    def test_missing_symbol_falls_back_to_grey_fill(self) -> None:
        """A renderer with no symbol takes the unknown-symbol fallback."""
        warnings: list[str] = []
        style = parse_simple_renderer({"type": "simple"}, source_layer="src", warnings=warnings)

        assert style["layers"][0]["paint"]["fill-color"] == "#888888"
        assert warnings == ["Unknown symbol type ''; defaulting to fill."]


class TestUniqueValueRendererDetails:
    """Field resolution, layer typing, and defaults in `parse_uniquevalue_renderer`."""

    @staticmethod
    def _renderer(**overrides: Any) -> dict[str, Any]:
        """A minimal two-class uniqueValue fill renderer."""
        renderer: dict[str, Any] = {
            "type": "uniqueValue",
            "field1": "CLASS",
            "uniqueValueInfos": [
                {"value": "a", "symbol": {"type": "esriSFS", "color": [255, 0, 0, 255]}},
                {"value": "b", "symbol": {"type": "esriSFS", "color": [0, 255, 0, 255]}},
            ],
        }
        renderer.update(overrides)
        return renderer

    def test_field1_wins_over_field(self) -> None:
        """field1 is ESRI's primary classification field and takes precedence."""
        renderer = self._renderer(field="IGNORED")
        style = parse_uniquevalue_renderer(renderer, source_layer="src")

        assert style["layers"][0]["paint"]["fill-color"][1] == ["get", "CLASS"]

    def test_field_used_when_field1_absent(self) -> None:
        """Renderers that only carry "field" still classify on it."""
        renderer = self._renderer()
        del renderer["field1"]
        renderer["field"] = "OTHER"
        style = parse_uniquevalue_renderer(renderer, source_layer="src")

        assert style["layers"][0]["paint"]["fill-color"][1] == ["get", "OTHER"]

    def test_value_field_fallback(self) -> None:
        """With neither field key, classification falls back to "value"."""
        renderer = self._renderer()
        del renderer["field1"]
        style = parse_uniquevalue_renderer(renderer, source_layer="src")

        assert style["layers"][0]["paint"]["fill-color"][1] == ["get", "value"]

    def test_match_expression_is_exact(self) -> None:
        """Cases follow input order and end with the #cccccc default."""
        style = parse_uniquevalue_renderer(self._renderer(), source_layer="src")

        assert style["layers"][0]["paint"]["fill-color"] == [
            "match",
            ["get", "CLASS"],
            "a",
            "#ff0000",
            "b",
            "#00ff00",
            "#cccccc",
        ]

    def test_style_name_and_fill_layer_id(self) -> None:
        """Fill classification is id "categorical-fill" under "Categorical Style"."""
        style = parse_uniquevalue_renderer(self._renderer(), source_layer="src")

        assert style["name"] == "Categorical Style"
        assert style["layers"][0]["id"] == "categorical-fill"

    def test_circle_layer_id_and_type(self) -> None:
        """Marker symbols produce a "categorical-circle" layer."""
        renderer = self._renderer(
            uniqueValueInfos=[
                {"value": "a", "symbol": {"type": "esriSMS", "color": [255, 0, 0, 255]}},
            ]
        )
        style = parse_uniquevalue_renderer(renderer, source_layer="src")

        assert style["layers"][0]["id"] == "categorical-circle"
        assert style["layers"][0]["type"] == "circle"

    def test_line_layer_id_and_type(self) -> None:
        """Line symbols produce a "categorical-line" layer."""
        renderer = self._renderer(
            uniqueValueInfos=[
                {"value": "a", "symbol": {"type": "esriSLS", "color": [255, 0, 0, 255]}},
            ]
        )
        style = parse_uniquevalue_renderer(renderer, source_layer="src")

        assert style["layers"][0]["id"] == "categorical-line"
        assert style["layers"][0]["type"] == "line"

    def test_unknown_symbol_type_produces_fill(self) -> None:
        """An unrecognized symbol type classifies as fill."""
        renderer = self._renderer(
            uniqueValueInfos=[{"value": "a", "symbol": {"type": "esriTS", "color": [1, 2, 3, 255]}}]
        )
        style = parse_uniquevalue_renderer(renderer, source_layer="src")

        assert style["layers"][0]["id"] == "categorical-fill"

    def test_first_info_decides_layer_type(self) -> None:
        """Layer type comes from the first info, not a later one."""
        renderer = self._renderer(
            uniqueValueInfos=[
                {"value": "a", "symbol": {"type": "esriSFS", "color": [255, 0, 0, 255]}},
                {"value": "b", "symbol": {"type": "esriSMS", "color": [0, 255, 0, 255]}},
            ]
        )
        style = parse_uniquevalue_renderer(renderer, source_layer="src")

        assert style["layers"][0]["id"] == "categorical-fill"

    def test_first_info_decides_opacity(self) -> None:
        """Opacity comes from the first info's alpha, not a later one."""
        renderer = self._renderer(
            uniqueValueInfos=[
                {"value": "a", "symbol": {"type": "esriSFS", "color": [255, 0, 0, 51]}},
                {"value": "b", "symbol": {"type": "esriSFS", "color": [0, 255, 0, 255]}},
            ]
        )
        style = parse_uniquevalue_renderer(renderer, source_layer="src")

        assert style["layers"][0]["paint"]["fill-opacity"] == 51 / 255.0

    def test_info_without_color_uses_default_gray(self) -> None:
        """An info whose symbol omits color contributes #808080."""
        renderer = self._renderer(uniqueValueInfos=[{"value": "a", "symbol": {"type": "esriSFS"}}])
        style = parse_uniquevalue_renderer(renderer, source_layer="src")

        assert style["layers"][0]["paint"]["fill-color"][2:] == ["a", DEFAULT_HEX, "#cccccc"]

    def test_info_without_symbol_uses_default_gray(self) -> None:
        """An info with no symbol at all contributes #808080 at full opacity."""
        renderer = self._renderer(uniqueValueInfos=[{"value": "a"}])
        style = parse_uniquevalue_renderer(renderer, source_layer="src")

        assert style["layers"][0]["paint"]["fill-color"][3] == DEFAULT_HEX
        assert style["layers"][0]["paint"]["fill-opacity"] == 1.0

    def test_info_without_value_maps_none(self) -> None:
        """A missing "value" is carried through as None rather than dropped."""
        renderer = self._renderer(
            uniqueValueInfos=[{"symbol": {"type": "esriSFS", "color": [255, 0, 0, 255]}}]
        )
        style = parse_uniquevalue_renderer(renderer, source_layer="src")

        assert style["layers"][0]["paint"]["fill-color"][2] is None

    def test_empty_infos_produce_placeholder_style(self) -> None:
        """No classes yields the grey placeholder style and a warning."""
        warnings: list[str] = []
        renderer = {"type": "uniqueValue", "field1": "CLASS", "uniqueValueInfos": []}
        style = parse_uniquevalue_renderer(renderer, source_layer="src", warnings=warnings)

        assert style["name"] == "Empty UniqueValue Style"
        assert style["layers"][0]["id"] == "layer-0"
        assert style["layers"][0]["paint"]["fill-color"] == "#888888"
        assert style["layers"][0]["paint"]["fill-opacity"] == 0.5
        assert warnings == ["UniqueValue renderer has no value infos; using default."]

    def test_missing_infos_key_produces_placeholder_style(self) -> None:
        """A renderer with no uniqueValueInfos key takes the same path."""
        style = parse_uniquevalue_renderer({"type": "uniqueValue"}, source_layer="src")

        assert style["name"] == "Empty UniqueValue Style"

    def test_empty_infos_without_warning_sink(self) -> None:
        """warnings=None on an empty renderer still produces a style."""
        renderer = {"type": "uniqueValue", "uniqueValueInfos": []}
        style = parse_uniquevalue_renderer(renderer, source_layer="src", warnings=None)

        assert style["name"] == "Empty UniqueValue Style"


class TestClassBreaksRendererDetails:
    """Graduated and choropleth branches of `parse_classbreaks_renderer`."""

    @staticmethod
    def _graduated() -> dict[str, Any]:
        """Marker classes whose sizes vary, i.e. a graduated-symbol renderer."""
        return {
            "type": "classBreaks",
            "field": "POP",
            "classBreakInfos": [
                {
                    "classMaxValue": 100,
                    "symbol": {"type": "esriSMS", "size": 8, "color": [255, 0, 0, 255]},
                },
                {
                    "classMaxValue": 500,
                    "symbol": {"type": "esriSMS", "size": 16, "color": [0, 255, 0, 255]},
                },
                {
                    "classMaxValue": 900,
                    "symbol": {"type": "esriSMS", "size": 24, "color": [0, 0, 255, 255]},
                },
            ],
        }

    @staticmethod
    def _choropleth() -> dict[str, Any]:
        """Fill classes whose colors vary, i.e. a choropleth renderer."""
        return {
            "type": "classBreaks",
            "field": "DENS",
            "classBreakInfos": [
                {"classMaxValue": 10, "symbol": {"type": "esriSFS", "color": [255, 0, 0, 255]}},
                {"classMaxValue": 20, "symbol": {"type": "esriSFS", "color": [0, 255, 0, 255]}},
                {"classMaxValue": 30, "symbol": {"type": "esriSFS", "color": [0, 0, 255, 255]}},
            ],
        }

    def test_graduated_radius_steps_are_exact(self) -> None:
        """Radii halve each size and step at the *previous* class maximum."""
        style = parse_classbreaks_renderer(self._graduated(), source_layer="src")

        assert style["layers"][0]["paint"]["circle-radius"] == [
            "step",
            ["get", "POP"],
            4.0,
            100,
            8.0,
            500,
            12.0,
        ]

    def test_graduated_layer_id_name_and_color(self) -> None:
        """The graduated layer is id "graduated-circle" colored by the first class."""
        style = parse_classbreaks_renderer(self._graduated(), source_layer="src")

        assert style["name"] == "Graduated Style"
        assert style["layers"][0]["id"] == "graduated-circle"
        assert style["layers"][0]["paint"]["circle-color"] == "#ff0000"

    def test_graduated_outline_sets_stroke(self) -> None:
        """A first-class outline supplies both graduated stroke properties."""
        renderer = self._graduated()
        renderer["classBreakInfos"][0]["symbol"]["outline"] = {
            "color": [0, 0, 0, 255],
            "width": 2,
        }
        style = parse_classbreaks_renderer(renderer, source_layer="src")

        assert style["layers"][0]["paint"]["circle-stroke-color"] == "#000000"
        assert style["layers"][0]["paint"]["circle-stroke-width"] == 2

    def test_graduated_outline_without_color_sets_no_stroke(self) -> None:
        """Unlike simple markers, a colorless outline here contributes no width."""
        renderer = self._graduated()
        renderer["classBreakInfos"][0]["symbol"]["outline"] = {"width": 2}
        style = parse_classbreaks_renderer(renderer, source_layer="src")

        assert "circle-stroke-color" not in style["layers"][0]["paint"]
        assert "circle-stroke-width" not in style["layers"][0]["paint"]

    def test_graduated_break_without_class_max_value_steps_at_zero(self) -> None:
        """A class missing classMaxValue contributes a 0 threshold."""
        renderer = self._graduated()
        del renderer["classBreakInfos"][0]["classMaxValue"]
        style = parse_classbreaks_renderer(renderer, source_layer="src")

        assert style["layers"][0]["paint"]["circle-radius"][3] == 0

    def test_graduated_break_without_size_uses_default_diameter(self) -> None:
        """A class missing size falls back to diameter 10, radius 5."""
        renderer = self._graduated()
        del renderer["classBreakInfos"][1]["symbol"]["size"]
        style = parse_classbreaks_renderer(renderer, source_layer="src")

        assert style["layers"][0]["paint"]["circle-radius"][4] == 5.0

    def test_uniform_marker_sizes_are_not_graduated(self) -> None:
        """Same-size markers are treated as color-varying, not size-varying.

        Sizes must actually differ for the graduated-symbol branch. With one
        distinct size the renderer falls through to the color branch, and
        because the layer type is not fill it emits a line layer.
        """
        renderer = self._graduated()
        for info in renderer["classBreakInfos"]:
            info["symbol"]["size"] = 8
        style = parse_classbreaks_renderer(renderer, source_layer="src")

        assert style["layers"][0]["id"] == "graduated-line"
        assert style["layers"][0]["type"] == "line"

    def test_sizeless_markers_are_not_graduated(self) -> None:
        """Markers with no size at all cannot be graduated by size."""
        renderer = self._graduated()
        for info in renderer["classBreakInfos"]:
            del info["symbol"]["size"]
        style = parse_classbreaks_renderer(renderer, source_layer="src")

        assert style["layers"][0]["id"] == "graduated-line"

    def test_choropleth_color_steps_are_exact(self) -> None:
        """Colors step at the previous class maximum, opening at minValue."""
        style = parse_classbreaks_renderer(self._choropleth(), source_layer="src")

        assert style["layers"][0]["paint"]["fill-color"] == [
            "step",
            ["get", "DENS"],
            "#ff0000",
            10,
            "#00ff00",
            20,
            "#0000ff",
        ]

    def test_choropleth_layer_id_and_opacity(self) -> None:
        """The choropleth layer is id "choropleth-fill" at fixed 0.7 opacity."""
        style = parse_classbreaks_renderer(self._choropleth(), source_layer="src")

        assert style["layers"][0]["id"] == "choropleth-fill"
        assert style["layers"][0]["paint"]["fill-opacity"] == 0.7

    def test_choropleth_class_without_color_uses_default_gray(self) -> None:
        """A class whose symbol omits color contributes #808080."""
        renderer = self._choropleth()
        del renderer["classBreakInfos"][1]["symbol"]["color"]
        style = parse_classbreaks_renderer(renderer, source_layer="src")

        assert style["layers"][0]["paint"]["fill-color"][4] == DEFAULT_HEX

    def test_choropleth_class_without_symbol_uses_default_gray(self) -> None:
        """A class with no symbol contributes #808080."""
        renderer = self._choropleth()
        del renderer["classBreakInfos"][1]["symbol"]
        style = parse_classbreaks_renderer(renderer, source_layer="src")

        assert style["layers"][0]["paint"]["fill-color"][4] == DEFAULT_HEX

    def test_line_classes_produce_graduated_line(self) -> None:
        """Line symbols produce a "graduated-line" layer colored by step."""
        renderer = self._choropleth()
        for info in renderer["classBreakInfos"]:
            info["symbol"]["type"] = "esriSLS"
        style = parse_classbreaks_renderer(renderer, source_layer="src")

        assert style["layers"][0]["id"] == "graduated-line"
        assert style["layers"][0]["paint"]["line-color"][0] == "step"

    def test_field_fallback(self) -> None:
        """With no field key, classification falls back to "value"."""
        renderer = self._choropleth()
        del renderer["field"]
        style = parse_classbreaks_renderer(renderer, source_layer="src")

        assert style["layers"][0]["paint"]["fill-color"][1] == ["get", "value"]

    def test_empty_break_infos_produce_placeholder_style(self) -> None:
        """No classes yields the grey placeholder style and a warning."""
        warnings: list[str] = []
        renderer = {"type": "classBreaks", "field": "POP", "classBreakInfos": []}
        style = parse_classbreaks_renderer(renderer, source_layer="src", warnings=warnings)

        assert style["name"] == "Empty ClassBreaks Style"
        assert style["layers"][0]["id"] == "layer-0"
        assert style["layers"][0]["paint"]["fill-color"] == "#888888"
        assert style["layers"][0]["paint"]["fill-opacity"] == 0.5
        assert warnings == ["ClassBreaks renderer has no break infos; using default."]

    def test_missing_break_infos_key_produces_placeholder_style(self) -> None:
        """A renderer with no classBreakInfos key takes the same path."""
        style = parse_classbreaks_renderer({"type": "classBreaks"}, source_layer="src")

        assert style["name"] == "Empty ClassBreaks Style"

    def test_empty_break_infos_without_warning_sink(self) -> None:
        """warnings=None on an empty renderer still produces a style."""
        renderer = {"type": "classBreaks", "classBreakInfos": []}
        style = parse_classbreaks_renderer(renderer, source_layer="src", warnings=None)

        assert style["name"] == "Empty ClassBreaks Style"


class TestConvertESRIRendererContract:
    """Return shape and error text of `convert_esri_renderer`."""

    def test_default_return_is_a_bare_style(self) -> None:
        """Without return_warnings the result is the style dict itself."""
        renderer = {"type": "simple", "symbol": {"type": "esriSFS", "color": [1, 2, 3, 255]}}
        result = convert_esri_renderer(renderer, source_layer="src")

        assert isinstance(result, dict)
        assert result["name"] == "Simple Style"

    def test_return_warnings_yields_style_and_warnings(self) -> None:
        """With return_warnings the result is a (style, warnings) pair."""
        renderer = {"type": "simple", "symbol": {"type": "esriSFS", "color": [1, 2, 3, 255]}}
        result = convert_esri_renderer(renderer, source_layer="src", return_warnings=True)

        style, warnings = result
        assert style["name"] == "Simple Style"
        assert warnings == []

    def test_warnings_propagate_from_nested_parser(self) -> None:
        """Warnings raised deep in symbol parsing reach the caller."""
        renderer = {"type": "simple", "symbol": {"type": "esriTS"}}
        _, warnings = convert_esri_renderer(renderer, source_layer="src", return_warnings=True)

        assert warnings == ["Unknown symbol type 'esriTS'; defaulting to fill."]

    def test_unsupported_type_error_message_is_exact(self) -> None:
        """The error names the offending type and the supported set."""
        with pytest.raises(ESRIConverterError) as excinfo:
            convert_esri_renderer({"type": "heatmap"}, source_layer="src")

        assert str(excinfo.value) == (
            "Unsupported ESRI renderer type: 'heatmap'. "
            "Supported types: simple, uniqueValue, classBreaks."
        )

    def test_missing_type_reports_empty_string(self) -> None:
        """A renderer with no type key reports an empty type."""
        with pytest.raises(ESRIConverterError) as excinfo:
            convert_esri_renderer({}, source_layer="src")

        assert "renderer type: ''." in str(excinfo.value)
