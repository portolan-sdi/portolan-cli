"""Tests for SLD to Mapbox GL converter.

Uses real fixture data from WMS GetStyles requests:
- sld_simple_point.xml: Pergamino aeropuertos (stacked circles with airplane glyph)
- sld_categorical.xml: Pergamino barrios_y_pueblos (12-category polygon fills)
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.extract.common.converters.sld import (
    SLDConverterError,
    convert_sld,
    parse_filter_to_value,
    parse_point_symbolizer,
    parse_polygon_symbolizer,
)

FIXTURES_DIR = Path(__file__).parent.parent.parent.parent.parent / "fixtures" / "styles"


@pytest.fixture
def simple_point_sld() -> str:
    """Load aeropuertos simple point SLD fixture."""
    return (FIXTURES_DIR / "sld_simple_point.xml").read_text()


@pytest.fixture
def categorical_sld() -> str:
    """Load barrios_y_pueblos categorical SLD fixture."""
    return (FIXTURES_DIR / "sld_categorical.xml").read_text()


class TestParseFilter:
    """Tests for OGC Filter to value extraction."""

    def test_property_is_equal_to_string(self) -> None:
        """PropertyIsEqualTo with string literal extracts value."""
        filter_xml = """
        <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
            <ogc:PropertyIsEqualTo>
                <ogc:PropertyName>type</ogc:PropertyName>
                <ogc:Literal>residential</ogc:Literal>
            </ogc:PropertyIsEqualTo>
        </ogc:Filter>
        """
        field, value = parse_filter_to_value(filter_xml)
        assert field == "type"
        assert value == "residential"

    def test_property_is_equal_to_numeric(self) -> None:
        """PropertyIsEqualTo attempts numeric coercion."""
        filter_xml = """
        <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
            <ogc:PropertyIsEqualTo>
                <ogc:PropertyName>class</ogc:PropertyName>
                <ogc:Literal>42</ogc:Literal>
            </ogc:PropertyIsEqualTo>
        </ogc:Filter>
        """
        field, value = parse_filter_to_value(filter_xml)
        assert field == "class"
        # Value should be coerced to int if it looks numeric
        assert value == 42 or value == "42"  # Either is acceptable


class TestParsePolygonSymbolizer:
    """Tests for PolygonSymbolizer extraction."""

    def test_solid_fill_with_stroke(self) -> None:
        """Extract fill color, opacity, and stroke from PolygonSymbolizer."""
        symbolizer_xml = """
        <sld:PolygonSymbolizer xmlns:sld="http://www.opengis.net/sld">
            <sld:Fill>
                <sld:CssParameter name="fill">#c4db69</sld:CssParameter>
                <sld:CssParameter name="fill-opacity">0.4</sld:CssParameter>
            </sld:Fill>
            <sld:Stroke>
                <sld:CssParameter name="stroke">#ffffff</sld:CssParameter>
                <sld:CssParameter name="stroke-width">2</sld:CssParameter>
            </sld:Stroke>
        </sld:PolygonSymbolizer>
        """
        result = parse_polygon_symbolizer(symbolizer_xml)

        assert result["fill_color"] == "#c4db69"
        assert result["fill_opacity"] == 0.4
        assert result["stroke_color"] == "#ffffff"
        assert result["stroke_width"] == 2

    def test_fill_without_opacity(self) -> None:
        """Fill without explicit opacity defaults to 1.0."""
        symbolizer_xml = """
        <sld:PolygonSymbolizer xmlns:sld="http://www.opengis.net/sld">
            <sld:Fill>
                <sld:CssParameter name="fill">#ff0000</sld:CssParameter>
            </sld:Fill>
        </sld:PolygonSymbolizer>
        """
        result = parse_polygon_symbolizer(symbolizer_xml)

        assert result["fill_color"] == "#ff0000"
        assert result["fill_opacity"] == 1.0


class TestParsePointSymbolizer:
    """Tests for PointSymbolizer extraction."""

    def test_circle_mark(self) -> None:
        """Circle WellKnownName extracts as circle layer."""
        symbolizer_xml = """
        <sld:PointSymbolizer xmlns:sld="http://www.opengis.net/sld">
            <sld:Graphic>
                <sld:Mark>
                    <sld:WellKnownName>circle</sld:WellKnownName>
                    <sld:Fill>
                        <sld:CssParameter name="fill">#232323</sld:CssParameter>
                    </sld:Fill>
                </sld:Mark>
                <sld:Size>24</sld:Size>
            </sld:Graphic>
        </sld:PointSymbolizer>
        """
        result = parse_point_symbolizer(symbolizer_xml)

        assert result["type"] == "circle"
        assert result["fill_color"] == "#232323"
        assert result["size"] == 24

    def test_ttf_glyph_warns(self) -> None:
        """TTF glyph (like airplane) warns but extracts color."""
        symbolizer_xml = """
        <sld:PointSymbolizer xmlns:sld="http://www.opengis.net/sld">
            <sld:Graphic>
                <sld:Mark>
                    <sld:WellKnownName>ttf://DejaVu Sans#0x2708</sld:WellKnownName>
                    <sld:Fill>
                        <sld:CssParameter name="fill">#000000</sld:CssParameter>
                    </sld:Fill>
                </sld:Mark>
                <sld:Size>14</sld:Size>
            </sld:Graphic>
        </sld:PointSymbolizer>
        """
        result = parse_point_symbolizer(symbolizer_xml)

        # Falls back to circle with warning
        assert result["type"] == "circle"
        assert result["fill_color"] == "#000000"
        assert result.get("warning") is not None


class TestConvertSLD:
    """Tests for complete SLD document conversion."""

    def test_simple_point_sld(self, simple_point_sld: str) -> None:
        """Aeropuertos point SLD converts to circle layer."""
        style = convert_sld(simple_point_sld, source_layer="aeropuertos")

        assert style["version"] == 8
        assert style["name"] == "aeropuertos"
        assert len(style["layers"]) >= 1

        # Should have circle layer(s) for the stacked symbols
        circle_layers = [lyr for lyr in style["layers"] if lyr["type"] == "circle"]
        assert len(circle_layers) >= 1

    def test_categorical_sld(self, categorical_sld: str) -> None:
        """Barrios categorical SLD converts to match expression."""
        style = convert_sld(categorical_sld, source_layer="barrios")

        assert style["version"] == 8
        assert len(style["layers"]) >= 1

        fill_layer = style["layers"][0]
        assert fill_layer["type"] == "fill"

        # Color should be a match expression for categorical
        fill_color = fill_layer["paint"]["fill-color"]
        assert isinstance(fill_color, list)
        assert fill_color[0] == "match"

    def test_categorical_extracts_field(self, categorical_sld: str) -> None:
        """Categorical SLD extracts correct field name."""
        style = convert_sld(categorical_sld, source_layer="barrios")
        fill_color = style["layers"][0]["paint"]["fill-color"]

        # Should be matching on color_id field
        assert fill_color[1] == ["get", "color_id"]

    def test_categorical_extracts_colors(self, categorical_sld: str) -> None:
        """Categorical SLD extracts correct color values."""
        style = convert_sld(categorical_sld, source_layer="barrios")
        fill_color = style["layers"][0]["paint"]["fill-color"]

        # First category color from fixture: #c4db69
        assert "#c4db69" in fill_color
        # Second category: #4bdf5c
        assert "#4bdf5c" in fill_color
        # Third category: #4b4eee
        assert "#4b4eee" in fill_color

    def test_preserves_opacity(self, categorical_sld: str) -> None:
        """Fill opacity from SLD is preserved."""
        style = convert_sld(categorical_sld, source_layer="barrios")
        fill_layer = style["layers"][0]

        # Fixture has 0.4 opacity
        assert fill_layer["paint"]["fill-opacity"] == 0.4


class TestWarningsAndPartialConversion:
    """Tests for warn-and-continue behavior on unsupported SLD features."""

    def test_text_symbolizer_warns_but_continues(self) -> None:
        """TextSymbolizer emits warning but still produces style."""
        sld = """<?xml version="1.0" encoding="UTF-8"?>
        <sld:StyledLayerDescriptor xmlns:sld="http://www.opengis.net/sld"
                                   xmlns:ogc="http://www.opengis.net/ogc">
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
                            <sld:TextSymbolizer>
                                <sld:Label>
                                    <ogc:PropertyName>name</ogc:PropertyName>
                                </sld:Label>
                            </sld:TextSymbolizer>
                        </sld:Rule>
                    </sld:FeatureTypeStyle>
                </sld:UserStyle>
            </sld:NamedLayer>
        </sld:StyledLayerDescriptor>
        """
        style, warnings = convert_sld(sld, source_layer="data", return_warnings=True)

        # Should still produce the fill layer
        assert style["version"] == 8
        assert len(style["layers"]) >= 1
        assert style["layers"][0]["type"] == "fill"

        # Should warn about TextSymbolizer
        assert len(warnings) > 0
        assert any("TextSymbolizer" in w for w in warnings)

    def test_invalid_sld_raises(self) -> None:
        """Invalid XML raises SLDConverterError."""
        with pytest.raises(SLDConverterError):
            convert_sld("not valid xml", source_layer="data")

    def test_empty_sld_raises(self) -> None:
        """Empty SLD (no rules) raises SLDConverterError."""
        sld = """<?xml version="1.0" encoding="UTF-8"?>
        <sld:StyledLayerDescriptor xmlns:sld="http://www.opengis.net/sld">
        </sld:StyledLayerDescriptor>
        """
        with pytest.raises(SLDConverterError, match="No.*rules"):
            convert_sld(sld, source_layer="data")
