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
    parse_text_symbolizer,
)

pytestmark = pytest.mark.unit

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


class TestParseTextSymbolizer:
    """Tests for TextSymbolizer extraction."""

    def test_basic_label_extraction(self) -> None:
        """Extract text field from Label/PropertyName."""
        symbolizer_xml = """
        <sld:TextSymbolizer xmlns:sld="http://www.opengis.net/sld"
                            xmlns:ogc="http://www.opengis.net/ogc">
            <sld:Label>
                <ogc:PropertyName>name</ogc:PropertyName>
            </sld:Label>
        </sld:TextSymbolizer>
        """
        result = parse_text_symbolizer(symbolizer_xml)

        assert result["text_field"] == ["get", "name"]

    def test_font_extraction(self) -> None:
        """Extract font family and size from Font element."""
        symbolizer_xml = """
        <sld:TextSymbolizer xmlns:sld="http://www.opengis.net/sld"
                            xmlns:ogc="http://www.opengis.net/ogc">
            <sld:Label>
                <ogc:PropertyName>label</ogc:PropertyName>
            </sld:Label>
            <sld:Font>
                <sld:CssParameter name="font-family">DejaVu Sans</sld:CssParameter>
                <sld:CssParameter name="font-size">14</sld:CssParameter>
            </sld:Font>
        </sld:TextSymbolizer>
        """
        result = parse_text_symbolizer(symbolizer_xml)

        assert result["text_size"] == 14
        # Font mapped to Noto Sans (fallback for unknown fonts)
        assert result["text_font"] == ["Noto Sans Regular"]

    def test_fill_color_extraction(self) -> None:
        """Extract text color from Fill element."""
        symbolizer_xml = """
        <sld:TextSymbolizer xmlns:sld="http://www.opengis.net/sld"
                            xmlns:ogc="http://www.opengis.net/ogc">
            <sld:Label>
                <ogc:PropertyName>label</ogc:PropertyName>
            </sld:Label>
            <sld:Fill>
                <sld:CssParameter name="fill">#333333</sld:CssParameter>
            </sld:Fill>
        </sld:TextSymbolizer>
        """
        result = parse_text_symbolizer(symbolizer_xml)

        assert result["text_color"] == "#333333"

    def test_halo_extraction(self) -> None:
        """Extract halo (text outline) properties."""
        symbolizer_xml = """
        <sld:TextSymbolizer xmlns:sld="http://www.opengis.net/sld"
                            xmlns:ogc="http://www.opengis.net/ogc">
            <sld:Label>
                <ogc:PropertyName>label</ogc:PropertyName>
            </sld:Label>
            <sld:Halo>
                <sld:Radius>2</sld:Radius>
                <sld:Fill>
                    <sld:CssParameter name="fill">#ffffff</sld:CssParameter>
                </sld:Fill>
            </sld:Halo>
        </sld:TextSymbolizer>
        """
        result = parse_text_symbolizer(symbolizer_xml)

        assert result["text_halo_color"] == "#ffffff"
        assert result["text_halo_width"] == 2.0

    def test_anchor_point_extraction(self) -> None:
        """Extract anchor point from LabelPlacement."""
        symbolizer_xml = """
        <sld:TextSymbolizer xmlns:sld="http://www.opengis.net/sld"
                            xmlns:ogc="http://www.opengis.net/ogc">
            <sld:Label>
                <ogc:PropertyName>label</ogc:PropertyName>
            </sld:Label>
            <sld:LabelPlacement>
                <sld:PointPlacement>
                    <sld:AnchorPoint>
                        <sld:AnchorPointX>0</sld:AnchorPointX>
                        <sld:AnchorPointY>0.5</sld:AnchorPointY>
                    </sld:AnchorPoint>
                </sld:PointPlacement>
            </sld:LabelPlacement>
        </sld:TextSymbolizer>
        """
        result = parse_text_symbolizer(symbolizer_xml)

        # SLD anchor (0, 0.5) maps to Mapbox "left" (left edge, vertical center)
        assert result["text_anchor"] == "left"

    def test_complete_text_symbolizer(self) -> None:
        """Parse complete TextSymbolizer with all properties."""
        symbolizer_xml = """
        <sld:TextSymbolizer xmlns:sld="http://www.opengis.net/sld"
                            xmlns:ogc="http://www.opengis.net/ogc">
            <sld:Label>
                <ogc:PropertyName>nombre</ogc:PropertyName>
            </sld:Label>
            <sld:Font>
                <sld:CssParameter name="font-family">DejaVu Sans</sld:CssParameter>
                <sld:CssParameter name="font-size">10</sld:CssParameter>
                <sld:CssParameter name="font-style">normal</sld:CssParameter>
                <sld:CssParameter name="font-weight">normal</sld:CssParameter>
            </sld:Font>
            <sld:LabelPlacement>
                <sld:PointPlacement>
                    <sld:AnchorPoint>
                        <sld:AnchorPointX>0</sld:AnchorPointX>
                        <sld:AnchorPointY>0.5</sld:AnchorPointY>
                    </sld:AnchorPoint>
                </sld:PointPlacement>
            </sld:LabelPlacement>
            <sld:Halo>
                <sld:Radius>2</sld:Radius>
                <sld:Fill>
                    <sld:CssParameter name="fill">#ffffff</sld:CssParameter>
                </sld:Fill>
            </sld:Halo>
            <sld:Fill>
                <sld:CssParameter name="fill">#000000</sld:CssParameter>
            </sld:Fill>
        </sld:TextSymbolizer>
        """
        result = parse_text_symbolizer(symbolizer_xml)

        assert result["text_field"] == ["get", "nombre"]
        assert result["text_color"] == "#000000"
        assert result["text_size"] == 10
        assert result["text_halo_color"] == "#ffffff"
        assert result["text_halo_width"] == 2.0
        assert result["text_anchor"] == "left"

    def test_defaults_for_missing_properties(self) -> None:
        """Missing optional properties get sensible defaults."""
        symbolizer_xml = """
        <sld:TextSymbolizer xmlns:sld="http://www.opengis.net/sld"
                            xmlns:ogc="http://www.opengis.net/ogc">
            <sld:Label>
                <ogc:PropertyName>name</ogc:PropertyName>
            </sld:Label>
        </sld:TextSymbolizer>
        """
        result = parse_text_symbolizer(symbolizer_xml)

        assert result["text_field"] == ["get", "name"]
        assert result["text_color"] == "#000000"  # Default black
        assert result["text_size"] == 12  # Default size
        assert result["text_anchor"] == "center"  # Default center
        assert result.get("text_halo_color") is None
        assert result.get("text_halo_width") is None


class TestWarningsAndPartialConversion:
    """Tests for warn-and-continue behavior on unsupported SLD features."""

    def test_text_symbolizer_creates_label_layer(self) -> None:
        """TextSymbolizer creates a symbol layer for labels."""
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
        style = convert_sld(sld, source_layer="data")

        # Should have fill layer AND symbol layer for labels
        assert style["version"] == 8
        assert len(style["layers"]) == 2

        fill_layer = style["layers"][0]
        assert fill_layer["type"] == "fill"

        label_layer = style["layers"][1]
        assert label_layer["type"] == "symbol"
        assert label_layer["id"] == "fill-0-labels"
        assert label_layer["layout"]["text-field"] == ["get", "name"]

    def test_text_symbolizer_with_halo(self) -> None:
        """TextSymbolizer with halo extracts halo properties."""
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
                                    <ogc:PropertyName>label</ogc:PropertyName>
                                </sld:Label>
                                <sld:Font>
                                    <sld:CssParameter name="font-size">12</sld:CssParameter>
                                </sld:Font>
                                <sld:Halo>
                                    <sld:Radius>2</sld:Radius>
                                    <sld:Fill>
                                        <sld:CssParameter name="fill">#ffffff</sld:CssParameter>
                                    </sld:Fill>
                                </sld:Halo>
                                <sld:Fill>
                                    <sld:CssParameter name="fill">#333333</sld:CssParameter>
                                </sld:Fill>
                            </sld:TextSymbolizer>
                        </sld:Rule>
                    </sld:FeatureTypeStyle>
                </sld:UserStyle>
            </sld:NamedLayer>
        </sld:StyledLayerDescriptor>
        """
        style = convert_sld(sld, source_layer="data")

        label_layer = style["layers"][1]
        assert label_layer["type"] == "symbol"
        assert label_layer["paint"]["text-color"] == "#333333"
        assert label_layer["paint"]["text-halo-color"] == "#ffffff"
        assert label_layer["paint"]["text-halo-width"] == 2.0
        assert label_layer["layout"]["text-size"] == 12

    def test_categorical_sld_with_labels(self, categorical_sld: str) -> None:
        """Categorical SLD with TextSymbolizer produces label layer."""
        style = convert_sld(categorical_sld, source_layer="barrios")

        # Should have fill layer + label layer
        assert len(style["layers"]) == 2

        fill_layer = style["layers"][0]
        assert fill_layer["type"] == "fill"
        assert fill_layer["id"] == "categorical-fill"

        label_layer = style["layers"][1]
        assert label_layer["type"] == "symbol"
        assert label_layer["id"] == "categorical-fill-labels"
        assert label_layer["layout"]["text-field"] == ["get", "nombre"]

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

    def test_sld_1_1_se_namespace(self) -> None:
        """SLD 1.1 with SE namespace is parsed correctly."""
        sld = """<?xml version="1.0" encoding="UTF-8"?>
        <StyledLayerDescriptor xmlns="http://www.opengis.net/sld"
                               xmlns:se="http://www.opengis.net/se"
                               xmlns:ogc="http://www.opengis.net/ogc"
                               version="1.1.0">
            <NamedLayer>
                <se:Name>test</se:Name>
                <UserStyle>
                    <se:Name>test</se:Name>
                    <se:FeatureTypeStyle>
                        <se:Rule>
                            <se:PolygonSymbolizer>
                                <se:Fill>
                                    <se:SvgParameter name="fill">#00ff00</se:SvgParameter>
                                </se:Fill>
                            </se:PolygonSymbolizer>
                        </se:Rule>
                    </se:FeatureTypeStyle>
                </UserStyle>
            </NamedLayer>
        </StyledLayerDescriptor>
        """
        style = convert_sld(sld, source_layer="data")

        # Should produce a valid Mapbox GL style
        assert style["version"] == 8
        assert len(style["layers"]) >= 1
