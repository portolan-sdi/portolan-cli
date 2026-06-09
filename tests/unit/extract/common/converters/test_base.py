"""Tests for base Mapbox GL style builders.

These are the DRY building blocks used by all style converters.
"""

from __future__ import annotations

import pytest

from portolan_cli.extract.common.converters.base import (
    esri_color_to_hex,
    hex_to_rgba,
    make_circle_layer,
    make_fill_layer,
    make_line_layer,
    make_mapbox_style,
    make_match_expression,
    make_step_expression,
    make_symbol_layer,
)

pytestmark = pytest.mark.unit


class TestColorConversion:
    """Tests for color format conversion utilities."""

    def test_esri_color_to_hex_rgb(self) -> None:
        """ESRI [r, g, b, a] array converts to #rrggbb hex."""
        assert esri_color_to_hex([255, 0, 0, 255]) == "#ff0000"
        assert esri_color_to_hex([0, 255, 0, 255]) == "#00ff00"
        assert esri_color_to_hex([0, 0, 255, 255]) == "#0000ff"

    def test_esri_color_to_hex_with_alpha(self) -> None:
        """Alpha channel is ignored in hex output (handled separately)."""
        assert esri_color_to_hex([255, 0, 0, 128]) == "#ff0000"
        assert esri_color_to_hex([255, 0, 0, 0]) == "#ff0000"

    def test_esri_color_to_hex_real_values(self) -> None:
        """Real values from ESRI Census fixture."""
        assert esri_color_to_hex([115, 178, 255, 255]) == "#73b2ff"
        assert esri_color_to_hex([100, 179, 131, 255]) == "#64b383"

    def test_hex_to_rgba_basic(self) -> None:
        """Hex color converts to rgba() string with opacity."""
        assert hex_to_rgba("#ff0000", 1.0) == "rgba(255, 0, 0, 1.0)"
        assert hex_to_rgba("#00ff00", 0.5) == "rgba(0, 255, 0, 0.5)"

    def test_hex_to_rgba_with_hash(self) -> None:
        """Works with or without leading #."""
        assert hex_to_rgba("ff0000", 1.0) == "rgba(255, 0, 0, 1.0)"
        assert hex_to_rgba("#ff0000", 1.0) == "rgba(255, 0, 0, 1.0)"


class TestMatchExpression:
    """Tests for Mapbox GL match expression builder."""

    def test_match_expression_simple(self) -> None:
        """Basic match expression with string field."""
        expr = make_match_expression(
            field="type",
            cases=[("A", "#ff0000"), ("B", "#00ff00")],
            default="#cccccc",
        )
        assert expr == [
            "match",
            ["get", "type"],
            "A",
            "#ff0000",
            "B",
            "#00ff00",
            "#cccccc",
        ]

    def test_match_expression_numeric_values(self) -> None:
        """Match expression with numeric field values."""
        expr = make_match_expression(
            field="color_id",
            cases=[(1, "#c4db69"), (2, "#4bdf5c"), (3, "#4b4eee")],
            default="#888888",
        )
        assert expr == [
            "match",
            ["get", "color_id"],
            1,
            "#c4db69",
            2,
            "#4bdf5c",
            3,
            "#4b4eee",
            "#888888",
        ]

    def test_match_expression_empty_cases(self) -> None:
        """Empty cases returns just default."""
        expr = make_match_expression(field="x", cases=[], default="#000000")
        assert expr == ["match", ["get", "x"], "#000000"]


class TestStepExpression:
    """Tests for Mapbox GL step expression builder (class breaks)."""

    def test_step_expression_basic(self) -> None:
        """Step expression for graduated values."""
        expr = make_step_expression(
            field="POP2000",
            breaks=[
                (0, 4),  # min value, initial size
                (61, 7.5),  # break at 61, size becomes 7.5
                (264, 11),  # break at 264, size becomes 11
            ],
        )
        assert expr == [
            "step",
            ["get", "POP2000"],
            4,  # initial value
            61,
            7.5,  # first break
            264,
            11,  # second break
        ]

    def test_step_expression_single_break(self) -> None:
        """Single break point."""
        expr = make_step_expression(
            field="value",
            breaks=[(0, 10), (100, 20)],
        )
        assert expr == ["step", ["get", "value"], 10, 100, 20]

    def test_step_expression_empty_raises(self) -> None:
        """Empty breaks raises ValueError."""
        import pytest

        with pytest.raises(ValueError, match="at least one break point"):
            make_step_expression(field="value", breaks=[])


class TestLayerBuilders:
    """Tests for Mapbox GL layer type builders."""

    def test_make_fill_layer_basic(self) -> None:
        """Fill layer with solid color."""
        layer = make_fill_layer(
            layer_id="fill-layer",
            source_layer="my-layer",
            fill_color="#ff0000",
            fill_opacity=0.6,
        )
        assert layer["id"] == "fill-layer"
        assert layer["type"] == "fill"
        assert layer["source-layer"] == "my-layer"
        assert layer["paint"]["fill-color"] == "#ff0000"
        assert layer["paint"]["fill-opacity"] == 0.6

    def test_make_fill_layer_with_outline(self) -> None:
        """Fill layer with outline color."""
        layer = make_fill_layer(
            layer_id="fill-layer",
            source_layer="my-layer",
            fill_color="#ff0000",
            fill_opacity=0.6,
            outline_color="#000000",
        )
        assert layer["paint"]["fill-outline-color"] == "#000000"

    def test_make_fill_layer_with_expression(self) -> None:
        """Fill layer with match expression for color."""
        expr = ["match", ["get", "type"], "A", "#ff0000", "#cccccc"]
        layer = make_fill_layer(
            layer_id="categorical-fill",
            source_layer="data",
            fill_color=expr,
            fill_opacity=0.4,
        )
        assert layer["paint"]["fill-color"] == expr

    def test_make_circle_layer_basic(self) -> None:
        """Circle layer for point data."""
        layer = make_circle_layer(
            layer_id="points",
            source_layer="data",
            circle_color="#73b2ff",
            circle_radius=8,
            circle_opacity=1.0,
        )
        assert layer["type"] == "circle"
        assert layer["paint"]["circle-color"] == "#73b2ff"
        assert layer["paint"]["circle-radius"] == 8
        assert layer["paint"]["circle-opacity"] == 1.0

    def test_make_circle_layer_with_stroke(self) -> None:
        """Circle layer with stroke."""
        layer = make_circle_layer(
            layer_id="points",
            source_layer="data",
            circle_color="#73b2ff",
            circle_radius=8,
            stroke_color="#000000",
            stroke_width=1,
        )
        assert layer["paint"]["circle-stroke-color"] == "#000000"
        assert layer["paint"]["circle-stroke-width"] == 1

    def test_make_circle_layer_with_expression(self) -> None:
        """Circle layer with step expression for radius."""
        expr = ["step", ["get", "POP2000"], 4, 61, 7.5, 264, 11]
        layer = make_circle_layer(
            layer_id="graduated-points",
            source_layer="data",
            circle_color="#73b2ff",
            circle_radius=expr,
        )
        assert layer["paint"]["circle-radius"] == expr

    def test_make_line_layer_basic(self) -> None:
        """Line layer for linestring data."""
        layer = make_line_layer(
            layer_id="roads",
            source_layer="data",
            line_color="#333333",
            line_width=2,
        )
        assert layer["type"] == "line"
        assert layer["paint"]["line-color"] == "#333333"
        assert layer["paint"]["line-width"] == 2

    def test_make_symbol_layer_basic(self) -> None:
        """Symbol layer for text labels."""
        layer = make_symbol_layer(
            layer_id="labels",
            source_layer="data",
            text_field=["get", "name"],
        )
        assert layer["id"] == "labels"
        assert layer["type"] == "symbol"
        assert layer["source"] == "data"
        assert layer["source-layer"] == "data"
        assert layer["layout"]["text-field"] == ["get", "name"]
        assert layer["paint"]["text-color"] == "#000000"  # default

    def test_make_symbol_layer_with_font(self) -> None:
        """Symbol layer with custom font."""
        layer = make_symbol_layer(
            layer_id="labels",
            source_layer="data",
            text_field=["get", "name"],
            text_font=["Open Sans Regular", "Noto Sans Regular"],
        )
        assert layer["layout"]["text-font"] == [
            "Open Sans Regular",
            "Noto Sans Regular",
        ]

    def test_make_symbol_layer_with_halo(self) -> None:
        """Symbol layer with text halo (outline)."""
        layer = make_symbol_layer(
            layer_id="labels",
            source_layer="data",
            text_field=["get", "name"],
            text_halo_color="#ffffff",
            text_halo_width=2.0,
        )
        assert layer["paint"]["text-halo-color"] == "#ffffff"
        assert layer["paint"]["text-halo-width"] == 2.0

    def test_make_symbol_layer_with_anchor_and_offset(self) -> None:
        """Symbol layer with anchor and offset."""
        layer = make_symbol_layer(
            layer_id="labels",
            source_layer="data",
            text_field=["get", "name"],
            text_anchor="left",
            text_offset=(1.0, 0.0),
        )
        assert layer["layout"]["text-anchor"] == "left"
        assert layer["layout"]["text-offset"] == [1.0, 0.0]

    def test_make_symbol_layer_full(self) -> None:
        """Symbol layer with all properties."""
        layer = make_symbol_layer(
            layer_id="categorical-fill-labels",
            source_layer="barrios",
            text_field=["get", "nombre"],
            text_color="#333333",
            text_size=14,
            text_font=["Noto Sans Regular"],
            text_halo_color="#ffffff",
            text_halo_width=1.5,
            text_anchor="bottom-left",
            text_offset=(0.5, -0.5),
        )
        assert layer["id"] == "categorical-fill-labels"
        assert layer["type"] == "symbol"
        assert layer["layout"]["text-field"] == ["get", "nombre"]
        assert layer["layout"]["text-size"] == 14
        assert layer["layout"]["text-font"] == ["Noto Sans Regular"]
        assert layer["layout"]["text-anchor"] == "bottom-left"
        assert layer["layout"]["text-offset"] == [0.5, -0.5]
        assert layer["paint"]["text-color"] == "#333333"
        assert layer["paint"]["text-halo-color"] == "#ffffff"
        assert layer["paint"]["text-halo-width"] == 1.5


class TestMapboxStyleBuilder:
    """Tests for complete Mapbox GL style document builder."""

    def test_make_mapbox_style_minimal(self) -> None:
        """Minimal style with one layer."""
        layer = make_fill_layer("fill", "data", "#ff0000", 0.5)
        style = make_mapbox_style(
            name="Test Style",
            source_layer="data",
            layers=[layer],
        )
        assert style["version"] == 8
        assert style["name"] == "Test Style"
        assert "data" in style["sources"]
        assert style["sources"]["data"]["type"] == "vector"
        assert len(style["layers"]) == 1

    def test_make_mapbox_style_with_pmtiles_url(self) -> None:
        """Style with PMTiles source URL."""
        layer = make_fill_layer("fill", "data", "#ff0000", 0.5)
        style = make_mapbox_style(
            name="Test",
            source_layer="data",
            layers=[layer],
            pmtiles_url="../data.pmtiles",
        )
        assert style["sources"]["data"]["url"] == "../data.pmtiles"

    def test_make_mapbox_style_multiple_layers(self) -> None:
        """Style with multiple layers (e.g., fill + outline)."""
        fill = make_fill_layer("fill", "data", "#ff0000", 0.5)
        outline = make_line_layer("outline", "data", "#000000", 1)
        style = make_mapbox_style(
            name="Multi-layer",
            source_layer="data",
            layers=[fill, outline],
        )
        assert len(style["layers"]) == 2
        assert style["layers"][0]["id"] == "fill"
        assert style["layers"][1]["id"] == "outline"
