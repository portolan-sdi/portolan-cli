"""ESRI REST API renderer to Mapbox GL converter.

Converts ESRI drawingInfo.renderer JSON to Mapbox GL style documents.

Supported renderer types:
- simple: Single symbol for all features
- uniqueValue: Categorical classification (match expression)
- classBreaks: Graduated classification (step expression)

Supported symbol types:
- esriSFS: Simple fill symbol (polygons)
- esriSLS: Simple line symbol (lines)
- esriSMS: Simple marker symbol (points → circles)
- esriPMS: Picture marker symbol (points → warns, falls back to circle)

Usage:
    renderer = layer_json["drawingInfo"]["renderer"]
    style = convert_esri_renderer(renderer, source_layer="my-layer")
"""

from __future__ import annotations

import logging
from typing import Any, Literal, overload

from portolan_cli.extract.common.converters.base import (
    esri_color_to_hex,
    esri_color_to_opacity,
    make_circle_layer,
    make_fill_layer,
    make_line_layer,
    make_mapbox_style,
    make_match_expression,
    make_step_expression,
)

logger = logging.getLogger(__name__)


class ESRIConverterError(Exception):
    """Error during ESRI renderer conversion."""

    pass


def _symbol_to_layer_type(symbol: dict[str, Any]) -> str:
    """Determine Mapbox GL layer type from ESRI symbol type."""
    symbol_type = symbol.get("type", "")
    if symbol_type == "esriSFS":
        return "fill"
    if symbol_type == "esriSLS":
        return "line"
    if symbol_type in ("esriSMS", "esriPMS"):
        return "circle"
    return "fill"  # Default fallback


def _parse_fill_symbol(
    symbol: dict[str, Any],
    layer_id: str,
    source_layer: str,
) -> dict[str, Any]:
    """Parse esriSFS (simple fill symbol) to Mapbox GL fill layer."""
    color = symbol.get("color", [128, 128, 128, 255])
    fill_color = esri_color_to_hex(color)
    fill_opacity = esri_color_to_opacity(color)

    outline = symbol.get("outline", {})
    outline_color = None
    if outline and outline.get("color"):
        outline_color = esri_color_to_hex(outline["color"])

    return make_fill_layer(
        layer_id=layer_id,
        source_layer=source_layer,
        fill_color=fill_color,
        fill_opacity=fill_opacity,
        outline_color=outline_color,
    )


def _parse_circle_symbol(
    symbol: dict[str, Any],
    layer_id: str,
    source_layer: str,
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    """Parse esriSMS/esriPMS to Mapbox GL circle layer."""
    symbol_type = symbol.get("type", "")

    # Picture marker - warn and fall back to circle
    if symbol_type == "esriPMS":
        if warnings is not None:
            warnings.append(
                f"Picture marker symbol (esriPMS) not fully supported; "
                f"falling back to circle. URL: {symbol.get('url', 'unknown')}"
            )
        # Use a default color for picture markers
        return make_circle_layer(
            layer_id=layer_id,
            source_layer=source_layer,
            circle_color="#888888",
            circle_radius=8,
        )

    # Check for unsupported marker styles
    style = symbol.get("style", "esriSMSCircle")
    if style != "esriSMSCircle":
        if warnings is not None:
            warnings.append(
                f"Marker style '{style}' not directly supported; falling back to circle."
            )

    color = symbol.get("color", [128, 128, 128, 255])
    circle_color = esri_color_to_hex(color)
    circle_opacity = esri_color_to_opacity(color)

    # ESRI size is diameter, Mapbox radius is half
    size = symbol.get("size", 10)
    radius = size / 2

    outline = symbol.get("outline", {})
    stroke_color = None
    stroke_width = None
    if outline:
        if outline.get("color"):
            stroke_color = esri_color_to_hex(outline["color"])
        stroke_width = outline.get("width")

    return make_circle_layer(
        layer_id=layer_id,
        source_layer=source_layer,
        circle_color=circle_color,
        circle_radius=radius,
        circle_opacity=circle_opacity,
        stroke_color=stroke_color,
        stroke_width=stroke_width,
    )


def _parse_line_symbol(
    symbol: dict[str, Any],
    layer_id: str,
    source_layer: str,
) -> dict[str, Any]:
    """Parse esriSLS (simple line symbol) to Mapbox GL line layer."""
    color = symbol.get("color", [128, 128, 128, 255])
    line_color = esri_color_to_hex(color)
    line_opacity = esri_color_to_opacity(color)
    line_width = symbol.get("width", 1)

    return make_line_layer(
        layer_id=layer_id,
        source_layer=source_layer,
        line_color=line_color,
        line_width=line_width,
        line_opacity=line_opacity,
    )


def _parse_symbol(
    symbol: dict[str, Any],
    layer_id: str,
    source_layer: str,
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    """Route symbol to appropriate parser based on type."""
    symbol_type = symbol.get("type", "")

    if symbol_type == "esriSFS":
        return _parse_fill_symbol(symbol, layer_id, source_layer)
    if symbol_type == "esriSLS":
        return _parse_line_symbol(symbol, layer_id, source_layer)
    if symbol_type in ("esriSMS", "esriPMS"):
        return _parse_circle_symbol(symbol, layer_id, source_layer, warnings)

    # Unknown symbol type - fall back to fill
    if warnings is not None:
        warnings.append(f"Unknown symbol type '{symbol_type}'; defaulting to fill.")
    return make_fill_layer(
        layer_id=layer_id,
        source_layer=source_layer,
        fill_color="#888888",
        fill_opacity=0.5,
    )


def parse_simple_renderer(
    renderer: dict[str, Any],
    source_layer: str,
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    """Convert simple renderer to Mapbox GL style.

    Simple renderers apply the same symbol to all features.

    Args:
        renderer: ESRI renderer dict with type="simple".
        source_layer: Name for the source-layer in output.
        warnings: Optional list to collect conversion warnings.

    Returns:
        Complete Mapbox GL style dict.
    """
    symbol = renderer.get("symbol", {})
    layer = _parse_symbol(symbol, "layer-0", source_layer, warnings)

    return make_mapbox_style(
        name="Simple Style",
        source_layer=source_layer,
        layers=[layer],
    )


def parse_uniquevalue_renderer(
    renderer: dict[str, Any],
    source_layer: str,
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    """Convert uniqueValue renderer to Mapbox GL style with match expression.

    UniqueValue renderers apply different symbols based on attribute values.
    Converts to a Mapbox GL match expression.

    Args:
        renderer: ESRI renderer dict with type="uniqueValue".
        source_layer: Name for the source-layer in output.
        warnings: Optional list to collect conversion warnings.

    Returns:
        Complete Mapbox GL style dict.
    """
    field = renderer.get("field1", renderer.get("field", "value"))
    infos = renderer.get("uniqueValueInfos", [])

    if not infos:
        if warnings is not None:
            warnings.append("UniqueValue renderer has no value infos; using default.")
        return make_mapbox_style(
            name="Empty UniqueValue Style",
            source_layer=source_layer,
            layers=[make_fill_layer("layer-0", source_layer, "#888888", 0.5)],
        )

    # Determine layer type from first symbol
    first_symbol = infos[0].get("symbol", {})
    layer_type = _symbol_to_layer_type(first_symbol)

    # Build match expression cases
    cases: list[tuple[Any, str]] = []
    for info in infos:
        value = info.get("value")
        symbol = info.get("symbol", {})
        color = symbol.get("color", [128, 128, 128, 255])
        cases.append((value, esri_color_to_hex(color)))

    color_expr = make_match_expression(field, cases, default="#cccccc")

    # Get opacity from first symbol
    first_color = first_symbol.get("color", [128, 128, 128, 255])
    opacity = esri_color_to_opacity(first_color)

    # Build appropriate layer type
    if layer_type == "fill":
        layer = make_fill_layer(
            layer_id="categorical-fill",
            source_layer=source_layer,
            fill_color=color_expr,
            fill_opacity=opacity,
        )
    elif layer_type == "circle":
        layer = make_circle_layer(
            layer_id="categorical-circle",
            source_layer=source_layer,
            circle_color=color_expr,
            circle_opacity=opacity,
        )
    else:
        layer = make_line_layer(
            layer_id="categorical-line",
            source_layer=source_layer,
            line_color=color_expr,
            line_opacity=opacity,
        )

    return make_mapbox_style(
        name="Categorical Style",
        source_layer=source_layer,
        layers=[layer],
    )


def parse_classbreaks_renderer(
    renderer: dict[str, Any],
    source_layer: str,
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    """Convert classBreaks renderer to Mapbox GL style with step expression.

    ClassBreaks renderers apply different symbols based on numeric ranges.
    Converts to a Mapbox GL step expression.

    Args:
        renderer: ESRI renderer dict with type="classBreaks".
        source_layer: Name for the source-layer in output.
        warnings: Optional list to collect conversion warnings.

    Returns:
        Complete Mapbox GL style dict.
    """
    field = renderer.get("field", "value")
    min_value = renderer.get("minValue", 0)
    break_infos = renderer.get("classBreakInfos", [])

    if not break_infos:
        if warnings is not None:
            warnings.append("ClassBreaks renderer has no break infos; using default.")
        return make_mapbox_style(
            name="Empty ClassBreaks Style",
            source_layer=source_layer,
            layers=[make_fill_layer("layer-0", source_layer, "#888888", 0.5)],
        )

    # Determine layer type and property to graduate from first symbol
    first_symbol = break_infos[0].get("symbol", {})
    layer_type = _symbol_to_layer_type(first_symbol)

    # For graduated symbols, we typically vary size (circles) or color
    # Check if sizes vary (graduated symbol) or colors vary (choropleth)
    sizes = [info.get("symbol", {}).get("size") for info in break_infos]
    sizes_vary = len({s for s in sizes if s is not None}) > 1

    if layer_type == "circle" and sizes_vary:
        # Graduated symbol sizes
        # step returns initial value for inputs < first stop
        # We need: [(min, size1), (break1, size2), (break2, size3), ...]
        breaks: list[tuple[Any, Any]] = []
        for i, info in enumerate(break_infos):
            if i == 0:
                # Initial value for the range [minValue, classMaxValue]
                symbol = info.get("symbol", {})
                size = symbol.get("size", 10)
                breaks.append((min_value, size / 2))
            else:
                # Break at previous classMaxValue
                prev_max = break_infos[i - 1].get("classMaxValue", 0)
                symbol = info.get("symbol", {})
                size = symbol.get("size", 10)
                breaks.append((prev_max, size / 2))

        radius_expr = make_step_expression(field, breaks)

        # Get color from first symbol (usually same for all in graduated size)
        color = first_symbol.get("color", [128, 128, 128, 255])
        circle_color = esri_color_to_hex(color)

        # Get stroke if present
        outline = first_symbol.get("outline", {})
        stroke_color = None
        stroke_width = None
        if outline and outline.get("color"):
            stroke_color = esri_color_to_hex(outline["color"])
            stroke_width = outline.get("width")

        layer = make_circle_layer(
            layer_id="graduated-circle",
            source_layer=source_layer,
            circle_color=circle_color,
            circle_radius=radius_expr,
            stroke_color=stroke_color,
            stroke_width=stroke_width,
        )
    else:
        # Choropleth (color varies by class)
        cases: list[tuple[float, str]] = []
        for info in break_infos:
            max_val = info.get("classMaxValue", 0)
            symbol = info.get("symbol", {})
            color = symbol.get("color", [128, 128, 128, 255])
            cases.append((max_val, esri_color_to_hex(color)))

        # Build step expression for colors
        # For color, we need the breaks to work correctly
        color_breaks: list[tuple[Any, Any]] = []
        for i, info in enumerate(break_infos):
            symbol = info.get("symbol", {})
            color = esri_color_to_hex(symbol.get("color", [128, 128, 128, 255]))
            if i == 0:
                color_breaks.append((min_value, color))
            else:
                prev_max = break_infos[i - 1].get("classMaxValue", 0)
                color_breaks.append((prev_max, color))

        color_expr = make_step_expression(field, color_breaks)

        if layer_type == "fill":
            layer = make_fill_layer(
                layer_id="choropleth-fill",
                source_layer=source_layer,
                fill_color=color_expr,
                fill_opacity=0.7,
            )
        else:
            layer = make_line_layer(
                layer_id="graduated-line",
                source_layer=source_layer,
                line_color=color_expr,
            )

    return make_mapbox_style(
        name="Graduated Style",
        source_layer=source_layer,
        layers=[layer],
    )


@overload
def convert_esri_renderer(
    renderer: dict[str, Any],
    source_layer: str,
    *,
    return_warnings: Literal[False] = False,
) -> dict[str, Any]: ...


@overload
def convert_esri_renderer(
    renderer: dict[str, Any],
    source_layer: str,
    *,
    return_warnings: Literal[True],
) -> tuple[dict[str, Any], list[str]]: ...


def convert_esri_renderer(
    renderer: dict[str, Any],
    source_layer: str,
    *,
    return_warnings: bool = False,
) -> dict[str, Any] | tuple[dict[str, Any], list[str]]:
    """Convert ESRI renderer to Mapbox GL style.

    Main entry point for ESRI → Mapbox GL conversion. Routes to appropriate
    parser based on renderer type.

    Args:
        renderer: ESRI drawingInfo.renderer dict.
        source_layer: Name for the source-layer in output.
        return_warnings: If True, return (style, warnings) tuple.

    Returns:
        Mapbox GL style dict, or (style, warnings) tuple if return_warnings=True.

    Raises:
        ESRIConverterError: If renderer type is not supported.
    """
    renderer_type = renderer.get("type", "")
    warnings: list[str] = []

    if renderer_type == "simple":
        style = parse_simple_renderer(renderer, source_layer, warnings)
    elif renderer_type == "uniqueValue":
        style = parse_uniquevalue_renderer(renderer, source_layer, warnings)
    elif renderer_type == "classBreaks":
        style = parse_classbreaks_renderer(renderer, source_layer, warnings)
    else:
        raise ESRIConverterError(
            f"Unsupported ESRI renderer type: '{renderer_type}'. "
            f"Supported types: simple, uniqueValue, classBreaks."
        )

    if return_warnings:
        return style, warnings
    return style
