"""Shared Mapbox GL style building blocks.

This module provides reusable functions for constructing Mapbox GL style
documents. All source format converters (SLD, ESRI, etc.) use these
primitives to generate consistent output.

The design follows DRY principles: parsing logic lives in format-specific
modules, while Mapbox GL generation is centralized here.

Mapbox GL Style Spec Reference:
https://docs.mapbox.com/mapbox-gl-js/style-spec/
"""

from __future__ import annotations

from typing import Any

# Type alias for Mapbox GL expressions
Expression = list[Any]
ColorValue = str | Expression
NumericValue = int | float | Expression


def esri_color_to_hex(color: list[int]) -> str:
    """Convert ESRI [r, g, b, a] array to #rrggbb hex string.

    Args:
        color: ESRI color array [red, green, blue, alpha] with 0-255 values.

    Returns:
        Hex color string like "#ff0000".
    """
    r, g, b = color[0], color[1], color[2]
    return f"#{r:02x}{g:02x}{b:02x}"


def esri_color_to_opacity(color: list[int]) -> float:
    """Extract opacity from ESRI [r, g, b, a] array.

    Args:
        color: ESRI color array [red, green, blue, alpha] with 0-255 values.

    Returns:
        Opacity as float 0.0-1.0.
    """
    if len(color) < 4:
        return 1.0
    return color[3] / 255.0


def hex_to_rgba(hex_color: str, opacity: float) -> str:
    """Convert hex color and opacity to rgba() CSS string.

    Args:
        hex_color: Color like "#ff0000" or "ff0000".
        opacity: Opacity 0.0-1.0.

    Returns:
        CSS rgba string like "rgba(255, 0, 0, 0.5)".
    """
    hex_color = hex_color.lstrip("#")
    r = int(hex_color[0:2], 16)
    g = int(hex_color[2:4], 16)
    b = int(hex_color[4:6], 16)
    return f"rgba({r}, {g}, {b}, {opacity})"


def make_match_expression(
    field: str,
    cases: list[tuple[Any, Any]],
    default: Any,
) -> Expression:
    """Build a Mapbox GL match expression for categorical styling.

    Creates: ["match", ["get", field], val1, output1, val2, output2, ..., default]

    Args:
        field: Property name to match on.
        cases: List of (value, output) tuples. Output can be colors (str) or numbers.
        default: Fallback value when no match.

    Returns:
        Mapbox GL match expression.
    """
    expr: list[Any] = ["match", ["get", field]]
    for value, output in cases:
        expr.append(value)
        expr.append(output)
    expr.append(default)
    return expr


def make_step_expression(
    field: str,
    breaks: list[tuple[Any, Any]],
) -> Expression:
    """Build a Mapbox GL step expression for graduated styling.

    Creates: ["step", ["get", field], initial, break1, val1, break2, val2, ...]

    The first tuple provides (min_value, initial_output). Subsequent tuples
    define break points where the output changes.

    Args:
        field: Property name for class breaks.
        breaks: List of (threshold, output_value) tuples. First tuple's
            threshold is the minimum value, subsequent are break points.

    Returns:
        Mapbox GL step expression.
    """
    if not breaks:
        raise ValueError("make_step_expression requires at least one break point")

    expr: list[Any] = ["step", ["get", field]]

    # First tuple provides the initial value (output below first break)
    _, initial = breaks[0]
    expr.append(initial)

    # Remaining tuples are break points
    for threshold, value in breaks[1:]:
        expr.append(threshold)
        expr.append(value)

    return expr


def make_fill_layer(
    layer_id: str,
    source_layer: str,
    fill_color: ColorValue,
    fill_opacity: NumericValue = 1.0,
    outline_color: ColorValue | None = None,
) -> dict[str, Any]:
    """Build a Mapbox GL fill layer for polygon data.

    Args:
        layer_id: Unique layer identifier.
        source_layer: Name of source layer in vector tiles.
        fill_color: Fill color (hex string or expression).
        fill_opacity: Fill opacity 0.0-1.0.
        outline_color: Optional outline color.

    Returns:
        Mapbox GL layer dict.
    """
    paint: dict[str, Any] = {
        "fill-color": fill_color,
        "fill-opacity": fill_opacity,
    }
    if outline_color:
        paint["fill-outline-color"] = outline_color

    return {
        "id": layer_id,
        "type": "fill",
        "source": "data",
        "source-layer": source_layer,
        "paint": paint,
    }


def make_circle_layer(
    layer_id: str,
    source_layer: str,
    circle_color: ColorValue,
    circle_radius: NumericValue = 5,
    circle_opacity: NumericValue = 1.0,
    stroke_color: ColorValue | None = None,
    stroke_width: NumericValue | None = None,
) -> dict[str, Any]:
    """Build a Mapbox GL circle layer for point data.

    Args:
        layer_id: Unique layer identifier.
        source_layer: Name of source layer in vector tiles.
        circle_color: Circle fill color (hex string or expression).
        circle_radius: Circle radius in pixels (number or expression).
        circle_opacity: Circle opacity 0.0-1.0.
        stroke_color: Optional stroke color.
        stroke_width: Optional stroke width.

    Returns:
        Mapbox GL layer dict.
    """
    paint: dict[str, Any] = {
        "circle-color": circle_color,
        "circle-radius": circle_radius,
        "circle-opacity": circle_opacity,
    }
    if stroke_color:
        paint["circle-stroke-color"] = stroke_color
    if stroke_width is not None:
        paint["circle-stroke-width"] = stroke_width

    return {
        "id": layer_id,
        "type": "circle",
        "source": "data",
        "source-layer": source_layer,
        "paint": paint,
    }


def make_line_layer(
    layer_id: str,
    source_layer: str,
    line_color: ColorValue,
    line_width: NumericValue = 1,
    line_opacity: NumericValue = 1.0,
) -> dict[str, Any]:
    """Build a Mapbox GL line layer for linestring data.

    Args:
        layer_id: Unique layer identifier.
        source_layer: Name of source layer in vector tiles.
        line_color: Line color (hex string or expression).
        line_width: Line width in pixels (number or expression).
        line_opacity: Line opacity 0.0-1.0.

    Returns:
        Mapbox GL layer dict.
    """
    return {
        "id": layer_id,
        "type": "line",
        "source": "data",
        "source-layer": source_layer,
        "paint": {
            "line-color": line_color,
            "line-width": line_width,
            "line-opacity": line_opacity,
        },
    }


def make_symbol_layer(
    layer_id: str,
    source_layer: str,
    text_field: str | Expression,
    text_color: ColorValue = "#000000",
    text_size: NumericValue = 12,
    text_font: list[str] | None = None,
    text_halo_color: ColorValue | None = None,
    text_halo_width: NumericValue = 1.0,
    text_anchor: str = "center",
    text_offset: tuple[float, float] | None = None,
) -> dict[str, Any]:
    """Build a Mapbox GL symbol layer for text labels.

    Args:
        layer_id: Unique layer identifier.
        source_layer: Name of source layer in vector tiles.
        text_field: Text content (string or ["get", "fieldname"] expression).
        text_color: Text color (hex string or expression).
        text_size: Font size in pixels (number or expression).
        text_font: List of font names (falls back to ["Noto Sans Regular"]).
        text_halo_color: Optional halo (outline) color.
        text_halo_width: Halo width in pixels.
        text_anchor: Text anchor position (center, left, right, top, bottom, etc.).
        text_offset: Optional (x, y) offset in ems.

    Returns:
        Mapbox GL symbol layer dict.
    """
    layout: dict[str, Any] = {
        "text-field": text_field,
        "text-size": text_size,
        "text-anchor": text_anchor,
    }

    if text_font:
        layout["text-font"] = text_font
    else:
        layout["text-font"] = ["Noto Sans Regular"]

    if text_offset:
        layout["text-offset"] = list(text_offset)

    paint: dict[str, Any] = {
        "text-color": text_color,
    }

    if text_halo_color is not None:
        paint["text-halo-color"] = text_halo_color
        paint["text-halo-width"] = text_halo_width

    return {
        "id": layer_id,
        "type": "symbol",
        "source": "data",
        "source-layer": source_layer,
        "layout": layout,
        "paint": paint,
    }


def make_mapbox_style(
    name: str,
    source_layer: str,
    layers: list[dict[str, Any]],
    pmtiles_url: str | None = None,
) -> dict[str, Any]:
    """Build a complete Mapbox GL style document.

    Args:
        name: Style name (appears in style["name"]).
        source_layer: Default source layer name.
        layers: List of layer dicts (from make_*_layer functions).
        pmtiles_url: Optional PMTiles URL for the source.

    Returns:
        Complete Mapbox GL style dict with version, sources, and layers.
    """
    source: dict[str, Any] = {"type": "vector"}
    if pmtiles_url:
        source["url"] = pmtiles_url

    return {
        "version": 8,
        "name": name,
        "sources": {
            "data": source,
        },
        "layers": layers,
    }
