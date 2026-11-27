"""Shared Mapbox GL style building blocks."""

from __future__ import annotations

from typing import Any

_RGBA_LEN = 4

# Type alias for Mapbox GL expressions
Expression = list[Any]
ColorValue = str | Expression
NumericValue = int | float | Expression


def esri_color_to_hex(color: list[int] | None) -> str:
    """Convert ESRI [r, g, b, a] array to #rrggbb hex string."""
    if color is None:
        return "#888888"
    r, g, b = color[0], color[1], color[2]
    return f"#{r:02x}{g:02x}{b:02x}"


def esri_color_to_opacity(color: list[int] | None) -> float:
    """Extract opacity from ESRI [r, g, b, a] array."""
    if color is None:
        return 1.0
    if len(color) < _RGBA_LEN:
        return 1.0
    return color[3] / 255.0


def hex_to_rgba(hex_color: str, opacity: float) -> str:
    """Convert hex color and opacity to rgba() CSS string."""
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
    """Build a Mapbox GL match expression for categorical styling."""
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
    """Build a Mapbox GL step expression for graduated styling."""
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
    """Build a Mapbox GL fill layer for polygon data."""
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
    """Build a Mapbox GL circle layer for point data."""
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
    """Build a Mapbox GL line layer for linestring data."""
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


def make_symbol_layer(  # noqa: PLR0917 - one argument per Mapbox layer property
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
    """Build a Mapbox GL symbol layer for text labels."""
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
    layers: list[dict[str, Any]],
    pmtiles_url: str | None = None,
    min_zoom: int | None = None,
    max_zoom: int | None = None,
) -> dict[str, Any]:
    """Build a complete Mapbox GL style document."""
    source: dict[str, Any] = {"type": "vector"}
    if pmtiles_url:
        source["url"] = pmtiles_url if "://" in pmtiles_url else f"pmtiles://{pmtiles_url}"
    if min_zoom is not None:
        source["minzoom"] = min_zoom
    if max_zoom is not None:
        source["maxzoom"] = max_zoom

    return {
        "version": 8,
        "name": name,
        "sources": {
            "data": source,
        },
        "layers": layers,
    }
