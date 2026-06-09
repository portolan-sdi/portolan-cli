"""Mapbox GL style parsing for thumbnail rendering.

This module extracts rendering parameters from Mapbox GL styles to apply
consistent colors when generating thumbnails. Supports:
- Simple fill colors (hex strings)
- Match expressions for categorical styling
- Case-insensitive field matching

For full Mapbox GL rendering, use a proper map renderer. This module provides
a minimal subset for matplotlib-based thumbnail generation.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class ThumbnailStyle:
    """Parsed style for thumbnail rendering.

    Attributes:
        fill_color: Default fill color (hex). Used for all features if no
            categorical mapping, or as fallback for unmapped values.
        fill_opacity: Fill opacity (0.0-1.0).
        edge_color: Optional outline color (hex).
        color_field: Field name for categorical coloring (from match expression).
        color_map: Mapping of field values to colors (from match expression).
    """

    fill_color: str
    fill_opacity: float
    edge_color: str | None
    color_field: str | None = None
    color_map: dict[Any, str] | None = None


def parse_match_expression(
    expr: Any,
) -> tuple[str, dict[Any, str], str] | None:
    """Parse a Mapbox GL match expression.

    Expected format: ["match", ["get", "field"], value1, color1, value2, color2, ..., default]

    Args:
        expr: The expression to parse (typically from paint["fill-color"]).

    Returns:
        Tuple of (field_name, {value: color}, default_color), or None if not
        a valid match expression.
    """
    if not isinstance(expr, list) or len(expr) < 4:
        return None

    if expr[0] != "match":
        return None

    # expr[1] should be ["get", "field_name"]
    get_expr = expr[1]
    if not isinstance(get_expr, list) or len(get_expr) != 2 or get_expr[0] != "get":
        return None

    field_name = get_expr[1]

    # Remaining items are value/color pairs followed by default
    # ["match", ["get", "x"], val1, c1, val2, c2, default]
    #                         ^--- expr[2:]
    pairs_and_default = expr[2:]
    if len(pairs_and_default) < 2:
        return None

    default_color = pairs_and_default[-1]
    pairs = pairs_and_default[:-1]

    # pairs should have even length (value, color, value, color, ...)
    if len(pairs) % 2 != 0:
        return None

    color_map: dict[Any, str] = {}
    for i in range(0, len(pairs), 2):
        value = pairs[i]
        color = pairs[i + 1]
        color_map[value] = color

    return field_name, color_map, default_color


def load_thumbnail_style(style_path: Path) -> ThumbnailStyle | None:
    """Load Mapbox GL style and extract thumbnail rendering parameters.

    Finds the first fill layer and extracts its paint properties. Supports
    both simple fill colors and match expressions for categorical styling.

    Args:
        style_path: Path to Mapbox GL style JSON file.

    Returns:
        ThumbnailStyle if a fill layer was found and parsed, None otherwise.
    """
    try:
        with open(style_path) as f:
            style = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.debug("Failed to load style %s: %s", style_path, e)
        return None

    # Find first fill layer
    layers = style.get("layers", [])
    fill_layer = None
    for layer in layers:
        if layer.get("type") == "fill":
            fill_layer = layer
            break

    if fill_layer is None:
        logger.debug("No fill layer found in style %s", style_path)
        return None

    paint = fill_layer.get("paint", {})
    fill_color_expr = paint.get("fill-color", "#3388ff")
    fill_opacity = paint.get("fill-opacity", 1.0)
    edge_color = paint.get("fill-outline-color")

    # Parse fill-color: either simple string or match expression
    color_field: str | None = None
    color_map: dict[Any, str] | None = None
    fill_color: str

    if isinstance(fill_color_expr, str):
        fill_color = fill_color_expr
    elif isinstance(fill_color_expr, list):
        match_result = parse_match_expression(fill_color_expr)
        if match_result:
            color_field, color_map, fill_color = match_result
        else:
            # Not a match expression we understand (e.g., interpolate)
            # Try to extract a reasonable default
            if len(fill_color_expr) > 0 and fill_color_expr[0] == "interpolate":
                # For interpolate, use the first color as default
                # Format: ["interpolate", ["linear"], ["get", "x"], stop1, color1, ...]
                fill_color = _extract_first_color_from_interpolate(fill_color_expr)
            else:
                fill_color = "#3388ff"
    else:
        fill_color = "#3388ff"

    return ThumbnailStyle(
        fill_color=fill_color,
        fill_opacity=fill_opacity,
        edge_color=edge_color,
        color_field=color_field,
        color_map=color_map,
    )


def _extract_first_color_from_interpolate(expr: list[Any]) -> str:
    """Extract first color from interpolate expression as fallback.

    Format: ["interpolate", ["linear"], ["get", "x"], stop1, color1, stop2, color2, ...]
    """
    # Skip "interpolate", interpolation method, and input expression
    if len(expr) >= 5:
        # expr[4] should be first color (after stop1)
        first_color = expr[4]
        if isinstance(first_color, str) and first_color.startswith("#"):
            return first_color
    return "#3388ff"


def resolve_colors_for_gdf(
    gdf: Any,
    style: ThumbnailStyle,
) -> Any:
    """Resolve per-feature colors from style and GeoDataFrame attributes.

    For categorical styles (with color_map), maps each feature's field value
    to its corresponding color. Handles case-insensitive field matching.

    For simple styles (no color_map), returns the uniform fill_color.

    Args:
        gdf: GeoDataFrame with feature attributes.
        style: Parsed thumbnail style.

    Returns:
        Either a pandas Series of hex colors (categorical) or a single hex
        string (uniform color).
    """
    if style.color_map is None or style.color_field is None:
        return style.fill_color

    # Case-insensitive field lookup
    field = style.color_field
    actual_field = _find_column_case_insensitive(gdf.columns, field)

    if actual_field is None:
        logger.debug("Field %s not found in GDF columns: %s", field, list(gdf.columns))
        return style.fill_color

    # Map field values to colors, using fill_color as default
    values = gdf[actual_field]

    # Convert color_map keys to strings for consistent matching
    # (GDF values might be strings, color_map keys might be from JSON)
    str_color_map = {str(k): v for k, v in style.color_map.items()}

    colors = values.astype(str).map(str_color_map).fillna(style.fill_color)

    return colors


def _find_column_case_insensitive(
    columns: Any,
    target: str,
) -> str | None:
    """Find column name matching target case-insensitively.

    Args:
        columns: DataFrame column index.
        target: Target column name (any case).

    Returns:
        Actual column name if found, None otherwise.
    """
    target_lower = target.lower()
    for col in columns:
        if str(col).lower() == target_lower:
            return str(col)
    return None


def resolve_color_for_properties(
    properties: dict[str, Any],
    style: ThumbnailStyle,
) -> str:
    """Resolve color for a single feature from its properties.

    Used for PMTiles path where features are dicts, not GeoDataFrame rows.

    Args:
        properties: Feature properties dict.
        style: Parsed thumbnail style.

    Returns:
        Hex color string.
    """
    if style.color_map is None or style.color_field is None:
        return style.fill_color

    # Case-insensitive property lookup
    field = style.color_field
    value = None
    field_lower = field.lower()
    for key, val in properties.items():
        if key.lower() == field_lower:
            value = val
            break

    if value is None:
        return style.fill_color

    # Look up in color_map (try both original and string representation)
    color = style.color_map.get(value)
    if color is None:
        color = style.color_map.get(str(value))
    if color is None:
        color = style.fill_color

    return color
