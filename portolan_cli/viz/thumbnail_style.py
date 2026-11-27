"""Mapbox GL style parsing for thumbnail rendering."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_MATCH_MIN_LEN = 4
_GET_EXPR_LEN = 2
_PAIRS_MIN_LEN = 2
_INTERPOLATE_COLOR_INDEX = 4

logger = logging.getLogger(__name__)


@dataclass
class ThumbnailStyle:
    """Parsed style for thumbnail rendering."""

    fill_color: str
    fill_opacity: float
    edge_color: str | None
    color_field: str | None = None
    color_map: dict[Any, str] | None = None


def parse_match_expression(
    expr: Any,
) -> tuple[str, dict[Any, str], str] | None:
    """Parse a Mapbox GL match expression."""
    if not isinstance(expr, list) or len(expr) < _MATCH_MIN_LEN:
        return None

    if expr[0] != "match":
        return None

    # expr[1] should be ["get", "field_name"]
    get_expr = expr[1]
    if not isinstance(get_expr, list) or len(get_expr) != _GET_EXPR_LEN or get_expr[0] != "get":
        return None

    field_name = get_expr[1]

    # Match expressions are operator, field expression, value/color pairs, default color.
    pairs_and_default = expr[2:]
    if len(pairs_and_default) < _PAIRS_MIN_LEN:
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
    """Load Mapbox GL style and extract thumbnail rendering parameters."""
    try:
        with Path(style_path).open(encoding="utf-8") as f:
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
        # Not a match expression we understand (e.g., interpolate)
        # Try to extract a reasonable default
        elif len(fill_color_expr) > 0 and fill_color_expr[0] == "interpolate":
            # Interpolate expressions are operator, type, field expression, stop/color pairs.
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
    if len(expr) >= _INTERPOLATE_COLOR_INDEX + 1:
        # expr[4] should be first color (after stop1)
        first_color = expr[4]
        if isinstance(first_color, str) and first_color.startswith("#"):
            return first_color
    return "#3388ff"


def resolve_colors_for_gdf(
    gdf: Any,
    style: ThumbnailStyle,
    fallback: str | None = None,
) -> Any:
    """Resolve per-feature colors from style and GeoDataFrame attributes."""
    default = fallback if fallback is not None else style.fill_color

    if style.color_map is None or style.color_field is None:
        return default

    # Case-insensitive field lookup
    field = style.color_field
    actual_field = _find_column_case_insensitive(gdf.columns, field)

    if actual_field is None:
        logger.debug("Field %s not found in GDF columns: %s", field, list(gdf.columns))
        return default

    # Map field values to colors, using the fallback as default
    values = gdf[actual_field]

    # Convert color_map keys to strings for consistent matching
    # (GDF values might be strings, color_map keys might be from JSON)
    str_color_map = {str(k): v for k, v in style.color_map.items()}

    return values.astype(str).map(str_color_map).fillna(default)


def _find_column_case_insensitive(
    columns: Any,
    target: str,
) -> str | None:
    """Find column name matching target case-insensitively."""
    target_lower = target.lower()
    for col in columns:
        if str(col).lower() == target_lower:
            return str(col)
    return None


def resolve_color_for_properties(
    properties: dict[str, Any],
    style: ThumbnailStyle,
    fallback: str | None = None,
) -> str:
    """Resolve color for a single feature from its properties."""
    default = fallback if fallback is not None else style.fill_color

    if style.color_map is None or style.color_field is None:
        return default

    # Case-insensitive property lookup
    field = style.color_field
    value = None
    field_lower = field.lower()
    for key, val in properties.items():
        if key.lower() == field_lower:
            value = val
            break

    if value is None:
        return default

    # Look up in color_map (try both original and string representation)
    color = style.color_map.get(value)
    if color is None:
        color = style.color_map.get(str(value))
    if color is None:
        color = default

    return color
