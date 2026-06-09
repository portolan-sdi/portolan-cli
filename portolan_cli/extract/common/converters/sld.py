"""OGC SLD XML to Mapbox GL converter.

Converts SLD (Styled Layer Descriptor) XML documents to Mapbox GL style JSON.

Supported symbolizers:
- PolygonSymbolizer: Fill and stroke for polygons
- PointSymbolizer: Circle markers for points
- LineSymbolizer: Line styling for linestrings
- TextSymbolizer: Symbol layers for text labels

Partially supported (warn and continue):
- TTF glyphs in PointSymbolizer: Falls back to circle
- Unknown fonts in TextSymbolizer: Falls back to Noto Sans Regular

OGC Filter support:
- PropertyIsEqualTo: Extracts field/value for categorical styling

Usage:
    sld_xml = fetch_wms_getstyles(url, layer)
    style = convert_sld(sld_xml, source_layer="my-layer")
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal, overload

import defusedxml.ElementTree as ET

if TYPE_CHECKING:
    from xml.etree.ElementTree import Element  # nosec B405 - type annotation only

from portolan_cli.extract.common.converters.base import (
    make_circle_layer,
    make_fill_layer,
    make_line_layer,
    make_mapbox_style,
    make_match_expression,
    make_symbol_layer,
)

logger = logging.getLogger(__name__)


class SLDConverterError(Exception):
    """Error during SLD conversion."""

    pass


# XML namespaces used in SLD
NAMESPACES = {
    "sld": "http://www.opengis.net/sld",
    "ogc": "http://www.opengis.net/ogc",
    "gml": "http://www.opengis.net/gml",
    "se": "http://www.opengis.net/se",
}


def _find_with_ns(element: Element, path: str) -> Element | None:
    """Find element with namespace-aware path."""
    return element.find(path, NAMESPACES)


def _findall_with_ns(element: Element, path: str) -> list[Element]:
    """Find all elements with namespace-aware path."""
    return element.findall(path, NAMESPACES)


def _find_symbolizer(element: Element, symbolizer_type: str) -> Element | None:
    """Find symbolizer with SLD or SE namespace fallback.

    Args:
        element: Parent element to search within.
        symbolizer_type: One of "Polygon", "Point", "Line".

    Returns:
        Found symbolizer element or None.
    """
    # Try SLD namespace first
    result = _find_with_ns(element, f".//sld:{symbolizer_type}Symbolizer")
    if result is not None:
        return result
    # Fallback to SE namespace (SLD 1.1)
    return _find_with_ns(element, f".//se:{symbolizer_type}Symbolizer")


def _findall_symbolizers(element: Element, symbolizer_type: str) -> list[Element]:
    """Find all symbolizers with SLD or SE namespace fallback.

    Args:
        element: Parent element to search within.
        symbolizer_type: One of "Polygon", "Point", "Line".

    Returns:
        List of found symbolizer elements.
    """
    # Try SLD namespace first
    results = _findall_with_ns(element, f".//sld:{symbolizer_type}Symbolizer")
    if results:
        return results
    # Fallback to SE namespace (SLD 1.1)
    return _findall_with_ns(element, f".//se:{symbolizer_type}Symbolizer")


def _find_sld_or_se(element: Element, local_name: str) -> Element | None:
    """Find element with SLD namespace, falling back to SE namespace.

    Args:
        element: Parent element to search within.
        local_name: Element local name (e.g., "Radius", "Fill").

    Returns:
        Found element or None.

    Note:
        Uses explicit `is not None` checks because Element objects with no
        children are falsy in Python, making `elem or fallback` unreliable.
    """
    result = _find_with_ns(element, f"sld:{local_name}")
    if result is not None:
        return result
    return _find_with_ns(element, f"se:{local_name}")


def _get_css_parameter(element: Element, name: str) -> str | None:
    """Extract CssParameter/SvgParameter value by name attribute.

    Handles both SLD 1.0 (CssParameter) and SLD 1.1 (SvgParameter).
    """
    # Try SLD 1.0 CssParameter
    for param in _findall_with_ns(element, ".//sld:CssParameter"):
        if param.get("name") == name:
            return param.text
    # Try SLD 1.1 SvgParameter
    for param in _findall_with_ns(element, ".//se:SvgParameter"):
        if param.get("name") == name:
            return param.text
    return None


def parse_filter_to_value(filter_xml: str | Element) -> tuple[str, Any]:
    """Extract field name and value from OGC Filter.

    Currently supports PropertyIsEqualTo for categorical classification.

    Args:
        filter_xml: Filter XML string or Element.

    Returns:
        Tuple of (field_name, value).

    Raises:
        SLDConverterError: If filter cannot be parsed.
    """
    if isinstance(filter_xml, str):
        # Parse as fragment - need to handle namespace
        try:
            root = ET.fromstring(filter_xml)
        except ET.ParseError as e:
            raise SLDConverterError(f"Invalid filter XML: {e}") from e
    else:
        root = filter_xml

    # Look for PropertyIsEqualTo
    prop_eq = _find_with_ns(root, ".//ogc:PropertyIsEqualTo")
    if prop_eq is None:
        # Try without namespace prefix (some SLDs don't use it)
        prop_eq = root.find(".//{http://www.opengis.net/ogc}PropertyIsEqualTo")

    if prop_eq is not None:
        prop_name = _find_with_ns(prop_eq, "ogc:PropertyName")
        literal = _find_with_ns(prop_eq, "ogc:Literal")

        if prop_name is not None and literal is not None:
            field = prop_name.text or ""
            value_str = literal.text or ""

            # Try to coerce to number if it looks numeric
            try:
                if "." in value_str:
                    value: Any = float(value_str)
                else:
                    value = int(value_str)
            except ValueError:
                value = value_str

            return field, value

    raise SLDConverterError("Could not extract field/value from filter")


def parse_polygon_symbolizer(symbolizer_xml: str | Element) -> dict[str, Any]:
    """Extract fill and stroke properties from PolygonSymbolizer.

    Args:
        symbolizer_xml: PolygonSymbolizer XML string or Element.

    Returns:
        Dict with fill_color, fill_opacity, stroke_color, stroke_width.
    """
    if isinstance(symbolizer_xml, str):
        try:
            root = ET.fromstring(symbolizer_xml)
        except ET.ParseError as e:
            raise SLDConverterError(f"Invalid symbolizer XML: {e}") from e
    else:
        root = symbolizer_xml

    result: dict[str, Any] = {
        "fill_color": "#888888",
        "fill_opacity": 1.0,
        "stroke_color": None,
        "stroke_width": None,
    }

    # Extract Fill (SLD 1.0 or SE)
    fill = _find_with_ns(root, ".//sld:Fill")
    if fill is None:
        fill = _find_with_ns(root, ".//se:Fill")
    if fill is not None:
        fill_color = _get_css_parameter(fill, "fill")
        if fill_color:
            result["fill_color"] = fill_color

        fill_opacity = _get_css_parameter(fill, "fill-opacity")
        if fill_opacity:
            try:
                result["fill_opacity"] = float(fill_opacity)
            except ValueError:
                pass

    # Extract Stroke (SLD 1.0 or SE)
    stroke = _find_with_ns(root, ".//sld:Stroke")
    if stroke is None:
        stroke = _find_with_ns(root, ".//se:Stroke")
    if stroke is not None:
        stroke_color = _get_css_parameter(stroke, "stroke")
        if stroke_color:
            result["stroke_color"] = stroke_color

        stroke_width = _get_css_parameter(stroke, "stroke-width")
        if stroke_width:
            try:
                result["stroke_width"] = float(stroke_width)
            except ValueError:
                pass

    return result


def parse_point_symbolizer(symbolizer_xml: str | Element) -> dict[str, Any]:
    """Extract point marker properties from PointSymbolizer.

    Args:
        symbolizer_xml: PointSymbolizer XML string or Element.

    Returns:
        Dict with type, fill_color, size, and optional warning.
    """
    if isinstance(symbolizer_xml, str):
        try:
            root = ET.fromstring(symbolizer_xml)
        except ET.ParseError as e:
            raise SLDConverterError(f"Invalid symbolizer XML: {e}") from e
    else:
        root = symbolizer_xml

    result: dict[str, Any] = {
        "type": "circle",
        "fill_color": "#888888",
        "size": 10,
    }

    # Get WellKnownName (SLD 1.0 or SE)
    wkn = _find_with_ns(root, ".//sld:WellKnownName")
    if wkn is None:
        wkn = _find_with_ns(root, ".//se:WellKnownName")
    if wkn is not None and wkn.text:
        wkn_text = wkn.text.lower()
        if wkn_text.startswith("ttf://"):
            result["warning"] = f"TTF glyph '{wkn.text}' not supported; using circle"
        elif wkn_text not in ("circle", "square"):
            result["warning"] = f"WellKnownName '{wkn.text}' mapped to circle"

    # Get fill color from Mark (SLD 1.0 or SE)
    mark = _find_with_ns(root, ".//sld:Mark")
    if mark is None:
        mark = _find_with_ns(root, ".//se:Mark")
    if mark is not None:
        fill = _find_with_ns(mark, "sld:Fill")
        if fill is None:
            fill = _find_with_ns(mark, "se:Fill")
        if fill is not None:
            fill_color = _get_css_parameter(fill, "fill")
            if fill_color:
                result["fill_color"] = fill_color

    # Get size (SLD 1.0 or SE)
    size_elem = _find_with_ns(root, ".//sld:Size")
    if size_elem is None:
        size_elem = _find_with_ns(root, ".//se:Size")
    if size_elem is not None and size_elem.text:
        try:
            result["size"] = float(size_elem.text)
        except ValueError:
            pass

    return result


def parse_line_symbolizer(symbolizer_xml: str | Element) -> dict[str, Any]:
    """Extract line properties from LineSymbolizer.

    Args:
        symbolizer_xml: LineSymbolizer XML string or Element.

    Returns:
        Dict with line_color, line_width, line_opacity.
    """
    if isinstance(symbolizer_xml, str):
        try:
            root = ET.fromstring(symbolizer_xml)
        except ET.ParseError as e:
            raise SLDConverterError(f"Invalid symbolizer XML: {e}") from e
    else:
        root = symbolizer_xml

    result: dict[str, Any] = {
        "line_color": "#888888",
        "line_width": 1,
        "line_opacity": 1.0,
    }

    stroke = _find_with_ns(root, ".//sld:Stroke")
    if stroke is None:
        stroke = _find_with_ns(root, ".//se:Stroke")
    if stroke is not None:
        color = _get_css_parameter(stroke, "stroke")
        if color:
            result["line_color"] = color

        width = _get_css_parameter(stroke, "stroke-width")
        if width:
            try:
                result["line_width"] = float(width)
            except ValueError:
                pass

        opacity = _get_css_parameter(stroke, "stroke-opacity")
        if opacity:
            try:
                result["line_opacity"] = float(opacity)
            except ValueError:
                pass

    return result


def _sld_anchor_to_mapbox(anchor_x: float, anchor_y: float) -> str:
    """Convert SLD anchor point coordinates to Mapbox GL text-anchor.

    SLD anchor: (0, 0) = top-left, (1, 1) = bottom-right, (0.5, 0.5) = center
    Mapbox anchor: named positions like "center", "left", "bottom-right"

    Args:
        anchor_x: Horizontal anchor 0.0 (left) to 1.0 (right).
        anchor_y: Vertical anchor 0.0 (top) to 1.0 (bottom).

    Returns:
        Mapbox GL text-anchor value.
    """
    # Map to horizontal position
    if anchor_x < 0.33:
        h = "left"
    elif anchor_x > 0.66:
        h = "right"
    else:
        h = ""

    # Map to vertical position
    if anchor_y < 0.33:
        v = "top"
    elif anchor_y > 0.66:
        v = "bottom"
    else:
        v = ""

    # Combine into Mapbox anchor name
    if h and v:
        return f"{v}-{h}"
    elif h:
        return h
    elif v:
        return v
    else:
        return "center"


def _parse_text_label(root: Element) -> list[str]:
    """Extract Label/PropertyName from TextSymbolizer."""
    label = _find_with_ns(root, ".//sld:Label")
    if label is None:
        label = _find_with_ns(root, ".//se:Label")
    if label is not None:
        prop_name = _find_with_ns(label, "ogc:PropertyName")
        if prop_name is not None and prop_name.text:
            return ["get", prop_name.text]
    return ["get", ""]


def _parse_text_font(root: Element) -> tuple[float, list[str]]:
    """Extract Font properties from TextSymbolizer."""
    text_size = 12.0
    text_font = ["Noto Sans Regular"]

    font = _find_with_ns(root, ".//sld:Font")
    if font is None:
        font = _find_with_ns(root, ".//se:Font")
    if font is None:
        return text_size, text_font

    font_size = _get_css_parameter(font, "font-size")
    if font_size:
        try:
            text_size = float(font_size)
        except ValueError:
            pass

    font_family = _get_css_parameter(font, "font-family")
    if font_family:
        logger.debug(f"Font '{font_family}' mapped to Noto Sans Regular")

    return text_size, text_font


def _parse_text_fill(root: Element) -> str:
    """Extract direct Fill color from TextSymbolizer (not Halo fill)."""
    for fill in _findall_with_ns(root, "sld:Fill"):
        fill_color = _get_css_parameter(fill, "fill")
        if fill_color:
            return fill_color
    for fill in _findall_with_ns(root, "se:Fill"):
        fill_color = _get_css_parameter(fill, "fill")
        if fill_color:
            return fill_color
    return "#000000"


def _parse_text_halo(root: Element) -> tuple[str | None, float | None]:
    """Extract Halo properties from TextSymbolizer."""
    halo = _find_with_ns(root, ".//sld:Halo")
    if halo is None:
        halo = _find_with_ns(root, ".//se:Halo")
    if halo is None:
        return None, None

    halo_width: float | None = None
    radius = _find_sld_or_se(halo, "Radius")
    if radius is not None and radius.text:
        try:
            halo_width = float(radius.text)
        except ValueError:
            pass

    halo_color: str | None = None
    halo_fill = _find_sld_or_se(halo, "Fill")
    if halo_fill is not None:
        color = _get_css_parameter(halo_fill, "fill")
        if color:
            halo_color = color

    return halo_color, halo_width


def _parse_text_anchor(root: Element) -> str:
    """Extract LabelPlacement/AnchorPoint from TextSymbolizer."""
    anchor_point = _find_with_ns(root, ".//sld:AnchorPoint")
    if anchor_point is None:
        anchor_point = _find_with_ns(root, ".//se:AnchorPoint")
    if anchor_point is None:
        return "center"

    anchor_x_elem = _find_sld_or_se(anchor_point, "AnchorPointX")
    anchor_y_elem = _find_sld_or_se(anchor_point, "AnchorPointY")

    anchor_x, anchor_y = 0.5, 0.5
    if anchor_x_elem is not None and anchor_x_elem.text:
        try:
            anchor_x = float(anchor_x_elem.text)
        except ValueError:
            pass
    if anchor_y_elem is not None and anchor_y_elem.text:
        try:
            anchor_y = float(anchor_y_elem.text)
        except ValueError:
            pass

    return _sld_anchor_to_mapbox(anchor_x, anchor_y)


def parse_text_symbolizer(symbolizer_xml: str | Element) -> dict[str, Any]:
    """Extract text label properties from TextSymbolizer.

    Args:
        symbolizer_xml: TextSymbolizer XML string or Element.

    Returns:
        Dict with text_field, text_color, text_size, text_font,
        text_halo_color, text_halo_width, text_anchor.
    """
    if isinstance(symbolizer_xml, str):
        try:
            root = ET.fromstring(symbolizer_xml)
        except ET.ParseError as e:
            raise SLDConverterError(f"Invalid symbolizer XML: {e}") from e
    else:
        root = symbolizer_xml

    text_size, text_font = _parse_text_font(root)
    halo_color, halo_width = _parse_text_halo(root)

    return {
        "text_field": _parse_text_label(root),
        "text_color": _parse_text_fill(root),
        "text_size": text_size,
        "text_font": text_font,
        "text_anchor": _parse_text_anchor(root),
        "text_halo_color": halo_color,
        "text_halo_width": halo_width,
    }


def _extract_rules(root: Element) -> list[Element]:
    """Extract all Rule elements from SLD document."""
    rules = _findall_with_ns(root, ".//sld:Rule")
    if not rules:
        # Try SE namespace (SLD 1.1)
        rules = root.findall(".//{http://www.opengis.net/se}Rule")
    return rules


def _determine_style_type(rules: list[Element]) -> str:
    """Determine if style is categorical (has filters) or simple."""
    for rule in rules:
        filter_elem = _find_with_ns(rule, "ogc:Filter")
        if filter_elem is not None:
            return "categorical"
    return "simple"


def _determine_geometry_type(rules: list[Element]) -> str:
    """Determine geometry type from symbolizer types."""
    for rule in rules:
        if _find_symbolizer(rule, "Polygon") is not None:
            return "polygon"
        if _find_symbolizer(rule, "Point") is not None:
            return "point"
        if _find_symbolizer(rule, "Line") is not None:
            return "line"
    return "polygon"  # Default


def _extract_style_name(root: Element) -> str:
    """Extract style name from SLD document."""
    # Try UserStyle Name
    name_elem = _find_with_ns(root, ".//sld:UserStyle/sld:Name")
    if name_elem is not None and name_elem.text:
        return name_elem.text

    # Try NamedLayer Name
    name_elem = _find_with_ns(root, ".//sld:NamedLayer/sld:Name")
    if name_elem is not None and name_elem.text:
        # Remove namespace prefix like "geonode:"
        name = name_elem.text
        if ":" in name:
            name = name.split(":")[-1]
        return name

    return "Converted Style"


def _get_uniform_opacity(opacity_cases: list[tuple[Any, float]]) -> float:
    """Get uniform opacity if all cases have same value, else return 1.0."""
    if not opacity_cases:
        return 1.0
    unique = {op for _, op in opacity_cases}
    return next(iter(unique)) if len(unique) == 1 else 1.0


def _build_categorical_fill(
    field: str,
    color_cases: list[tuple[Any, str]],
    opacity_cases: list[tuple[Any, float]],
    stroke_color_cases: list[tuple[Any, str | None]],
    stroke_width_cases: list[tuple[Any, float | None]],
    source_layer: str,
    warnings: list[str],
) -> list[dict[str, Any]]:
    """Build categorical fill layer with per-rule property support.

    Builds match expressions for fill color, opacity, stroke color, and stroke width
    when values vary across rules. Uses uniform values when all rules share the same
    value to keep the style simpler.
    """
    color_expr = make_match_expression(field, color_cases, default="#cccccc")

    # Opacity: use expression if values vary, else uniform
    unique_opacities = {op for _, op in opacity_cases}
    opacity_value: float | list[Any]
    if len(unique_opacities) == 1:
        opacity_value = next(iter(unique_opacities))
    else:
        opacity_value = make_match_expression(field, opacity_cases, default=1.0)

    # Stroke color: filter out None values, build expression if non-empty and varying
    stroke_color_defined = [(v, c) for v, c in stroke_color_cases if c is not None]
    stroke_color_value: str | list[Any] | None = None
    if stroke_color_defined:
        unique_stroke_colors = {c for _, c in stroke_color_defined}
        if len(unique_stroke_colors) == 1:
            stroke_color_value = next(iter(unique_stroke_colors))
        elif len(stroke_color_defined) < len(stroke_color_cases):
            warnings.append("Some rules missing stroke-color; using default for unspecified")
            stroke_color_value = make_match_expression(
                field, stroke_color_defined, default="#000000"
            )
        else:
            stroke_color_value = make_match_expression(
                field, stroke_color_defined, default="#000000"
            )

    # Stroke width: filter out None values, build expression if non-empty and varying
    stroke_width_defined = [(v, w) for v, w in stroke_width_cases if w is not None]
    stroke_width_value: float | list[Any] | None = None
    if stroke_width_defined:
        unique_stroke_widths = {w for _, w in stroke_width_defined}
        if len(unique_stroke_widths) == 1:
            stroke_width_value = next(iter(unique_stroke_widths))
        elif len(stroke_width_defined) < len(stroke_width_cases):
            warnings.append("Some rules missing stroke-width; using default for unspecified")
            stroke_width_value = make_match_expression(field, stroke_width_defined, default=1.0)
        else:
            stroke_width_value = make_match_expression(field, stroke_width_defined, default=1.0)

    # Build base layer with computed expressions
    layer = make_fill_layer(
        "categorical-fill",
        source_layer,
        color_expr,
        opacity_value,
        stroke_color_value,
    )

    # Add stroke-width if we have one (not a standard fill property, use line layer)
    # Note: fill-outline only supports color, not width. For stroke width, we'd need
    # a separate line layer. Log this limitation.
    if stroke_width_value is not None:
        warnings.append(
            "Mapbox fill layers only support outline color, not width; "
            "stroke-width ignored for categorical fills"
        )

    return [layer]


def _build_categorical_circle(
    field: str,
    color_cases: list[tuple[Any, str]],
    size_cases: list[tuple[Any, float]],
    source_layer: str,
) -> list[dict[str, Any]]:
    """Build categorical circle layer for point data."""
    color_expr = make_match_expression(field, color_cases, default="#cccccc")
    unique_sizes = {s for _, s in size_cases}
    size_value: float | list[Any]
    if len(unique_sizes) == 1:
        size_value = next(iter(unique_sizes))
    else:
        size_value = make_match_expression(field, size_cases, default=5.0)
    return [
        make_circle_layer("categorical-circle", source_layer, color_expr, circle_radius=size_value)
    ]


def _build_categorical_line(
    field: str,
    color_cases: list[tuple[Any, str]],
    opacity_cases: list[tuple[Any, float]],
    width_cases: list[tuple[Any, float]],
    source_layer: str,
) -> list[dict[str, Any]]:
    """Build categorical line layer with per-rule width and opacity."""
    color_expr = make_match_expression(field, color_cases, default="#cccccc")

    unique_widths = {w for _, w in width_cases}
    width_value: float | list[Any]
    if len(unique_widths) == 1:
        width_value = next(iter(unique_widths))
    else:
        width_value = make_match_expression(field, width_cases, default=1.0)

    unique_opacities = {op for _, op in opacity_cases}
    opacity_value: float | list[Any]
    if len(unique_opacities) == 1:
        opacity_value = next(iter(unique_opacities))
    else:
        opacity_value = make_match_expression(field, opacity_cases, default=1.0)

    return [
        make_line_layer(
            "categorical-line",
            source_layer,
            color_expr,
            line_width=width_value,
            line_opacity=opacity_value,
        )
    ]


def _build_categorical_layers(
    rules: list[Element],
    geom_type: str,
    source_layer: str,
    warnings: list[str],
) -> list[dict[str, Any]]:
    """Build layers for categorical (filtered) SLD rules."""
    field: str | None = None
    color_cases: list[tuple[Any, str]] = []
    opacity_cases: list[tuple[Any, float]] = []
    stroke_color_cases: list[tuple[Any, str | None]] = []
    stroke_width_cases: list[tuple[Any, float | None]] = []
    size_cases: list[tuple[Any, float]] = []
    line_width_cases: list[tuple[Any, float]] = []

    # Track TextSymbolizer from rules (may be in filtered or unfiltered rules)
    text_symbolizer: Element | None = None

    for rule in rules:
        filter_elem = _find_with_ns(rule, "ogc:Filter")

        # Check for TextSymbolizer in any rule (filtered or not)
        rule_text_sym = _find_symbolizer(rule, "Text")
        if rule_text_sym is not None:
            text_symbolizer = rule_text_sym

        if filter_elem is None:
            continue
        try:
            rule_field, value = parse_filter_to_value(filter_elem)
            if field is None:
                field = rule_field
        except SLDConverterError:
            continue

        symbolizer_type = {"polygon": "Polygon", "point": "Point", "line": "Line"}.get(geom_type)
        symbolizer = _find_symbolizer(rule, symbolizer_type) if symbolizer_type else None
        if symbolizer is None:
            continue

        if geom_type == "polygon":
            props = parse_polygon_symbolizer(symbolizer)
            color_cases.append((value, props["fill_color"]))
            opacity_cases.append((value, props["fill_opacity"]))
            stroke_color_cases.append((value, props.get("stroke_color")))
            stroke_width_cases.append((value, props.get("stroke_width")))
        elif geom_type == "point":
            props = parse_point_symbolizer(symbolizer)
            color_cases.append((value, props["fill_color"]))
            size_cases.append((value, props["size"]))
            if props.get("warning"):
                warnings.append(props["warning"])
        elif geom_type == "line":
            props = parse_line_symbolizer(symbolizer)
            color_cases.append((value, props["line_color"]))
            opacity_cases.append((value, props["line_opacity"]))
            line_width_cases.append((value, props["line_width"]))

    if not field or not color_cases:
        return []

    layers: list[dict[str, Any]] = []
    geom_layer_id: str = ""

    if geom_type == "polygon":
        geom_layer_id = "categorical-fill"
        layers = _build_categorical_fill(
            field,
            color_cases,
            opacity_cases,
            stroke_color_cases,
            stroke_width_cases,
            source_layer,
            warnings,
        )
    elif geom_type == "point":
        geom_layer_id = "categorical-circle"
        layers = _build_categorical_circle(field, color_cases, size_cases, source_layer)
    elif geom_type == "line":
        geom_layer_id = "categorical-line"
        layers = _build_categorical_line(
            field, color_cases, opacity_cases, line_width_cases, source_layer
        )

    # Add label layer if TextSymbolizer was found
    if text_symbolizer is not None and layers:
        text_props = parse_text_symbolizer(text_symbolizer)
        label_layer_id = f"{geom_layer_id}-labels"
        layers.append(
            make_symbol_layer(
                label_layer_id,
                source_layer,
                text_field=text_props["text_field"],
                text_color=text_props["text_color"],
                text_size=text_props["text_size"],
                text_font=text_props["text_font"],
                text_halo_color=text_props.get("text_halo_color"),
                text_halo_width=text_props.get("text_halo_width") or 1.0,
                text_anchor=text_props["text_anchor"],
            )
        )

    return layers


def _build_simple_layers(
    rules: list[Element],
    geom_type: str,
    source_layer: str,
    warnings: list[str],
) -> list[dict[str, Any]]:
    """Build layers for simple (non-filtered) SLD rules."""
    layers: list[dict[str, Any]] = []

    for rule in rules:
        geom_layer_id: str | None = None

        if geom_type == "polygon":
            symbolizer = _find_symbolizer(rule, "Polygon")
            if symbolizer is not None:
                props = parse_polygon_symbolizer(symbolizer)
                geom_layer_id = f"fill-{len(layers)}"
                layers.append(
                    make_fill_layer(
                        geom_layer_id,
                        source_layer,
                        props["fill_color"],
                        props["fill_opacity"],
                        props.get("stroke_color"),
                    )
                )
        elif geom_type == "point":
            for symbolizer in _findall_symbolizers(rule, "Point"):
                props = parse_point_symbolizer(symbolizer)
                geom_layer_id = f"circle-{len(layers)}"
                layers.append(
                    make_circle_layer(
                        geom_layer_id,
                        source_layer,
                        props["fill_color"],
                        props["size"] / 2,
                    )
                )
                if props.get("warning"):
                    warnings.append(props["warning"])
        elif geom_type == "line":
            symbolizer = _find_symbolizer(rule, "Line")
            if symbolizer is not None:
                props = parse_line_symbolizer(symbolizer)
                geom_layer_id = f"line-{len(layers)}"
                layers.append(
                    make_line_layer(
                        geom_layer_id,
                        source_layer,
                        props["line_color"],
                        props["line_width"],
                    )
                )

        # Check for TextSymbolizer and add label layer
        text_symbolizer = _find_symbolizer(rule, "Text")
        if text_symbolizer is not None and geom_layer_id is not None:
            text_props = parse_text_symbolizer(text_symbolizer)
            label_layer_id = f"{geom_layer_id}-labels"
            layers.append(
                make_symbol_layer(
                    label_layer_id,
                    source_layer,
                    text_field=text_props["text_field"],
                    text_color=text_props["text_color"],
                    text_size=text_props["text_size"],
                    text_font=text_props["text_font"],
                    text_halo_color=text_props.get("text_halo_color"),
                    text_halo_width=text_props.get("text_halo_width") or 1.0,
                    text_anchor=text_props["text_anchor"],
                )
            )

    return layers


@overload
def convert_sld(
    sld_xml: str,
    source_layer: str,
    *,
    return_warnings: Literal[False] = False,
) -> dict[str, Any]: ...


@overload
def convert_sld(
    sld_xml: str,
    source_layer: str,
    *,
    return_warnings: Literal[True],
) -> tuple[dict[str, Any], list[str]]: ...


def convert_sld(
    sld_xml: str,
    source_layer: str,
    *,
    return_warnings: bool = False,
) -> dict[str, Any] | tuple[dict[str, Any], list[str]]:
    """Convert SLD XML to Mapbox GL style.

    Main entry point for SLD → Mapbox GL conversion.

    Args:
        sld_xml: Complete SLD XML document as string.
        source_layer: Name for the source-layer in output.
        return_warnings: If True, return (style, warnings) tuple.

    Returns:
        Mapbox GL style dict, or (style, warnings) tuple if return_warnings=True.

    Raises:
        SLDConverterError: If SLD cannot be parsed or has no rules.
    """
    warnings: list[str] = []

    try:
        root = ET.fromstring(sld_xml)
    except ET.ParseError as e:
        raise SLDConverterError(f"Invalid SLD XML: {e}") from e

    rules = _extract_rules(root)
    if not rules:
        raise SLDConverterError("No rules found in SLD document")

    style_type = _determine_style_type(rules)
    geom_type = _determine_geometry_type(rules)
    style_name = _extract_style_name(root)

    if style_type == "categorical":
        layers = _build_categorical_layers(rules, geom_type, source_layer, warnings)
    else:
        layers = _build_simple_layers(rules, geom_type, source_layer, warnings)

    if not layers:
        raise SLDConverterError("No valid symbolizers found in SLD rules")

    style = make_mapbox_style(style_name, source_layer, layers)

    if return_warnings:
        return style, warnings
    return style
