"""OGC SLD XML to Mapbox GL converter.

Converts SLD (Styled Layer Descriptor) XML documents to Mapbox GL style JSON.

Supported symbolizers:
- PolygonSymbolizer: Fill and stroke for polygons
- PointSymbolizer: Circle markers for points
- LineSymbolizer: Line styling for linestrings

Partially supported (warn and continue):
- TextSymbolizer: Skipped with warning (labels not supported)
- TTF glyphs in PointSymbolizer: Falls back to circle

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

    for rule in rules:
        filter_elem = _find_with_ns(rule, "ogc:Filter")
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

    if geom_type == "polygon":
        return _build_categorical_fill(
            field,
            color_cases,
            opacity_cases,
            stroke_color_cases,
            stroke_width_cases,
            source_layer,
            warnings,
        )
    if geom_type == "point":
        return _build_categorical_circle(field, color_cases, size_cases, source_layer)
    if geom_type == "line":
        return _build_categorical_line(
            field, color_cases, opacity_cases, line_width_cases, source_layer
        )
    return []


def _build_simple_layers(
    rules: list[Element],
    geom_type: str,
    source_layer: str,
    warnings: list[str],
) -> list[dict[str, Any]]:
    """Build layers for simple (non-filtered) SLD rules."""
    layers: list[dict[str, Any]] = []

    for rule in rules:
        if geom_type == "polygon":
            symbolizer = _find_symbolizer(rule, "Polygon")
            if symbolizer is not None:
                props = parse_polygon_symbolizer(symbolizer)
                layers.append(
                    make_fill_layer(
                        f"fill-{len(layers)}",
                        source_layer,
                        props["fill_color"],
                        props["fill_opacity"],
                        props.get("stroke_color"),
                    )
                )
        elif geom_type == "point":
            for symbolizer in _findall_symbolizers(rule, "Point"):
                props = parse_point_symbolizer(symbolizer)
                layers.append(
                    make_circle_layer(
                        f"circle-{len(layers)}",
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
                layers.append(
                    make_line_layer(
                        f"line-{len(layers)}",
                        source_layer,
                        props["line_color"],
                        props["line_width"],
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

    for rule in rules:
        if _find_symbolizer(rule, "Text") is not None:
            warnings.append("TextSymbolizer not supported; labels will be omitted")
            break

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
