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
import xml.etree.ElementTree as ET
from typing import Any, Literal, overload

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


def _find_with_ns(element: ET.Element, path: str) -> ET.Element | None:
    """Find element with namespace-aware path."""
    return element.find(path, NAMESPACES)


def _findall_with_ns(element: ET.Element, path: str) -> list[ET.Element]:
    """Find all elements with namespace-aware path."""
    return element.findall(path, NAMESPACES)


def _find_symbolizer(element: ET.Element, symbolizer_type: str) -> ET.Element | None:
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


def _findall_symbolizers(element: ET.Element, symbolizer_type: str) -> list[ET.Element]:
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


def _get_css_parameter(element: ET.Element, name: str) -> str | None:
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


def parse_filter_to_value(filter_xml: str | ET.Element) -> tuple[str, Any]:
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


def parse_polygon_symbolizer(symbolizer_xml: str | ET.Element) -> dict[str, Any]:
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


def parse_point_symbolizer(symbolizer_xml: str | ET.Element) -> dict[str, Any]:
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


def parse_line_symbolizer(symbolizer_xml: str | ET.Element) -> dict[str, Any]:
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


def _extract_rules(root: ET.Element) -> list[ET.Element]:
    """Extract all Rule elements from SLD document."""
    rules = _findall_with_ns(root, ".//sld:Rule")
    if not rules:
        # Try SE namespace (SLD 1.1)
        rules = root.findall(".//{http://www.opengis.net/se}Rule")
    return rules


def _determine_style_type(rules: list[ET.Element]) -> str:
    """Determine if style is categorical (has filters) or simple."""
    for rule in rules:
        filter_elem = _find_with_ns(rule, "ogc:Filter")
        if filter_elem is not None:
            return "categorical"
    return "simple"


def _determine_geometry_type(rules: list[ET.Element]) -> str:
    """Determine geometry type from symbolizer types."""
    for rule in rules:
        if _find_symbolizer(rule, "Polygon") is not None:
            return "polygon"
        if _find_symbolizer(rule, "Point") is not None:
            return "point"
        if _find_symbolizer(rule, "Line") is not None:
            return "line"
    return "polygon"  # Default


def _extract_style_name(root: ET.Element) -> str:
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


def _build_categorical_layers(
    rules: list[ET.Element],
    geom_type: str,
    source_layer: str,
    warnings: list[str],
) -> list[dict[str, Any]]:
    """Build layers for categorical (filtered) SLD rules."""
    field: str | None = None
    cases: list[tuple[Any, str]] = []
    opacity: float = 1.0

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

        if geom_type == "polygon":
            symbolizer = _find_symbolizer(rule, "Polygon")
            if symbolizer is not None:
                props = parse_polygon_symbolizer(symbolizer)
                cases.append((value, props["fill_color"]))
                opacity = props["fill_opacity"]
        elif geom_type == "point":
            symbolizer = _find_symbolizer(rule, "Point")
            if symbolizer is not None:
                props = parse_point_symbolizer(symbolizer)
                cases.append((value, props["fill_color"]))
                if props.get("warning"):
                    warnings.append(props["warning"])
        elif geom_type == "line":
            symbolizer = _find_symbolizer(rule, "Line")
            if symbolizer is not None:
                props = parse_line_symbolizer(symbolizer)
                cases.append((value, props["line_color"]))

    if not field or not cases:
        return []

    color_expr = make_match_expression(field, cases, default="#cccccc")

    if geom_type == "polygon":
        return [make_fill_layer("categorical-fill", source_layer, color_expr, opacity)]
    if geom_type == "point":
        return [make_circle_layer("categorical-circle", source_layer, color_expr)]
    if geom_type == "line":
        return [make_line_layer("categorical-line", source_layer, color_expr)]
    return []


def _build_simple_layers(
    rules: list[ET.Element],
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
