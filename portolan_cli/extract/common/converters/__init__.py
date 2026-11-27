"""Style format converters for extraction."""

from __future__ import annotations

from portolan_cli.extract.common.converters.base import (
    esri_color_to_hex,
    esri_color_to_opacity,
    make_circle_layer,
    make_fill_layer,
    make_line_layer,
    make_mapbox_style,
    make_match_expression,
    make_step_expression,
    make_symbol_layer,
)
from portolan_cli.extract.common.converters.esri import (
    ESRIConverterError,
    convert_esri_renderer,
)
from portolan_cli.extract.common.converters.sld import (
    SLDConverterError,
    convert_sld,
)

__all__ = [
    "ESRIConverterError",
    "SLDConverterError",
    # ESRI converter
    "convert_esri_renderer",
    # SLD converter
    "convert_sld",
    # Color utilities
    "esri_color_to_hex",
    "esri_color_to_opacity",
    "make_circle_layer",
    "make_fill_layer",
    "make_line_layer",
    # Base builders
    "make_mapbox_style",
    "make_match_expression",
    "make_step_expression",
    "make_symbol_layer",
]
