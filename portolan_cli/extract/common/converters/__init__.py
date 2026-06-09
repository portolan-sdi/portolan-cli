"""Style format converters for extraction.

This package provides converters from source style formats (SLD, ESRI REST)
to Mapbox GL JSON. The architecture is designed for extensibility:

- base.py: Shared Mapbox GL building blocks (DRY)
- sld.py: OGC SLD XML → Mapbox GL
- esri.py: ESRI REST renderer JSON → Mapbox GL

To add a new source format (e.g., QGIS QML):
1. Create converters/qml.py
2. Use base.py helpers for Mapbox GL generation
3. Only write parsing logic for the new format
"""

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
    # Base builders
    "make_mapbox_style",
    "make_fill_layer",
    "make_circle_layer",
    "make_line_layer",
    "make_symbol_layer",
    "make_match_expression",
    "make_step_expression",
    # Color utilities
    "esri_color_to_hex",
    "esri_color_to_opacity",
    # ESRI converter
    "convert_esri_renderer",
    "ESRIConverterError",
    # SLD converter
    "convert_sld",
    "SLDConverterError",
]
