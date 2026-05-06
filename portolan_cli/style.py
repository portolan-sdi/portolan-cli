"""Style generation for vector and raster assets (Issue #13).

Generates Mapbox GL style specs for PMTiles and render extension properties for COGs.

Public API:
- VectorStyleConfig: Configuration for vector styling
- RasterStyleConfig: Configuration for raster styling
- build_pmtiles_style: Generate Mapbox GL style for PMTiles
- build_raster_style: Generate render extension properties for COG
- get_vector_style_config: Load vector style config from catalog
- get_raster_style_config: Load raster style config from catalog
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from portolan_cli.config import load_config

logger = logging.getLogger(__name__)


# =============================================================================
# Configuration Dataclasses
# =============================================================================


@dataclass(frozen=True)
class VectorStyleConfig:
    """Configuration for vector styling.

    Defines default colors, sizes, and opacities for point, line, and polygon
    geometries. These values are used to generate Mapbox GL style specs for
    PMTiles assets.

    Attributes:
        point_color: Circle fill color for points (default #3388ff).
        point_radius: Circle radius in pixels (default 4).
        point_opacity: Circle opacity 0.0-1.0 (default 0.8).
        line_color: Line color for linestrings (default #3388ff).
        line_width: Line width in pixels (default 2).
        line_opacity: Line opacity 0.0-1.0 (default 0.8).
        polygon_fill_color: Fill color for polygons (default #3388ff).
        polygon_fill_opacity: Fill opacity 0.0-1.0 (default 0.6).
        polygon_outline_color: Outline color for polygons (default #2266cc).
    """

    point_color: str = "#3388ff"
    point_radius: int = 4
    point_opacity: float = 0.8
    line_color: str = "#3388ff"
    line_width: int = 2
    line_opacity: float = 0.8
    polygon_fill_color: str = "#3388ff"
    polygon_fill_opacity: float = 0.6
    polygon_outline_color: str = "#2266cc"


@dataclass(frozen=True)
class RasterStyleConfig:
    """Configuration for raster styling (render extension).

    Defines colormap and rescale settings for COG visualization.

    Attributes:
        colormap: Named colormap (default 'viridis').
        rescale_min: Minimum value for rescaling (None = auto).
        rescale_max: Maximum value for rescaling (None = auto).
    """

    colormap: str = "viridis"
    rescale_min: float | None = None
    rescale_max: float | None = None


# =============================================================================
# Style Building Functions
# =============================================================================


def build_pmtiles_style(
    geometry_type: str,
    source_layer: str,
    config: VectorStyleConfig,
) -> dict[str, Any]:
    """Build Mapbox GL style spec for PMTiles based on geometry type.

    Generates a minimal Mapbox GL style spec (version 8) with a single layer
    appropriate for the geometry type.

    Args:
        geometry_type: OGC geometry type (Point, LineString, Polygon, etc.).
        source_layer: Name of the source layer in PMTiles.
        config: Style configuration.

    Returns:
        Mapbox GL style spec dict with version and layers.
    """
    # Normalize geometry type to layer type
    geom_lower = geometry_type.lower()

    if "point" in geom_lower:
        layer_type = "circle"
        paint = {
            "circle-color": config.point_color,
            "circle-radius": config.point_radius,
            "circle-opacity": config.point_opacity,
        }
        suffix = "circle"
    elif "line" in geom_lower:
        layer_type = "line"
        paint = {
            "line-color": config.line_color,
            "line-width": config.line_width,
            "line-opacity": config.line_opacity,
        }
        suffix = "line"
    else:
        # Polygon, MultiPolygon, GeometryCollection, or unknown -> fill
        layer_type = "fill"
        paint = {
            "fill-color": config.polygon_fill_color,
            "fill-opacity": config.polygon_fill_opacity,
            "fill-outline-color": config.polygon_outline_color,
        }
        suffix = "fill"

    layer = {
        "id": f"{source_layer}-{suffix}",
        "type": layer_type,
        "source-layer": source_layer,
        "paint": paint,
    }

    return {
        "version": 8,
        "layers": [layer],
    }


def build_raster_style(config: RasterStyleConfig) -> dict[str, Any]:
    """Build render extension properties for COG styling.

    Generates STAC render extension properties for COG visualization.

    Args:
        config: Raster style configuration.

    Returns:
        Dict with render:* properties.
    """
    props: dict[str, Any] = {
        "render:colormap_name": config.colormap,
    }

    # Only include rescale if both min and max are set
    if config.rescale_min is not None and config.rescale_max is not None:
        props["render:rescale"] = [[config.rescale_min, config.rescale_max]]

    return props


# =============================================================================
# Config Loading
# =============================================================================


def _get_dict(data: dict[str, Any], key: str) -> dict[str, Any]:
    """Safely get a dict value, returning empty dict if not a dict."""
    value = data.get(key, {})
    return value if isinstance(value, dict) else {}


def _get_list(data: dict[str, Any], key: str) -> list[Any]:
    """Safely get a list value, returning empty list if not a list."""
    value = data.get(key, [])
    return value if isinstance(value, list) else []


def get_vector_style_config(catalog_path: Path) -> VectorStyleConfig:
    """Load vector style config from catalog's config.yaml.

    Reads the 'styles.vector' section and returns a VectorStyleConfig instance.

    Config format:
        styles:
          vector:
            point:
              circle-color: "#ff0000"
              circle-radius: 8
            line:
              line-color: "#00ff00"
              line-width: 3
            polygon:
              fill-color: "#0000ff"
              fill-opacity: 0.5

    Args:
        catalog_path: Root path of the catalog.

    Returns:
        VectorStyleConfig instance. Returns defaults if no config exists.
    """
    config = load_config(catalog_path)

    styles = _get_dict(config, "styles")
    if not styles:
        return VectorStyleConfig()

    vector = _get_dict(styles, "vector")
    if not vector:
        return VectorStyleConfig()

    # Parse point settings
    point = _get_dict(vector, "point")
    point_color = point.get("circle-color")
    if not isinstance(point_color, str):
        point_color = "#3388ff"

    point_radius = point.get("circle-radius")
    if not isinstance(point_radius, int):
        point_radius = 4

    point_opacity = point.get("circle-opacity")
    if not isinstance(point_opacity, (int, float)):
        point_opacity = 0.8

    # Parse line settings
    line = _get_dict(vector, "line")
    line_color = line.get("line-color")
    if not isinstance(line_color, str):
        line_color = "#3388ff"

    line_width = line.get("line-width")
    if not isinstance(line_width, int):
        line_width = 2

    line_opacity = line.get("line-opacity")
    if not isinstance(line_opacity, (int, float)):
        line_opacity = 0.8

    # Parse polygon settings
    polygon = _get_dict(vector, "polygon")
    polygon_fill_color = polygon.get("fill-color")
    if not isinstance(polygon_fill_color, str):
        polygon_fill_color = "#3388ff"

    polygon_fill_opacity = polygon.get("fill-opacity")
    if not isinstance(polygon_fill_opacity, (int, float)):
        polygon_fill_opacity = 0.6

    polygon_outline_color = polygon.get("fill-outline-color")
    if not isinstance(polygon_outline_color, str):
        polygon_outline_color = "#2266cc"

    return VectorStyleConfig(
        point_color=point_color,
        point_radius=point_radius,
        point_opacity=float(point_opacity),
        line_color=line_color,
        line_width=line_width,
        line_opacity=float(line_opacity),
        polygon_fill_color=polygon_fill_color,
        polygon_fill_opacity=float(polygon_fill_opacity),
        polygon_outline_color=polygon_outline_color,
    )


def get_raster_style_config(catalog_path: Path) -> RasterStyleConfig:
    """Load raster style config from catalog's config.yaml.

    Reads the 'styles.raster' section and returns a RasterStyleConfig instance.

    Config format:
        styles:
          raster:
            colormap: terrain
            rescale: [0, 1000]

    Args:
        catalog_path: Root path of the catalog.

    Returns:
        RasterStyleConfig instance. Returns defaults if no config exists.
    """
    config = load_config(catalog_path)

    styles = _get_dict(config, "styles")
    if not styles:
        return RasterStyleConfig()

    raster = _get_dict(styles, "raster")
    if not raster:
        return RasterStyleConfig()

    colormap = raster.get("colormap")
    if not isinstance(colormap, str):
        colormap = "viridis"

    rescale = _get_list(raster, "rescale")
    rescale_min: float | None = None
    rescale_max: float | None = None

    if len(rescale) >= 2:
        if isinstance(rescale[0], (int, float)):
            rescale_min = float(rescale[0])
        if isinstance(rescale[1], (int, float)):
            rescale_max = float(rescale[1])

    return RasterStyleConfig(
        colormap=colormap,
        rescale_min=rescale_min,
        rescale_max=rescale_max,
    )
