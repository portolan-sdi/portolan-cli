"""Style generation for vector and raster assets (Issue #13).

Generates Mapbox GL style specs for PMTiles and render extension properties for COGs.

Public API:
- VectorStyleConfig: Configuration for vector styling
- RasterStyleConfig: Configuration for raster styling
- StyleInfo: Discovered style file metadata
- build_full_style: Generate complete Mapbox GL style with sources
- write_style_file: Write style dict to JSON file
- write_default_style: Convenience function to write default.json
- build_raster_style: Generate render extension properties for COG
- get_vector_style_config: Load vector style config from catalog
- get_raster_style_config: Load raster style config from catalog
- discover_styles: Discover style JSON files in styles/ directory
- build_styles_manifest: Build portolan:styles manifest array
- register_style_assets: Register styles as STAC assets in collection.json
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from portolan_cli.config import load_config
from portolan_cli.utils import get_dict, get_list

logger = logging.getLogger(__name__)


# =============================================================================
# Config Parsing Helpers
# =============================================================================


def _parse_config_str(value: Any, key: str, default: str) -> str:
    """Parse config value as string, warn and return default if invalid."""
    if value is None:
        return default
    if isinstance(value, str):
        return value
    logger.warning("%s must be str, got %s; using default", key, type(value).__name__)
    return default


def _parse_config_int(value: Any, key: str, default: int) -> int:
    """Parse config value as int, warn and return default if invalid."""
    if value is None:
        return default
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    logger.warning("%s must be int, got %s; using default", key, type(value).__name__)
    return default


def _parse_config_float(value: Any, key: str, default: float) -> float:
    """Parse config value as float, warn and return default if invalid."""
    if value is None:
        return default
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    logger.warning("%s must be number, got %s; using default", key, type(value).__name__)
    return default


# =============================================================================
# Configuration Dataclasses
# =============================================================================


@dataclass(frozen=True)
class StyleInfo:
    """Metadata for a discovered style file.

    Attributes:
        key: STAC asset key (e.g., "styles/default").
        href: Relative href for STAC asset (e.g., "./styles/default.json").
        title: Human-readable title from style's "name" field or filename.
        description: Description from style file (empty string if absent).
        path: Absolute path to the style file on disk.
    """

    key: str
    href: str
    title: str
    description: str
    path: Path


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


def build_full_style(
    name: str,
    geometry_type: str,
    source_layer: str,
    pmtiles_relative_path: str,
    config: VectorStyleConfig,
) -> dict[str, Any]:
    """Build complete Mapbox GL v8 style with sources and layers.

    Generates a full Mapbox GL style spec (version 8) including sources
    section with PMTiles URL and a single layer appropriate for the
    geometry type.

    Args:
        name: Style name (e.g., "Default").
        geometry_type: OGC geometry type (Point, LineString, Polygon, etc.).
        source_layer: Name of the source layer in PMTiles.
        pmtiles_relative_path: Relative path to PMTiles file (e.g., "../data.pmtiles").
        config: Style configuration.

    Returns:
        Complete Mapbox GL style spec dict with version, name, sources, and layers.
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
        "source": "data",
        "source-layer": source_layer,
        "paint": paint,
    }

    return {
        "version": 8,
        "name": name,
        "sources": {
            "data": {
                "type": "vector",
                "url": pmtiles_relative_path,
            }
        },
        "layers": [layer],
    }


def write_style_file(
    style_dir: Path,
    name: str,
    style_dict: dict[str, Any],
) -> Path:
    """Write a style dict to a JSON file.

    Creates the directory if needed, writes {style_dir}/{name}.json with
    indented JSON formatting.

    Args:
        style_dir: Directory to write the style file into.
        name: Style filename (without .json extension). Must not contain
            path separators or parent directory references.
        style_dict: Style dict to serialize.

    Returns:
        Path to the written file.

    Raises:
        ValueError: If name contains path traversal characters.
    """
    if "/" in name or "\\" in name or ".." in name:
        msg = f"Style name must not contain path separators or '..': {name!r}"
        raise ValueError(msg)
    style_dir.mkdir(parents=True, exist_ok=True)
    style_path = style_dir / f"{name}.json"
    style_path.write_text(json.dumps(style_dict, indent=2))
    return style_path


def write_default_style(
    collection_path: Path,
    geometry_type: str,
    source_layer: str,
    pmtiles_relative_path: str,
    config: VectorStyleConfig | None = None,
) -> Path | None:
    """Write default style to {collection_path}/styles/default.json.

    Convenience function that creates a default.json style file. Does NOT
    overwrite if file already exists (returns None).

    Args:
        collection_path: Path to the collection directory.
        geometry_type: OGC geometry type (Point, LineString, Polygon, etc.).
        source_layer: Name of the source layer in PMTiles.
        pmtiles_relative_path: PMTiles path relative to collection (e.g., "data.pmtiles"
            or "sub/data.pmtiles"). Will be prefixed with "../" for styles/ directory.
        config: Optional style configuration (uses defaults if None).

    Returns:
        Path to written file, or None if default.json already exists.
    """
    styles_dir = collection_path / "styles"
    default_path = styles_dir / "default.json"

    # Don't overwrite existing file
    if default_path.exists():
        return None

    if config is None:
        config = VectorStyleConfig()

    style_dict = build_full_style(
        name="Default",
        geometry_type=geometry_type,
        source_layer=source_layer,
        pmtiles_relative_path=f"../{pmtiles_relative_path}",
        config=config,
    )

    return write_style_file(styles_dir, "default", style_dict)


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


_COG_MEDIA_TYPES = (
    "image/tiff; application=geotiff; profile=cloud-optimized",
    "image/tiff",
)


def enrich_cog_asset_with_style(
    asset: Any,
    catalog_path: Path | None = None,
) -> None:
    """Add render extension properties to a COG asset in-place.

    Modifies the asset's extra_fields to include render:colormap_name and
    optionally render:rescale based on catalog style config.

    Args:
        asset: A pystac.Asset object for a COG file.
        catalog_path: Optional catalog path for loading style config.
    """
    if catalog_path:
        config = get_raster_style_config(catalog_path)
    else:
        config = RasterStyleConfig()

    style_props = build_raster_style(config)

    # pystac.Asset stores extra properties in extra_fields dict
    if not hasattr(asset, "extra_fields") or asset.extra_fields is None:
        asset.extra_fields = {}

    asset.extra_fields.update(style_props)


def enrich_cog_assets(
    assets: dict[str, Any],
    catalog_path: Path | None = None,
) -> None:
    """Enrich all COG assets in a dict with render extension properties.

    Args:
        assets: Dict of asset_key -> pystac.Asset.
        catalog_path: Catalog root for loading style config.
    """
    for asset in assets.values():
        if getattr(asset, "media_type", None) in _COG_MEDIA_TYPES:
            enrich_cog_asset_with_style(asset, catalog_path)


# =============================================================================
# Style Discovery
# =============================================================================


def discover_styles(collection_path: Path) -> list[StyleInfo]:
    """Discover style JSON files in {collection_path}/styles/ directory.

    Args:
        collection_path: Path to the collection directory.

    Returns:
        List of StyleInfo objects. Empty list if no styles/ directory exists.
    """
    styles_dir = collection_path / "styles"

    if not styles_dir.exists():
        return []

    styles: list[StyleInfo] = []

    for path in sorted(styles_dir.glob("*.json")):
        try:
            style_data = json.loads(path.read_text())
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Skipping style file %s: %s", path, e)
            continue

        # Skip if parsed result is not a dict
        if not isinstance(style_data, dict):
            continue

        name = path.stem
        title = style_data.get("name", name)
        description = style_data.get("description", "")

        styles.append(
            StyleInfo(
                key=f"styles/{name}",
                href=f"./styles/{path.name}",
                title=title,
                description=description,
                path=path,
            )
        )

    return styles


def build_styles_manifest(styles: list[StyleInfo]) -> list[str]:
    """Build the portolan:styles manifest array.

    Returns ordered list of asset keys with styles/default first if present,
    then remaining styles sorted alphabetically.

    Args:
        styles: List of StyleInfo objects (from discover_styles).

    Returns:
        List of style asset keys.
    """
    keys = [s.key for s in styles]

    if "styles/default" in keys:
        # Put default first, sort the rest
        remaining = [k for k in keys if k != "styles/default"]
        return ["styles/default"] + sorted(remaining)

    # No default, just sort all
    return sorted(keys)


def register_style_assets(
    collection_path: Path,
    styles: list[StyleInfo],
) -> None:
    """Register discovered styles as STAC assets and set portolan:styles manifest.

    Updates collection.json to add/update style assets and remove stale ones.

    Args:
        collection_path: Path to the collection directory.
        styles: List of StyleInfo objects from discover_styles().
    """
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        return

    data = json.loads(collection_json_path.read_text())
    assets = data.get("assets", {})

    # Remove stale style assets (assets with "styles/" prefix that no longer have files)
    current_keys = {s.key for s in styles}
    stale_keys = [k for k in assets if k.startswith("styles/") and k not in current_keys]
    for key in stale_keys:
        del assets[key]

    # Add/update style assets
    for style_info in styles:
        asset_dict: dict[str, Any] = {
            "href": style_info.href,
            "type": "application/json",
            "title": style_info.title,
            "roles": ["style"],
        }
        if style_info.description:
            asset_dict["description"] = style_info.description
        assets[style_info.key] = asset_dict

    data["assets"] = assets

    # Set or remove portolan:styles manifest
    if styles:
        data["portolan:styles"] = build_styles_manifest(styles)
    else:
        data.pop("portolan:styles", None)

    collection_json_path.write_text(json.dumps(data, indent=2))


# =============================================================================
# Config Loading
# =============================================================================


def get_vector_style_config(catalog_path: Path) -> VectorStyleConfig:
    """Load vector style config from catalog's config.yaml.

    Reads the 'styles.vector' section and returns a VectorStyleConfig instance.

    Args:
        catalog_path: Root path of the catalog.

    Returns:
        VectorStyleConfig instance. Returns defaults if no config exists.
    """
    config = load_config(catalog_path)
    styles = get_dict(config, "styles")
    if not styles:
        return VectorStyleConfig()
    vector = get_dict(styles, "vector")
    if not vector:
        return VectorStyleConfig()

    point = get_dict(vector, "point")
    line = get_dict(vector, "line")
    polygon = get_dict(vector, "polygon")

    return VectorStyleConfig(
        point_color=_parse_config_str(
            point.get("circle-color"), "styles.vector.point.circle-color", "#3388ff"
        ),
        point_radius=_parse_config_int(
            point.get("circle-radius"), "styles.vector.point.circle-radius", 4
        ),
        point_opacity=_parse_config_float(
            point.get("circle-opacity"), "styles.vector.point.circle-opacity", 0.8
        ),
        line_color=_parse_config_str(
            line.get("line-color"), "styles.vector.line.line-color", "#3388ff"
        ),
        line_width=_parse_config_int(line.get("line-width"), "styles.vector.line.line-width", 2),
        line_opacity=_parse_config_float(
            line.get("line-opacity"), "styles.vector.line.line-opacity", 0.8
        ),
        polygon_fill_color=_parse_config_str(
            polygon.get("fill-color"), "styles.vector.polygon.fill-color", "#3388ff"
        ),
        polygon_fill_opacity=_parse_config_float(
            polygon.get("fill-opacity"), "styles.vector.polygon.fill-opacity", 0.6
        ),
        polygon_outline_color=_parse_config_str(
            polygon.get("fill-outline-color"), "styles.vector.polygon.fill-outline-color", "#2266cc"
        ),
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

    styles = get_dict(config, "styles")
    if not styles:
        return RasterStyleConfig()

    raster = get_dict(styles, "raster")
    if not raster:
        return RasterStyleConfig()

    colormap = raster.get("colormap")
    if colormap is not None and not isinstance(colormap, str):
        logger.warning(
            "styles.raster.colormap must be str, got %s; using default", type(colormap).__name__
        )
        colormap = "viridis"
    elif colormap is None:
        colormap = "viridis"

    rescale = get_list(raster, "rescale")
    rescale_min: float | None = None
    rescale_max: float | None = None

    if rescale:
        if len(rescale) < 2:
            logger.warning(
                "styles.raster.rescale must be [min, max], got %d values; ignoring", len(rescale)
            )
        else:
            if isinstance(rescale[0], (int, float)):
                rescale_min = float(rescale[0])
            else:
                logger.warning(
                    "styles.raster.rescale[0] must be number, got %s; ignoring",
                    type(rescale[0]).__name__,
                )
            if isinstance(rescale[1], (int, float)):
                rescale_max = float(rescale[1])
            else:
                logger.warning(
                    "styles.raster.rescale[1] must be number, got %s; ignoring",
                    type(rescale[1]).__name__,
                )

    return RasterStyleConfig(
        colormap=colormap,
        rescale_min=rescale_min,
        rescale_max=rescale_max,
    )
