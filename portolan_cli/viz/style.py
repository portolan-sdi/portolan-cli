"""Style and legend generation for vector and raster assets."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from portolan_cli.report import emit
from portolan_cli.stac_parquet import stamp_file_fields, sync_file_extension
from portolan_cli.utils import get_dict, get_list
from portolan_core.config import ConfigSource, load_catalog_config
from portolan_core.constants import (
    LEGACY_LEGEND_MANIFEST_FIELD,
    LEGACY_STYLE_MANIFEST_FIELD,
    ROLE_DEFAULT,
    ROLE_STYLE,
)
from portolan_core.json_io import write_json_atomic

if TYPE_CHECKING:
    from pathlib import Path

_RESCALE_LEN = 2

logger = logging.getLogger(__name__)


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


@dataclass(frozen=True)
class StyleInfo:
    """Metadata for a discovered style file."""

    key: str
    href: str
    title: str
    description: str
    path: Path


@dataclass(frozen=True)
class LegendInfo:
    """Metadata for a discovered legend file."""

    key: str
    href: str
    title: str
    media_type: str
    path: Path


@dataclass(frozen=True)
class VectorStyleConfig:
    """Configuration for vector styling."""

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
    """Configuration for raster styling (render extension)."""

    colormap: str = "viridis"
    rescale_min: float | None = None
    rescale_max: float | None = None


def pmtiles_source_url(pmtiles_path: str) -> str:
    """Return a MapLibre-loadable source URL for a PMTiles path."""
    if "://" in pmtiles_path:
        return pmtiles_path
    return f"pmtiles://{pmtiles_path}"


def build_full_style(
    name: str,
    geometry_type: str,
    source_layer: str,
    pmtiles_relative_path: str,
    config: VectorStyleConfig,
    min_zoom: int | None = None,
    max_zoom: int | None = None,
) -> dict[str, Any]:
    """Build complete Mapbox GL v8 style with sources and layers."""
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

    source: dict[str, Any] = {
        "type": "vector",
        "url": pmtiles_source_url(pmtiles_relative_path),
    }
    if min_zoom is not None:
        source["minzoom"] = min_zoom
    if max_zoom is not None:
        source["maxzoom"] = max_zoom

    return {
        "version": 8,
        "name": name,
        "sources": {"data": source},
        "layers": [layer],
    }


def write_style_file(
    style_dir: Path,
    name: str,
    style_dict: dict[str, Any],
) -> Path:
    """Write a style dict to a JSON file."""
    if "/" in name or "\\" in name or ".." in name:
        msg = f"Style name must not contain path separators or '..': {name!r}"
        raise ValueError(msg)
    style_dir.mkdir(parents=True, exist_ok=True)
    style_path = style_dir / f"{name}.json"
    write_json_atomic(style_path, style_dict)
    return style_path


def complete_style_sources(
    collection_path: Path,
    pmtiles_relative_path: str,
    min_zoom: int | None = None,
    max_zoom: int | None = None,
) -> list[Path]:
    """Make every style under styles/ point at a loadable PMTiles source."""
    styles_dir = collection_path / "styles"
    if not styles_dir.is_dir():
        return []

    modified: list[Path] = []
    for style_path in sorted(styles_dir.glob("*.json")):
        try:
            style = json.loads(style_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        sources = style.get("sources")
        if not isinstance(sources, dict):
            continue
        changed = False
        for source in sources.values():
            if not isinstance(source, dict) or source.get("type") != "vector":
                continue
            url = source.get("url")
            if not url:
                source["url"] = pmtiles_source_url(f"../{pmtiles_relative_path}")
                changed = True
            elif "://" not in url:
                source["url"] = pmtiles_source_url(url)
                changed = True
            if min_zoom is not None and "minzoom" not in source:
                source["minzoom"] = min_zoom
                changed = True
            if max_zoom is not None and "maxzoom" not in source:
                source["maxzoom"] = max_zoom
                changed = True
        if changed:
            write_json_atomic(style_path, style)
            modified.append(style_path)
    return modified


def write_default_style(
    collection_path: Path,
    geometry_type: str,
    source_layer: str,
    pmtiles_relative_path: str,
    config: VectorStyleConfig | None = None,
    min_zoom: int | None = None,
    max_zoom: int | None = None,
) -> Path | None:
    """Write default style to {collection_path}/styles/default.json."""
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
        min_zoom=min_zoom,
        max_zoom=max_zoom,
    )

    return write_style_file(styles_dir, "default", style_dict)


def build_raster_style(config: RasterStyleConfig) -> dict[str, Any]:
    """Build render extension properties for COG styling."""
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


def enrich_cog_assets(
    assets: dict[str, Any],
    catalog_path: Path | None = None,
    *,
    state: ConfigSource | None = None,
) -> None:
    """Add render extension properties to each COG asset in a dict, in place."""
    cogs = [a for a in assets.values() if getattr(a, "media_type", None) in _COG_MEDIA_TYPES]
    if not cogs:
        return
    config = (
        get_raster_style_config(catalog_path, state=state) if catalog_path else RasterStyleConfig()
    )
    for asset in cogs:
        # pystac.Asset stores extra properties in extra_fields dict
        if not hasattr(asset, "extra_fields") or asset.extra_fields is None:
            asset.extra_fields = {}
        # A new dict per asset, so no two assets share the rescale list.
        asset.extra_fields.update(build_raster_style(config))


#: Role every style asset carries. A client discovers a collection's styles by
#: filtering assets on it, which is why the spec defines no manifest property.
#: The name comes from ``constants``, the one home for the role vocabulary.
STYLE_ROLE = ROLE_STYLE

#: Second role marking the style a client should draw first (core.md,
#: Visualization Styles).
DEFAULT_ROLE = ROLE_DEFAULT

#: Asset key :func:`write_default_style` writes. Nothing in STAC reads meaning
#: into a key, so this only breaks the tie when no style is marked yet.
GENERATED_DEFAULT_STYLE_KEY = "styles/default"

#: Media type every style asset declares. A style is a MapLibre GL style file
#: (formats.md), and rashid PTL-VIZ-005 checks the asset says so.
STYLE_MEDIA_TYPE = "application/vnd.mapbox.style+json"


def discover_styles(collection_path: Path) -> list[StyleInfo]:
    """Discover style JSON files in {collection_path}/styles/ directory."""
    styles_dir = collection_path / "styles"

    if not styles_dir.exists():
        return []

    styles: list[StyleInfo] = []

    for path in sorted(styles_dir.glob("*.json")):
        try:
            style_data = json.loads(path.read_text(encoding="utf-8"))
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


def _marks_default(asset: Any) -> bool:
    """Whether a raw asset already carries the ``default`` role."""
    if not isinstance(asset, dict):
        return False
    roles = asset.get("roles")
    return isinstance(roles, list) and DEFAULT_ROLE in roles


def _choose_default(
    styles: list[StyleInfo],
    existing_assets: dict[str, Any],
) -> tuple[str | None, bool]:
    """Pick the default style key and say whether Portolan picked it unaided."""
    keys = {style.key for style in styles}
    marked = [key for key in keys if _marks_default(existing_assets.get(key))]
    if len(marked) == 1:
        return marked[0], False
    if GENERATED_DEFAULT_STYLE_KEY in keys:
        return GENERATED_DEFAULT_STYLE_KEY, True
    if not keys:
        return None, False
    return min(keys), True


def select_default_style_key(
    styles: list[StyleInfo],
    existing_assets: dict[str, Any],
) -> str | None:
    """Choose the style asset key that carries the ``default`` role."""
    return _choose_default(styles, existing_assets)[0]


def _merge_style_asset(
    existing: Any,
    style_info: StyleInfo,
    roles: list[str],
) -> dict[str, Any]:
    """Update one style asset in place-ish, keeping whatever a human put there."""
    asset: dict[str, Any] = dict(existing) if isinstance(existing, dict) else {}

    asset["href"] = style_info.href
    asset["type"] = STYLE_MEDIA_TYPE
    asset["roles"] = roles

    # Seed from the style file only where the collection says nothing. The style's
    # own "name" is a fallback title, not an authority over one a publisher wrote.
    if not asset.get("title"):
        asset["title"] = style_info.title
    if not asset.get("description") and style_info.description:
        asset["description"] = style_info.description

    return asset


def _announce_default(default_key: str, style_count: int) -> None:
    """Tell the publisher which style Portolan made the default, and how to move it."""
    emit(
        "info",
        f"Default style: {default_key} (chosen from {style_count} styles). "
        f'To pick another, move the "{DEFAULT_ROLE}" role to that style asset '
        "in collection.json.",
    )


def register_style_assets(
    collection_path: Path,
    styles: list[StyleInfo],
) -> None:
    """Register discovered styles as STAC assets on the collection."""
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        return

    data = json.loads(collection_json_path.read_text(encoding="utf-8"))
    assets = data.get("assets", {})

    # Remove stale style assets
    current_keys = {s.key for s in styles}
    stale_keys = [k for k in assets if k.startswith("styles/") and k not in current_keys]
    for key in stale_keys:
        del assets[key]

    # Read the default off the assets as they stand, before the merge below
    # overwrites the roles a previous run wrote.
    default_key, chosen_by_portolan = _choose_default(styles, assets)

    for style_info in styles:
        roles = [STYLE_ROLE]
        if style_info.key == default_key:
            roles.append(DEFAULT_ROLE)
        asset = _merge_style_asset(assets.get(style_info.key), style_info, roles)
        stamp_file_fields(asset, collection_path)
        assets[style_info.key] = asset

    data["assets"] = assets
    data.pop(LEGACY_STYLE_MANIFEST_FIELD, None)
    sync_file_extension(data, assets, collection_json_path)

    write_json_atomic(collection_json_path, data)

    if default_key is not None and chosen_by_portolan and len(styles) > 1:
        _announce_default(default_key, len(styles))


def discover_legends(collection_path: Path) -> list[LegendInfo]:
    """Discover legend PNG files in {collection_path}/legends/ directory."""
    legends_dir = collection_path / "legends"

    if not legends_dir.exists():
        return []

    legends: list[LegendInfo] = []

    for path in sorted(legends_dir.glob("*.png")):
        name = path.stem
        title = f"{name} Legend"

        legends.append(
            LegendInfo(
                key=f"legends/{name}",
                href=f"./legends/{path.name}",
                title=title,
                media_type="image/png",
                path=path,
            )
        )

    return legends


def register_legend_assets(
    collection_path: Path,
    legends: list[LegendInfo],
) -> None:
    """Register discovered legends as STAC assets carrying the ``legend`` role."""
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        return

    data = json.loads(collection_json_path.read_text(encoding="utf-8"))
    assets = data.get("assets", {})

    # Remove stale legend assets
    current_keys = {lg.key for lg in legends}
    stale_keys = [k for k in assets if k.startswith("legends/") and k not in current_keys]
    for key in stale_keys:
        del assets[key]

    # Add/update legend assets
    for legend_info in legends:
        asset_dict: dict[str, Any] = {
            "href": legend_info.href,
            "type": legend_info.media_type,
            "title": legend_info.title,
            "roles": ["legend"],
        }
        assets[legend_info.key] = asset_dict

    data["assets"] = assets
    data.pop(LEGACY_LEGEND_MANIFEST_FIELD, None)

    write_json_atomic(collection_json_path, data)


def get_vector_style_config(
    catalog_path: Path, *, state: ConfigSource | None = None
) -> VectorStyleConfig:
    """Load vector style config from catalog's config.yaml."""
    config = load_catalog_config(catalog_path, state=state).data
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


def get_raster_style_config(
    catalog_path: Path, *, state: ConfigSource | None = None
) -> RasterStyleConfig:
    """Load raster style config from catalog's config.yaml."""
    config = load_catalog_config(catalog_path, state=state).data

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
        if len(rescale) < _RESCALE_LEN:
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
