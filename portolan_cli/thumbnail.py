"""Vector thumbnail generation module (Issue #13).

Generates JPEG thumbnails from PMTiles and GeoParquet files with optional basemaps.
Mirrors the COG thumbnail pattern in convert.py.

Public API:
- ThumbnailConfig: Configuration dataclass for thumbnail generation
- generate_vector_thumbnail: Orchestrator (prefers PMTiles, falls back to GeoParquet)
- generate_thumbnail_from_pmtiles: Generate from PMTiles
- generate_thumbnail_from_geoparquet: Generate from GeoParquet
- add_basemap: Add contextily basemap to matplotlib axes
- get_thumbnail_config: Load config from catalog's config.yaml
"""

from __future__ import annotations

import gzip
import json
import logging
import math
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from portolan_cli.config import load_config
from portolan_cli.utils import get_dict

if TYPE_CHECKING:
    from matplotlib.axes import Axes

logger = logging.getLogger(__name__)

# Thread-safe lazy import for optional contextily dependency
_ctx_lock = threading.Lock()
_ctx_module: Any = None
_ctx_loaded = False


def _ensure_contextily() -> Any:
    """Lazy-load contextily, returning module or None if unavailable.

    Thread-safe: uses a lock to prevent race conditions on first import.
    """
    global _ctx_module, _ctx_loaded
    if _ctx_loaded:
        return _ctx_module

    with _ctx_lock:
        if _ctx_loaded:
            return _ctx_module
        try:
            import contextily as ctx  # type: ignore

            _ctx_module = ctx
        except ImportError:
            logger.debug("contextily not available, basemaps disabled")
            _ctx_module = None
        _ctx_loaded = True
        return _ctx_module


# =============================================================================
# Config Parsing Helpers
# =============================================================================


def _parse_bool(value: Any, key: str, default: bool) -> bool:
    """Parse config value as bool, warn and return default if invalid."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    logger.warning("%s must be bool, got %s; using default", key, type(value).__name__)
    return default


def _parse_positive_int(value: Any, key: str, default: int) -> int:
    """Parse config value as positive int, warn and return default if invalid."""
    if value is None:
        return default
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    logger.warning("%s must be positive int, got %r; using default %d", key, value, default)
    return default


def _parse_bounded_int(value: Any, key: str, default: int, lo: int, hi: int) -> int:
    """Parse config value as int in [lo, hi], warn and return default if invalid."""
    if value is None:
        return default
    if isinstance(value, int) and not isinstance(value, bool) and lo <= value <= hi:
        return value
    logger.warning("%s must be int %d-%d, got %r; using default %d", key, lo, hi, value, default)
    return default


def _parse_str(value: Any, key: str, default: str) -> str:
    """Parse config value as string, warn and return default if invalid."""
    if value is None:
        return default
    if isinstance(value, str):
        return value
    logger.warning("%s must be str, got %s; using default", key, type(value).__name__)
    return default


def _parse_bounded_float(value: Any, key: str, default: float, lo: float, hi: float) -> float:
    """Parse config value as float in [lo, hi], warn and return default if invalid."""
    if value is None:
        return default
    if isinstance(value, (int, float)) and not isinstance(value, bool) and lo <= value <= hi:
        return float(value)
    logger.warning("%s must be float %g-%g, got %r; using default %g", key, lo, hi, value, default)
    return default


def _parse_int(value: Any, key: str, default: int) -> int:
    """Parse config value as int, warn and return default if invalid."""
    if value is None:
        return default
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    logger.warning("%s must be int, got %s; using default %d", key, type(value).__name__, default)
    return default


# =============================================================================
# Configuration
# =============================================================================


@dataclass(frozen=True)
class ThumbnailConfig:
    """Configuration for thumbnail generation.

    Attributes:
        enabled: Whether to generate thumbnails (default True).
        max_size: Maximum pixel dimension for longest edge (default 512).
        quality: JPEG quality 1-100 (default 75).
        basemap_provider: Contextily basemap provider name (default 'CartoDB.Positron').
            Set to 'none' to disable basemap.
        basemap_opacity: Basemap opacity 0.0-1.0 (default 1.0).
        basemap_zoom_adjust: Zoom level adjustment for basemap (default 0).
    """

    enabled: bool = True
    max_size: int = 512
    quality: int = 75
    basemap_provider: str = "CartoDB.Positron"
    basemap_opacity: float = 1.0
    basemap_zoom_adjust: int = 0


def get_thumbnail_config(catalog_path: Path) -> ThumbnailConfig:
    """Load thumbnail config from catalog's config.yaml.

    Reads the 'thumbnails' section and returns a ThumbnailConfig instance.

    Args:
        catalog_path: Root path of the catalog.

    Returns:
        ThumbnailConfig instance. Returns defaults if no config exists.
    """
    config = load_config(catalog_path)
    thumbnails = get_dict(config, "thumbnails")
    if not thumbnails:
        return ThumbnailConfig()

    basemap = get_dict(thumbnails, "basemap")

    return ThumbnailConfig(
        enabled=_parse_bool(thumbnails.get("enabled"), "thumbnails.enabled", True),
        max_size=_parse_positive_int(thumbnails.get("max_size"), "thumbnails.max_size", 512),
        quality=_parse_bounded_int(thumbnails.get("quality"), "thumbnails.quality", 75, 1, 100),
        basemap_provider=_parse_str(
            basemap.get("provider"), "thumbnails.basemap.provider", "CartoDB.Positron"
        ),
        basemap_opacity=_parse_bounded_float(
            basemap.get("opacity"), "thumbnails.basemap.opacity", 1.0, 0.0, 1.0
        ),
        basemap_zoom_adjust=_parse_int(
            basemap.get("zoom_adjust"), "thumbnails.basemap.zoom_adjust", 0
        ),
    )


# =============================================================================
# Tile Coordinate Transformation
# =============================================================================

# MVT default tile extent (coordinates range from 0 to EXTENT)
MVT_EXTENT = 4096


def _tile_to_lon(x: int, z: int) -> float:
    """Convert tile X coordinate to longitude."""
    n = float(2**z)
    return x / n * 360.0 - 180.0


def _tile_to_lat(y: int, z: int) -> float:
    """Convert tile Y coordinate to latitude (Web Mercator)."""
    n = 2**z
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
    return math.degrees(lat_rad)


def _tile_bounds(z: int, x: int, y: int) -> tuple[float, float, float, float]:
    """Get geographic bounds for a tile.

    Returns:
        (lon_min, lat_min, lon_max, lat_max)
    """
    lon_min = _tile_to_lon(x, z)
    lon_max = _tile_to_lon(x + 1, z)
    lat_max = _tile_to_lat(y, z)  # Y=0 is north
    lat_min = _tile_to_lat(y + 1, z)
    return (lon_min, lat_min, lon_max, lat_max)


def _transform_coord(
    mvt_x: float,
    mvt_y: float,
    tile_bounds: tuple[float, float, float, float],
    extent: int = MVT_EXTENT,
) -> tuple[float, float]:
    """Transform MVT tile-space coordinate to geographic.

    Args:
        mvt_x: X coordinate in tile extent space (0 to extent).
        mvt_y: Y coordinate in tile extent space (0 to extent).
        tile_bounds: (lon_min, lat_min, lon_max, lat_max).
        extent: MVT tile extent (default 4096).

    Returns:
        (longitude, latitude)
    """
    lon_min, lat_min, lon_max, lat_max = tile_bounds
    lon = lon_min + (mvt_x / extent) * (lon_max - lon_min)
    # Y is inverted in MVT (0 at top)
    lat = lat_max - (mvt_y / extent) * (lat_max - lat_min)
    return (lon, lat)


def _transform_coords(
    coords: Any,
    tile_bounds: tuple[float, float, float, float],
    depth: int = 0,
) -> Any:
    """Recursively transform coordinate arrays from tile-space to geographic.

    Handles Point, LineString, Polygon, and Multi* geometry coordinate structures.
    """
    # Depth limit for coordinate nesting:
    # - Point: 0 (coords = [x, y])
    # - LineString: 1 (coords = [[x,y], ...])
    # - Polygon: 2 (coords = [[[x,y], ...]])
    # - MultiPolygon: 3
    # - GeometryCollection containing MultiPolygon: 4
    # - Nested GeometryCollection edge cases: 5-6
    # Beyond 6 is almost certainly malformed data.
    if depth > 6:
        return coords

    if not coords:
        return coords

    # Check if this is a coordinate pair [x, y]
    if (
        isinstance(coords, (list, tuple))
        and len(coords) >= 2
        and isinstance(coords[0], (int, float))
        and isinstance(coords[1], (int, float))
    ):
        lon, lat = _transform_coord(coords[0], coords[1], tile_bounds)
        return [lon, lat]

    # Otherwise recurse into nested arrays
    if isinstance(coords, list):
        return [_transform_coords(c, tile_bounds, depth + 1) for c in coords]

    return coords


# =============================================================================
# PMTiles Reading (Internal)
# =============================================================================

# Limit geometries to prevent OOM on dense tiles (thumbnails don't need all features)
_MAX_GEOMETRIES = 10000
_MAX_TILES_PER_ZOOM = 256


def _extract_coord_bounds(
    coords: Any,
    lons: list[float],
    lats: list[float],
    depth: int = 0,
) -> None:
    """Recursively extract lon/lat bounds from coordinate arrays.

    Handles all GeoJSON geometry coordinate structures (Point, LineString,
    Polygon, Multi* types). Appends found coordinates to the lons/lats lists.

    Args:
        coords: Coordinate array from GeoJSON geometry.
        lons: List to append longitude values to.
        lats: List to append latitude values to.
        depth: Recursion depth (safety limit).
    """
    if depth > 6 or not coords:
        return

    # Check if this is a coordinate pair [lon, lat]
    if (
        isinstance(coords, (list, tuple))
        and len(coords) >= 2
        and isinstance(coords[0], (int, float))
        and isinstance(coords[1], (int, float))
    ):
        lons.append(float(coords[0]))
        lats.append(float(coords[1]))
        return

    # Otherwise recurse into nested arrays
    if isinstance(coords, list):
        for c in coords:
            _extract_coord_bounds(c, lons, lats, depth + 1)


def _process_tile_data(
    tile_data: bytes,
    z: int,
    x: int,
    y: int,
    geometries: list[dict[str, Any]],
    all_lons: list[float],
    all_lats: list[float],
    mvt_decoder: Any,
) -> bool:
    """Process a single tile's data and extract geometries.

    Returns True if geometry limit reached, False otherwise.
    """
    # Decompress if gzipped
    if tile_data[:2] == b"\x1f\x8b":
        tile_data = gzip.decompress(tile_data)

    decoded = mvt_decoder.decode(tile_data)
    tile_bounds = _tile_bounds(z, x, y)

    for layer in decoded.values():
        for feature in layer.get("features", []):
            geom = feature.get("geometry", {})
            if geom.get("type") and geom.get("coordinates"):
                transformed = _transform_coords(geom["coordinates"], tile_bounds)
                props = feature.get("properties", {})
                geometries.append(
                    {
                        "type": geom["type"],
                        "coordinates": transformed,
                        "properties": props,
                    }
                )
                # Extract actual geometry bounds (not tile bounds) for accurate basemap extent
                _extract_coord_bounds(transformed, all_lons, all_lats)

                if len(geometries) >= _MAX_GEOMETRIES:
                    return True
    return False


def _read_pmtiles_geometries(
    pmtiles_path: Path,
) -> tuple[list[dict[str, Any]], tuple[float, float, float, float] | None]:
    """Read geometries from low-zoom PMTiles tiles with geographic coordinates.

    Transforms MVT tile-space coordinates to geographic (lon/lat) coordinates.

    Args:
        pmtiles_path: Path to PMTiles file.

    Returns:
        Tuple of (geometries, bounds) where:
        - geometries: List of geometry dicts with 'type' and 'coordinates' keys.
        - bounds: (minx, miny, maxx, maxy) bounding box, or None if no geometries.
    """
    try:
        from pmtiles.reader import MmapSource, Reader
    except ImportError:
        logger.debug("pmtiles library not available")
        return [], None

    try:
        import mapbox_vector_tile  # type: ignore
    except ImportError:
        logger.debug("mapbox-vector-tile library not available")
        return [], None

    geometries: list[dict[str, Any]] = []
    all_lons: list[float] = []
    all_lats: list[float] = []

    with open(pmtiles_path, "rb") as f:
        reader: Any = Reader(MmapSource(f))  # type: ignore[no-untyped-call]
        header: dict[str, Any] = reader.header()
        min_zoom = header.get("min_zoom", 0) or 0

        # Try min_zoom through min_zoom+2, collecting geometries
        for z in range(min_zoom, min_zoom + 3):
            limit_reached = _collect_geometries_at_zoom(
                reader, z, geometries, all_lons, all_lats, mapbox_vector_tile
            )
            if geometries or limit_reached:
                break

    bounds: tuple[float, float, float, float] | None = None
    if all_lons and all_lats:
        bounds = (min(all_lons), min(all_lats), max(all_lons), max(all_lats))

    return geometries, bounds


def _collect_geometries_at_zoom(
    reader: Any,
    z: int,
    geometries: list[dict[str, Any]],
    all_lons: list[float],
    all_lats: list[float],
    mvt_decoder: Any,
) -> bool:
    """Collect geometries from all tiles at a zoom level.

    Returns True if limit reached, False otherwise.
    """
    max_tile = 2**z
    tiles_checked = 0

    for x in range(max_tile):
        for y in range(max_tile):
            tile_data: bytes | None = reader.get(z, x, y)
            tiles_checked += 1

            if tile_data:
                limit_reached = _process_tile_data(
                    tile_data, z, x, y, geometries, all_lons, all_lats, mvt_decoder
                )
                if limit_reached:
                    return True

            if tiles_checked > _MAX_TILES_PER_ZOOM or len(geometries) >= _MAX_GEOMETRIES:
                return len(geometries) >= _MAX_GEOMETRIES
        if tiles_checked > _MAX_TILES_PER_ZOOM or len(geometries) >= _MAX_GEOMETRIES:
            return len(geometries) >= _MAX_GEOMETRIES

    return False


def _add_polygon_patches(coords: list[Any], patches: list[Any], mpl_polygon_cls: type) -> None:
    """Add polygon patches from coordinates."""
    if coords and coords[0]:
        patches.append(mpl_polygon_cls(coords[0], closed=True))


def _add_multipolygon_patches(coords: list[Any], patches: list[Any], mpl_polygon_cls: type) -> None:
    """Add multipolygon patches from coordinates."""
    for polygon in coords:
        if polygon and polygon[0]:
            patches.append(mpl_polygon_cls(polygon[0], closed=True))


def _plot_points(ax: Any, coords: list[Any], geom_type: str, color: str = "#3388ff") -> None:
    """Plot point or multipoint geometries."""
    if geom_type == "Point":
        ax.plot(coords[0], coords[1], "o", markersize=2, color=color)
    else:
        for pt in coords:
            ax.plot(pt[0], pt[1], "o", markersize=2, color=color)


def _plot_lines(ax: Any, coords: list[Any], geom_type: str, color: str = "#3388ff") -> None:
    """Plot linestring or multilinestring geometries."""
    if geom_type == "LineString":
        xs, ys = [c[0] for c in coords], [c[1] for c in coords]
        ax.plot(xs, ys, linewidth=1, color=color)
    else:
        for line in coords:
            xs, ys = [c[0] for c in line], [c[1] for c in line]
            ax.plot(xs, ys, linewidth=1, color=color)


def _render_geometries(
    geometries: list[dict[str, Any]],
    output_path: Path,
    config: ThumbnailConfig,
    bounds: tuple[float, float, float, float] | None = None,
    style_path: Path | None = None,
) -> bool:
    """Render geometries to JPEG thumbnail with optional basemap.

    Args:
        geometries: List of geometry dicts with 'type', 'coordinates', and 'properties'.
        output_path: Where to write the JPEG.
        config: Thumbnail configuration.
        bounds: Geographic bounds (minx, miny, maxx, maxy) for basemap.
        style_path: Optional path to Mapbox GL style for categorical coloring.

    Returns:
        True if successful, False otherwise.
    """
    try:
        import matplotlib.pyplot as plt
        from matplotlib.collections import PatchCollection
        from matplotlib.patches import Polygon as MplPolygon
    except ImportError:
        logger.debug("matplotlib not available")
        return False

    # Load style if provided
    style = None
    if style_path:
        from portolan_cli.thumbnail_style import load_thumbnail_style

        style = load_thumbnail_style(style_path)

    # Default colors
    default_fill = "#3388ff"
    default_edge = "#2266cc"
    default_opacity = 0.6

    if style:
        default_fill = style.fill_color
        default_edge = style.edge_color or default_edge
        default_opacity = style.fill_opacity

    fig, ax = plt.subplots(figsize=(config.max_size / 100, config.max_size / 100), dpi=100)
    ax.set_aspect("equal")
    ax.axis("off")

    # Plot data FIRST (establishes axes extent for basemap zoom calculation)
    patches: list[Any] = []
    patch_colors: list[str] = []

    for geom in geometries:
        geom_type, coords = geom["type"], geom["coordinates"]
        props = geom.get("properties", {})

        # Resolve color for this geometry
        if style and style.color_map and style.color_field:
            from portolan_cli.thumbnail_style import resolve_color_for_properties

            fill_color = resolve_color_for_properties(props, style)
        else:
            fill_color = default_fill

        if geom_type == "Polygon":
            n_before = len(patches)
            _add_polygon_patches(coords, patches, MplPolygon)
            patch_colors.extend([fill_color] * (len(patches) - n_before))
        elif geom_type == "MultiPolygon":
            n_before = len(patches)
            _add_multipolygon_patches(coords, patches, MplPolygon)
            patch_colors.extend([fill_color] * (len(patches) - n_before))
        elif geom_type in ("Point", "MultiPoint"):
            _plot_points(ax, coords, geom_type, color=fill_color)
        elif geom_type in ("LineString", "MultiLineString"):
            _plot_lines(ax, coords, geom_type, color=fill_color)

    if patches:
        # Apply individual colors to patches
        for patch, color in zip(patches, patch_colors, strict=True):
            patch.set_facecolor(color)
            patch.set_edgecolor(default_edge)
            patch.set_alpha(default_opacity)
            patch.set_linewidth(0.5)
        pc = PatchCollection(patches, match_original=True)
        ax.add_collection(pc)

    # Set axis limits from bounds (required before adding basemap)
    if bounds is not None:
        ax.set_xlim(bounds[0], bounds[2])
        ax.set_ylim(bounds[1], bounds[3])
    else:
        ax.autoscale()

    # Add basemap AFTER data (contextily needs axes extent for zoom calculation)
    if bounds is not None and config.basemap_provider != "none":
        add_basemap(
            ax,
            bounds,
            config.basemap_provider,
            config.basemap_opacity,
            config.basemap_zoom_adjust,
            crs="EPSG:4326",  # PMTiles are always WGS84
        )

    plt.savefig(
        output_path,
        bbox_inches="tight",
        pad_inches=0,
        facecolor="white",
        edgecolor="none",
        pil_kwargs={"quality": config.quality},
    )
    plt.close()

    return output_path.exists()


# =============================================================================
# GeoParquet Reading (Internal)
# =============================================================================


def _read_geoparquet_bounds(gpq_path: Path) -> tuple[float, float, float, float] | None:
    """Read bounding box from GeoParquet file metadata (O(1) operation).

    Attempts to read bbox from GeoParquet metadata without loading geometry data.
    Falls back to computing bounds from data if metadata is unavailable.

    Args:
        gpq_path: Path to GeoParquet file.

    Returns:
        Tuple of (minx, miny, maxx, maxy) or None if empty/unavailable.
    """
    try:
        import pyarrow.parquet as pq
    except ImportError:
        logger.debug("pyarrow not available")
        return None

    try:
        pq_file = pq.ParquetFile(gpq_path)

        # Try to get bbox from GeoParquet metadata (O(1), no geometry parsing)
        schema_meta = pq_file.schema_arrow.metadata or {}
        geo_meta_bytes = schema_meta.get(b"geo", b"{}")
        geo_meta = json.loads(geo_meta_bytes.decode("utf-8"))

        # GeoParquet spec: columns.<geom_col>.bbox = [minx, miny, maxx, maxy]
        columns = geo_meta.get("columns", {})
        for col_meta in columns.values():
            bbox = col_meta.get("bbox")
            if bbox and len(bbox) >= 4:
                return (bbox[0], bbox[1], bbox[2], bbox[3])

        # No bbox in metadata — fall back to reading data
        logger.debug("No bbox in GeoParquet metadata, falling back to data read")
        return _read_geoparquet_bounds_from_data(gpq_path)

    except Exception as e:
        logger.debug("Failed to read GeoParquet metadata: %s", e)
        return _read_geoparquet_bounds_from_data(gpq_path)


def _read_geoparquet_bounds_from_data(gpq_path: Path) -> tuple[float, float, float, float] | None:
    """Fallback: compute bounds by reading GeoParquet data."""
    try:
        import geopandas as gpd  # type: ignore[import-untyped]
    except ImportError:
        return None

    try:
        gdf = gpd.read_parquet(gpq_path)
        if gdf.empty:
            return None
        bounds = gdf.total_bounds
        return (bounds[0], bounds[1], bounds[2], bounds[3])
    except Exception as e:
        logger.debug("Failed to read GeoParquet bounds from data: %s", e)
        return None


def _read_geoparquet_for_thumbnail(
    gpq_path: Path,
) -> tuple[Any, tuple[float, float, float, float] | None, Any]:
    """Read GeoParquet for thumbnail rendering.

    Gets bbox from metadata (O(1)) for accurate extent, then reads the full file.
    We render ALL features without sampling — contextily handles CRS reprojection
    of basemap tiles, which is far more efficient than reprojecting geometry data.

    Args:
        gpq_path: Path to GeoParquet file.

    Returns:
        Tuple of (gdf, full_bbox, source_crs) where:
        - gdf: GeoDataFrame with all features (or None if failed)
        - full_bbox: (minx, miny, maxx, maxy) from metadata or computed
        - source_crs: Original CRS of the data
    """
    try:
        import geopandas as gpd
    except ImportError:
        logger.debug("geopandas not available")
        return None, None, None

    try:
        # Get bbox from metadata first (O(1), no geometry parsing)
        full_bbox = _read_geoparquet_bounds(gpq_path)

        # Read full file — no sampling, render all features
        gdf = gpd.read_parquet(gpq_path)
        if gdf.empty:
            return None, None, None

        source_crs = gdf.crs

        # If no bbox from metadata, compute from data
        if full_bbox is None:
            bounds = gdf.total_bounds
            full_bbox = (bounds[0], bounds[1], bounds[2], bounds[3])

        return gdf, full_bbox, source_crs

    except Exception as e:
        logger.debug("Failed to read GeoParquet for thumbnail: %s", e)
        return None, None, None


def _render_geoparquet(
    gpq_path: Path,
    output_path: Path,
    config: ThumbnailConfig,
    style_path: Path | None = None,
) -> bool:
    """Render GeoParquet to JPEG thumbnail.

    Renders ALL features in their native CRS without reprojection. Contextily's
    `crs` parameter handles basemap tile reprojection, which is far more efficient
    than transforming potentially millions of geometry vertices.

    Args:
        gpq_path: Path to GeoParquet file.
        output_path: Where to write the JPEG.
        config: Thumbnail configuration.
        style_path: Optional path to Mapbox GL style for categorical coloring.

    Returns:
        True if successful, False otherwise.
    """
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        logger.debug("matplotlib not available")
        return False

    try:
        # Read full file + bbox from metadata
        gdf, full_bounds, source_crs = _read_geoparquet_for_thumbnail(gpq_path)
        if gdf is None or full_bounds is None:
            return False

        # Load style if provided
        fill_color: str | Any = "#3388ff"  # Any allows pd.Series
        edge_color = "#2266cc"
        fill_opacity = 0.6

        if style_path:
            from portolan_cli.thumbnail_style import (
                load_thumbnail_style,
                resolve_colors_for_gdf,
            )

            style = load_thumbnail_style(style_path)
            if style:
                fill_color = resolve_colors_for_gdf(gdf, style)
                edge_color = style.edge_color or edge_color
                fill_opacity = style.fill_opacity

        fig, ax = plt.subplots(figsize=(config.max_size / 100, config.max_size / 100), dpi=100)
        ax.set_aspect("equal")
        ax.axis("off")

        # Plot data first (establishes axes extent for basemap zoom calculation)
        gdf.plot(
            ax=ax,
            facecolor=fill_color,
            edgecolor=edge_color,
            alpha=fill_opacity,
            linewidth=0.5,
        )

        # Set axis limits to full bounds from metadata
        ax.set_xlim(full_bounds[0], full_bounds[2])
        ax.set_ylim(full_bounds[1], full_bounds[3])

        # Add basemap AFTER data (contextily needs axes extent for zoom calculation)
        # zorder=-1 renders basemap behind data
        if config.basemap_provider != "none":
            crs_str = str(source_crs) if source_crs is not None else "EPSG:4326"
            add_basemap(
                ax,
                full_bounds,
                config.basemap_provider,
                config.basemap_opacity,
                config.basemap_zoom_adjust,
                crs=crs_str,
            )

        plt.savefig(
            output_path,
            bbox_inches="tight",
            pad_inches=0,
            facecolor="white",
            edgecolor="none",
        )
        plt.close()

        return output_path.exists()
    except Exception as e:
        logger.debug("Failed to render GeoParquet: %s", e)
        return False


# =============================================================================
# Public API
# =============================================================================


def add_basemap(
    ax: Axes,
    bounds: tuple[float, float, float, float],
    provider: str,
    opacity: float = 1.0,
    zoom_adjust: int = 0,
    crs: str | None = None,
) -> None:
    """Add a contextily basemap to matplotlib axes.

    Args:
        ax: Matplotlib Axes object.
        bounds: Bounding box (minx, miny, maxx, maxy).
        provider: Contextily provider name (e.g., 'CartoDB.Positron').
            Pass 'none' to skip basemap.
        opacity: Basemap opacity 0.0-1.0.
        zoom_adjust: Zoom level adjustment.
        crs: Coordinate reference system (e.g., 'EPSG:4326'). Required for
            correct basemap alignment when data is not in Web Mercator.
    """
    if provider == "none":
        return

    ctx_module = _ensure_contextily()
    if ctx_module is None:
        return

    try:
        # Get the provider object from contextily.providers
        provider_parts = provider.split(".")
        tile_provider = ctx_module.providers
        for part in provider_parts:
            tile_provider = getattr(tile_provider, part)

        ctx_module.add_basemap(
            ax,
            source=tile_provider,
            alpha=opacity,
            zoom_adjust=zoom_adjust,
            crs=crs,
        )
    except AttributeError:
        logger.warning(
            "Unknown basemap provider '%s'. Valid examples: CartoDB.Positron, "
            "CartoDB.DarkMatter, OpenStreetMap.Mapnik",
            provider,
        )
    except Exception as e:
        logger.debug("Failed to add basemap '%s': %s", provider, e)


def generate_thumbnail_from_pmtiles(
    pmtiles_path: Path,
    config: ThumbnailConfig,
    style_path: Path | None = None,
) -> Path | None:
    """Generate JPEG thumbnail from PMTiles file.

    Reads low-zoom tiles, extracts geometries, and renders to JPEG.

    Args:
        pmtiles_path: Path to source PMTiles file.
        config: Thumbnail configuration.
        style_path: Optional path to Mapbox GL style for categorical coloring.

    Returns:
        Path to generated thumbnail, or None if generation failed.
    """
    thumb_path = pmtiles_path.with_name(f"{pmtiles_path.stem}.thumb.jpg")

    try:
        geometries, bounds = _read_pmtiles_geometries(pmtiles_path)
        if not geometries:
            logger.debug("No geometries found in PMTiles: %s", pmtiles_path)
            return None

        if _render_geometries(geometries, thumb_path, config, bounds=bounds, style_path=style_path):
            logger.debug("Generated PMTiles thumbnail: %s", thumb_path)
            return thumb_path
        return None
    except Exception as e:
        logger.debug("Failed to generate PMTiles thumbnail: %s", e)
        return None


def generate_thumbnail_from_geoparquet(
    gpq_path: Path,
    config: ThumbnailConfig,
    style_path: Path | None = None,
) -> Path | None:
    """Generate JPEG thumbnail from GeoParquet file.

    Reads geometry and renders to JPEG using geopandas.

    Args:
        gpq_path: Path to source GeoParquet file.
        config: Thumbnail configuration.
        style_path: Optional path to Mapbox GL style for categorical coloring.

    Returns:
        Path to generated thumbnail, or None if generation failed.
    """
    thumb_path = gpq_path.with_name(f"{gpq_path.stem}.thumb.jpg")

    bounds = _read_geoparquet_bounds(gpq_path)
    if bounds is None:
        logger.debug("No bounds found in GeoParquet: %s", gpq_path)
        return None

    if _render_geoparquet(gpq_path, thumb_path, config, style_path=style_path):
        logger.debug("Generated GeoParquet thumbnail: %s", thumb_path)
        return thumb_path
    return None


def generate_vector_thumbnail(
    *,
    pmtiles_path: Path | None,
    geoparquet_path: Path | None,
    config: ThumbnailConfig,
    style_path: Path | None = None,
) -> Path | None:
    """Generate thumbnail for vector data, preferring PMTiles.

    Orchestrator function that tries PMTiles first, then falls back to GeoParquet.
    This is the main entry point for vector thumbnail generation.

    Args:
        pmtiles_path: Path to PMTiles file (optional).
        geoparquet_path: Path to GeoParquet file (optional, used as fallback).
        config: Thumbnail configuration.
        style_path: Optional path to Mapbox GL style for categorical coloring.

    Returns:
        Path to generated thumbnail, or None if generation failed or disabled.
    """
    if not config.enabled:
        logger.debug("Thumbnail generation disabled")
        return None

    if pmtiles_path is None and geoparquet_path is None:
        logger.debug("No source files provided for thumbnail")
        return None

    # Try PMTiles first
    if pmtiles_path is not None:
        result = generate_thumbnail_from_pmtiles(pmtiles_path, config, style_path)
        if result is not None:
            return result
        logger.debug("PMTiles thumbnail failed, falling back to GeoParquet")

    # Fall back to GeoParquet
    if geoparquet_path is not None:
        return generate_thumbnail_from_geoparquet(geoparquet_path, config, style_path)

    return None
