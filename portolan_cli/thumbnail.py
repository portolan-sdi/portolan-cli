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
import logging
import math
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from portolan_cli.config import load_config
from portolan_cli.utils import get_dict

if TYPE_CHECKING:
    from matplotlib.axes import Axes  # type: ignore[import-not-found]

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
            import contextily as ctx  # type: ignore[import-not-found]

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
    # Depth limit: Point=0, LineString=1, Polygon=2 (ring), MultiPolygon=3.
    # GeometryCollection with nested Multi* could reach 4. Beyond that is malformed.
    if depth > 4:
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
        import mapbox_vector_tile  # type: ignore[import-not-found]
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
            max_tile = 2**z
            tiles_checked = 0

            for x in range(max_tile):
                for y in range(max_tile):
                    tile_data: bytes | None = reader.get(z, x, y)
                    tiles_checked += 1

                    if tile_data:
                        # Decompress if gzipped
                        if tile_data[:2] == b"\x1f\x8b":
                            tile_data = gzip.decompress(tile_data)

                        decoded = mapbox_vector_tile.decode(tile_data)
                        tile_bounds = _tile_bounds(z, x, y)

                        for layer in decoded.values():
                            for feature in layer.get("features", []):
                                geom = feature.get("geometry", {})
                                if geom.get("type") and geom.get("coordinates"):
                                    # Transform coords from tile-space to geographic
                                    transformed = _transform_coords(
                                        geom["coordinates"], tile_bounds
                                    )
                                    geometries.append(
                                        {
                                            "type": geom["type"],
                                            "coordinates": transformed,
                                        }
                                    )
                                    # Collect bounds from tile
                                    all_lons.extend([tile_bounds[0], tile_bounds[2]])
                                    all_lats.extend([tile_bounds[1], tile_bounds[3]])

                    # Limit search at higher zooms
                    if tiles_checked > 256:
                        break
                if tiles_checked > 256:
                    break

            if geometries:
                break  # Got data, stop

    # Calculate overall bounds
    bounds: tuple[float, float, float, float] | None = None
    if all_lons and all_lats:
        bounds = (min(all_lons), min(all_lats), max(all_lons), max(all_lats))

    return geometries, bounds


def _add_polygon_patches(coords: list[Any], patches: list[Any], mpl_polygon_cls: type) -> None:
    """Add polygon patches from coordinates."""
    if coords and coords[0]:
        patches.append(mpl_polygon_cls(coords[0], closed=True))


def _add_multipolygon_patches(coords: list[Any], patches: list[Any], mpl_polygon_cls: type) -> None:
    """Add multipolygon patches from coordinates."""
    for polygon in coords:
        if polygon and polygon[0]:
            patches.append(mpl_polygon_cls(polygon[0], closed=True))


def _plot_points(ax: Any, coords: list[Any], geom_type: str) -> None:
    """Plot point or multipoint geometries."""
    if geom_type == "Point":
        ax.plot(coords[0], coords[1], "o", markersize=2, color="#3388ff")
    else:
        for pt in coords:
            ax.plot(pt[0], pt[1], "o", markersize=2, color="#3388ff")


def _plot_lines(ax: Any, coords: list[Any], geom_type: str) -> None:
    """Plot linestring or multilinestring geometries."""
    if geom_type == "LineString":
        xs, ys = [c[0] for c in coords], [c[1] for c in coords]
        ax.plot(xs, ys, linewidth=1, color="#3388ff")
    else:
        for line in coords:
            xs, ys = [c[0] for c in line], [c[1] for c in line]
            ax.plot(xs, ys, linewidth=1, color="#3388ff")


def _render_geometries(
    geometries: list[dict[str, Any]],
    output_path: Path,
    config: ThumbnailConfig,
    bounds: tuple[float, float, float, float] | None = None,
) -> bool:
    """Render geometries to JPEG thumbnail with optional basemap.

    Args:
        geometries: List of geometry dicts with 'type' and 'coordinates'.
        output_path: Where to write the JPEG.
        config: Thumbnail configuration.
        bounds: Geographic bounds (minx, miny, maxx, maxy) for basemap.

    Returns:
        True if successful, False otherwise.
    """
    try:
        import matplotlib.pyplot as plt  # type: ignore[import-not-found]
        from matplotlib.collections import PatchCollection  # type: ignore[import-not-found]
        from matplotlib.patches import Polygon as MplPolygon  # type: ignore[import-not-found]
    except ImportError:
        logger.debug("matplotlib not available")
        return False

    fig, ax = plt.subplots(figsize=(config.max_size / 100, config.max_size / 100), dpi=100)
    ax.set_aspect("equal")
    ax.axis("off")

    # Add basemap first (behind data) if bounds available
    if bounds is not None and config.basemap_provider != "none":
        add_basemap(
            ax,
            bounds,
            config.basemap_provider,
            config.basemap_opacity,
            config.basemap_zoom_adjust,
        )

    patches: list[Any] = []
    for geom in geometries:
        geom_type, coords = geom["type"], geom["coordinates"]
        if geom_type == "Polygon":
            _add_polygon_patches(coords, patches, MplPolygon)
        elif geom_type == "MultiPolygon":
            _add_multipolygon_patches(coords, patches, MplPolygon)
        elif geom_type in ("Point", "MultiPoint"):
            _plot_points(ax, coords, geom_type)
        elif geom_type in ("LineString", "MultiLineString"):
            _plot_lines(ax, coords, geom_type)

    if patches:
        pc = PatchCollection(
            patches, facecolor="#3388ff", edgecolor="#2266cc", alpha=0.6, linewidth=0.5
        )
        ax.add_collection(pc)
        ax.autoscale()

    plt.savefig(
        output_path,
        bbox_inches="tight",
        pad_inches=0,
        facecolor="white",
        edgecolor="none",
        quality=config.quality,
    )
    plt.close()

    return output_path.exists()


# =============================================================================
# GeoParquet Reading (Internal)
# =============================================================================


def _read_geoparquet_bounds(gpq_path: Path) -> tuple[float, float, float, float] | None:
    """Read bounding box from GeoParquet file.

    Args:
        gpq_path: Path to GeoParquet file.

    Returns:
        Tuple of (minx, miny, maxx, maxy) or None if empty.
    """
    try:
        import geopandas as gpd  # type: ignore[import-untyped]
    except ImportError:
        logger.debug("geopandas not available")
        return None

    try:
        gdf = gpd.read_parquet(gpq_path)
        if gdf.empty:
            return None
        return tuple(gdf.total_bounds)
    except Exception as e:
        logger.debug("Failed to read GeoParquet bounds: %s", e)
        return None


def _render_geoparquet(
    gpq_path: Path,
    output_path: Path,
    config: ThumbnailConfig,
) -> bool:
    """Render GeoParquet to JPEG thumbnail.

    Args:
        gpq_path: Path to GeoParquet file.
        output_path: Where to write the JPEG.
        config: Thumbnail configuration.

    Returns:
        True if successful, False otherwise.
    """
    try:
        import geopandas as gpd
        import matplotlib.pyplot as plt
    except ImportError:
        logger.debug("geopandas/matplotlib not available")
        return False

    try:
        gdf = gpd.read_parquet(gpq_path)
        if gdf.empty:
            return False

        fig, ax = plt.subplots(figsize=(config.max_size / 100, config.max_size / 100), dpi=100)
        ax.set_aspect("equal")
        ax.axis("off")

        # Add basemap first (behind data)
        bounds = gdf.total_bounds
        if config.basemap_provider != "none":
            add_basemap(
                ax,
                tuple(bounds),
                config.basemap_provider,
                config.basemap_opacity,
                config.basemap_zoom_adjust,
            )

        # Plot data on top
        gdf.plot(
            ax=ax,
            facecolor="#3388ff",
            edgecolor="#2266cc",
            alpha=0.6,
            linewidth=0.5,
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
) -> None:
    """Add a contextily basemap to matplotlib axes.

    Args:
        ax: Matplotlib Axes object.
        bounds: Bounding box (minx, miny, maxx, maxy).
        provider: Contextily provider name (e.g., 'CartoDB.Positron').
            Pass 'none' to skip basemap.
        opacity: Basemap opacity 0.0-1.0.
        zoom_adjust: Zoom level adjustment.
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
        )
    except Exception as e:
        logger.debug("Failed to add basemap: %s", e)


def generate_thumbnail_from_pmtiles(
    pmtiles_path: Path,
    config: ThumbnailConfig,
) -> Path | None:
    """Generate JPEG thumbnail from PMTiles file.

    Reads low-zoom tiles, extracts geometries, and renders to JPEG.

    Args:
        pmtiles_path: Path to source PMTiles file.
        config: Thumbnail configuration.

    Returns:
        Path to generated thumbnail, or None if generation failed.
    """
    thumb_path = pmtiles_path.with_name(f"{pmtiles_path.stem}.thumb.jpg")

    try:
        geometries, bounds = _read_pmtiles_geometries(pmtiles_path)
        if not geometries:
            logger.debug("No geometries found in PMTiles: %s", pmtiles_path)
            return None

        if _render_geometries(geometries, thumb_path, config, bounds=bounds):
            logger.debug("Generated PMTiles thumbnail: %s", thumb_path)
            return thumb_path
        return None
    except Exception as e:
        logger.debug("Failed to generate PMTiles thumbnail: %s", e)
        return None


def generate_thumbnail_from_geoparquet(
    gpq_path: Path,
    config: ThumbnailConfig,
) -> Path | None:
    """Generate JPEG thumbnail from GeoParquet file.

    Reads geometry and renders to JPEG using geopandas.

    Args:
        gpq_path: Path to source GeoParquet file.
        config: Thumbnail configuration.

    Returns:
        Path to generated thumbnail, or None if generation failed.
    """
    thumb_path = gpq_path.with_name(f"{gpq_path.stem}.thumb.jpg")

    bounds = _read_geoparquet_bounds(gpq_path)
    if bounds is None:
        logger.debug("No bounds found in GeoParquet: %s", gpq_path)
        return None

    if _render_geoparquet(gpq_path, thumb_path, config):
        logger.debug("Generated GeoParquet thumbnail: %s", thumb_path)
        return thumb_path
    return None


def generate_vector_thumbnail(
    *,
    pmtiles_path: Path | None,
    geoparquet_path: Path | None,
    config: ThumbnailConfig,
) -> Path | None:
    """Generate thumbnail for vector data, preferring PMTiles.

    Orchestrator function that tries PMTiles first, then falls back to GeoParquet.
    This is the main entry point for vector thumbnail generation.

    Args:
        pmtiles_path: Path to PMTiles file (optional).
        geoparquet_path: Path to GeoParquet file (optional, used as fallback).
        config: Thumbnail configuration.

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
        result = generate_thumbnail_from_pmtiles(pmtiles_path, config)
        if result is not None:
            return result
        logger.debug("PMTiles thumbnail failed, falling back to GeoParquet")

    # Fall back to GeoParquet
    if geoparquet_path is not None:
        return generate_thumbnail_from_geoparquet(geoparquet_path, config)

    return None
