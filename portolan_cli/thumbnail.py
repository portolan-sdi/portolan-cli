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
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from portolan_cli.config import load_config

if TYPE_CHECKING:
    from matplotlib.axes import Axes  # type: ignore[import-not-found]

logger = logging.getLogger(__name__)

# Lazy import for optional dependencies
ctx: Any = None  # contextily module, set on first use


def _ensure_contextily() -> Any:
    """Lazy-load contextily, returning module or None if unavailable."""
    global ctx
    if ctx is None:
        try:
            import contextily as _ctx  # type: ignore[import-not-found]

            ctx = _ctx
        except ImportError:
            logger.debug("contextily not available, basemaps disabled")
            return None
    return ctx


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


def _get_dict(data: dict[str, Any], key: str) -> dict[str, Any]:
    """Safely get a dict value, returning empty dict if not a dict."""
    value = data.get(key, {})
    return value if isinstance(value, dict) else {}


def get_thumbnail_config(catalog_path: Path) -> ThumbnailConfig:
    """Load thumbnail config from catalog's config.yaml.

    Reads the 'thumbnails' section and returns a ThumbnailConfig instance.

    Args:
        catalog_path: Root path of the catalog.

    Returns:
        ThumbnailConfig instance. Returns defaults if no config exists.
    """
    config = load_config(catalog_path)

    thumbnails = _get_dict(config, "thumbnails")
    if not thumbnails:
        return ThumbnailConfig()

    # Parse basemap subsection
    basemap = _get_dict(thumbnails, "basemap")

    enabled = thumbnails.get("enabled")
    if not isinstance(enabled, bool):
        enabled = True

    max_size = thumbnails.get("max_size")
    if not isinstance(max_size, int) or max_size <= 0:
        max_size = 512

    quality = thumbnails.get("quality")
    if not isinstance(quality, int) or not 1 <= quality <= 100:
        quality = 75

    basemap_provider = basemap.get("provider")
    if not isinstance(basemap_provider, str):
        basemap_provider = "CartoDB.Positron"

    basemap_opacity = basemap.get("opacity")
    if not isinstance(basemap_opacity, (int, float)) or not 0 <= basemap_opacity <= 1:
        basemap_opacity = 1.0

    basemap_zoom_adjust = basemap.get("zoom_adjust")
    if not isinstance(basemap_zoom_adjust, int):
        basemap_zoom_adjust = 0

    return ThumbnailConfig(
        enabled=enabled,
        max_size=max_size,
        quality=quality,
        basemap_provider=basemap_provider,
        basemap_opacity=float(basemap_opacity),
        basemap_zoom_adjust=basemap_zoom_adjust,
    )


# =============================================================================
# PMTiles Reading (Internal)
# =============================================================================


def _read_pmtiles_geometries(pmtiles_path: Path) -> list[dict[str, Any]]:
    """Read geometries from low-zoom PMTiles tiles.

    Args:
        pmtiles_path: Path to PMTiles file.

    Returns:
        List of geometry dicts with 'type' and 'coordinates' keys.

    Raises:
        Exception: If PMTiles cannot be read.
    """
    try:
        from pmtiles.reader import MmapSource, Reader
    except ImportError:
        logger.debug("pmtiles library not available")
        return []

    try:
        import mapbox_vector_tile  # type: ignore[import-not-found]
    except ImportError:
        logger.debug("mapbox-vector-tile library not available")
        return []

    geometries: list[dict[str, Any]] = []

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

                        for layer in decoded.values():
                            for feature in layer.get("features", []):
                                geom = feature.get("geometry", {})
                                if geom.get("type") and geom.get("coordinates"):
                                    geometries.append(
                                        {
                                            "type": geom["type"],
                                            "coordinates": geom["coordinates"],
                                        }
                                    )

                    # Limit search at higher zooms
                    if tiles_checked > 256:
                        break
                if tiles_checked > 256:
                    break

            if geometries:
                break  # Got data, stop

    return geometries


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
) -> bool:
    """Render geometries to JPEG thumbnail."""
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
        geometries = _read_pmtiles_geometries(pmtiles_path)
        if not geometries:
            logger.debug("No geometries found in PMTiles: %s", pmtiles_path)
            return None

        if _render_geometries(geometries, thumb_path, config):
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
