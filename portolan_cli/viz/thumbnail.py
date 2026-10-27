"""Vector thumbnail generation module (Issue #13).

Generates JPEG thumbnails from PMTiles and GeoParquet files with optional basemaps.
Mirrors the COG thumbnail pattern in convert.py.

Public API:
- ThumbnailConfig: Configuration dataclass for thumbnail generation
- generate_vector_thumbnail: Orchestrator (prefers GeoParquet, falls back to PMTiles)
- generate_thumbnail_from_pmtiles: Generate from PMTiles
- generate_thumbnail_from_geoparquet: Generate from GeoParquet
- add_basemap: Draw basemap tiles under the data of an EPSG:3857 axes
- get_thumbnail_config: Load config from catalog's config.yaml
"""

from __future__ import annotations

import functools
import gzip
import json
import logging
import math
import threading
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any

from portolan_cli.config import load_catalog_config
from portolan_cli.trace import count
from portolan_cli.utils import get_dict

if TYPE_CHECKING:
    from matplotlib.axes import Axes

    from portolan_cli.config import ConfigSource

logger = logging.getLogger(__name__)

# Thread-safe lazy import for optional contextily dependency
_ctx_lock = threading.Lock()
_ctx_module: Any = None
_ctx_loaded = False


def _ensure_contextily() -> Any:
    """Lazy-load contextily, returning module or None if unavailable.

    Thread-safe: every call takes the lock, so only one thread runs the import.
    The lock is cheap next to a thumbnail render.
    """
    global _ctx_module, _ctx_loaded
    with _ctx_lock:
        if not _ctx_loaded:
            try:
                import contextily as ctx

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


#: The basemap a thumbnail gets when the config names none. CARTO tiles need a
#: key now: a request without one gets a tile that reads "API KEY REQUIRED"
#: (etag ``wm-da89c20e77c1-light``). Esri serves this light gray canvas with
#: no key.
DEFAULT_BASEMAP_PROVIDER = "Esri.WorldGrayCanvas"


@dataclass(frozen=True)
class ThumbnailConfig:
    """Configuration for thumbnail generation.

    Attributes:
        enabled: Whether to generate thumbnails (default True).
        max_size: Maximum pixel dimension for longest edge (default 512).
        quality: JPEG quality 1-100 (default 75).
        basemap_provider: Contextily basemap provider name (default
            ``DEFAULT_BASEMAP_PROVIDER``). Set to 'none' to disable basemap.
        basemap_opacity: Basemap opacity 0.0-1.0 (default 1.0).
        basemap_zoom_adjust: Zoom level adjustment for basemap (default 0).
        basemap_api_key: Key for the tile provider (default None). CARTO
            needs one. The env var PORTOLAN_THUMBNAILS_BASEMAP_API_KEY sets it.
        max_features: Feature ceiling for a single render (default 100,000).
            Beyond it the reader samples, so render cost stays bounded in file
            size. Thumbnails became part of the default add pipeline in Issue
            #683, which is what makes an unbounded read a problem.
    """

    enabled: bool = True
    max_size: int = 512
    quality: int = 75
    basemap_provider: str = DEFAULT_BASEMAP_PROVIDER
    basemap_opacity: float = 1.0
    basemap_zoom_adjust: int = 0
    basemap_api_key: str | None = None
    max_features: int = 100_000


def get_thumbnail_config(
    catalog_path: Path, *, state: ConfigSource | None = None
) -> ThumbnailConfig:
    """Load thumbnail config from catalog's config.yaml.

    Reads the 'thumbnails' section and returns a ThumbnailConfig instance.

    Args:
        catalog_path: Root path of the catalog.
        state: The run state, which parses the config once per run.

    Returns:
        ThumbnailConfig instance. Returns defaults if no config exists.
    """
    config = load_catalog_config(catalog_path, state=state).data
    thumbnails = get_dict(config, "thumbnails")
    if not thumbnails:
        return ThumbnailConfig()

    basemap = get_dict(thumbnails, "basemap")

    return ThumbnailConfig(
        enabled=_parse_bool(thumbnails.get("enabled"), "thumbnails.enabled", True),
        max_size=_parse_positive_int(thumbnails.get("max_size"), "thumbnails.max_size", 512),
        quality=_parse_bounded_int(thumbnails.get("quality"), "thumbnails.quality", 75, 1, 100),
        basemap_provider=_parse_str(
            basemap.get("provider"), "thumbnails.basemap.provider", DEFAULT_BASEMAP_PROVIDER
        ),
        basemap_opacity=_parse_bounded_float(
            basemap.get("opacity"), "thumbnails.basemap.opacity", 1.0, 0.0, 1.0
        ),
        basemap_zoom_adjust=_parse_int(
            basemap.get("zoom_adjust"), "thumbnails.basemap.zoom_adjust", 0
        ),
        basemap_api_key=_parse_str(basemap.get("api_key"), "thumbnails.basemap.api_key", "")
        or None,
        max_features=_parse_positive_int(
            thumbnails.get("max_features"), "thumbnails.max_features", 100_000
        ),
    )


#: Infix that marks an image Portolan drew, as in ``scene1.thumb.jpg``. It keeps
#: a render off a hand-curated sibling such as ``scene1.jpg``. That also makes it
#: the one reliable mark of our own output. ``convert.py``,
#: ``collection_thumbnail.py`` and ``derived_assets.py`` all read the name from
#: here, so no module repeats it.
THUMBNAIL_STEM_SUFFIX = ".thumb"


def thumbnail_path_for(data_path: Path) -> Path:
    """Return the thumbnail path for a data file (single source of the convention).

    Thumbnails are written next to their source as ``{stem}.thumb.jpg``. Both the
    PMTiles and GeoParquet render paths use this, and the PMTiles side-step relies
    on it to locate an already-generated thumbnail when generation is skipped.

    Args:
        data_path: Path to the source data file (PMTiles or GeoParquet).

    Returns:
        Path to the sibling thumbnail file.
    """
    return data_path.with_name(f"{data_path.stem}{THUMBNAIL_STEM_SUFFIX}.jpg")


def is_generated_thumbnail(filename: str) -> bool:
    """True when ``filename`` carries the ``{stem}.thumb.{ext}`` name we write.

    The test reads the basename alone. It answers for a versions.json href and
    for a path on disk. An adopted ``thumbnail.png`` returns False, because that
    image belongs to the user. Only a file Portolan drew is safe to redraw
    (Issue #735).
    """
    return PurePosixPath(filename.lower()).stem.endswith(THUMBNAIL_STEM_SUFFIX)


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

    with Path(pmtiles_path).open("rb") as f:
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


# =============================================================================
# Thumbnail rendering presets and data-aware parameters (Issue #518)
# =============================================================================
#
# The matplotlib floor renders a deliberately punchy, data-aware preset instead
# of honoring the extracted Mapbox/WFS style, which is pale by design (e.g.
# fill-opacity 0.2, hairline outlines) and washes out on a 512 px tile over a
# light basemap. Only the style's *categorical* fill colors are reused; opacity,
# edge color, and stroke widths always come from these presets. The real style
# is rendered by the opt-in MapLibre-native skill (Track 2), not here.

THUMB_FILL_COLOR = "#3388ff"  # punchy default fill / categorical fallback
THUMB_EDGE_COLOR = "#13447a"  # bold outline (vs the old hairline #2266cc)

# Feature-count thresholds for data-aware parameter selection.
_DENSE_FEATURE_COUNT = 2000
_MID_FEATURE_COUNT = 200

# Visibility floors for the off-axis dimensions. A single RenderParams is
# applied to every feature in a layer (the GeoParquet path issues one
# ``gdf.plot`` for the whole frame), so a layer with mixed geometry types would
# otherwise drop the non-dominant ones to nothing: points vanish when
# marker_size is 0, lines/edges vanish when stroke_width is 0. Flooring both to
# a small positive value keeps every geometry type visible. Pure single-type
# layers are unaffected (a polygon layer has no points to size, etc.).
_MIN_VISIBLE_MARKER = 2.0
_MIN_VISIBLE_STROKE = 0.4


@dataclass(frozen=True)
class RenderParams:
    """Data-aware cosmetic parameters for one thumbnail render.

    Attributes:
        marker_size: Point marker size (matplotlib ``markersize``).
        stroke_width: Line thickness (lines) or polygon edge width.
        fill_opacity: Fill/marker alpha. Always >= 0.5 to avoid washout.
    """

    marker_size: float
    stroke_width: float
    fill_opacity: float


def _compute_render_params(geom_category: str, feature_count: int) -> RenderParams:
    """Select punchy, data-aware render parameters (Issue #518).

    Scales marker size / stroke width / fill opacity to geometry type and
    feature density: sparse layers get bigger, bolder marks; dense layers get
    smaller marks and (for polygons) lower opacity so they don't blob to solid.
    Fill opacity is clamped to >= 0.5 so the result is never washed out.
    """
    dense = feature_count >= _DENSE_FEATURE_COUNT
    mid = feature_count >= _MID_FEATURE_COUNT

    # The dominant geometry type drives the *primary* dimension's density
    # scaling; the off-axis dimension is floored (not zeroed) so a mixed-geometry
    # layer never renders a minority type invisibly (see _MIN_VISIBLE_* above).
    if geom_category == "point":
        marker = 1.5 if dense else 3.0 if mid else 6.0
        return RenderParams(marker_size=marker, stroke_width=_MIN_VISIBLE_STROKE, fill_opacity=0.8)

    if geom_category == "line":
        width = 0.4 if dense else 0.8 if mid else 1.5
        return RenderParams(marker_size=_MIN_VISIBLE_MARKER, stroke_width=width, fill_opacity=0.9)

    # polygon (also the default for unknown categories)
    opacity = 0.5 if dense else 0.6 if mid else 0.7
    edge = 0.2 if dense else 0.4 if mid else 0.8
    return RenderParams(marker_size=_MIN_VISIBLE_MARKER, stroke_width=edge, fill_opacity=opacity)


def _frame_bounds(
    bounds: tuple[float, float, float, float],
    max_aspect: float = 2.5,
    margin: float = 0.05,
) -> tuple[float, float, float, float]:
    """Frame a bbox for a square thumbnail.

    The function pads the extent, gives a degenerate extent a box, and bounds
    the limit aspect ratio (#518).

    Two real jobs and one caveat:

    1. **Margin** — adds a uniform ``margin`` so geometry is not flush to the
       figure edge.
    2. **Degenerate extents** — a single point (both axes zero) or a perfectly
       vertical/horizontal extent (one axis zero) would otherwise collapse
       ``set_xlim``/``set_ylim``; the aspect cap grows the zero/short axis to a
       finite span so the limits stay valid and contextily can derive a zoom.

    Caveat: this does **not** make an elongated *geometry* more legible. Under
    ``set_aspect('equal')`` the displayed scale is fixed by the longer axis, so
    widening the short axis's *limits* only adds whitespace around the geometry —
    a 35:1 polygon is still rendered as a thin strip either way (measured: ~11px
    vs ~14px wide at 512px). ``max_aspect`` therefore bounds the surrounding
    whitespace, it does not de-elongate the data. Operates on the bbox only
    (O(1)); never reads geometry.

    Args:
        bounds: (minx, miny, maxx, maxy).
        max_aspect: Maximum allowed width/height (or height/width) ratio.
        margin: Fractional padding added to each side after capping.

    Returns:
        Framed (minx, miny, maxx, maxy).
    """
    minx, miny, maxx, maxy = bounds
    width = maxx - minx
    height = maxy - miny

    # Degenerate extent (a single point, both axes zero): give it a finite box
    # so set_xlim/set_ylim don't collapse. True scale is unknown, so a nominal
    # span is fine — contextily derives a sensible zoom from it.
    if width <= 0 and height <= 0:
        half = 0.5
        minx, maxx = minx - half, maxx + half
        miny, maxy = miny - half, maxy + half
        width = height = 1.0

    # Cap aspect ratio by growing the short axis around its center.
    if width > max_aspect * height:
        target = width / max_aspect
        pad = (target - height) / 2.0
        miny -= pad
        maxy += pad
        height = target
    elif height > max_aspect * width:
        target = height / max_aspect
        pad = (target - width) / 2.0
        minx -= pad
        maxx += pad
        width = target

    mx = width * margin
    my = height * margin
    return (minx - mx, miny - my, maxx + mx, maxy + my)


def _profile(geoms: Any) -> tuple[str, int]:
    """Dominant geometry category and feature count, for both render paths."""
    import numpy as np
    import shapely

    count = len(geoms)
    if count == 0:
        return "polygon", 0
    # Type ids 0 to 6 are Point, LineString, LinearRing, Polygon, MultiPoint,
    # MultiLineString, MultiPolygon. A collection (7) counts as a polygon.
    category = np.array([0, 1, 1, 2, 0, 1, 2, 2])[shapely.get_type_id(geoms).clip(0, 7)]
    dominant = int(np.bincount(category, minlength=3).argmax())
    return ("point", "line", "polygon")[dominant], count


# =============================================================================
# Drawing (shared by the GeoParquet and the PMTiles paths)
# =============================================================================

#: Pixels per inch of the thumbnail figure. Only the ratio to ``max_size``
#: matters. The value keeps the stroke and marker presets of #518 in points.
_DPI = 100

#: The EPSG:3857 world spans 2 x this value in x. pyproj maps latitude +-90 to
#: infinity, so the render clips projected y to the same value.
_MERCATOR_LIMIT = 20037508.342789244

#: The latitude whose EPSG:3857 y is ``_MERCATOR_LIMIT``.
_MERCATOR_MAX_LAT = 85.0511287798066


def _count_draw(_event: object) -> None:
    """Count one full figure draw."""
    count("thumbnail.draws")


def _new_figure(frame: tuple[float, float, float, float], max_size: int) -> tuple[Any, Any]:
    """Make a figure whose one axes fills it and shows ``frame`` at equal scale.

    The longer side gets ``max_size`` pixels. The figure has its own Agg canvas
    and no pyplot state, so a render in a worker thread does not touch the
    process-wide backend. Every draw of the figure adds 1 to the
    ``thumbnail.draws`` counter.
    """
    from matplotlib.backends.backend_agg import FigureCanvasAgg
    from matplotlib.figure import Figure

    minx, miny, maxx, maxy = frame
    width, height = maxx - minx, maxy - miny
    if width >= height:
        size_px = (max_size, max(1, round(max_size * height / width)))
    else:
        size_px = (max(1, round(max_size * width / height)), max_size)

    # Agg truncates width x dpi to whole pixels, and 477 / 100 * 100 is
    # 476.99999999999994. The small bump keeps the pixel count exact.
    figure = Figure(figsize=((size_px[0] + 1e-6) / _DPI, (size_px[1] + 1e-6) / _DPI), dpi=_DPI)
    FigureCanvasAgg(figure)
    figure.canvas.mpl_connect("draw_event", _count_draw)
    ax = figure.add_axes((0.0, 0.0, 1.0, 1.0))
    ax.set_axis_off()
    ax.set_xlim(minx, maxx)
    ax.set_ylim(miny, maxy)
    return figure, ax


def _ring_path(rings: Any, closed: bool) -> Any:
    """One matplotlib Path that holds every ring or line in ``rings``.

    Each part starts with MOVETO. A closed part ends with CLOSEPOLY. The
    vertices come from shapely in one call, so the cost does not depend on
    how many Python objects the layer has (#993).
    """
    import numpy as np
    import shapely
    from matplotlib.path import Path as MplPath

    sizes = shapely.get_num_coordinates(rings)
    sizes = sizes[sizes > 0]
    coords = shapely.get_coordinates(rings)
    codes = np.full(len(coords), MplPath.LINETO, dtype=MplPath.code_type)
    starts = np.concatenate(([0], np.cumsum(sizes)[:-1]))
    codes[starts] = MplPath.MOVETO
    if closed:
        codes[starts + sizes - 1] = MplPath.CLOSEPOLY
    return MplPath(coords, codes)


def _explode(geoms: Any) -> tuple[Any, Any]:
    """Split multi-part geometries and collections into single parts.

    Returns:
        The parts, and for each part the index of the feature it came from.
    """
    import numpy as np
    import shapely

    keep = ~(shapely.is_missing(geoms) | shapely.is_empty(geoms))
    index = np.flatnonzero(keep)
    parts = geoms[keep]
    # A collection can hold multi-part members, so split until none is left.
    while len(parts) and (shapely.get_type_id(parts) > 3).any():
        parts, part_index = shapely.get_parts(parts, return_index=True)
        index = index[part_index]
    return parts, index


def _draw_features(ax: Any, geoms: Any, colors: Any, params: RenderParams) -> None:
    """Draw every feature with one artist per geometry kind and colour.

    Polygons are one filled path, lines one stroked path, and points one
    scatter. The paths go in with ``add_artist``, not ``add_patch``: the frame
    is already set, and ``add_patch`` walks every Bezier segment to grow the
    data limits (14 s at 100,000 polygons). Geopandas made one PathPatch per polygon, and the PMTiles path one
    ``ax.plot`` per point and line. That took 14 s of a 17 s render at 100,000
    polygons (#993).

    Args:
        ax: Axes to draw on.
        geoms: Array of shapely geometries in the axes CRS.
        colors: One fill colour, or one colour per feature.
        params: Render presets for the layer.
    """
    import numpy as np
    import shapely
    from matplotlib.patches import PathPatch

    parts, index = _explode(geoms)
    if not len(parts):
        return
    if isinstance(colors, str):
        part_colors = np.full(len(parts), colors, dtype=object)
    else:
        part_colors = np.asarray(colors, dtype=object)[index]
    type_ids = shapely.get_type_id(parts)

    polygons = type_ids == 3
    lines = (type_ids == 1) | (type_ids == 2)
    points = type_ids == 0
    for color in dict.fromkeys(part_colors[polygons]):
        # The nonzero fill rule needs the shell and the holes wound in
        # opposite directions. Data does not guarantee that, so orient them.
        selected = shapely.orient_polygons(parts[polygons & (part_colors == color)])
        path = _ring_path(shapely.get_rings(selected), closed=True)
        ax.add_artist(
            PathPatch(
                path,
                facecolor=color,
                edgecolor=THUMB_EDGE_COLOR,
                linewidth=params.stroke_width,
                alpha=params.fill_opacity,
            )
        )
    for color in dict.fromkeys(part_colors[lines]):
        path = _ring_path(parts[lines & (part_colors == color)], closed=False)
        ax.add_artist(
            PathPatch(
                path,
                fill=False,
                edgecolor=color,
                linewidth=params.stroke_width,
                alpha=params.fill_opacity,
            )
        )
    if points.any():
        xy = shapely.get_coordinates(parts[points])
        ax.scatter(
            xy[:, 0],
            xy[:, 1],
            s=params.marker_size,
            c=list(part_colors[points]),
            edgecolors=THUMB_EDGE_COLOR,
            linewidths=params.stroke_width,
            alpha=params.fill_opacity,
        )


@functools.lru_cache(maxsize=16)
def _mercator_transformer(source: Any) -> Any:
    """Return the transformer from ``source`` to EPSG:3857.

    pyproj builds a transformer in about 8 ms, which was a sixth of a
    collection thumbnail. A run renders many collections in a few CRSs.
    """
    from pyproj import Transformer

    return Transformer.from_crs(source, "EPSG:3857", always_xy=True)


def _to_web_mercator(
    geoms: Any,
    bounds: tuple[float, float, float, float],
    crs: Any,
) -> tuple[Any, tuple[float, float, float, float]] | None:
    """Reproject the drawn geometries and their bounds to EPSG:3857.

    Basemap tiles are in EPSG:3857. Drawing the data there means no tile is
    warped. contextily warped the tiles into the data CRS for each collection,
    which took 17 s of a 36 s 100-collection add (#993). The geometries are a
    sample of at most ``max_features``, so this is cheap.

    Returns:
        The projected geometries and bounds, or None when the CRS is unknown
        or the coordinates do not fit it (for example projected values under a
        geographic CRS, #516). The caller then draws in the source CRS with no
        basemap.
    """
    import numpy as np
    import shapely
    from pyproj import CRS

    if crs is None:
        return None
    try:
        source = CRS.from_user_input(crs)
        transformer = _mercator_transformer(source)
    except Exception as exc:  # an unknown or engineering CRS has no web mapping
        logger.debug("No EPSG:3857 transform for %s: %s", crs, exc)
        return None

    geographic = source.is_geographic

    def clip_lat(lat: Any) -> Any:
        # Clip a valid latitude to the EPSG:3857 limit. A value beyond +-90 is
        # not a latitude, so it stays and fails the finite test below.
        if not geographic:
            return lat
        return np.where(np.abs(lat) <= 90, np.clip(lat, -_MERCATOR_MAX_LAT, _MERCATOR_MAX_LAT), lat)

    def project(xy: Any) -> Any:
        x, y = transformer.transform(xy[:, 0], clip_lat(xy[:, 1]))
        return np.column_stack((x, y))

    minx, miny, maxx, maxy = bounds
    with np.errstate(invalid="ignore", over="ignore"):
        projected = shapely.transform(geoms, project)
        box = transformer.transform_bounds(
            minx,
            float(clip_lat(np.array(miny))),
            maxx,
            float(clip_lat(np.array(maxy))),
            densify_pts=21,
        )
    coords = shapely.get_coordinates(projected)
    if not (np.isfinite(box).all() and np.isfinite(coords).all()):
        logger.debug("Coordinates do not fit %s; drawing without a basemap", crs)
        return None
    return projected, (box[0], box[1], box[2], box[3])


def _render(
    geoms: Any,
    colors: Any,
    bounds: tuple[float, float, float, float],
    crs: Any,
    output_path: Path,
    config: ThumbnailConfig,
) -> bool:
    """Draw one thumbnail with one figure draw, and write it as a JPEG.

    Both render paths end here, so they cannot drift apart (#468).

    Args:
        geoms: Array of shapely geometries.
        colors: One fill colour, or one colour per geometry.
        bounds: Extent of the data in ``crs``, for the frame.
        crs: CRS of ``geoms`` and ``bounds``. None means unknown.
        output_path: Where to write the JPEG.
        config: Thumbnail configuration.

    Returns:
        True when the file was written.
    """
    params = _compute_render_params(*_profile(geoms))
    minx, miny, maxx, maxy = bounds
    if maxx <= minx and maxy <= miny:
        # One point has no extent. Give it the nominal box of _frame_bounds in
        # the source units (one degree for lon/lat) before the projection, so
        # it does not become a 1 m box in EPSG:3857.
        bounds = (minx - 0.5, miny - 0.5, maxx + 0.5, maxy + 0.5)
    projected = _to_web_mercator(geoms, bounds, crs)
    if projected is not None:
        geoms, bounds = projected

    figure, ax = _new_figure(_frame_bounds(bounds), config.max_size)
    _draw_features(ax, geoms, colors, params)
    if projected is not None:
        add_basemap(
            ax,
            config.basemap_provider,
            config.basemap_opacity,
            config.basemap_zoom_adjust,
            api_key=config.basemap_api_key,
        )
    # No bbox_inches="tight": the axes fill the figure, and a tight bbox costs
    # a second draw.
    figure.savefig(
        output_path,
        facecolor="white",
        edgecolor="none",
        pil_kwargs={"quality": config.quality},
    )
    return output_path.exists()


def _render_geometries(
    geometries: list[dict[str, Any]],
    output_path: Path,
    config: ThumbnailConfig,
    bounds: tuple[float, float, float, float] | None = None,
    style_path: Path | None = None,
) -> bool:
    """Render PMTiles geometries to a JPEG thumbnail.

    Args:
        geometries: List of geometry dicts with 'type', 'coordinates', and 'properties'.
        output_path: Where to write the JPEG.
        config: Thumbnail configuration.
        bounds: Geographic bounds (minx, miny, maxx, maxy). None takes the
            bounds of the geometries.
        style_path: Optional path to Mapbox GL style for categorical coloring.

    Returns:
        True if successful, False otherwise.
    """
    import numpy as np
    import shapely
    from shapely.geometry import shape

    # Reuse only the style's categorical fill colors (when present); opacity,
    # edge, and stroke always come from the punchy preset, never the pale
    # extracted paint (#518).
    colors: str | list[str] = THUMB_FILL_COLOR
    if style_path:
        from portolan_cli.viz.thumbnail_style import (
            load_thumbnail_style,
            resolve_color_for_properties,
        )

        style = load_thumbnail_style(style_path)
        if style and style.color_map and style.color_field:
            colors = [
                resolve_color_for_properties(
                    geom.get("properties", {}), style, fallback=THUMB_FILL_COLOR
                )
                for geom in geometries
            ]

    geoms = np.array([shape(geom) for geom in geometries], dtype=object)
    if bounds is None:
        minx, miny, maxx, maxy = (float(v) for v in shapely.total_bounds(geoms))
        bounds = (minx, miny, maxx, maxy)
    # PMTiles coordinates are always WGS84 longitude and latitude.
    return _render(geoms, colors, bounds, "OGC:CRS84", output_path, config)


# =============================================================================
# GeoParquet Reading (Internal)
# =============================================================================


def _bbox_from_geo(geo_meta: dict[str, Any]) -> tuple[float, float, float, float] | None:
    """Return the finite 2D bbox that GeoParquet metadata declares, or None.

    GeoParquet spec: columns.<geom_col>.bbox is [minx, miny, maxx, maxy] in 2D
    and [minx, miny, minz, maxx, maxy, maxz] in 3D, so to_2d_bbox does the
    reduction. The CRS may not be WGS84, and inf/nan values are rejected
    (issue #516).
    """
    from portolan_cli.bbox import is_finite_bbox, to_2d_bbox

    for col_meta in get_dict(geo_meta, "columns").values():
        bbox = col_meta.get("bbox") if isinstance(col_meta, dict) else None
        if bbox and len(bbox) >= 4:
            bbox_list = to_2d_bbox(bbox)
            if is_finite_bbox(bbox_list):
                return (bbox_list[0], bbox_list[1], bbox_list[2], bbox_list[3])
            logger.warning("Invalid bbox in GeoParquet metadata (inf/nan): %s", bbox_list)
    return None


def _read_geoparquet_bounds(source: Path | Any) -> tuple[float, float, float, float] | None:
    """Read bounding box from GeoParquet file metadata (O(1) operation).

    Falls back to computing bounds from the geometry column when the metadata
    has no bbox.

    Args:
        source: Path to a GeoParquet file, or an open ``pyarrow.parquet.ParquetFile``.

    Returns:
        Tuple of (minx, miny, maxx, maxy) or None if empty/unavailable.
    """
    from portolan_cli.formats import read_geo_metadata

    # The read goes through the raw footer: the Arrow schema hides a ``geo``
    # key the writer appended at close (issue #864).
    geo_meta = read_geo_metadata(source) or {}
    bbox = _bbox_from_geo(geo_meta)
    if bbox is not None:
        return bbox
    logger.debug("No bbox in GeoParquet metadata, falling back to data read")
    path = Path(source.reader.path) if hasattr(source, "reader") else Path(source)
    return _read_geoparquet_bounds_from_data(path, geo_meta.get("primary_column"))


def _read_geoparquet_bounds_from_data(
    gpq_path: Path, geometry_column: str | None = None
) -> tuple[float, float, float, float] | None:
    """Fallback: compute bounds by reading the geometry column.

    Validates bounds for inf/nan values (issue #516). CRS may not be WGS84.
    """
    try:
        import geopandas as gpd
    except ImportError:
        return None

    try:
        from portolan_cli.bbox import is_finite_bbox

        columns = [geometry_column] if geometry_column else None
        gdf = gpd.read_parquet(gpq_path, columns=columns, partitioning=None)
        if gdf.empty:
            return None
        bounds = gdf.total_bounds
        bbox_list = [bounds[0], bounds[1], bounds[2], bounds[3]]
        if is_finite_bbox(bbox_list):
            return (bounds[0], bounds[1], bounds[2], bounds[3])
        logger.warning("Invalid bounds from GeoParquet data (inf/nan): %s", bbox_list)
        return None
    except Exception as e:
        logger.debug("Failed to read GeoParquet bounds from data: %s", e)
        return None


#: Rows per Arrow batch when the reader takes every n-th row. A multiple of the
#: stride keeps the picks evenly spaced across batch boundaries.
_SAMPLE_BATCH_ROWS = 65_536


def _sample_rows(parquet_file: Any, columns: list[str], max_features: int) -> Any:
    """Read about ``max_features`` rows spread evenly across the file.

    Keeps every n-th row of every row group, and streams the batches, so
    memory stays bounded by one batch even for one huge row group. The read
    takes **every** row group. geoparquet-io writes spatially sorted output,
    so each row group covers one patch of the extent. The old sampler read 2
    of the 21 row groups of a 1M-polygon file and drew two strips of the data.
    Each group restarts the stride, so the count can pass ``max_features`` by
    at most one row per group.
    """
    import math

    import numpy as np
    import pyarrow as pa

    step = max(1, math.ceil(parquet_file.metadata.num_rows / max_features))
    batches = [
        batch.take(np.arange(0, batch.num_rows, step))
        for batch in parquet_file.iter_batches(
            batch_size=step * max(1, _SAMPLE_BATCH_ROWS // step), columns=columns
        )
    ]
    schema = parquet_file.schema_arrow
    return pa.Table.from_batches(batches, schema=pa.schema([schema.field(c) for c in columns]))


def _sample_geoparquet(
    source: Path | Any, max_features: int, columns: list[str] | None = None
) -> Any:
    """Read a spread-out sample of a GeoParquet too large to render whole.

    The frame is unaffected: the extent comes from file metadata in O(1), so a
    sampled render still spans the whole bbox. Only feature density drops.

    Args:
        source: Path to the file, or an open ``pyarrow.parquet.ParquetFile``.
        max_features: About how many rows to read.
        columns: Columns to read. None reads the geometry column only.

    Returns:
        GeoDataFrame of the sampled rows.
    """
    import pyarrow.parquet as pq

    count("parquet.opens", 0 if isinstance(source, pq.ParquetFile) else 1)
    parquet_file = source if isinstance(source, pq.ParquetFile) else pq.ParquetFile(str(source))
    geo_meta = json.loads((parquet_file.metadata.metadata or {})[b"geo"].decode("utf-8"))
    primary = geo_meta["primary_column"]
    table = _sample_rows(parquet_file, columns or [primary], max_features)
    return _wkb_frame(table, primary, _declared_crs(geo_meta, primary))


def _declared_crs(geo_meta: dict[str, Any], primary: str) -> Any:
    """Return the CRS that GeoParquet metadata declares for ``primary``.

    The spec reads an absent ``crs`` as OGC:CRS84 and an explicit null as an
    unknown CRS.
    """
    from pyproj import CRS

    column = get_dict(get_dict(geo_meta, "columns"), primary)
    if "crs" not in column:
        return CRS.from_user_input("OGC:CRS84")
    projjson = column["crs"]
    return CRS.from_user_input(projjson) if projjson else None


def _wkb_frame(table: Any, primary: str, crs: Any) -> Any:
    """Build a GeoDataFrame from an Arrow table whose ``primary`` column is WKB.

    A GeoParquet table is a WKB column plus `geo` file metadata, not GeoArrow
    extension types, so GeoDataFrame.from_arrow cannot read it.
    """
    import geopandas as gpd

    geometry = gpd.GeoSeries.from_wkb(table.column(primary).to_numpy(zero_copy_only=False), crs=crs)
    frame = table.drop_columns([primary]).to_pandas()
    frame[primary] = geometry
    return gpd.GeoDataFrame(frame, geometry=primary, crs=crs)


def _read_geoparquet_for_thumbnail(
    gpq_path: Path,
    max_features: int | None = None,
    color_field: str | None = None,
) -> tuple[Any, tuple[float, float, float, float] | None, Any]:
    """Read the geometry of a GeoParquet for its thumbnail.

    One ``ParquetFile`` gives the footer, the ``geo`` metadata, the bbox, and
    the rows. Only the geometry column is read, plus ``color_field`` when a
    categorical style needs it. Below ``max_features`` every row is read.
    Above it the read is sampled, so a default ``add`` cannot spend unbounded
    time on one collection (Issue #683).

    Args:
        gpq_path: Path to GeoParquet file.
        max_features: Feature ceiling. None reads every feature.
        color_field: Attribute that a categorical style colours by. Matched
            without regard to case.

    Returns:
        Tuple of (gdf, full_bbox, source_crs) where:
        - gdf: GeoDataFrame of features to draw (or None if failed)
        - full_bbox: (minx, miny, maxx, maxy) from metadata or computed
        - source_crs: CRS of the data, or None when unknown
    """
    try:
        import geopandas as gpd
        import pyarrow.parquet as pq
    except ImportError:
        logger.debug("geopandas not available")
        return None, None, None

    try:
        from portolan_cli.formats import read_geo_metadata
        from portolan_cli.viz.thumbnail_style import _find_column_case_insensitive

        count("parquet.opens")
        parquet_file = pq.ParquetFile(str(gpq_path))
        geo_meta = read_geo_metadata(parquet_file) or {}
        primary = geo_meta.get("primary_column", "geometry")
        columns = [primary]
        if color_field:
            field = _find_column_case_insensitive(parquet_file.schema_arrow.names, color_field)
            if field is not None and field != primary:
                columns.insert(0, field)
        full_bbox = _bbox_from_geo(geo_meta)
        encoding = str(get_dict(get_dict(geo_meta, "columns"), primary).get("encoding", "WKB"))
        sampled = max_features is not None and parquet_file.metadata.num_rows > max_features

        if encoding.upper() != "WKB":
            # A GeoArrow-encoded column: geopandas decodes it. Rare enough
            # that the reader does not sample it.
            gdf = gpd.read_parquet(gpq_path, columns=columns, partitioning=None)
            sampled = False
        elif sampled and max_features is not None:
            gdf = _sample_geoparquet(parquet_file, max_features, columns)
        else:
            table = parquet_file.read(columns=columns)
            gdf = _wkb_frame(table, primary, _declared_crs(geo_meta, primary))
        if gdf.empty:
            return None, None, None

        if full_bbox is None:
            # No bbox in the footer. A full read knows the exact extent. A
            # sample does not, so read the geometry column for it.
            if sampled:
                full_bbox = _read_geoparquet_bounds_from_data(gpq_path, primary)
            else:
                full_bbox = _finite_bounds(gdf)
            if full_bbox is None:
                return None, None, None

        return gdf, full_bbox, gdf.crs

    except Exception as e:
        logger.warning("Could not read %s for its thumbnail: %s", gpq_path.name, e)
        return None, None, None


def _finite_bounds(gdf: Any) -> tuple[float, float, float, float] | None:
    """Return the total bounds of ``gdf``, or None when they are not finite.

    Invalid bounds (inf/nan) would poison the frame, so the render fails fast
    rather than draw a garbage extent (issue #516).
    """
    from portolan_cli.bbox import is_finite_bbox

    bounds = gdf.total_bounds
    bbox_list = [bounds[0], bounds[1], bounds[2], bounds[3]]
    if not is_finite_bbox(bbox_list):
        logger.warning("Invalid bounds from GeoParquet data (inf/nan): %s", bbox_list)
        return None
    return (bounds[0], bounds[1], bounds[2], bounds[3])


def _render_geoparquet(
    gpq_path: Path,
    output_path: Path,
    config: ThumbnailConfig,
    style_path: Path | None = None,
) -> bool:
    """Render GeoParquet to JPEG thumbnail.

    Args:
        gpq_path: Path to GeoParquet file.
        output_path: Where to write the JPEG.
        config: Thumbnail configuration.
        style_path: Optional path to Mapbox GL style for categorical coloring.

    Returns:
        True if successful, False otherwise.
    """
    try:
        # Reuse only the style's categorical fill colors (with a punchy
        # fallback), never its pale opacity/edge paint (#518).
        style = None
        if style_path:
            from portolan_cli.viz.thumbnail_style import load_thumbnail_style

            loaded = load_thumbnail_style(style_path)
            if loaded and loaded.color_map and loaded.color_field:
                style = loaded

        gdf, full_bounds, source_crs = _read_geoparquet_for_thumbnail(
            gpq_path,
            max_features=config.max_features,
            color_field=style.color_field if style is not None else None,
        )
        if gdf is None or full_bounds is None:
            return False

        colors: Any = THUMB_FILL_COLOR
        if style is not None:
            from portolan_cli.viz.thumbnail_style import resolve_colors_for_gdf

            colors = resolve_colors_for_gdf(gdf, style, fallback=THUMB_FILL_COLOR)
            if not isinstance(colors, str):
                colors = colors.to_numpy(dtype=object)

        return _render(
            gdf.geometry.to_numpy(), colors, full_bounds, source_crs, output_path, config
        )
    except Exception as e:
        logger.debug("Failed to render GeoParquet: %s", e)
        return False


# =============================================================================
# Basemap
# =============================================================================

#: Parallel tile requests per basemap. Neither Esri (the default) nor CARTO
#: publishes a concurrency limit for its tile host. CARTO publishes only a
#: monthly quota (https://github.com/CartoDB/basemap-styles). The OpenStreetMap
#: tile policy allows at most 2 download threads
#: (https://operations.osmfoundation.org/policies/tiles/). That is the strictest
#: published limit among the providers a user can name, so it is the value for
#: every provider. Measured on this repo's dev VM, 25 CARTO tiles took 1.37 s
#: on one connection and 0.95 s on two.
_TILE_CONNECTIONS = 2

#: Tile hosts that serve a watermark tile to a request without a key.
_KEYED_TILE_HOSTS = ("basemaps.cartocdn.com",)
_warned_keyless: set[str] = set()

#: Merged tile images that the run keeps, newest last. One ``add`` renders
#: many collections, often over one extent. contextily kept the tiles in a
#: joblib disk cache, but with that cache and more than one connection it
#: fetches in worker processes. So the cache is off and this memo holds the
#: basemap image of each frame instead. 8 images of 512 x 512 RGBA take 8 MB.
_TILE_MEMO_SIZE = 8
_tile_memo: OrderedDict[tuple[Any, ...], tuple[Any, tuple[float, float, float, float]]] = (
    OrderedDict()
)
_tile_memo_lock = threading.Lock()

#: Pixels on each side of a web map tile.
_TILE_PX = 256


def _with_api_key(provider: Any, api_key: str | None) -> Any:
    """Return ``provider`` with ``api_key`` in its tile URL.

    A provider whose URL holds a token field (xyzservices marks it with
    ``<insert your ...>``) gets the key in that field. Any other provider gets
    a ``key`` query parameter, which is how CARTO reads it
    (https://www.carto.com/basemaps/apikey/). CARTO serves a watermark tile to
    a request without a key.
    """
    if not api_key:
        return provider
    token_fields = {
        name: api_key
        for name, value in provider.items()
        if isinstance(value, str) and "<insert your" in value
    }
    if token_fields:
        return provider(**token_fields)
    url = str(provider["url"])
    return provider(url=f"{url}{'&' if '?' in url else '?'}key={api_key}")


def _needs_key(name: str, provider: Any) -> bool:
    """True when ``provider`` serves only a watermark without a key.

    The first such render of a run logs one warning. The thumbnail then gets
    no basemap, because a watermark tile is never the map that the user wants.
    """
    url = str(provider.get("url", ""))
    if not any(host in url for host in _KEYED_TILE_HOSTS):
        return False
    with _tile_memo_lock:
        first = name not in _warned_keyless
        _warned_keyless.add(name)
    if first:
        logger.warning(
            "Basemap provider %s needs an API key, and without one it serves a "
            "watermark tile. Thumbnails get no basemap. Set thumbnails.basemap.api_key "
            "in .portolan/config.yaml or the PORTOLAN_THUMBNAILS_BASEMAP_API_KEY "
            "environment variable.",
            name,
        )
    return True


def _basemap_zoom(ax: Axes, provider: Any, zoom_adjust: int) -> int:
    """Pick the lowest zoom whose tiles give one tile pixel per output pixel.

    The figure has a fixed pixel size, so the zoom follows from the metres per
    pixel of the frame. One level lower would blur the basemap, and one level
    higher would fetch about four times the tiles.
    """
    import math

    left, right = ax.get_xlim()
    bottom, top = ax.get_ylim()
    width_px, height_px = ax.bbox.width, ax.bbox.height
    world = 2 * _MERCATOR_LIMIT
    needed = max(width_px / (right - left), height_px / (top - bottom)) * world / _TILE_PX
    zoom = math.ceil(math.log2(needed) - 1e-9) + zoom_adjust
    return int(min(max(zoom, provider.get("min_zoom", 0)), provider.get("max_zoom", 22)))


def _fetch_basemap(
    ctx_module: Any,
    provider: Any,
    frame: tuple[float, float, float, float],
    zoom: int,
    size_px: tuple[int, int],
) -> tuple[Any, tuple[float, float, float, float]]:
    """Return the basemap of ``frame`` (EPSG:3857) at the output resolution.

    The merged tiles are cut to the frame and resized once, with Pillow. Then
    the draw copies pixels and does not resample a 1280 px tile mosaic on each
    render, which took half of a 85 ms collection thumbnail. The result is
    memoized for the run.

    Returns:
        An RGBA array, and its extent (left, right, bottom, top). The extent is
        smaller than the frame when the frame passes the edge of the tile
        world, as a frame around global data does.
    """
    key = (provider.build_url(), zoom, *frame, *size_px)
    with _tile_memo_lock:
        hit = _tile_memo.get(key)
        if hit is not None:
            _tile_memo.move_to_end(key)
            return hit

    import numpy as np
    from PIL import Image

    left, bottom, right, top = frame
    mosaic, (img_left, img_right, img_bottom, img_top) = ctx_module.bounds2img(
        left,
        bottom,
        right,
        top,
        zoom=zoom,
        source=provider,
        ll=False,
        n_connections=_TILE_CONNECTIONS,
        use_cache=False,
    )
    count("tiles.fetched", ctx_module.howmany(left, bottom, right, top, zoom, verbose=False))
    # Tile pixels are linear in EPSG:3857, so the covered part of the frame
    # maps to a pixel box of the mosaic.
    cut_left, cut_right = max(left, img_left), min(right, img_right)
    cut_bottom, cut_top = max(bottom, img_bottom), min(top, img_top)
    height, width = mosaic.shape[:2]
    x_scale = width / (img_right - img_left)
    y_scale = height / (img_top - img_bottom)
    box = (
        (cut_left - img_left) * x_scale,
        (img_top - cut_top) * y_scale,
        (cut_right - img_left) * x_scale,
        (img_top - cut_bottom) * y_scale,
    )
    out_size = (
        max(1, round(size_px[0] * (cut_right - cut_left) / (right - left))),
        max(1, round(size_px[1] * (cut_top - cut_bottom) / (top - bottom))),
    )
    fitted = Image.fromarray(mosaic).convert("RGBA")
    image = np.asarray(fitted.resize(out_size, Image.Resampling.BILINEAR, box=box))
    result = (image, (cut_left, cut_right, cut_bottom, cut_top))
    with _tile_memo_lock:
        _tile_memo[key] = result
        while len(_tile_memo) > _TILE_MEMO_SIZE:
            _tile_memo.popitem(last=False)
    return result


def add_basemap(
    ax: Axes,
    provider: str,
    opacity: float = 1.0,
    zoom_adjust: int = 0,
    api_key: str | None = None,
) -> None:
    """Draw basemap tiles under the data of an EPSG:3857 axes.

    The tiles are fetched with ``contextily.bounds2img`` on
    ``_TILE_CONNECTIONS`` connections and drawn with ``imshow``. The
    attribution is an ``ax.text``. contextily's ``add_attribution`` drew the
    whole figure once more to wrap the text.

    Args:
        ax: Matplotlib Axes in EPSG:3857, with its limits set to the frame.
        provider: Contextily provider name (e.g., 'Esri.WorldGrayCanvas').
            Pass 'none' to skip basemap.
        opacity: Basemap opacity 0.0-1.0.
        zoom_adjust: Zoom level adjustment.
        api_key: Key for the tile provider. CARTO needs one.
    """
    if provider == "none":
        return

    ctx_module = _ensure_contextily()
    if ctx_module is None:
        return

    try:
        tile_provider = ctx_module.providers
        for part in provider.split("."):
            tile_provider = getattr(tile_provider, part)
    except AttributeError:
        logger.warning(
            "Unknown basemap provider '%s'. Valid examples: Esri.WorldGrayCanvas, "
            "CartoDB.Positron (with thumbnails.basemap.api_key), OpenStreetMap.Mapnik",
            provider,
        )
        return
    if not api_key and _needs_key(provider, tile_provider):
        return

    try:
        tile_provider = _with_api_key(tile_provider, api_key)
        left, right = ax.get_xlim()
        bottom, top = ax.get_ylim()
        zoom = _basemap_zoom(ax, tile_provider, zoom_adjust)
        size_px = (round(ax.bbox.width), round(ax.bbox.height))
        frame = (left, bottom, right, top)
        image, extent = _fetch_basemap(ctx_module, tile_provider, frame, zoom, size_px)
    except Exception as e:
        logger.debug("Failed to add basemap '%s': %s", provider, e)
        return

    from matplotlib import patheffects

    # zorder 0 puts the tiles under the data patches (zorder 1).
    ax.imshow(
        image,
        extent=extent,
        # One image pixel per output pixel, so no smoothing is needed.
        interpolation="nearest",
        # "auto" keeps the axes box. The default "equal" would shrink it by a
        # rounding error and leave a white line at the edge.
        aspect="auto",
        alpha=opacity,
        zorder=0,
    )
    ax.set_xlim(left, right)
    ax.set_ylim(bottom, top)
    attribution = tile_provider.get("attribution")
    if attribution:
        ax.text(
            0.005,
            0.005,
            attribution,
            transform=ax.transAxes,
            size=8,
            path_effects=[patheffects.withStroke(linewidth=2, foreground="w")],
            wrap=True,
        )


# =============================================================================
# Public API
# =============================================================================


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
    thumb_path = thumbnail_path_for(pmtiles_path)

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

    Args:
        gpq_path: Path to source GeoParquet file.
        config: Thumbnail configuration.
        style_path: Optional path to Mapbox GL style for categorical coloring.

    Returns:
        Path to generated thumbnail, or None if generation failed.
    """
    thumb_path = thumbnail_path_for(gpq_path)
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
    """Generate thumbnail for vector data, preferring the GeoParquet source.

    Orchestrator function: renders from the GeoParquet when it is locally
    available, falling back to decoding PMTiles. The parquet carries exact
    geometry; PMTiles at thumbnail zoom carry tippecanoe's simplification and
    its tiny-polygon placeholder squares, so datasets whose features are small
    at collection scale (parcels, forests, POI clusters) rendered as floating
    rectangles instead of their real shapes (issue #786). Tiles remain the
    right source when no local parquet exists (e.g. remote data).

    Args:
        pmtiles_path: Path to PMTiles file (optional, used as fallback).
        geoparquet_path: Path to GeoParquet file (optional, preferred).
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

    # Prefer the exact geometry: render from the local GeoParquet
    if geoparquet_path is not None and geoparquet_path.exists():
        result = generate_thumbnail_from_geoparquet(geoparquet_path, config, style_path)
        if result is not None:
            return result
        logger.debug("GeoParquet thumbnail failed, falling back to PMTiles")

    # Fall back to PMTiles (simplified tile geometry)
    if pmtiles_path is not None:
        return generate_thumbnail_from_pmtiles(pmtiles_path, config, style_path)

    return None
