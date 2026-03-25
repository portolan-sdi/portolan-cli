"""CRS transformation utilities.

Provides functions for transforming coordinates between coordinate reference systems,
with a focus on STAC's requirement for WGS84 (EPSG:4326) bounding boxes.

Handles antimeridian crossings per RFC 7946: when a bbox spans the antimeridian,
the western bound (minx) will be greater than the eastern bound (maxx).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import antimeridian
from pyproj import CRS, Transformer
from pyproj.exceptions import CRSError

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# WGS84 CRS (cached for reuse)
WGS84 = CRS.from_epsg(4326)

# Number of points to sample along each edge for accurate transformation
# Higher values = more accurate for curved projections, but slower
EDGE_SAMPLE_POINTS = 10


def transform_bbox_to_wgs84(
    bbox: tuple[float, float, float, float],
    source_crs: str | None,
) -> tuple[float, float, float, float]:
    """Transform bbox from source CRS to WGS84 with antimeridian handling.

    STAC requires Item bbox and geometry to be in WGS84 (EPSG:4326) per RFC 7946.
    This function transforms a bbox from any source CRS to WGS84, properly handling
    antimeridian crossings.

    For antimeridian-crossing bboxes, returns (west, south, east, north) where
    west > east per RFC 7946 Section 5.2.

    Args:
        bbox: Bounding box as (minx, miny, maxx, maxy) in source CRS.
        source_crs: Source CRS as EPSG code (e.g., "EPSG:32610") or WKT string.
                   If None, returns bbox unchanged (assumed WGS84).

    Returns:
        Bounding box as (west, south, east, north) in WGS84.
        For antimeridian-crossing bboxes, west > east.

    Raises:
        No exceptions raised - returns original bbox with warning on CRS errors.
    """
    if source_crs is None:
        return bbox

    # Parse source CRS with specific exception handling
    try:
        src_crs = CRS.from_user_input(source_crs)
    except CRSError as e:
        logger.warning("Could not parse CRS '%s': %s. Returning bbox unchanged.", source_crs, e)
        return bbox
    except TypeError as e:
        # CRS.from_user_input can raise TypeError for invalid input types
        logger.warning("Invalid CRS type '%s': %s. Returning bbox unchanged.", type(source_crs), e)
        return bbox

    # Check if already WGS84
    if _is_wgs84(src_crs):
        return bbox

    # Transform bbox to WGS84 polygon and compute RFC 7946 compliant bbox
    try:
        return _transform_and_compute_bbox(bbox, src_crs)
    except Exception as e:
        # Catch transformation errors (e.g., coordinates outside projection bounds)
        logger.warning(
            "CRS transformation failed for bbox %s from %s: %s. Returning bbox unchanged.",
            bbox,
            source_crs,
            e,
        )
        return bbox


def _is_wgs84(crs: CRS) -> bool:
    """Check if a CRS is WGS84 (EPSG:4326)."""
    epsg = crs.to_epsg()
    if epsg == 4326:
        return True
    # Also check by comparing CRS objects (handles WKT inputs)
    return crs.equals(WGS84)


def _transform_and_compute_bbox(
    bbox: tuple[float, float, float, float],
    src_crs: CRS,
) -> tuple[float, float, float, float]:
    """Transform bbox and compute RFC 7946 compliant WGS84 bbox.

    Samples points along bbox edges for accuracy with curved projections,
    then uses the antimeridian library to compute a proper bbox that handles
    antimeridian crossings.
    """
    minx, miny, maxx, maxy = bbox

    # Create transformer (always_xy ensures lon/lat order)
    transformer = Transformer.from_crs(src_crs, WGS84, always_xy=True)

    # Sample points along all four edges for accuracy with curved projections
    edge_points = _sample_bbox_edges(minx, miny, maxx, maxy, EDGE_SAMPLE_POINTS)

    # Transform all points to WGS84
    src_x = [p[0] for p in edge_points]
    src_y = [p[1] for p in edge_points]
    wgs84_x, wgs84_y = transformer.transform(src_x, src_y)

    # Build a polygon from the transformed points (closed ring)
    ring = list(zip(wgs84_x, wgs84_y, strict=True))
    ring.append(ring[0])  # Close the ring

    geojson_polygon: dict[str, object] = {
        "type": "Polygon",
        "coordinates": [ring],
    }

    # Fix antimeridian crossings if present
    fixed_geojson = antimeridian.fix_geojson(geojson_polygon)

    # Compute RFC 7946 compliant bbox (handles antimeridian crossing)
    bbox_list = antimeridian.bbox(fixed_geojson)

    return (bbox_list[0], bbox_list[1], bbox_list[2], bbox_list[3])


def _sample_bbox_edges(
    minx: float,
    miny: float,
    maxx: float,
    maxy: float,
    n_points: int,
) -> list[tuple[float, float]]:
    """Sample points along bbox edges for accurate transformation.

    Returns points in order: bottom edge → right edge → top edge → left edge.
    This forms a closed ring when the first point is appended at the end.
    """
    points: list[tuple[float, float]] = []

    # Bottom edge (left to right)
    for i in range(n_points):
        t = i / n_points
        x = minx + t * (maxx - minx)
        points.append((x, miny))

    # Right edge (bottom to top)
    for i in range(n_points):
        t = i / n_points
        y = miny + t * (maxy - miny)
        points.append((maxx, y))

    # Top edge (right to left)
    for i in range(n_points):
        t = i / n_points
        x = maxx - t * (maxx - minx)
        points.append((x, maxy))

    # Left edge (top to bottom)
    for i in range(n_points):
        t = i / n_points
        y = maxy - t * (maxy - miny)
        points.append((minx, y))

    return points
