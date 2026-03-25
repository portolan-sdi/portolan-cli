"""CRS transformation utilities.

Provides functions for transforming coordinates between coordinate reference systems,
with a focus on STAC's requirement for WGS84 (EPSG:4326) bounding boxes.
"""

from __future__ import annotations

from pyproj import CRS, Transformer


def transform_bbox_to_wgs84(
    bbox: tuple[float, float, float, float],
    source_crs: str | None,
) -> tuple[float, float, float, float]:
    """Transform bbox from source CRS to WGS84.

    STAC requires Item bbox and geometry to be in WGS84 (EPSG:4326) per RFC 7946.
    This function transforms a bbox from any source CRS to WGS84.

    Args:
        bbox: Bounding box as (minx, miny, maxx, maxy) in source CRS.
        source_crs: Source CRS as EPSG code (e.g., "EPSG:32610") or WKT string.
                   If None, returns bbox unchanged (assumed WGS84).

    Returns:
        Bounding box as (minx, miny, maxx, maxy) in WGS84.
    """
    if source_crs is None:
        return bbox

    # Parse source CRS
    try:
        src_crs = CRS.from_user_input(source_crs)
    except Exception:
        # If we can't parse the CRS, return unchanged
        return bbox

    # Check if already WGS84
    epsg = src_crs.to_epsg()
    if epsg == 4326:
        return bbox

    # Also check by comparing to WGS84 CRS object for WKT inputs
    wgs84 = CRS.from_epsg(4326)
    if src_crs.equals(wgs84):
        return bbox

    # Create transformer
    transformer = Transformer.from_crs(src_crs, wgs84, always_xy=True)

    # Transform all four corners to handle projection distortion
    minx, miny, maxx, maxy = bbox
    corners_x = [minx, maxx, minx, maxx]
    corners_y = [miny, miny, maxy, maxy]

    xs, ys = transformer.transform(corners_x, corners_y)

    # Compute new bbox from transformed corners
    return (min(xs), min(ys), max(xs), max(ys))
