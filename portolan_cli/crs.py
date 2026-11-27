"""CRS transformation utilities."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import antimeridian
from pyproj import CRS, Transformer
from pyproj.exceptions import CRSError

from portolan_core.errors import CRSMismatchError, CRSTransformError
from portolan_core.trace import count

if TYPE_CHECKING:
    from pathlib import Path
from portolan_core.bbox import LAT_MAX, LAT_MIN, LON_MAX, LON_MIN

_WGS84_EPSG = 4326

logger = logging.getLogger(__name__)


@dataclass
class CRSMismatchWarning:
    """Warning about CRS mismatch between declared CRS and actual coordinates."""

    declared_crs: str
    likely_actual_crs: str
    message: str
    bbox: tuple[float, float, float, float]


# WGS84 CRS (cached for reuse)
WGS84 = CRS.from_epsg(4326)

# Number of points to sample along each edge for accurate transformation
# Higher values = more accurate for curved projections, but slower
EDGE_SAMPLE_POINTS = 10


def describe_crs(crs: str | dict[str, Any] | None) -> str:
    """Return a short label for a CRS, for use in a message."""
    if crs is None:
        return "no CRS"
    if isinstance(crs, dict):
        name = crs.get("name")
        if isinstance(name, str) and name:
            return name
        return "a PROJJSON CRS"
    return str(crs)


def transform_bbox_to_wgs84(
    bbox: tuple[float, float, float, float],
    source_crs: str | dict[str, Any] | None,
    *,
    allow_guess: bool = False,
) -> tuple[float, float, float, float]:
    """Transform bbox from source CRS to WGS84 with antimeridian handling."""
    # Validate input bbox for inf/nan
    import math

    if not all(math.isfinite(c) for c in bbox):
        raise ValueError(f"Input bbox contains non-finite values (inf/nan): {bbox}")

    if source_crs is None:
        # No CRS means assume WGS84, but out-of-range coordinates disprove it: fail fast.
        _reject_non_wgs84_magnitudes(bbox, "no CRS (assumed WGS84)", allow_guess)
        return _snapped(bbox)

    label = describe_crs(source_crs)

    # An unreadable CRS or failed transform must not pass the bbox off as WGS84.
    try:
        src_crs = CRS.from_user_input(source_crs)
    except (CRSError, TypeError) as e:
        # CRS.from_user_input can raise TypeError for invalid input types
        raise CRSTransformError(label, str(e)) from e

    # Check if already WGS84
    if _is_wgs84(src_crs):
        _reject_non_wgs84_magnitudes(bbox, label, allow_guess)
        return _snapped(bbox)

    # Check for CRS mismatch BEFORE attempting transformation
    # This catches the common bug where data is WGS84 but labeled as a projected CRS
    mismatch = validate_bbox_crs(bbox, source_crs)
    if mismatch:
        if allow_guess:
            logger.warning(
                "CRS mismatch detected: %s declares %s but coordinates %s appear to be WGS84. "
                "Returning bbox unchanged (treating as WGS84).",
                label,
                label,
                bbox,
            )
            return bbox
        raise CRSMismatchError(
            source_crs=label,
            bbox=bbox,
            likely_actual_crs=mismatch.likely_actual_crs,
        )

    # Transform bbox to WGS84 polygon and compute RFC 7946 compliant bbox
    try:
        return _snapped(_transform_and_compute_bbox(bbox, src_crs))
    except Exception as e:
        # Catch transformation errors (e.g., coordinates outside projection bounds)
        raise CRSTransformError(label, str(e)) from e


def _snapped(bbox: tuple[float, float, float, float]) -> tuple[float, float, float, float]:
    """Pull a coordinate that rounds past a WGS84 bound back onto it."""
    from portolan_core.bbox import snap_to_wgs84_range

    west, south, east, north = snap_to_wgs84_range(list(bbox))
    return (west, south, east, north)


def _reject_non_wgs84_magnitudes(
    bbox: tuple[float, float, float, float],
    declared: str,
    allow_guess: bool,
) -> None:
    """Fail fast when a bbox treated as WGS84 is outside lon/lat range."""
    from portolan_core.bbox import WGS84_EDGE_TOLERANCE

    minx, miny, maxx, maxy = bbox
    lon_limit = 180 + WGS84_EDGE_TOLERANCE
    lat_limit = 90 + WGS84_EDGE_TOLERANCE
    if (
        abs(minx) <= lon_limit
        and abs(maxx) <= lon_limit
        and abs(miny) <= lat_limit
        and abs(maxy) <= lat_limit
    ):
        return
    # Infinite sentinels are not projected coordinates; leave them to invalid-bbox filtering.
    from portolan_core.bbox import MAX_SANE_COORD

    if any(abs(c) > MAX_SANE_COORD for c in bbox):
        return
    if allow_guess:
        logger.warning(
            "Bbox %s treated as WGS84 (%s) but coordinates exceed lon/lat range; "
            "passing through unchanged.",
            bbox,
            declared,
        )
        return
    raise CRSMismatchError(
        source_crs=declared,
        bbox=bbox,
        likely_actual_crs="a projected CRS (values exceed lon/lat range)",
    )


def _is_wgs84(crs: CRS) -> bool:
    """Check if a CRS is WGS84 (EPSG:4326 or CRS84)."""
    epsg = crs.to_epsg()
    if epsg == _WGS84_EPSG:
        return True

    # Check for CRS84 by name (pyproj recognizes it)
    crs_name = crs.name.lower() if crs.name else ""
    # Verify it's actually a geographic CRS (not just a name match)
    if ("crs84" in crs_name or "wgs 84" in crs_name or "wgs84" in crs_name) and crs.is_geographic:
        return True

    # Also check by comparing CRS objects (handles WKT inputs)
    return crs.equals(WGS84)


def _transform_and_compute_bbox(
    bbox: tuple[float, float, float, float],
    src_crs: CRS,
) -> tuple[float, float, float, float]:
    """Transform bbox and compute RFC 7946 compliant WGS84 bbox."""
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


def validate_bbox_crs(
    bbox: tuple[float, float, float, float],
    crs: str | dict[str, Any] | None,
) -> CRSMismatchWarning | None:
    """Validate that bbox coordinates match declared CRS."""
    # No CRS declared = assumed WGS84, no validation needed
    if crs is None:
        return None

    # Check if CRS is WGS84 or another geographic CRS (no mismatch possible)
    try:
        parsed_crs = CRS.from_user_input(crs)
        if _is_wgs84(parsed_crs):
            return None
        # Geographic CRSes like ETRS89 (EPSG:4258) naturally have coordinates
        # in the same lon/lat range as WGS84 — not a mismatch.
        if parsed_crs.is_geographic:
            return None
    except (CRSError, TypeError):
        # Invalid CRS, can't validate
        return None

    # CRS is projected - check if coordinates look like WGS84
    if is_likely_wgs84_bbox(bbox):
        label = describe_crs(crs)
        return CRSMismatchWarning(
            declared_crs=label,
            likely_actual_crs="EPSG:4326",
            message=(
                f"CRS mismatch detected: file declares {label} but coordinates "
                f"({bbox[0]:.2f}, {bbox[1]:.2f}, {bbox[2]:.2f}, {bbox[3]:.2f}) "
                "appear to be WGS84 (EPSG:4326). This often happens when ArcGIS "
                "services return WGS84 data but declare the original projection."
            ),
            bbox=bbox,
        )

    # No mismatch detected
    return None


def is_likely_wgs84_bbox(bbox: tuple[float, float, float, float]) -> bool:
    """Detect if bbox coordinates look like WGS84 (lon/lat)."""
    minx, miny, maxx, maxy = bbox

    # Check latitude bounds (y coordinates must be -90 to 90)
    if not (LAT_MIN <= miny <= LAT_MAX and LAT_MIN <= maxy <= LAT_MAX):
        return False

    # Check longitude bounds (x coordinates must be -180 to 180)
    # Note: for antimeridian crossing, minx > maxx is valid
    return LON_MIN <= minx <= LON_MAX and LON_MIN <= maxx <= LON_MAX


def _sample_bbox_edges(
    minx: float,
    miny: float,
    maxx: float,
    maxy: float,
    n_points: int,
) -> list[tuple[float, float]]:
    """Sample points along bbox edges for accurate transformation."""
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


@dataclass
class _MeasuredBounds:
    """Running WGS84 bounds, with longitude tracked in two frames."""

    minx: float = float("inf")
    miny: float = float("inf")
    maxx: float = float("-inf")
    maxy: float = float("-inf")
    min_shifted: float = float("inf")
    max_shifted: float = float("-inf")

    def update(self, lon: Any, lat: Any, *, np: Any) -> None:
        self.minx, self.maxx = min(self.minx, float(lon.min())), max(self.maxx, float(lon.max()))
        self.miny, self.maxy = min(self.miny, float(lat.min())), max(self.maxy, float(lat.max()))
        shifted = np.where(lon < 0.0, lon + 360.0, lon)
        self.min_shifted = min(self.min_shifted, float(shifted.min()))
        self.max_shifted = max(self.max_shifted, float(shifted.max()))

    def is_finite(self, math: Any) -> bool:
        return all(math.isfinite(v) for v in (self.minx, self.miny, self.maxx, self.maxy))

    def plain_span(self) -> float:
        return self.maxx - self.minx

    def shifted_span(self) -> float:
        return self.max_shifted - self.min_shifted


def measure_wgs84_bbox(
    data_path: Path,
    geometry_column: str,
    source_crs: str | dict[str, Any] | None,
) -> tuple[float, float, float, float] | None:
    """Measure a GeoParquet's WGS84 extent from its geometry, not its bbox."""
    if not source_crs:
        return None
    try:
        src = CRS.from_user_input(source_crs)
    except (CRSError, TypeError):
        logger.debug("Cannot measure bbox: unresolvable CRS %s", describe_crs(source_crs))
        return None
    if _is_wgs84(src):
        return None  # stored coordinates are already WGS84; nothing to measure

    files = sorted(data_path.rglob("*.parquet")) if data_path.is_dir() else [data_path]
    if not files:
        return None

    bounds = _stream_wgs84_bounds(files, geometry_column, src, data_path)
    if bounds is None:
        return None
    return _snapped(_bbox_from_bounds(bounds))


def _stream_wgs84_bounds(
    files: list[Path],
    geometry_column: str,
    src: CRS,
    data_path: Path,
) -> _MeasuredBounds | None:
    """Accumulate WGS84 bounds over the geometry column, one row group at a time."""
    try:
        import math

        import numpy as np
        import pyarrow.parquet as pq
        import shapely
    except ImportError:  # pragma: no cover - shapely/pyarrow are core deps
        logger.debug("Cannot measure bbox: pyarrow/shapely unavailable")
        return None

    bounds = _MeasuredBounds()
    try:
        # Inside the guard: pyproj raises here when it can build no pipeline
        # between the two CRSs, and that must fall back, not escape.
        transformer = Transformer.from_crs(src, WGS84, always_xy=True)
        for file in files:
            count("parquet.opens")
            parquet = pq.ParquetFile(file)
            if geometry_column not in parquet.schema_arrow.names:
                return None
            for batch in parquet.iter_batches(columns=[geometry_column]):
                wkb = [value for value in batch.column(0).to_pylist() if value is not None]
                if not wkb:
                    continue
                coords = shapely.get_coordinates(shapely.from_wkb(wkb))
                if coords.size == 0:
                    continue
                lon, lat = transformer.transform(coords[:, 0], coords[:, 1])
                # Points pyproj cannot transform come back as infinity; drop them
                # rather than let one poison the whole extent.
                finite = np.isfinite(lon) & np.isfinite(lat)
                if not finite.any():
                    continue
                bounds.update(lon[finite], lat[finite], np=np)
    except Exception as exc:  # measurement is best-effort
        logger.debug("Cannot measure bbox from %s: %s", data_path, exc)
        return None

    return bounds if bounds.is_finite(math) else None


def _bbox_from_bounds(bounds: _MeasuredBounds) -> tuple[float, float, float, float]:
    """Resolve accumulated bounds to a bbox, honouring an antimeridian crossing."""
    if bounds.shifted_span() < bounds.plain_span():
        west = bounds.min_shifted - 360.0 if bounds.min_shifted >= LON_MAX else bounds.min_shifted
        east = bounds.max_shifted - 360.0 if bounds.max_shifted > LON_MAX else bounds.max_shifted
        return (west, bounds.miny, east, bounds.maxy)
    return (bounds.minx, bounds.miny, bounds.maxx, bounds.maxy)
