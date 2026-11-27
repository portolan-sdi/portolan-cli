"""Bounding box validation, WGS84 range checks, antimeridian splits, and unions.

Inf and NaN coordinates poison a union, so every check here rejects them first.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

# WGS84 coordinate bounds
LON_MIN = -180.0
LON_MAX = 180.0
LAT_MIN = -90.0
LAT_MAX = 90.0

# Bbox lengths. 2D is [west, south, east, north]. 3D adds min and max elevation.
BBOX_2D_LEN = 4
BBOX_3D_LEN = 6

# Whole-world extents land on 180.00000000000006 after reprojection; 1e-6 degrees
# (about 10 cm) absorbs that but no projected coordinate.
WGS84_EDGE_TOLERANCE = 1e-6

# No real coordinate in any CRS nears 1e9; larger values are sentinels such as
# WFS-served ±1.79e308.
MAX_SANE_COORD = 1e9


def _snap(value: float, low: float, high: float) -> float:
    """Return the nearer bound when ``value`` sits just outside [low, high]."""
    if low - WGS84_EDGE_TOLERANCE <= value < low:
        return low
    if high < value <= high + WGS84_EDGE_TOLERANCE:
        return high
    return value


def snap_to_wgs84_range(bbox: list[float]) -> list[float]:
    """Pull each coordinate that rounds past a WGS84 bound back onto it.

    Values further out than :data:`WGS84_EDGE_TOLERANCE`, and elevations, pass through unchanged.
    """
    snapped = list(bbox)
    for i, (low, high) in enumerate(_wgs84_bounds(len(bbox))):
        snapped[i] = _snap(snapped[i], low, high)
    return snapped


def _wgs84_bounds(length: int) -> list[tuple[float, float]]:
    """The lon/lat bound pairs for each position of a 4- or 6-element bbox."""
    if length == BBOX_3D_LEN:
        # Order: west, south, min elevation, east, north, max elevation.
        return [
            (LON_MIN, LON_MAX),
            (LAT_MIN, LAT_MAX),
            (-math.inf, math.inf),
            (LON_MIN, LON_MAX),
            (LAT_MIN, LAT_MAX),
            (-math.inf, math.inf),
        ]
    return [(LON_MIN, LON_MAX), (LAT_MIN, LAT_MAX), (LON_MIN, LON_MAX), (LAT_MIN, LAT_MAX)]


@dataclass
class BboxValidationResult:
    """Result of filtering a list of bboxes for validity."""

    valid: list[list[float]]
    invalid: list[tuple[list[float], str]]  # (bbox, reason)

    @property
    def has_valid(self) -> bool:
        """Check if any valid bboxes exist."""
        return len(self.valid) > 0


@dataclass
class BboxUnionResult:
    """Result of computing a bbox union."""

    bbox: list[float] | None
    """The overall envelope, or None if no input is valid."""

    bboxes: list[list[float]] | None = None
    """The STAC 1.1.0 ``extent.spatial.bbox`` list, or None if no input is valid.

    One bbox, or the envelope then the western and eastern parts; never two entries.
    """

    skipped: list[tuple[list[float], str]] = field(default_factory=list)
    """Bboxes that were skipped due to validation failures."""


def to_2d_bbox(bbox: Sequence[float]) -> list[float]:
    """Reduce a STAC bbox to ``[west, south, east, north]`` using indices 0, 1, 3, 4 for 3D.

    ``bbox[:4]`` is wrong for 3D bboxes; other lengths pass through as ``list(bbox[:4])``.
    """
    if len(bbox) == BBOX_3D_LEN:
        return [bbox[0], bbox[1], bbox[3], bbox[4]]
    return list(bbox[:4])


def is_finite_bbox(bbox: list[float]) -> bool:
    """Return True if a bbox has 4 or 6 elements, all finite, in any CRS.

    Use :func:`is_valid_bbox` for the WGS84 range checks.
    """
    if len(bbox) not in (4, 6):
        return False

    return all(math.isfinite(c) for c in bbox)


def is_valid_bbox(bbox: list[float], *, wgs84_only: bool = True) -> bool:
    """Return True if a bbox is finite and, with ``wgs84_only``, in WGS84 range with south <= north.

    West > east is valid: RFC 7946 uses it for antimeridian-crossing bboxes.
    """
    # Check finite first (universal for all CRS)
    if not is_finite_bbox(bbox):
        return False

    # For non-WGS84 bboxes, finite check is sufficient
    if not wgs84_only:
        return True

    # Snap first so a coordinate such as 180.00000000000006 passes the range check.
    west, south, east, north = to_2d_bbox(snap_to_wgs84_range(bbox))

    # Check longitude range (WGS84 only)
    if not (LON_MIN <= west <= LON_MAX and LON_MIN <= east <= LON_MAX):
        return False

    # Check latitude range (WGS84 only)
    if not (LAT_MIN <= south <= LAT_MAX and LAT_MIN <= north <= LAT_MAX):
        return False

    # Check latitude ordering (south must be <= north)
    return not south > north


def _reason_non_finite(bbox: list[float]) -> str | None:
    """Return the reason a bbox holds a NaN or an infinite value, or None."""
    west, south, east, north = to_2d_bbox(bbox)
    for name, val in [("west", west), ("south", south), ("east", east), ("north", north)]:
        if math.isnan(val):
            return f"{name} is NaN"
        if math.isinf(val):
            return f"{name} is infinite ({val})"

    # Check elevation values too, so a poisoned Z value does not pass.
    for i in range(4, len(bbox)):
        if math.isnan(bbox[i]):
            return f"elevation coordinate at index {i} is NaN"
        if math.isinf(bbox[i]):
            return f"elevation coordinate at index {i} is infinite ({bbox[i]})"
    return None


def _reason_out_of_wgs84_range(bbox: list[float]) -> str | None:
    """Return the reason a bbox falls outside the WGS84 ranges, or None."""
    west, south, east, north = to_2d_bbox(bbox)
    if not (LON_MIN <= west <= LON_MAX):
        return f"west longitude {west} out of range [{LON_MIN}, {LON_MAX}]"
    if not (LON_MIN <= east <= LON_MAX):
        return f"east longitude {east} out of range [{LON_MIN}, {LON_MAX}]"
    if not (LAT_MIN <= south <= LAT_MAX):
        return f"south latitude {south} out of range [{LAT_MIN}, {LAT_MAX}]"
    if not (LAT_MIN <= north <= LAT_MAX):
        return f"north latitude {north} out of range [{LAT_MIN}, {LAT_MAX}]"
    if south > north:
        return f"south ({south}) > north ({north})"
    return None


def get_bbox_validation_reason(bbox: list[float], *, wgs84_only: bool = True) -> str | None:
    """Return the reason a bbox is invalid, or None if valid.

    Set ``wgs84_only`` to False for projected bboxes, where only finiteness and magnitude apply.
    """
    if len(bbox) not in (4, 6):
        return f"wrong element count: {len(bbox)} (expected 4 or 6)"

    non_finite = _reason_non_finite(bbox)
    if non_finite is not None:
        return non_finite

    # Reject a finite sentinel such as 1.79e308, also for projected bboxes.
    for i, val in enumerate(bbox):
        if abs(val) > MAX_SANE_COORD:
            return f"coordinate at index {i} ({val}) exceeds sane magnitude"

    if wgs84_only:
        return _reason_out_of_wgs84_range(snap_to_wgs84_range(bbox))

    return None


def is_antimeridian_crossing(bbox: list[float]) -> bool:
    """Return True if a bbox crosses the antimeridian, that is west > east (RFC 7946).

    Example: Fiji ``[177, -20, -175, -15]``.
    """
    if len(bbox) < BBOX_2D_LEN:
        return False
    reduced = to_2d_bbox(bbox)
    west, east = reduced[0], reduced[2]
    return west > east


def normalize_antimeridian_bbox(bbox: list[float]) -> list[list[float]]:
    """Split an antimeridian-crossing bbox into west-to-180 and -180-to-east parts.

    A bbox that does not cross returns as a one-element list.
    """
    if not is_antimeridian_crossing(bbox):
        return [to_2d_bbox(bbox)]

    west, south, east, north = to_2d_bbox(bbox)

    # Split into western and eastern parts
    western_part = [west, south, LON_MAX, north]  # West to 180°
    eastern_part = [LON_MIN, south, east, north]  # -180° to east

    return [western_part, eastern_part]


def filter_valid_bboxes(
    bboxes: list[list[float]],
    *,
    wgs84_only: bool = True,
) -> BboxValidationResult:
    """Split bboxes into valid (snapped) and invalid with reasons.

    Set ``wgs84_only`` to False for projected bboxes, where only finiteness applies.
    """
    valid: list[list[float]] = []
    invalid: list[tuple[list[float], str]] = []

    for bbox in bboxes:
        reason = get_bbox_validation_reason(bbox, wgs84_only=wgs84_only)
        if reason is None:
            # The reason was judged on the snapped bbox, so keep that one. The
            # raw value otherwise reaches the union and the collection extent.
            valid.append(snap_to_wgs84_range(bbox) if wgs84_only else bbox)
        else:
            invalid.append((bbox, reason))

    return BboxValidationResult(valid=valid, invalid=invalid)


def compute_bbox_union(
    bboxes: list[list[float]],
    *,
    collection_ids: list[str] | None = None,
    wgs84_only: bool = True,
) -> BboxUnionResult:
    """Union the valid bboxes into a STAC extent, splitting at the antimeridian when needed.

    Set ``wgs84_only`` to False for projected bboxes; ``collection_ids`` label skip warnings.
    """
    if not bboxes:
        return BboxUnionResult(bbox=None, skipped=[])

    # Filter invalid bboxes
    validation = filter_valid_bboxes(bboxes, wgs84_only=wgs84_only)
    _log_skipped_bboxes(validation.invalid, collection_ids)

    if not validation.valid:
        return BboxUnionResult(bbox=None, skipped=validation.invalid)

    # Union on 2D only; validation above already checked the elevations.
    valid_2d = [to_2d_bbox(b) for b in validation.valid]

    # Separate crossing and non-crossing bboxes
    crossing = [b for b in valid_2d if is_antimeridian_crossing(b)]
    normal = [b for b in valid_2d if not is_antimeridian_crossing(b)]

    # If no crossing bboxes, simple union
    if not crossing:
        envelope = _compute_simple_union(normal)
        return BboxUnionResult(
            bbox=envelope,
            bboxes=None if envelope is None else [envelope],
            skipped=validation.invalid,
        )

    # Handle antimeridian crossing
    return _compute_antimeridian_union(normal, crossing, validation.invalid)


def _log_skipped_bboxes(
    invalid: list[tuple[list[float], str]],
    collection_ids: list[str] | None,
) -> None:
    """Log warnings for skipped invalid bboxes."""
    for i, (bbox, reason) in enumerate(invalid):
        coll_id = collection_ids[i] if collection_ids and i < len(collection_ids) else "unknown"
        logger.warning(
            "Skipping invalid bbox from collection '%s': %s (bbox: %s)",
            coll_id,
            reason,
            bbox,
        )


def _compute_antimeridian_union(
    normal_bboxes: list[list[float]],
    crossing_bboxes: list[list[float]],
    skipped: list[tuple[list[float], str]],
) -> BboxUnionResult:
    """Compute union when anti-meridian crossing bboxes are present."""
    # Collect all bbox parts (normal + split crossing)
    all_parts = list(normal_bboxes)
    for bbox in crossing_bboxes:
        all_parts.extend(normalize_antimeridian_bbox(bbox))

    # Group by hemisphere
    western = [b for b in all_parts if b[2] <= 0]
    eastern = [b for b in all_parts if b[0] >= 0]
    spanning = [b for b in all_parts if b[0] < 0 < b[2]]

    # Spanning bboxes contribute to both hemispheres
    if spanning:
        western.extend(spanning)
        eastern.extend(spanning)

    # Build result bboxes from hemisphere unions. Each crossing bbox puts one
    # part in each hemisphere, so neither list is empty.
    result_bboxes = _build_hemisphere_unions(western, eastern)
    envelope = cast("list[float]", _compute_simple_union(all_parts))
    # The envelope spans every longitude: rashid PTL-DAT-005 warns on a tighter
    # west > east envelope.
    parts = result_bboxes if len(result_bboxes) > 1 else []
    return BboxUnionResult(bbox=envelope, bboxes=[envelope, *parts], skipped=skipped)


def _build_hemisphere_unions(
    western: list[list[float]],
    eastern: list[list[float]],
) -> list[list[float]]:
    """Build union bboxes for western and eastern hemispheres."""
    result: list[list[float]] = []

    if western:
        western_union = _compute_simple_union(western)
        if western_union:
            result.append(western_union)

    if eastern:
        eastern_union = _compute_simple_union(eastern)
        if eastern_union and (not result or eastern_union != result[-1]):
            result.append(eastern_union)

    return result


def _compute_simple_union(bboxes: list[list[float]]) -> list[float] | None:
    """Return the min/max union of non-crossing bboxes, or None if empty."""
    if not bboxes:
        return None

    reduced = [to_2d_bbox(b) for b in bboxes]

    west = min(b[0] for b in reduced)
    south = min(b[1] for b in reduced)
    east = max(b[2] for b in reduced)
    north = max(b[3] for b in reduced)

    return [west, south, east, north]
