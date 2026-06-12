"""Bounding box validation and utilities.

Centralized bbox validation for issue #516:
- Filter inf/nan coordinates that poison union computations
- Validate WGS84 coordinate ranges
- Handle antimeridian-crossing bboxes per RFC 7946 / STAC spec
- Compute unions with proper multi-bbox support
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# WGS84 coordinate bounds
LON_MIN = -180.0
LON_MAX = 180.0
LAT_MIN = -90.0
LAT_MAX = 90.0

# Sanity bound for any CRS. No real-world coordinate on Earth, in any projection,
# approaches this magnitude (Web Mercator maxes near 2e7 m; the most extreme
# projected grids stay well under 1e8). Values beyond it are "effectively
# infinite" sentinels (e.g. WFS-served ±1.79e308 ~ ±float-max), not real data.
# Used to reject such poison even for projected (non-WGS84) bboxes (issue #516).
MAX_SANE_COORD = 1e9


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
    """Single union bbox, or None if no valid inputs. For multi-bbox, this is the overall envelope."""

    bboxes: list[list[float]] | None = None
    """Multi-bbox list for STAC output when antimeridian crossing is involved."""

    is_multi_bbox: bool = False
    """True if the union requires multi-bbox representation."""

    skipped: list[tuple[list[float], str]] = field(default_factory=list)
    """Bboxes that were skipped due to validation failures."""


def is_finite_bbox(bbox: list[float]) -> bool:
    """Check if a bbox has finite coordinates (no inf/nan).

    This is the universal validity check that applies to ANY CRS.
    Use is_valid_wgs84_bbox() for WGS84-specific range checks.

    Validates:
    - Exactly 4 or 6 elements (2D or 3D bbox)
    - All coordinates are finite (no inf, -inf, nan)

    Args:
        bbox: Bounding box as [west, south, east, north] or 6-element 3D variant.

    Returns:
        True if bbox has finite values, False otherwise.
    """
    if len(bbox) not in (4, 6):
        return False

    return all(math.isfinite(c) for c in bbox)


def is_valid_bbox(bbox: list[float], *, wgs84_only: bool = True) -> bool:
    """Check if a bbox is valid.

    Validates:
    - Exactly 4 or 6 elements (2D or 3D bbox)
    - All coordinates are finite (no inf, -inf, nan)
    - If wgs84_only=True: Longitude in [-180, 180], Latitude in [-90, 90]
    - South <= North (latitude ordering, only checked for WGS84)

    Note: West > East is VALID per RFC 7946 for antimeridian-crossing bboxes.

    Args:
        bbox: Bounding box as [west, south, east, north] or 6-element 3D variant.
        wgs84_only: If True (default), check WGS84 coordinate ranges.
                   Set to False for projected coordinate bboxes.

    Returns:
        True if bbox is valid, False otherwise.
    """
    # Check finite first (universal for all CRS)
    if not is_finite_bbox(bbox):
        return False

    # For non-WGS84 bboxes, finite check is sufficient
    if not wgs84_only:
        return True

    west, south, east, north = bbox[0], bbox[1], bbox[2], bbox[3]

    # Check longitude range (WGS84 only)
    if not (LON_MIN <= west <= LON_MAX and LON_MIN <= east <= LON_MAX):
        return False

    # Check latitude range (WGS84 only)
    if not (LAT_MIN <= south <= LAT_MAX and LAT_MIN <= north <= LAT_MAX):
        return False

    # Check latitude ordering (south must be <= north)
    if south > north:
        return False

    return True


def get_bbox_validation_reason(bbox: list[float], *, wgs84_only: bool = True) -> str | None:
    """Get the reason why a bbox is invalid, or None if valid.

    Args:
        bbox: Bounding box to validate.
        wgs84_only: If True (default), check WGS84 coordinate ranges.
                   Set to False for projected coordinate bboxes.

    Returns:
        String describing why invalid, or None if valid.
    """
    if len(bbox) not in (4, 6):
        return f"wrong element count: {len(bbox)} (expected 4 or 6)"

    west, south, east, north = bbox[0], bbox[1], bbox[2], bbox[3]

    # Check for non-finite values (universal for all CRS)
    for name, val in [("west", west), ("south", south), ("east", east), ("north", north)]:
        if math.isnan(val):
            return f"{name} is NaN"
        if math.isinf(val):
            return f"{name} is infinite ({val})"

    # 6-element (3D) bboxes carry elevation values beyond the first four;
    # validate their finiteness too so poisoned Z values are caught (issue #516).
    for i in range(4, len(bbox)):
        if math.isnan(bbox[i]):
            return f"elevation coordinate at index {i} is NaN"
        if math.isinf(bbox[i]):
            return f"elevation coordinate at index {i} is infinite ({bbox[i]})"

    # Reject "effectively infinite" sentinel coordinates: finite, but far beyond
    # any real-world magnitude in any CRS (e.g. ±1.79e308). Applied universally
    # so poison is caught even for projected bboxes (wgs84_only=False) (#516).
    for i, val in enumerate(bbox):
        if abs(val) > MAX_SANE_COORD:
            return f"coordinate at index {i} ({val}) exceeds sane magnitude"

    # WGS84-specific checks
    if wgs84_only:
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


def is_antimeridian_crossing(bbox: list[float]) -> bool:
    """Check if a bbox crosses the antimeridian (180°/-180° line).

    Per RFC 7946, a bbox crosses the antimeridian when west > east.
    Example: Fiji [177, -20, -175, -15] crosses because 177 > -175.

    Args:
        bbox: Bounding box as [west, south, east, north].

    Returns:
        True if the bbox crosses the antimeridian.
    """
    if len(bbox) < 4:
        return False
    west, east = bbox[0], bbox[2]
    return west > east


def normalize_antimeridian_bbox(bbox: list[float]) -> list[list[float]]:
    """Split an antimeridian-crossing bbox into two non-crossing bboxes.

    Per STAC spec, antimeridian-crossing extents should be represented
    as multiple bboxes: one from west to 180°, one from -180° to east.

    Args:
        bbox: Bounding box as [west, south, east, north].

    Returns:
        List of bboxes. Single-element list if not crossing,
        two-element list if crossing antimeridian.
    """
    if not is_antimeridian_crossing(bbox):
        return [bbox]

    west, south, east, north = bbox[0], bbox[1], bbox[2], bbox[3]

    # Split into western and eastern parts
    western_part = [west, south, LON_MAX, north]  # West to 180°
    eastern_part = [LON_MIN, south, east, north]  # -180° to east

    return [western_part, eastern_part]


def filter_valid_bboxes(
    bboxes: list[list[float]],
    *,
    wgs84_only: bool = True,
) -> BboxValidationResult:
    """Filter a list of bboxes, separating valid from invalid.

    Args:
        bboxes: List of bboxes to filter.
        wgs84_only: When True, also reject bboxes outside WGS84 ranges. Set to
            False for source-CRS (e.g. projected) bboxes, where only finiteness
            is universally meaningful (issue #516).

    Returns:
        BboxValidationResult with valid and invalid (with reasons) lists.
    """
    valid: list[list[float]] = []
    invalid: list[tuple[list[float], str]] = []

    for bbox in bboxes:
        reason = get_bbox_validation_reason(bbox, wgs84_only=wgs84_only)
        if reason is None:
            valid.append(bbox)
        else:
            invalid.append((bbox, reason))

    return BboxValidationResult(valid=valid, invalid=invalid)


def compute_bbox_union(
    bboxes: list[list[float]],
    *,
    collection_ids: list[str] | None = None,
    wgs84_only: bool = True,
) -> BboxUnionResult:
    """Compute the union of multiple bboxes with validation and antimeridian handling.

    Filters out invalid bboxes, handles antimeridian-crossing bboxes properly,
    and produces STAC-compliant multi-bbox output when needed.

    Args:
        bboxes: List of bboxes to union.
        collection_ids: Optional list of collection IDs for logging (parallel to bboxes).
        wgs84_only: When True, also reject bboxes outside WGS84 ranges. Set to
            False when unioning source-CRS (e.g. projected) bboxes, where only
            finiteness is universally meaningful (issue #516).

    Returns:
        BboxUnionResult with union bbox(es) and any skipped invalid bboxes.
    """
    if not bboxes:
        return BboxUnionResult(bbox=None, skipped=[])

    # Filter invalid bboxes
    validation = filter_valid_bboxes(bboxes, wgs84_only=wgs84_only)
    _log_skipped_bboxes(validation.invalid, collection_ids)

    if not validation.valid:
        return BboxUnionResult(bbox=None, skipped=validation.invalid)

    # Separate crossing and non-crossing bboxes
    crossing = [b for b in validation.valid if is_antimeridian_crossing(b)]
    normal = [b for b in validation.valid if not is_antimeridian_crossing(b)]

    # If no crossing bboxes, simple union
    if not crossing:
        return BboxUnionResult(
            bbox=_compute_simple_union(normal),
            bboxes=None,
            is_multi_bbox=False,
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

    # Build result bboxes from hemisphere unions
    result_bboxes = _build_hemisphere_unions(western, eastern)

    # Fallback if no results
    if not result_bboxes:
        result_bboxes = all_parts

    envelope = _compute_simple_union(all_parts) if all_parts else None

    return BboxUnionResult(
        bbox=envelope,
        bboxes=result_bboxes if len(result_bboxes) > 1 else None,
        is_multi_bbox=len(result_bboxes) > 1,
        skipped=skipped,
    )


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
    """Compute simple min/max union of bboxes (no antimeridian handling).

    Args:
        bboxes: List of valid, non-crossing bboxes.

    Returns:
        Union bbox or None if empty.
    """
    if not bboxes:
        return None

    west = min(b[0] for b in bboxes)
    south = min(b[1] for b in bboxes)
    east = max(b[2] for b in bboxes)
    north = max(b[3] for b in bboxes)

    return [west, south, east, north]
