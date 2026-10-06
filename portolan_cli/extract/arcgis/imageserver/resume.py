"""Resume logic for interrupted ImageServer extractions.

This module provides tile-based resume functionality for ImageServer extractions:

- ImageServerResumeState: Tracks succeeded/failed tile coordinates
- should_process_tile: Determines if a tile needs processing
- load_resume_state: Loads state from extraction report
- save_resume_state: Persists state to extraction report

Usage:
    state = load_resume_state(Path(".portolan/extraction-report.json"))

    for x, y in tile_grid:
        if should_process_tile(x, y, state):
            # Extract this tile
            ...
"""

from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from portolan_cli.json_io import write_json_atomic

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

# A tile coordinate may be negative, but its magnitude is capped. A range of
# 100 million by 100 million covers any tiling scheme the extractor meets.
MAX_COORD = 100_000


#: Relative tolerance for the extent comparison that guards a resume. The two
#: extents come from the same service JSON through one JSON round trip, so any
#: difference this small is float noise rather than a different area.
GRID_TOLERANCE = 1e-9


@dataclass(frozen=True)
class TileGrid:
    """The tile grid that a run extracted.

    A tile id names a position in one grid. A run with another tile size, or
    over another extent, builds a different grid, so the same id then names a
    different area on the ground (pull request #871 review).

    Attributes:
        tile_size: Tile size in pixels that the run asked for.
        extent: Extent the run covered, as (xmin, ymin, xmax, ymax) in the
            service CRS.
    """

    tile_size: int
    extent: tuple[float, float, float, float]

    def matches(self, other: TileGrid) -> bool:
        """Report whether another grid names the same tiles as this one.

        Args:
            other: Grid to compare against.

        Returns:
            True when both the tile size and the extent agree.
        """
        if self.tile_size != other.tile_size:
            return False
        return all(
            math.isclose(mine, theirs, rel_tol=GRID_TOLERANCE, abs_tol=0.0)
            for mine, theirs in zip(self.extent, other.extent, strict=True)
        )


@dataclass
class ImageServerResumeState:
    """State for resuming an interrupted ImageServer extraction.

    Tracks which tiles have already been processed, enabling
    the extraction to skip succeeded tiles and retry failed ones.

    Attributes:
        succeeded_tiles: Set of (x, y) tile coordinates that succeeded (to skip).
        failed_tiles: Set of (x, y) tile coordinates that failed (to retry).
        service_url: The ImageServer service URL being extracted.
        started_at: When the extraction started.
        coarse_empty_tiles: Set of (x, y) tile coordinates that the coarse scan
            called empty. The scan reads a coarse cache level, which can drop a
            thin feature, so these tiles stay apart from the succeeded ones.
        grid: Tile grid the run extracted, or None for a report that an older
            version wrote.
    """

    succeeded_tiles: set[tuple[int, int]]
    failed_tiles: set[tuple[int, int]]
    service_url: str
    started_at: datetime
    coarse_empty_tiles: set[tuple[int, int]] = field(default_factory=set)
    grid: TileGrid | None = None


def should_process_tile(
    x: int,
    y: int,
    state: ImageServerResumeState | None,
    *,
    coarse_scan: bool = True,
) -> bool:
    """Determine if a tile should be processed.

    Decision logic:
    - If no resume state: process all tiles
    - If tile succeeded previously: skip (return False)
    - If the coarse scan called the tile empty: skip while the scan runs, and
      read it again under --no-coarse-scan
    - If tile failed previously: retry (return True)
    - If tile is new (not in state): process (return True)

    A coarse verdict is a heuristic, so --no-coarse-scan must be able to
    recover a block that the scan dropped (pull request #871 review).

    Args:
        x: The tile X coordinate.
        y: The tile Y coordinate.
        state: Resume state from previous extraction, or None.
        coarse_scan: Whether this run runs the coarse scan.

    Returns:
        True if the tile should be processed, False if it should be skipped.
    """
    if state is None:
        # No resume state = fresh extraction, process everything
        return True

    if (x, y) in state.succeeded_tiles:
        return False

    # Anything else either failed and needs a retry, or is new and needs
    # processing. A coarse-empty block waits only while the scan runs.
    return not (coarse_scan and (x, y) in state.coarse_empty_tiles)


def load_resume_state(
    report_path: Path,
    expected_service_url: str | None = None,
) -> ImageServerResumeState | None:
    """Load resume state from extraction report.

    Safely handles missing, corrupted, or incompatible report files by
    returning None rather than raising exceptions.

    Args:
        report_path: Path to the extraction report JSON file.
        expected_service_url: If provided, returns None if the report's
            service URL doesn't match (prevents resuming wrong extraction).

    Returns:
        ImageServerResumeState if successfully loaded, None otherwise.
    """
    if not report_path.exists():
        return None

    try:
        content = report_path.read_text(encoding="utf-8")
        if not content.strip():
            return None

        data = json.loads(content)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Failed to load resume state from %s: %s", report_path, e)
        return None

    return _parse_report_data(data, expected_service_url)


def _validate_tile_coordinate(coord: tuple[int, int]) -> bool:
    """Validate that a tile coordinate is within reasonable bounds.

    Protects against unreasonably large coordinates that could indicate
    data corruption or memory exhaustion attacks.

    Note: Negative coordinates are allowed because some coordinate systems
    use negative offsets. Security validation for file paths (path traversal)
    should happen in the extractor where file paths are constructed.

    Args:
        coord: Tile (x, y) coordinate tuple.

    Returns:
        True if coordinate is valid, False otherwise.
    """
    x, y = coord
    return -MAX_COORD <= x <= MAX_COORD and -MAX_COORD <= y <= MAX_COORD


def _parse_report_data(
    data: dict[str, Any],
    expected_service_url: str | None = None,
) -> ImageServerResumeState | None:
    """Parse report data into resume state.

    Args:
        data: Parsed JSON data from report file.
        expected_service_url: If provided, validates URL match.

    Returns:
        ImageServerResumeState if valid, None otherwise.
    """
    # Check required fields
    if "service_url" not in data or "tiles" not in data:
        return None

    service_url = data["service_url"]

    # Validate service URL if expected
    if expected_service_url is not None and service_url != expected_service_url:
        return None

    tiles = data.get("tiles", {})
    if not isinstance(tiles, dict):
        return None

    # Parse tile coordinates
    succeeded_raw = tiles.get("succeeded", [])
    failed_raw = tiles.get("failed", [])
    coarse_empty_raw = tiles.get("coarse_empty", [])

    try:
        succeeded_tiles = {(int(coord[0]), int(coord[1])) for coord in succeeded_raw}
        failed_tiles = {(int(coord[0]), int(coord[1])) for coord in failed_raw}
        coarse_empty_tiles = {(int(coord[0]), int(coord[1])) for coord in coarse_empty_raw}
    except (TypeError, IndexError, ValueError):
        return None

    # Validate coordinates are within reasonable bounds
    # This protects against malicious resume state files
    invalid_succeeded = [c for c in succeeded_tiles if not _validate_tile_coordinate(c)]
    invalid_failed = [c for c in failed_tiles if not _validate_tile_coordinate(c)]
    coarse_empty_tiles = {c for c in coarse_empty_tiles if _validate_tile_coordinate(c)}

    if invalid_succeeded or invalid_failed:
        logger.warning(
            "Resume state contains invalid coordinates (out of bounds): "
            "succeeded=%s, failed=%s. These will be ignored.",
            invalid_succeeded[:5],  # Only log first 5
            invalid_failed[:5],
        )
        succeeded_tiles = {c for c in succeeded_tiles if _validate_tile_coordinate(c)}
        failed_tiles = {c for c in failed_tiles if _validate_tile_coordinate(c)}

    # Parse timestamp
    started_at_str = data.get("started_at", "")
    try:
        started_at = datetime.fromisoformat(started_at_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        started_at = datetime.now(timezone.utc)

    return ImageServerResumeState(
        succeeded_tiles=succeeded_tiles,
        failed_tiles=failed_tiles,
        service_url=service_url,
        started_at=started_at,
        coarse_empty_tiles=coarse_empty_tiles,
        grid=_parse_grid(data.get("grid")),
    )


def _parse_grid(raw: Any) -> TileGrid | None:
    """Read the tile grid from a report.

    Args:
        raw: Value of the ``grid`` key, which an older report does not carry.

    Returns:
        The grid, or None when the report declares none or declares a broken
        one.
    """
    if not isinstance(raw, dict):
        return None
    extent = raw.get("extent")
    if not isinstance(extent, list) or len(extent) != 4:
        return None
    try:
        return TileGrid(
            tile_size=int(raw["tile_size"]),
            extent=(float(extent[0]), float(extent[1]), float(extent[2]), float(extent[3])),
        )
    except (KeyError, TypeError, ValueError):
        return None


def save_resume_state(state: ImageServerResumeState, report_path: Path) -> None:
    """Persist resume state to extraction report.

    Creates parent directories if they don't exist. Overwrites any
    existing file at the path.

    Args:
        state: The resume state to save.
        report_path: Path to write the JSON file.
    """
    report_path.parent.mkdir(parents=True, exist_ok=True)

    data: dict[str, Any] = {
        "extraction_type": "imageserver",
        "service_url": state.service_url,
        "started_at": state.started_at.isoformat().replace("+00:00", "Z"),
        "tiles": {
            "succeeded": sorted([list(coord) for coord in state.succeeded_tiles]),
            "failed": sorted([list(coord) for coord in state.failed_tiles]),
            "coarse_empty": sorted([list(coord) for coord in state.coarse_empty_tiles]),
        },
    }
    if state.grid is not None:
        data["grid"] = {
            "tile_size": state.grid.tile_size,
            "extent": list(state.grid.extent),
        }

    write_json_atomic(report_path, data)
