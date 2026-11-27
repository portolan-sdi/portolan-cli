"""Resume logic for interrupted ImageServer extractions."""

from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from portolan_core.json_io import write_json_atomic

if TYPE_CHECKING:
    from pathlib import Path
from portolan_core.bbox import BBOX_2D_LEN

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
    """The tile grid that a run extracted."""

    tile_size: int
    extent: tuple[float, float, float, float]

    def matches(self, other: TileGrid) -> bool:
        """Report whether another grid names the same tiles as this one."""
        if self.tile_size != other.tile_size:
            return False
        return all(
            math.isclose(mine, theirs, rel_tol=GRID_TOLERANCE, abs_tol=0.0)
            for mine, theirs in zip(self.extent, other.extent, strict=True)
        )


@dataclass
class ImageServerResumeState:
    """State for resuming an interrupted ImageServer extraction."""

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
    """Determine if a tile should be processed."""
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
    """Load resume state from extraction report."""
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
    """Validate that a tile coordinate is within reasonable bounds."""
    x, y = coord
    return -MAX_COORD <= x <= MAX_COORD and -MAX_COORD <= y <= MAX_COORD


def _parse_report_data(
    data: dict[str, Any],
    expected_service_url: str | None = None,
) -> ImageServerResumeState | None:
    """Parse report data into resume state."""
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
        started_at = datetime.fromisoformat(started_at_str)
    except (ValueError, TypeError):
        started_at = datetime.now(UTC)

    return ImageServerResumeState(
        succeeded_tiles=succeeded_tiles,
        failed_tiles=failed_tiles,
        service_url=service_url,
        started_at=started_at,
        coarse_empty_tiles=coarse_empty_tiles,
        grid=_parse_grid(data.get("grid")),
    )


def _parse_grid(raw: Any) -> TileGrid | None:
    """Read the tile grid from a report."""
    if not isinstance(raw, dict):
        return None
    extent = raw.get("extent")
    if not isinstance(extent, list) or len(extent) != BBOX_2D_LEN:
        return None
    try:
        return TileGrid(
            tile_size=int(raw["tile_size"]),
            extent=(float(extent[0]), float(extent[1]), float(extent[2]), float(extent[3])),
        )
    except (KeyError, TypeError, ValueError):
        return None


def save_resume_state(state: ImageServerResumeState, report_path: Path) -> None:
    """Persist resume state to extraction report."""
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
