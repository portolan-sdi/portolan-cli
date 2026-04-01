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
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


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
    """

    succeeded_tiles: set[tuple[int, int]]
    failed_tiles: set[tuple[int, int]]
    service_url: str
    started_at: datetime


def should_process_tile(x: int, y: int, state: ImageServerResumeState | None) -> bool:
    """Determine if a tile should be processed.

    Decision logic:
    - If no resume state: process all tiles
    - If tile succeeded previously: skip (return False)
    - If tile failed previously: retry (return True)
    - If tile is new (not in state): process (return True)

    Args:
        x: The tile X coordinate.
        y: The tile Y coordinate.
        state: Resume state from previous extraction, or None.

    Returns:
        True if the tile should be processed, False if it should be skipped.
    """
    if state is None:
        # No resume state = fresh extraction, process everything
        return True

    if (x, y) in state.succeeded_tiles:
        # Already succeeded, skip
        return False

    # Either failed (retry) or new (process)
    return True


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
        content = report_path.read_text()
        if not content.strip():
            return None

        data = json.loads(content)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Failed to load resume state from %s: %s", report_path, e)
        return None

    return _parse_report_data(data, expected_service_url)


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

    try:
        succeeded_tiles = {(int(coord[0]), int(coord[1])) for coord in succeeded_raw}
        failed_tiles = {(int(coord[0]), int(coord[1])) for coord in failed_raw}
    except (TypeError, IndexError, ValueError):
        return None

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
    )


def save_resume_state(state: ImageServerResumeState, report_path: Path) -> None:
    """Persist resume state to extraction report.

    Creates parent directories if they don't exist. Overwrites any
    existing file at the path.

    Args:
        state: The resume state to save.
        report_path: Path to write the JSON file.
    """
    report_path.parent.mkdir(parents=True, exist_ok=True)

    data = {
        "extraction_type": "imageserver",
        "service_url": state.service_url,
        "started_at": state.started_at.isoformat().replace("+00:00", "Z"),
        "tiles": {
            "succeeded": sorted([list(coord) for coord in state.succeeded_tiles]),
            "failed": sorted([list(coord) for coord in state.failed_tiles]),
        },
    }

    report_path.write_text(json.dumps(data, indent=2))
