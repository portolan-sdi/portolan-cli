"""Unit tests for ImageServer extraction resume logic.

Tests the tile-based resume functionality for interrupted ImageServer extractions:
- ImageServerResumeState: Tracks succeeded/failed tile coordinates
- should_process_tile: Determines if tile needs processing
- load_resume_state: Loads state from extraction report
- save_resume_state: Persists state to extraction report

Following TDD: tests written before implementation.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from portolan_cli.extract.arcgis.imageserver.resume import (
    ImageServerResumeState,
    load_resume_state,
    save_resume_state,
    should_process_tile,
)


class TestImageServerResumeState:
    """Tests for ImageServerResumeState dataclass."""

    def test_creation(self) -> None:
        """Create resume state with succeeded and failed tiles."""
        state = ImageServerResumeState(
            succeeded_tiles={(0, 0), (0, 1), (1, 0)},
            failed_tiles={(1, 1), (2, 0)},
            service_url="https://example.com/ImageServer",
            started_at=datetime(2026, 4, 1, 10, 30, 0, tzinfo=timezone.utc),
        )

        assert state.succeeded_tiles == {(0, 0), (0, 1), (1, 0)}
        assert state.failed_tiles == {(1, 1), (2, 0)}
        assert state.service_url == "https://example.com/ImageServer"
        assert state.started_at.year == 2026

    def test_empty_state(self) -> None:
        """Create empty resume state."""
        state = ImageServerResumeState(
            succeeded_tiles=set(),
            failed_tiles=set(),
            service_url="https://example.com/ImageServer",
            started_at=datetime.now(timezone.utc),
        )

        assert len(state.succeeded_tiles) == 0
        assert len(state.failed_tiles) == 0

    def test_tile_coordinates_are_tuples(self) -> None:
        """Tile coordinates should be (x, y) integer tuples."""
        state = ImageServerResumeState(
            succeeded_tiles={(10, 20), (100, 200)},
            failed_tiles={(5, 5)},
            service_url="https://example.com/ImageServer",
            started_at=datetime.now(timezone.utc),
        )

        # Verify coordinate structure
        for x, y in state.succeeded_tiles:
            assert isinstance(x, int)
            assert isinstance(y, int)


class TestShouldProcessTile:
    """Tests for should_process_tile function."""

    def test_no_resume_state_always_process(self) -> None:
        """Without resume state, all tiles should be processed."""
        assert should_process_tile(0, 0, None) is True
        assert should_process_tile(5, 10, None) is True
        assert should_process_tile(100, 200, None) is True

    def test_skip_succeeded_tiles(self) -> None:
        """Succeeded tiles should be skipped."""
        state = ImageServerResumeState(
            succeeded_tiles={(0, 0), (0, 1), (1, 0)},
            failed_tiles={(2, 2)},
            service_url="https://example.com/ImageServer",
            started_at=datetime.now(timezone.utc),
        )

        assert should_process_tile(0, 0, state) is False
        assert should_process_tile(0, 1, state) is False
        assert should_process_tile(1, 0, state) is False

    def test_retry_failed_tiles(self) -> None:
        """Failed tiles should be retried."""
        state = ImageServerResumeState(
            succeeded_tiles={(0, 0)},
            failed_tiles={(1, 1), (2, 2)},
            service_url="https://example.com/ImageServer",
            started_at=datetime.now(timezone.utc),
        )

        assert should_process_tile(1, 1, state) is True
        assert should_process_tile(2, 2, state) is True

    def test_process_new_tiles(self) -> None:
        """New tiles (not in state) should be processed."""
        state = ImageServerResumeState(
            succeeded_tiles={(0, 0), (0, 1)},
            failed_tiles={(1, 0)},
            service_url="https://example.com/ImageServer",
            started_at=datetime.now(timezone.utc),
        )

        # Tiles not in previous extraction
        assert should_process_tile(5, 5, state) is True
        assert should_process_tile(10, 20, state) is True

    def test_comprehensive_scenario(self) -> None:
        """Comprehensive test with all tile types."""
        state = ImageServerResumeState(
            succeeded_tiles={(0, 0), (0, 1), (1, 0), (1, 1)},
            failed_tiles={(2, 0), (2, 1)},
            service_url="https://example.com/ImageServer",
            started_at=datetime.now(timezone.utc),
        )

        # Succeeded - skip
        assert should_process_tile(0, 0, state) is False
        assert should_process_tile(1, 1, state) is False

        # Failed - retry
        assert should_process_tile(2, 0, state) is True
        assert should_process_tile(2, 1, state) is True

        # New - process
        assert should_process_tile(3, 0, state) is True
        assert should_process_tile(5, 5, state) is True


class TestLoadResumeState:
    """Tests for load_resume_state function."""

    def test_load_nonexistent_file_returns_none(self, tmp_path: Path) -> None:
        """Loading from nonexistent file returns None."""
        report_path = tmp_path / ".portolan" / "extraction-report.json"

        state = load_resume_state(report_path)

        assert state is None

    def test_load_valid_state(self, tmp_path: Path) -> None:
        """Load resume state from valid extraction report."""
        report_path = tmp_path / ".portolan" / "extraction-report.json"
        report_path.parent.mkdir(parents=True)

        report_data = _create_imageserver_report(
            service_url="https://example.com/ImageServer",
            succeeded_tiles=[(0, 0), (0, 1), (1, 0)],
            failed_tiles=[(1, 1)],
            started_at="2026-04-01T10:30:00Z",
        )
        report_path.write_text(json.dumps(report_data, indent=2))

        state = load_resume_state(report_path)

        assert state is not None
        assert state.succeeded_tiles == {(0, 0), (0, 1), (1, 0)}
        assert state.failed_tiles == {(1, 1)}
        assert state.service_url == "https://example.com/ImageServer"

    def test_load_empty_tiles(self, tmp_path: Path) -> None:
        """Load state with no tiles processed yet."""
        report_path = tmp_path / ".portolan" / "extraction-report.json"
        report_path.parent.mkdir(parents=True)

        report_data = _create_imageserver_report(
            service_url="https://example.com/ImageServer",
            succeeded_tiles=[],
            failed_tiles=[],
            started_at="2026-04-01T10:30:00Z",
        )
        report_path.write_text(json.dumps(report_data, indent=2))

        state = load_resume_state(report_path)

        assert state is not None
        assert state.succeeded_tiles == set()
        assert state.failed_tiles == set()

    def test_load_corrupted_json_returns_none(self, tmp_path: Path) -> None:
        """Corrupted JSON file returns None (safe failure)."""
        report_path = tmp_path / ".portolan" / "extraction-report.json"
        report_path.parent.mkdir(parents=True)
        report_path.write_text("{ invalid json }")

        state = load_resume_state(report_path)

        assert state is None

    def test_load_missing_required_fields_returns_none(self, tmp_path: Path) -> None:
        """Report missing required fields returns None."""
        report_path = tmp_path / ".portolan" / "extraction-report.json"
        report_path.parent.mkdir(parents=True)
        # Valid JSON but missing imageserver-specific fields
        report_path.write_text(json.dumps({"some_field": "value"}))

        state = load_resume_state(report_path)

        assert state is None

    def test_load_report_for_different_service_returns_none(self, tmp_path: Path) -> None:
        """Report for different service URL is not a match for resume."""
        report_path = tmp_path / ".portolan" / "extraction-report.json"
        report_path.parent.mkdir(parents=True)

        report_data = _create_imageserver_report(
            service_url="https://example.com/OtherImageServer",
            succeeded_tiles=[(0, 0)],
            failed_tiles=[],
            started_at="2026-04-01T10:30:00Z",
        )
        report_path.write_text(json.dumps(report_data, indent=2))

        # Try to load for a different service URL
        state = load_resume_state(
            report_path, expected_service_url="https://example.com/ImageServer"
        )

        assert state is None

    def test_load_matching_service_url(self, tmp_path: Path) -> None:
        """Load succeeds when service URL matches expected."""
        report_path = tmp_path / ".portolan" / "extraction-report.json"
        report_path.parent.mkdir(parents=True)

        report_data = _create_imageserver_report(
            service_url="https://example.com/ImageServer",
            succeeded_tiles=[(0, 0), (1, 1)],
            failed_tiles=[(2, 2)],
            started_at="2026-04-01T10:30:00Z",
        )
        report_path.write_text(json.dumps(report_data, indent=2))

        state = load_resume_state(
            report_path, expected_service_url="https://example.com/ImageServer"
        )

        assert state is not None
        assert state.succeeded_tiles == {(0, 0), (1, 1)}


class TestSaveResumeState:
    """Tests for save_resume_state function."""

    def test_save_creates_parent_directories(self, tmp_path: Path) -> None:
        """Save creates parent directories if needed."""
        report_path = tmp_path / "nested" / "deep" / "extraction-report.json"
        state = ImageServerResumeState(
            succeeded_tiles={(0, 0)},
            failed_tiles=set(),
            service_url="https://example.com/ImageServer",
            started_at=datetime(2026, 4, 1, 10, 30, 0, tzinfo=timezone.utc),
        )

        save_resume_state(state, report_path)

        assert report_path.exists()
        assert report_path.parent.exists()

    def test_save_and_load_roundtrip(self, tmp_path: Path) -> None:
        """State survives save/load roundtrip."""
        report_path = tmp_path / ".portolan" / "extraction-report.json"
        original_state = ImageServerResumeState(
            succeeded_tiles={(0, 0), (1, 2), (100, 200)},
            failed_tiles={(5, 5), (10, 10)},
            service_url="https://example.com/ImageServer",
            started_at=datetime(2026, 4, 1, 10, 30, 0, tzinfo=timezone.utc),
        )

        save_resume_state(original_state, report_path)
        loaded_state = load_resume_state(report_path)

        assert loaded_state is not None
        assert loaded_state.succeeded_tiles == original_state.succeeded_tiles
        assert loaded_state.failed_tiles == original_state.failed_tiles
        assert loaded_state.service_url == original_state.service_url
        # Compare timestamps (may lose microseconds in JSON)
        assert loaded_state.started_at.replace(microsecond=0) == original_state.started_at.replace(
            microsecond=0
        )

    def test_save_overwrites_existing_file(self, tmp_path: Path) -> None:
        """Save overwrites existing report file."""
        report_path = tmp_path / ".portolan" / "extraction-report.json"
        report_path.parent.mkdir(parents=True)
        report_path.write_text("old content")

        state = ImageServerResumeState(
            succeeded_tiles={(0, 0)},
            failed_tiles=set(),
            service_url="https://example.com/ImageServer",
            started_at=datetime.now(timezone.utc),
        )

        save_resume_state(state, report_path)

        content = report_path.read_text()
        assert "old content" not in content
        assert "ImageServer" in content

    def test_save_produces_valid_json(self, tmp_path: Path) -> None:
        """Saved file contains valid JSON."""
        report_path = tmp_path / "report.json"
        state = ImageServerResumeState(
            succeeded_tiles={(0, 0), (1, 1)},
            failed_tiles={(2, 2)},
            service_url="https://example.com/ImageServer",
            started_at=datetime(2026, 4, 1, 10, 30, 0, tzinfo=timezone.utc),
        )

        save_resume_state(state, report_path)

        # Should not raise
        data = json.loads(report_path.read_text())
        assert "tiles" in data
        assert "service_url" in data

    def test_save_large_tile_set(self, tmp_path: Path) -> None:
        """Save handles large number of tiles efficiently."""
        report_path = tmp_path / "report.json"
        # 10,000 tiles
        succeeded = {(x, y) for x in range(100) for y in range(100)}
        state = ImageServerResumeState(
            succeeded_tiles=succeeded,
            failed_tiles={(1000, 1000)},
            service_url="https://example.com/ImageServer",
            started_at=datetime.now(timezone.utc),
        )

        save_resume_state(state, report_path)
        loaded_state = load_resume_state(report_path)

        assert loaded_state is not None
        assert len(loaded_state.succeeded_tiles) == 10000


class TestEdgeCases:
    """Edge case and error handling tests."""

    def test_empty_file_returns_none(self, tmp_path: Path) -> None:
        """Empty file returns None."""
        report_path = tmp_path / "report.json"
        report_path.write_text("")

        state = load_resume_state(report_path)

        assert state is None

    def test_concurrent_save_last_write_wins(self, tmp_path: Path) -> None:
        """Multiple saves - last one wins (no corruption)."""
        report_path = tmp_path / "report.json"

        state1 = ImageServerResumeState(
            succeeded_tiles={(0, 0)},
            failed_tiles=set(),
            service_url="https://example.com/ImageServer",
            started_at=datetime.now(timezone.utc),
        )
        state2 = ImageServerResumeState(
            succeeded_tiles={(1, 1), (2, 2)},
            failed_tiles={(3, 3)},
            service_url="https://example.com/ImageServer",
            started_at=datetime.now(timezone.utc),
        )

        save_resume_state(state1, report_path)
        save_resume_state(state2, report_path)

        loaded = load_resume_state(report_path)
        assert loaded is not None
        assert loaded.succeeded_tiles == {(1, 1), (2, 2)}
        assert loaded.failed_tiles == {(3, 3)}

    def test_negative_coordinates(self, tmp_path: Path) -> None:
        """Handle negative tile coordinates."""
        report_path = tmp_path / "report.json"
        state = ImageServerResumeState(
            succeeded_tiles={(-1, -1), (0, -5), (-10, 10)},
            failed_tiles=set(),
            service_url="https://example.com/ImageServer",
            started_at=datetime.now(timezone.utc),
        )

        save_resume_state(state, report_path)
        loaded = load_resume_state(report_path)

        assert loaded is not None
        assert (-1, -1) in loaded.succeeded_tiles
        assert (0, -5) in loaded.succeeded_tiles
        assert (-10, 10) in loaded.succeeded_tiles

    def test_special_characters_in_service_url(self, tmp_path: Path) -> None:
        """Handle special characters in service URL."""
        report_path = tmp_path / "report.json"
        url = "https://example.com/arcgis/rest/services/My Service/ImageServer?token=abc123"
        state = ImageServerResumeState(
            succeeded_tiles={(0, 0)},
            failed_tiles=set(),
            service_url=url,
            started_at=datetime.now(timezone.utc),
        )

        save_resume_state(state, report_path)
        loaded = load_resume_state(report_path)

        assert loaded is not None
        assert loaded.service_url == url


def _create_imageserver_report(
    service_url: str,
    succeeded_tiles: list[tuple[int, int]],
    failed_tiles: list[tuple[int, int]],
    started_at: str,
) -> dict[str, Any]:
    """Create ImageServer extraction report dict for testing.

    Args:
        service_url: ImageServer service URL.
        succeeded_tiles: List of (x, y) tuples for succeeded tiles.
        failed_tiles: List of (x, y) tuples for failed tiles.
        started_at: ISO 8601 timestamp.

    Returns:
        Dict representing extraction report JSON.
    """
    return {
        "extraction_type": "imageserver",
        "service_url": service_url,
        "started_at": started_at,
        "tiles": {
            "succeeded": [[x, y] for x, y in succeeded_tiles],
            "failed": [[x, y] for x, y in failed_tiles],
        },
    }
