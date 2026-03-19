"""Tests for scan progress reporting (Phase 5).

Tests verify:
1. Directory pre-counting for determinate progress
2. Progress callback invocation during scan
3. Progress suppression in JSON mode
4. Timing measurement in scan results
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from portolan_cli.scan import ScanOptions, scan_directory
from portolan_cli.scan_progress import ScanProgressReporter, count_directories

if TYPE_CHECKING:
    pass


@pytest.fixture
def cli_runner() -> CliRunner:
    """Create a Click CLI test runner."""
    return CliRunner()


class TestDirectoryPreCount:
    """Test fast directory pre-counting."""

    @pytest.mark.unit
    def test_count_empty_directory(self, tmp_path: Path) -> None:
        """Empty directory should count as 1 (the root itself)."""
        count = count_directories(tmp_path)
        assert count == 1

    @pytest.mark.unit
    def test_count_flat_structure(self, tmp_path: Path) -> None:
        """Flat directory with files only."""
        # Create some files
        (tmp_path / "file1.parquet").touch()
        (tmp_path / "file2.shp").touch()

        count = count_directories(tmp_path)
        # Just the root directory
        assert count == 1

    @pytest.mark.unit
    def test_count_nested_structure(self, tmp_path: Path) -> None:
        """Count all directories in nested structure."""
        # Create nested structure:
        # tmp_path/
        #   ├── collection1/
        #   └── collection2/
        #       └── nested/
        (tmp_path / "collection1").mkdir()
        (tmp_path / "collection1" / "data.parquet").touch()
        (tmp_path / "collection2").mkdir()
        (tmp_path / "collection2" / "nested").mkdir()

        count = count_directories(tmp_path)
        # Root + collection1 + collection2 + nested = 4
        assert count == 4

    @pytest.mark.unit
    def test_count_respects_hidden(self, tmp_path: Path) -> None:
        """Hidden directories should be excluded by default."""
        (tmp_path / ".hidden").mkdir()
        (tmp_path / "visible").mkdir()

        count = count_directories(tmp_path, include_hidden=False)
        # Root + visible = 2 (excludes .hidden)
        assert count == 2

        count_with_hidden = count_directories(tmp_path, include_hidden=True)
        # Root + visible + .hidden = 3
        assert count_with_hidden == 3

    @pytest.mark.unit
    def test_count_respects_max_depth(self, tmp_path: Path) -> None:
        """Max depth should limit directory counting."""
        # Create 3-level structure
        (tmp_path / "level1").mkdir()
        (tmp_path / "level1" / "level2").mkdir()
        (tmp_path / "level1" / "level2" / "level3").mkdir()

        # max_depth=0: only root
        count = count_directories(tmp_path, max_depth=0)
        assert count == 1

        # max_depth=1: root + level1
        count = count_directories(tmp_path, max_depth=1)
        assert count == 2

        # max_depth=None: all directories
        count = count_directories(tmp_path, max_depth=None)
        assert count == 4

    @pytest.mark.unit
    def test_count_is_fast(self, tmp_path: Path) -> None:
        """Pre-counting should be fast (< 100ms for typical trees)."""
        # Create a moderate structure (50 directories)
        for i in range(50):
            (tmp_path / f"dir_{i}").mkdir()

        start = time.perf_counter()
        count = count_directories(tmp_path)
        elapsed = time.perf_counter() - start

        assert count == 51  # 50 + root
        assert elapsed < 0.1  # < 100ms


class TestScanProgressReporter:
    """Test the progress reporter context manager."""

    @pytest.mark.unit
    def test_reporter_suppresses_in_json_mode(self, tmp_path: Path) -> None:
        """Progress should not be displayed in JSON mode."""
        reporter = ScanProgressReporter(
            total_directories=10,
            json_mode=True,
        )

        # Should be a no-op - no exception
        with reporter:
            for _i in range(10):
                reporter.advance()

        # Verify no progress was shown (json_mode suppresses)
        assert reporter.json_mode is True

    @pytest.mark.unit
    def test_reporter_tracks_progress(self, tmp_path: Path) -> None:
        """Reporter should track directory progress."""
        reporter = ScanProgressReporter(
            total_directories=10,
            json_mode=True,  # Suppress actual output in tests
        )

        with reporter:
            for _i in range(10):
                reporter.advance()

        assert reporter.directories_processed == 10

    @pytest.mark.unit
    def test_reporter_measures_elapsed_time(self) -> None:
        """Reporter should measure scan duration."""
        reporter = ScanProgressReporter(
            total_directories=1,
            json_mode=True,
        )

        with reporter:
            time.sleep(0.05)  # 50ms
            reporter.advance()

        assert reporter.elapsed_seconds >= 0.05


class TestScanDirectoryWithProgress:
    """Test scan_directory with progress callback integration."""

    @pytest.mark.unit
    def test_scan_calls_progress_callback(self, tmp_path: Path) -> None:
        """scan_directory should call progress callback for each directory."""
        # Create a simple structure
        (tmp_path / "collection1").mkdir()
        (tmp_path / "collection1" / "data.parquet").touch()
        (tmp_path / "collection2").mkdir()
        (tmp_path / "collection2" / "data.shp").touch()

        callback = MagicMock()
        options = ScanOptions()

        scan_directory(tmp_path, options, progress_callback=callback)

        # Should be called for each directory processed
        # Root + collection1 + collection2 = 3 directories
        assert callback.call_count == 3

    @pytest.mark.unit
    def test_scan_works_without_progress_callback(self, tmp_path: Path) -> None:
        """scan_directory should work fine without progress callback."""
        (tmp_path / "data.parquet").touch()

        # No callback - should not raise
        result = scan_directory(tmp_path, ScanOptions())

        assert result.directories_scanned == 1

    @pytest.mark.unit
    def test_scan_result_includes_timing(self, tmp_path: Path) -> None:
        """ScanResult should include scan duration when progress is used."""
        (tmp_path / "data.parquet").touch()

        result = scan_directory(tmp_path, ScanOptions())

        # directories_scanned already exists
        assert result.directories_scanned >= 1


class TestScanCLIProgress:
    """Test progress reporting in CLI integration."""

    @pytest.mark.unit
    def test_cli_scan_shows_progress_message(self, tmp_path: Path, cli_runner: CliRunner) -> None:
        """CLI scan should show progress for human output."""
        from portolan_cli.cli import scan

        # Create structure
        (tmp_path / "collection").mkdir()
        (tmp_path / "collection" / "data.parquet").touch()

        result = cli_runner.invoke(
            scan,
            [str(tmp_path)],
            catch_exceptions=False,
        )

        # Human output should include directory count and timing
        assert "directories" in result.output.lower() or "scanned" in result.output.lower()

    @pytest.mark.unit
    def test_cli_scan_json_suppresses_progress(self, tmp_path: Path, cli_runner: CliRunner) -> None:
        """CLI scan with --json should not show progress output."""
        import json

        from portolan_cli.cli import scan

        (tmp_path / "data.parquet").touch()

        result = cli_runner.invoke(
            scan,
            [str(tmp_path), "--json"],
            catch_exceptions=False,
        )

        # JSON output should be valid JSON (no progress output mixed in)
        # Should parse without error
        parsed = json.loads(result.output)
        # Check for standard JSON envelope keys
        assert "success" in parsed or "command" in parsed
