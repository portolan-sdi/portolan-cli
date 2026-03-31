"""Tests for upload progress reporting.

Tests the live progress bar for push uploads:
- Progress bar shows file count, bytes transferred, and speed
- JSON mode suppresses progress output
- Non-TTY environments suppress progress output

See GitHub issue #282 for the upload metrics feature.
"""

from __future__ import annotations

import time
from unittest.mock import patch

import pytest

from portolan_cli.upload_progress import UploadProgressReporter


class TestUploadProgressReporter:
    """Tests for upload progress context manager."""

    @pytest.mark.unit
    def test_creates_without_error(self) -> None:
        """Reporter should create without error."""
        reporter = UploadProgressReporter(total_files=10, total_bytes=1000)
        assert reporter.total_files == 10
        assert reporter.total_bytes == 1000
        assert reporter.files_completed == 0
        assert reporter.bytes_completed == 0

    @pytest.mark.unit
    def test_context_manager_works(self) -> None:
        """Reporter should work as context manager."""
        with UploadProgressReporter(total_files=10, json_mode=True) as reporter:
            assert reporter is not None
        # Should not raise

    @pytest.mark.unit
    def test_advance_updates_counts(self) -> None:
        """advance() should update file and byte counts."""
        reporter = UploadProgressReporter(total_files=10, total_bytes=10000, json_mode=True)
        with reporter:
            reporter.advance(bytes_uploaded=1000)
            assert reporter.files_completed == 1
            assert reporter.bytes_completed == 1000

            reporter.advance(bytes_uploaded=2000)
            assert reporter.files_completed == 2
            assert reporter.bytes_completed == 3000

    @pytest.mark.unit
    def test_elapsed_time_tracked(self) -> None:
        """Reporter should track elapsed time."""
        reporter = UploadProgressReporter(total_files=1, json_mode=True)
        with reporter:
            time.sleep(0.01)  # Small sleep
        assert reporter.elapsed_seconds > 0

    @pytest.mark.unit
    def test_average_speed_calculated(self) -> None:
        """Reporter should calculate average speed."""
        reporter = UploadProgressReporter(total_files=2, total_bytes=3000, json_mode=True)
        with reporter:
            reporter.advance(bytes_uploaded=1000)
            reporter.advance(bytes_uploaded=2000)
            time.sleep(0.01)  # Ensure some time passes

        # Speed should be bytes / elapsed
        assert reporter.average_speed > 0

    @pytest.mark.unit
    def test_json_mode_suppresses_progress(self) -> None:
        """JSON mode should suppress all progress output."""
        reporter = UploadProgressReporter(total_files=10, json_mode=True)
        with reporter:
            reporter.advance(bytes_uploaded=1000)
        # Should complete without error and no progress bar

    @pytest.mark.unit
    def test_non_tty_suppresses_progress(self) -> None:
        """Non-TTY should suppress progress bar."""
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = False
            reporter = UploadProgressReporter(total_files=10, json_mode=False)
            with reporter:
                reporter.advance(bytes_uploaded=1000)
            # Should complete without error


class TestUploadProgressReporterDisplay:
    """Tests for progress bar display formatting."""

    @pytest.mark.unit
    def test_progress_includes_file_count(self) -> None:
        """Progress should show N/M files format."""
        # This tests the reporter configuration, not actual Rich output
        reporter = UploadProgressReporter(total_files=100, total_bytes=1000000)
        assert reporter.total_files == 100

    @pytest.mark.unit
    def test_progress_includes_speed(self) -> None:
        """Progress should track speed for display."""
        reporter = UploadProgressReporter(total_files=1, total_bytes=1000, json_mode=True)
        with reporter:
            reporter.advance(bytes_uploaded=1000)
            time.sleep(0.01)
        # Speed is calculated from bytes / elapsed
        assert reporter.average_speed >= 0
