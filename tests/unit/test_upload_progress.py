"""Tests for upload progress reporting.

Tests the live progress bar for push uploads:
- Progress bar shows file count, bytes transferred, and speed
- JSON mode suppresses progress output
- Non-TTY environments suppress progress output

See GitHub issue #282 for the upload metrics feature.
"""

from __future__ import annotations

import sys
import time
from collections.abc import Iterator
from unittest.mock import MagicMock, patch

import pytest

from portolan_cli.sync.upload_progress import UploadProgressReporter


@pytest.fixture
def tty_progress() -> Iterator[MagicMock]:
    """Run the reporter down its Rich path with ``Progress`` replaced by a spy.

    ``__enter__`` builds the progress bar only when stdout is a TTY and JSON mode
    is off, which no test reached before — the whole Rich branch (bar columns,
    task setup, advance calls) went unverified, and mutation testing scored the
    module at 18%. Yielding the patched class lets a test assert how the bar was
    configured, not merely that nothing raised.
    """
    with (
        patch("rich.progress.Progress") as progress_cls,
        patch("sys.stdout.isatty", return_value=True),
    ):
        yield progress_cls


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

    # A non-TTY test used to live here. It patched sys.stderr while __enter__
    # gates on sys.stdout, so it asserted nothing about the branch it named.
    # TestRichProgressPath.test_no_bar_without_a_tty replaces it.


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


class TestConstruction:
    """Tests pinning the attributes __init__ sets, including defaults."""

    @pytest.mark.unit
    def test_every_attribute_starts_from_the_arguments(self) -> None:
        """Each field is seeded from its own argument, with counters at zero."""
        reporter = UploadProgressReporter(total_files=7, total_bytes=4096, json_mode=True)

        assert reporter.total_files == 7
        assert reporter.total_bytes == 4096
        assert reporter.json_mode is True
        assert reporter.files_completed == 0
        assert reporter.bytes_completed == 0
        assert reporter.elapsed_seconds == 0.0

    @pytest.mark.unit
    def test_defaults_are_zero_bytes_and_progress_on(self) -> None:
        """Omitted arguments default to unknown size and visible progress."""
        reporter = UploadProgressReporter(total_files=3)

        assert reporter.total_bytes == 0
        assert reporter.json_mode is False


class TestRichProgressPath:
    """Tests for the Rich branch of __enter__ (TTY, JSON mode off)."""

    @pytest.mark.unit
    def test_progress_bar_is_configured_and_started(self, tty_progress: MagicMock) -> None:
        """On a TTY the bar is built, entered, and given a byte-sized task."""
        with UploadProgressReporter(total_files=10, total_bytes=5000) as reporter:
            assert reporter is not None

        tty_progress.assert_called_once()
        kwargs = tty_progress.call_args.kwargs
        assert kwargs["transient"] is True  # bar clears itself when the push ends
        assert kwargs["console"].file is sys.stderr  # stdout stays machine-readable

        instance = tty_progress.return_value
        instance.__enter__.assert_called_once()
        instance.add_task.assert_called_once_with("Uploading", total=5000)

    @pytest.mark.unit
    def test_columns_cover_percentage_speed_and_time(self, tty_progress: MagicMock) -> None:
        """The bar shows the columns the feature promises (see issue #282)."""
        from rich.progress import (
            BarColumn,
            DownloadColumn,
            TextColumn,
            TimeElapsedColumn,
            TimeRemainingColumn,
            TransferSpeedColumn,
        )

        with UploadProgressReporter(total_files=1, total_bytes=10):
            pass

        columns = tty_progress.call_args.args
        types = [type(column) for column in columns]
        for expected in (
            BarColumn,
            DownloadColumn,
            TransferSpeedColumn,
            TimeElapsedColumn,
            TimeRemainingColumn,
        ):
            assert expected in types

        texts = [c.text_format for c in columns if isinstance(c, TextColumn)]
        assert "[bold blue]Uploading" in texts
        assert "[progress.percentage]{task.percentage:>3.0f}%" in texts

    @pytest.mark.unit
    def test_task_falls_back_to_file_count_without_byte_totals(
        self, tty_progress: MagicMock
    ) -> None:
        """With no byte total the bar counts files instead, so it still fills."""
        with UploadProgressReporter(total_files=12, total_bytes=0):
            pass

        tty_progress.return_value.add_task.assert_called_once_with("Uploading", total=12)

    @pytest.mark.unit
    def test_json_mode_skips_the_bar_on_a_tty(self, tty_progress: MagicMock) -> None:
        """JSON output must stay clean even when a terminal is attached."""
        with UploadProgressReporter(total_files=5, total_bytes=100, json_mode=True):
            pass

        tty_progress.assert_not_called()

    @pytest.mark.unit
    def test_no_bar_without_a_tty(self) -> None:
        """Piped output gets no progress bar (it would corrupt the stream)."""
        with (
            patch("rich.progress.Progress") as progress_cls,
            patch("sys.stdout.isatty", return_value=False),
        ):
            with UploadProgressReporter(total_files=5, total_bytes=100):
                pass

        progress_cls.assert_not_called()

    @pytest.mark.unit
    def test_missing_rich_degrades_silently(self) -> None:
        """Rich is optional: without it the upload proceeds, unadorned."""
        with (
            patch("sys.stdout.isatty", return_value=True),
            patch.dict(sys.modules, {"rich.progress": None}),
        ):
            with UploadProgressReporter(total_files=2, total_bytes=10) as reporter:
                reporter.advance(bytes_uploaded=10)

            assert reporter.files_completed == 1  # counting survives the missing bar

    @pytest.mark.unit
    def test_context_manager_yields_the_reporter_itself(self, tty_progress: MagicMock) -> None:
        """``with ... as r`` must bind the reporter, not None."""
        reporter = UploadProgressReporter(total_files=1)
        with reporter as bound:
            assert bound is reporter


class TestAdvance:
    """Tests for advance() bookkeeping and its calls into Rich."""

    @pytest.mark.unit
    def test_advance_reports_bytes_when_byte_totals_are_known(
        self, tty_progress: MagicMock
    ) -> None:
        """With byte totals the bar advances by bytes, one call per file."""
        with UploadProgressReporter(total_files=2, total_bytes=3000) as reporter:
            reporter.advance(bytes_uploaded=1000)
            reporter.advance(bytes_uploaded=2000)

        instance = tty_progress.return_value
        task = instance.add_task.return_value
        assert instance.advance.call_count == 2
        instance.advance.assert_called_with(task, advance=2000)
        assert reporter.files_completed == 2
        assert reporter.bytes_completed == 3000

    @pytest.mark.unit
    def test_advance_reports_one_unit_per_file_without_byte_totals(
        self, tty_progress: MagicMock
    ) -> None:
        """Without byte totals the bar advances one unit per file, not by bytes."""
        with UploadProgressReporter(total_files=2, total_bytes=0) as reporter:
            reporter.advance(bytes_uploaded=999)

        task = tty_progress.return_value.add_task.return_value
        tty_progress.return_value.advance.assert_called_once_with(task, advance=1)
        assert reporter.bytes_completed == 999  # still tracked for the final report

    @pytest.mark.unit
    def test_advance_defaults_to_a_file_with_no_byte_count(self) -> None:
        """A caller that knows no size still moves the file counter."""
        reporter = UploadProgressReporter(total_files=3, json_mode=True)
        with reporter:
            reporter.advance()

        assert reporter.files_completed == 1
        assert reporter.bytes_completed == 0


class TestExit:
    """Tests for __exit__ timing and teardown."""

    @pytest.mark.unit
    def test_elapsed_is_a_duration_not_a_clock_reading(self) -> None:
        """elapsed_seconds measures the block, so it stays near zero here."""
        reporter = UploadProgressReporter(total_files=1, json_mode=True)
        with reporter:
            time.sleep(0.01)

        assert 0 < reporter.elapsed_seconds < 5

    @pytest.mark.unit
    def test_exception_details_reach_the_progress_bar(self, tty_progress: MagicMock) -> None:
        """Rich needs the live exception triple to tear the display down cleanly."""
        error = RuntimeError("upload failed")
        with pytest.raises(RuntimeError):
            with UploadProgressReporter(total_files=1, total_bytes=10):
                raise error

        exit_args = tty_progress.return_value.__exit__.call_args.args
        assert exit_args[0] is RuntimeError
        assert exit_args[1] is error
        assert exit_args[2] is error.__traceback__

    @pytest.mark.unit
    def test_clean_exit_passes_no_exception(self, tty_progress: MagicMock) -> None:
        """A successful upload tears the bar down with an empty triple."""
        with UploadProgressReporter(total_files=1, total_bytes=10):
            pass

        assert tty_progress.return_value.__exit__.call_args.args == (None, None, None)

    @pytest.mark.unit
    def test_average_speed_is_bytes_over_elapsed(self) -> None:
        """Speed divides transferred bytes by the measured duration."""
        reporter = UploadProgressReporter(total_files=1, total_bytes=1000, json_mode=True)
        with reporter:
            reporter.advance(bytes_uploaded=1000)
            time.sleep(0.01)

        assert reporter.average_speed == pytest.approx(
            reporter.bytes_completed / reporter.elapsed_seconds
        )

    @pytest.mark.unit
    def test_average_speed_is_zero_before_any_elapsed_time(self) -> None:
        """Reading speed before the context exits must not divide by zero."""
        reporter = UploadProgressReporter(total_files=1, total_bytes=1000)

        assert reporter.elapsed_seconds == 0.0
        assert reporter.average_speed == 0.0
