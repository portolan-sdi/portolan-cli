"""Progress reporting for file uploads.

This module provides:
1. A progress reporter context manager using Rich
2. Live display of upload progress with speed and ETA
3. Suppression of progress in JSON mode (agent/batch usage)

Example:
    >>> from portolan_cli.upload_progress import UploadProgressReporter
    >>> with UploadProgressReporter(total_files=100, total_bytes=1_000_000) as reporter:
    ...     for file in files:
    ...         upload(file)
    ...         reporter.advance(bytes_uploaded=file.size)
    ... print(f"Uploaded in {reporter.elapsed_seconds:.1f}s at {reporter.average_speed}")
"""

from __future__ import annotations

import sys
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import TracebackType

    from rich.progress import Progress, TaskID


class UploadProgressReporter:
    """Context manager for upload progress reporting.

    Provides a progress bar using Rich library, with automatic
    suppression in JSON mode or non-TTY environments.

    Displays:
    - File progress: N/M files
    - Data progress: transferred / total (e.g., "54.2 MB / 1.2 GB")
    - Upload speed: current speed (e.g., "10.5 MiB/s")
    - Time elapsed and ETA

    Attributes:
        total_files: Total number of files to upload.
        total_bytes: Total bytes to upload.
        json_mode: If True, suppress all progress output.
        files_completed: Number of files uploaded.
        bytes_completed: Bytes uploaded so far.
        elapsed_seconds: Time elapsed since entering context.
    """

    def __init__(
        self,
        total_files: int,
        total_bytes: int = 0,
        *,
        json_mode: bool = False,
    ) -> None:
        """Initialize the progress reporter.

        Args:
            total_files: Total number of files to upload.
            total_bytes: Total bytes to upload (for accurate progress bar).
            json_mode: If True, suppress progress output.
        """
        self.total_files = total_files
        self.total_bytes = total_bytes
        self.json_mode = json_mode
        self.files_completed = 0
        self.bytes_completed = 0
        self.elapsed_seconds: float = 0.0

        self._start_time: float | None = None
        self._progress: Progress | None = None
        self._task_id: TaskID | None = None

    def __enter__(self) -> UploadProgressReporter:
        """Enter the context and start progress display."""
        self._start_time = time.perf_counter()

        if not self.json_mode and sys.stdout.isatty():
            try:
                from rich.console import Console
                from rich.progress import (
                    BarColumn,
                    DownloadColumn,
                    Progress,
                    SpinnerColumn,
                    TextColumn,
                    TimeElapsedColumn,
                    TimeRemainingColumn,
                    TransferSpeedColumn,
                )

                # Use stderr console for progress output
                console = Console(file=sys.stderr, force_terminal=None)

                self._progress = Progress(
                    SpinnerColumn(),
                    TextColumn("[bold blue]Uploading"),
                    BarColumn(),
                    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                    DownloadColumn(),  # Shows "X.X MB / Y.Y MB"
                    TransferSpeedColumn(),  # Shows "X.X MB/s"
                    TimeElapsedColumn(),
                    TimeRemainingColumn(),
                    transient=True,  # Remove progress bar when done
                    console=console,
                )
                self._progress.__enter__()
                self._task_id = self._progress.add_task(
                    "Uploading",
                    total=self.total_bytes if self.total_bytes > 0 else self.total_files,
                )
            except ImportError:
                # rich not available, silently continue without progress
                self._progress = None

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit the context and cleanup progress display."""
        if self._start_time is not None:
            self.elapsed_seconds = time.perf_counter() - self._start_time

        if self._progress is not None:
            self._progress.__exit__(exc_type, exc_val, exc_tb)

    def advance(self, bytes_uploaded: int = 0) -> None:
        """Advance the progress by one file and the given bytes.

        Args:
            bytes_uploaded: Number of bytes uploaded for this file.
        """
        self.files_completed += 1
        self.bytes_completed += bytes_uploaded

        if self._progress is not None and self._task_id is not None:
            # Advance by bytes if we have byte tracking, otherwise by file count
            advance_amount = bytes_uploaded if self.total_bytes > 0 else 1
            self._progress.advance(self._task_id, advance=advance_amount)

    @property
    def average_speed(self) -> float:
        """Average upload speed in bytes per second."""
        if self.elapsed_seconds == 0:
            return 0.0
        return self.bytes_completed / self.elapsed_seconds
