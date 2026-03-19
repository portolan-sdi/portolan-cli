"""Progress reporting for directory scanning.

This module provides:
1. Fast directory pre-counting for determinate progress bars
2. A progress reporter context manager using rich
3. Suppression of progress in JSON mode (agent/batch usage)

Example:
    >>> from portolan_cli.scan_progress import count_directories, ScanProgressReporter
    >>> total = count_directories(Path("/data"))
    >>> with ScanProgressReporter(total, json_mode=False) as reporter:
    ...     for directory in scan_directories():
    ...         reporter.advance()
    ... print(f"Scanned in {reporter.elapsed_seconds:.1f}s")
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import TracebackType

    from rich.progress import Progress, TaskID


def count_directories(
    root: Path,
    *,
    include_hidden: bool = False,
    max_depth: int | None = None,
    recursive: bool = True,
    follow_symlinks: bool = False,
) -> int:
    """Fast pre-count of directories for progress reporting.

    Uses os.scandir for efficient directory enumeration without
    reading file contents.

    Args:
        root: Root directory to count from.
        include_hidden: Include hidden directories (starting with .).
        max_depth: Maximum recursion depth (None = unlimited).
        recursive: If False, only count the root directory.
        follow_symlinks: If True, follow symlinks when counting directories.
            Should match the value used for actual scanning to ensure
            accurate progress reporting.

    Returns:
        Total number of directories (including root).

    Note:
        This should complete in < 100ms for typical directory trees.
        For accurate progress bars, use the same follow_symlinks value
        as the actual scan operation.
    """
    if not root.is_dir():
        return 0

    count = 1  # Start with root

    if not recursive:
        return count

    # Track visited directories by (device, inode) to prevent symlink loops
    visited: set[tuple[int, int]] = set()
    if follow_symlinks:
        try:
            root_stat = os.stat(root, follow_symlinks=True)
            visited.add((root_stat.st_dev, root_stat.st_ino))
        except OSError:
            pass

    def _count_recursive(path: Path, current_depth: int) -> int:
        """Recursively count directories."""
        nonlocal count

        # Check depth limit
        if max_depth is not None and current_depth > max_depth:
            return count

        try:
            with os.scandir(path) as entries:
                for entry in entries:
                    # Skip hidden if not included
                    if not include_hidden and entry.name.startswith("."):
                        continue

                    try:
                        if entry.is_dir(follow_symlinks=follow_symlinks):
                            # Check for symlink loops when following symlinks
                            if follow_symlinks:
                                try:
                                    entry_stat = os.stat(entry.path, follow_symlinks=True)
                                    inode_key = (entry_stat.st_dev, entry_stat.st_ino)
                                    if inode_key in visited:
                                        continue  # Skip already-visited directory
                                    visited.add(inode_key)
                                except OSError:
                                    continue  # Skip inaccessible entries

                            count += 1
                            _count_recursive(Path(entry.path), current_depth + 1)
                    except (PermissionError, OSError):
                        # Skip inaccessible directories
                        continue
        except (PermissionError, OSError):
            pass

        return count

    _count_recursive(root, 1)
    return count


class ScanProgressReporter:
    """Context manager for scan progress reporting.

    Provides a progress bar using rich library, with automatic
    suppression in JSON mode or non-TTY environments.

    Attributes:
        total_directories: Total number of directories to scan.
        json_mode: If True, suppress all progress output.
        directories_processed: Number of directories processed.
        elapsed_seconds: Time elapsed since entering context.
    """

    def __init__(
        self,
        total_directories: int,
        *,
        json_mode: bool = False,
    ) -> None:
        """Initialize the progress reporter.

        Args:
            total_directories: Total number of directories to scan.
            json_mode: If True, suppress progress output.
        """
        self.total_directories = total_directories
        self.json_mode = json_mode
        self.directories_processed = 0
        self.elapsed_seconds: float = 0.0

        self._start_time: float | None = None
        self._progress: Progress | None = None
        self._task_id: TaskID | None = None

    def __enter__(self) -> ScanProgressReporter:
        """Enter the context and start progress display."""
        self._start_time = time.perf_counter()

        if not self.json_mode and sys.stderr.isatty():
            try:
                from rich.console import Console
                from rich.progress import (
                    BarColumn,
                    MofNCompleteColumn,
                    Progress,
                    SpinnerColumn,
                    TextColumn,
                    TimeElapsedColumn,
                )

                # Use stderr console for progress output
                console = Console(file=sys.stderr, force_terminal=None)

                self._progress = Progress(
                    SpinnerColumn(),
                    TextColumn("[bold blue]Scanning..."),
                    BarColumn(),
                    MofNCompleteColumn(),
                    TextColumn("directories"),
                    TimeElapsedColumn(),
                    transient=True,  # Remove progress bar when done
                    console=console,
                )
                self._progress.__enter__()
                self._task_id = self._progress.add_task(
                    "Scanning",
                    total=self.total_directories,
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

    def advance(self, steps: int = 1) -> None:
        """Advance the progress by the given number of directories.

        Args:
            steps: Number of directories processed.
        """
        self.directories_processed += steps

        if self._progress is not None and self._task_id is not None:
            self._progress.advance(self._task_id, advance=steps)
