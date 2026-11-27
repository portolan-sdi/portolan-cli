"""Progress reporting for file add operations."""

from __future__ import annotations

import os
import sys
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from types import TracebackType

    from rich.progress import Progress, TaskID


def count_files(
    paths: list[Path],
    *,
    include_hidden: bool = False,
) -> int:
    """Fast pre-count of files for progress reporting."""
    count = 0

    for path in paths:
        if path.is_file():
            # Explicitly passed file paths are always counted (user intent)
            count += 1
        elif path.is_dir():
            count += _count_dir_files(path, include_hidden=include_hidden)

    return count


def _count_dir_files(root: Path, *, include_hidden: bool = False) -> int:
    """Recursively count files in a directory."""
    count = 0

    try:
        with os.scandir(root) as entries:
            for entry in entries:
                if not include_hidden and entry.name.startswith("."):
                    continue

                try:
                    if entry.is_file(follow_symlinks=False):
                        count += 1
                    elif entry.is_dir(follow_symlinks=False):
                        count += _count_dir_files(Path(entry.path), include_hidden=include_hidden)
                except (PermissionError, OSError):
                    continue
    except (PermissionError, OSError):
        pass

    return count


class AddProgressReporter:
    """Thread-safe context manager for add progress reporting."""

    def __init__(
        self,
        total_files: int,
        *,
        json_mode: bool = False,
    ) -> None:
        """Initialize the progress reporter."""
        self.total_files = total_files
        self.json_mode = json_mode
        self.files_processed = 0
        self.elapsed_seconds: float = 0.0

        self._start_time: float | None = None
        self._progress: Progress | None = None
        self._task_id: TaskID | None = None
        self._lock = threading.Lock()  # Thread-safety for parallel workers

    def __enter__(self) -> Self:
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

                console = Console(file=sys.stderr, force_terminal=None)

                self._progress = Progress(
                    SpinnerColumn(),
                    TextColumn("[bold blue]Adding..."),
                    BarColumn(),
                    MofNCompleteColumn(),
                    TextColumn("files"),
                    TimeElapsedColumn(),
                    transient=True,
                    console=console,
                )
                self._progress.__enter__()
                self._task_id = self._progress.add_task(
                    "Adding",
                    total=self.total_files,
                )
            except ImportError:
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
        """Advance the progress by the given number of files."""
        with self._lock:
            self.files_processed += steps

            if self._progress is not None and self._task_id is not None:
                self._progress.advance(self._task_id, advance=steps)
