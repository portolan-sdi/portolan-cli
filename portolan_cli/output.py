"""Standardized terminal output utilities.

All user-facing CLI messages should use these functions for consistent
formatting across the application.

Basic Usage:
    from portolan_cli.output import success, info, warn, error, detail

    success("Wrote output.parquet (1.2 MB)")
    info("Reading data.shp (4,231 features)")
    warn("Missing thumbnail (recommended)")
    error("No geometry column (required)")
    detail("Processing chunk 3/10...")

Dry-Run Mode:
    Add dry_run=True to prefix messages with [DRY RUN], indicating what
    *would* happen without actually performing the operation:

    success("Would write output.parquet", dry_run=True)
    # Output: ✓ [DRY RUN] Would write output.parquet

    Use dry-run mode for commands that modify state (add, sync,
    prune, repair) to preview operations before execution.

"""

from __future__ import annotations

import sys
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, TextIO

import click

if TYPE_CHECKING:
    from collections.abc import Generator

# Reentrant lock for console output to prevent interleaved output from concurrent threads
# RLock allows nested acquisition (e.g., output_section() calling error() which also locks)
_output_lock = threading.RLock()


@contextmanager
def output_section() -> Generator[None, None, None]:
    """Context manager for atomic multi-line output sections.

    Use this when you need to output multiple related lines that shouldn't
    be interleaved with output from other threads.

    Example:
        with output_section():
            error(f"Failed {name}: {msg}")
            for detail_line in details:
                warn(f"  {detail_line}")

    Note: Individual output functions (success, error, etc.) already use the
    lock for single-line output. This context manager is for multi-line
    sections that must stay together.
    """
    with _output_lock:
        yield


# ANSI color codes via click's style system
_STYLES: dict[str, dict[str, str | bool]] = {
    "success": {"fg": "green"},
    "info": {"fg": "blue"},
    "warn": {"fg": "yellow"},
    "error": {"fg": "red"},
    "detail": {"dim": True},
}

_PREFIXES = {
    "success": "\u2713",  # checkmark
    "info": "\u2192",  # arrow
    "warn": "\u26a0",  # warning
    "error": "\u2717",  # X
    "detail": " ",  # space (no prefix, just indent)
}


def _output(
    message: str,
    style: str,
    *,
    file: TextIO | None = None,
    nl: bool = True,
    dry_run: bool = False,
) -> None:
    """Internal helper for styled output.

    Args:
        message: The message to display.
        style: The style name (success, error, info, warn, detail).
        file: File to write to.
        nl: Whether to print a newline after the message.
        dry_run: If True, prefix message with [DRY RUN].
    """
    # Add dry-run prefix if enabled
    if dry_run:
        message = f"[DRY RUN] {message}"

    prefix = _PREFIXES[style]
    style_kwargs = _STYLES[style]
    fg = str(style_kwargs["fg"]) if "fg" in style_kwargs else None
    dim = bool(style_kwargs.get("dim", False))
    styled_prefix = click.style(prefix, fg=fg, dim=dim)
    styled_message = click.style(message, fg=fg, dim=dim)

    # Thread-safe output to prevent interleaving from concurrent threads
    with _output_lock:
        click.echo(f"{styled_prefix} {styled_message}", file=file, nl=nl)


def success(
    message: str,
    *,
    file: TextIO | None = None,
    nl: bool = True,
    dry_run: bool = False,
) -> None:
    """Print a success message with green checkmark.

    Args:
        message: The message to display.
        file: File to write to (default: stdout).
        nl: Whether to print a newline after the message.
        dry_run: If True, prefix with [DRY RUN] to indicate simulation mode.

    Example:
        >>> success("Wrote output.parquet (1.2 MB)")
        ✓ Wrote output.parquet (1.2 MB)

        >>> success("Would write file", dry_run=True)
        ✓ [DRY RUN] Would write file
    """
    _output(message, "success", file=file, nl=nl, dry_run=dry_run)


def info(
    message: str,
    *,
    file: TextIO | None = None,
    nl: bool = True,
    dry_run: bool = False,
) -> None:
    """Print an info message with blue arrow.

    Args:
        message: The message to display.
        file: File to write to (default: stdout).
        nl: Whether to print a newline after the message.
        dry_run: If True, prefix with [DRY RUN] to indicate simulation mode.

    Example:
        >>> info("Reading data.shp (4,231 features)")
        → Reading data.shp (4,231 features)

        >>> info("Would read file", dry_run=True)
        → [DRY RUN] Would read file
    """
    _output(message, "info", file=file, nl=nl, dry_run=dry_run)


def warn(
    message: str,
    *,
    file: TextIO | None = None,
    nl: bool = True,
    dry_run: bool = False,
) -> None:
    """Print a warning message with yellow warning symbol.

    Args:
        message: The message to display.
        file: File to write to (default: stderr).
        nl: Whether to print a newline after the message.
        dry_run: If True, prefix with [DRY RUN] to indicate simulation mode.

    Example:
        >>> warn("Missing thumbnail (recommended)")
        ⚠ Missing thumbnail (recommended)

        >>> warn("Would skip validation", dry_run=True)
        ⚠ [DRY RUN] Would skip validation
    """
    _output(message, "warn", file=file or sys.stderr, nl=nl, dry_run=dry_run)


def error(
    message: str,
    *,
    file: TextIO | None = None,
    nl: bool = True,
    dry_run: bool = False,
) -> None:
    """Print an error message with red X.

    Args:
        message: The message to display.
        file: File to write to (default: stderr).
        nl: Whether to print a newline after the message.
        dry_run: If True, prefix with [DRY RUN] to indicate simulation mode.

    Example:
        >>> error("No geometry column (required)")
        ✗ No geometry column (required)

        >>> error("Would fail validation", dry_run=True)
        ✗ [DRY RUN] Would fail validation
    """
    _output(message, "error", file=file or sys.stderr, nl=nl, dry_run=dry_run)


def detail(
    message: str,
    *,
    file: TextIO | None = None,
    nl: bool = True,
    dry_run: bool = False,
) -> None:
    """Print a detail/progress message in dimmed text.

    Args:
        message: The message to display.
        file: File to write to (default: stdout).
        nl: Whether to print a newline after the message.
        dry_run: If True, prefix with [DRY RUN] to indicate simulation mode.

    Example:
        >>> detail("Processing chunk 3/10...")
          Processing chunk 3/10...

        >>> detail("Would process chunk", dry_run=True)
          [DRY RUN] Would process chunk
    """
    _output(message, "detail", file=file, nl=nl, dry_run=dry_run)


def progress(
    filename: str | Path,
    *,
    current: int,
    total: int,
    context: str | None = None,
    file: TextIO | None = None,
) -> None:
    """Print a progress message showing file N of M.

    Use this for long-running batch operations (conversion, add, etc.)
    to show users that work is happening.

    Args:
        filename: The file being processed (Path or string).
        current: Current file number (1-indexed).
        total: Total number of files to process.
        context: Optional context string (e.g., "Converting to GeoParquet").
        file: File to write to (default: stdout).

    Example:
        >>> progress("census.shp", current=3, total=10)
        → Processing file 3 of 10: census.shp

        >>> progress("data.tif", current=1, total=5, context="Converting to COG")
        → Converting to COG (1 of 5): data.tif
    """
    # Extract just the filename, not the full path
    name = filename.name if isinstance(filename, Path) else Path(filename).name

    # Build the message
    if context:
        message = f"{context} ({current} of {total}): {name}"
    else:
        message = f"Processing file {current} of {total}: {name}"

    _output(message, "info", file=file, nl=True, dry_run=False)
