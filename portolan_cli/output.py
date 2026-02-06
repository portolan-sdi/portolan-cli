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

    Use dry-run mode for commands that modify state (dataset add, sync,
    prune, repair) to preview operations before execution.

Verbose Mode:
    Add verbose=True to enable verbose output (reserved for future use):

    info("Reading data.shp", verbose=True)

    Currently, verbose mode has the same behavior as default. Future
    enhancements may add technical details like file paths, sizes,
    checksums, or internal library calls.

Combined Modes:
    Both modes can be active simultaneously:

    success("Would write file.parquet (1.2 MB)", dry_run=True, verbose=True)
    # Output: ✓ [DRY RUN] Would write file.parquet (1.2 MB)
"""

from __future__ import annotations

import sys
from typing import TextIO

import click

# ANSI color codes via click's style system
_STYLES = {
    "success": {"fg": "green"},
    "info": {"fg": "blue"},
    "warn": {"fg": "yellow"},
    "error": {"fg": "red"},
    "detail": {"fg": "bright_black"},  # Dimmed/gray
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
    verbose: bool = False,
) -> None:
    """Internal helper for styled output.

    Args:
        message: The message to display.
        style: The style name (success, error, info, warn, detail).
        file: File to write to.
        nl: Whether to print a newline after the message.
        dry_run: If True, prefix message with [DRY RUN].
        verbose: If True, show message (currently same behavior as default).
    """
    # Add dry-run prefix if enabled
    if dry_run:
        message = f"[DRY RUN] {message}"

    prefix = _PREFIXES[style]
    fg_color = _STYLES[style]["fg"]
    styled_prefix = click.style(prefix, fg=fg_color)
    styled_message = click.style(message, fg=fg_color)
    click.echo(f"{styled_prefix} {styled_message}", file=file, nl=nl)


def success(
    message: str,
    *,
    file: TextIO | None = None,
    nl: bool = True,
    dry_run: bool = False,
    verbose: bool = False,
) -> None:
    """Print a success message with green checkmark.

    Args:
        message: The message to display.
        file: File to write to (default: stdout).
        nl: Whether to print a newline after the message.
        dry_run: If True, prefix with [DRY RUN] to indicate simulation mode.
        verbose: If True, include verbose details (reserved for future use).

    Example:
        >>> success("Wrote output.parquet (1.2 MB)")
        ✓ Wrote output.parquet (1.2 MB)

        >>> success("Would write file", dry_run=True)
        ✓ [DRY RUN] Would write file
    """
    _output(message, "success", file=file, nl=nl, dry_run=dry_run, verbose=verbose)


def info(
    message: str,
    *,
    file: TextIO | None = None,
    nl: bool = True,
    dry_run: bool = False,
    verbose: bool = False,
) -> None:
    """Print an info message with blue arrow.

    Args:
        message: The message to display.
        file: File to write to (default: stdout).
        nl: Whether to print a newline after the message.
        dry_run: If True, prefix with [DRY RUN] to indicate simulation mode.
        verbose: If True, include verbose details (reserved for future use).

    Example:
        >>> info("Reading data.shp (4,231 features)")
        → Reading data.shp (4,231 features)

        >>> info("Would read file", dry_run=True)
        → [DRY RUN] Would read file
    """
    _output(message, "info", file=file, nl=nl, dry_run=dry_run, verbose=verbose)


def warn(
    message: str,
    *,
    file: TextIO | None = None,
    nl: bool = True,
    dry_run: bool = False,
    verbose: bool = False,
) -> None:
    """Print a warning message with yellow warning symbol.

    Args:
        message: The message to display.
        file: File to write to (default: stderr).
        nl: Whether to print a newline after the message.
        dry_run: If True, prefix with [DRY RUN] to indicate simulation mode.
        verbose: If True, include verbose details (reserved for future use).

    Example:
        >>> warn("Missing thumbnail (recommended)")
        ⚠ Missing thumbnail (recommended)

        >>> warn("Would skip validation", dry_run=True)
        ⚠ [DRY RUN] Would skip validation
    """
    _output(message, "warn", file=file or sys.stderr, nl=nl, dry_run=dry_run, verbose=verbose)


def error(
    message: str,
    *,
    file: TextIO | None = None,
    nl: bool = True,
    dry_run: bool = False,
    verbose: bool = False,
) -> None:
    """Print an error message with red X.

    Args:
        message: The message to display.
        file: File to write to (default: stderr).
        nl: Whether to print a newline after the message.
        dry_run: If True, prefix with [DRY RUN] to indicate simulation mode.
        verbose: If True, include verbose details (reserved for future use).

    Example:
        >>> error("No geometry column (required)")
        ✗ No geometry column (required)

        >>> error("Would fail validation", dry_run=True)
        ✗ [DRY RUN] Would fail validation
    """
    _output(message, "error", file=file or sys.stderr, nl=nl, dry_run=dry_run, verbose=verbose)


def detail(
    message: str,
    *,
    file: TextIO | None = None,
    nl: bool = True,
    dry_run: bool = False,
    verbose: bool = False,
) -> None:
    """Print a detail/progress message in dimmed text.

    Args:
        message: The message to display.
        file: File to write to (default: stdout).
        nl: Whether to print a newline after the message.
        dry_run: If True, prefix with [DRY RUN] to indicate simulation mode.
        verbose: If True, include verbose details (reserved for future use).

    Example:
        >>> detail("Processing chunk 3/10...")
          Processing chunk 3/10...

        >>> detail("Would process chunk", dry_run=True)
          [DRY RUN] Would process chunk
    """
    _output(message, "detail", file=file, nl=nl, dry_run=dry_run, verbose=verbose)
