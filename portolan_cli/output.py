"""Standardized terminal output utilities.

All user-facing CLI messages should use these functions for consistent
formatting across the application.

Usage:
    from portolan_cli.output import success, info, warn, error, detail

    success("Wrote output.parquet (1.2 MB)")
    info("Reading data.shp (4,231 features)")
    warn("Missing thumbnail (recommended)")
    error("No geometry column (required)")
    detail("Processing chunk 3/10...")
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
) -> None:
    """Internal helper for styled output."""
    prefix = _PREFIXES[style]
    fg_color = _STYLES[style]["fg"]
    styled_prefix = click.style(prefix, fg=fg_color)
    styled_message = click.style(message, fg=fg_color)
    click.echo(f"{styled_prefix} {styled_message}", file=file, nl=nl)


def success(message: str, *, file: TextIO | None = None, nl: bool = True) -> None:
    """Print a success message with green checkmark.

    Args:
        message: The message to display.
        file: File to write to (default: stdout).
        nl: Whether to print a newline after the message.

    Example:
        >>> success("Wrote output.parquet (1.2 MB)")
        ✓ Wrote output.parquet (1.2 MB)
    """
    _output(message, "success", file=file, nl=nl)


def info(message: str, *, file: TextIO | None = None, nl: bool = True) -> None:
    """Print an info message with blue arrow.

    Args:
        message: The message to display.
        file: File to write to (default: stdout).
        nl: Whether to print a newline after the message.

    Example:
        >>> info("Reading data.shp (4,231 features)")
        → Reading data.shp (4,231 features)
    """
    _output(message, "info", file=file, nl=nl)


def warn(message: str, *, file: TextIO | None = None, nl: bool = True) -> None:
    """Print a warning message with yellow warning symbol.

    Args:
        message: The message to display.
        file: File to write to (default: stderr).
        nl: Whether to print a newline after the message.

    Example:
        >>> warn("Missing thumbnail (recommended)")
        ⚠ Missing thumbnail (recommended)
    """
    _output(message, "warn", file=file or sys.stderr, nl=nl)


def error(message: str, *, file: TextIO | None = None, nl: bool = True) -> None:
    """Print an error message with red X.

    Args:
        message: The message to display.
        file: File to write to (default: stderr).
        nl: Whether to print a newline after the message.

    Example:
        >>> error("No geometry column (required)")
        ✗ No geometry column (required)
    """
    _output(message, "error", file=file or sys.stderr, nl=nl)


def detail(message: str, *, file: TextIO | None = None, nl: bool = True) -> None:
    """Print a detail/progress message in dimmed text.

    Args:
        message: The message to display.
        file: File to write to (default: stdout).
        nl: Whether to print a newline after the message.

    Example:
        >>> detail("Processing chunk 3/10...")
          Processing chunk 3/10...
    """
    _output(message, "detail", file=file, nl=nl)
