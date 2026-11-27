"""Standardized terminal output utilities."""

from __future__ import annotations

import sys
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, TextIO

if TYPE_CHECKING:
    from collections.abc import Generator

# Reentrant lock for console output to prevent interleaved output from concurrent threads
_output_lock = threading.RLock()


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

# The SGR codes that click.style emits for the colors above.
_ANSI_FG = {"green": 32, "blue": 34, "yellow": 33, "red": 31}
_ANSI_DIM = 2
_ANSI_RESET = "\x1b[0m"


class Writer(Protocol):
    """Write one styled message to a stream."""

    def __call__(
        self,
        prefix: str,
        message: str,
        *,
        fg: str | None,
        dim: bool,
        file: TextIO | None,
        nl: bool,
    ) -> None:
        """Write ``prefix`` and ``message``, styled with ``fg`` and ``dim``."""


def _ansi(text: str, fg: str | None, dim: bool) -> str:
    """Wrap text in the ANSI codes ``click.style(text, fg=fg, dim=dim)`` emits."""
    codes = ([f"\x1b[{_ANSI_FG[fg]}m"] if fg else []) + ([f"\x1b[{_ANSI_DIM}m"] if dim else [])
    return "".join(codes) + text + _ANSI_RESET


def default_writer(
    prefix: str,
    message: str,
    *,
    fg: str | None,
    dim: bool,
    file: TextIO | None,
    nl: bool,
) -> None:
    """Write without Click. Add color only when the stream is a terminal.

    ``sys.stdout`` resolves at call time, as it does in ``click.echo``.
    """
    stream = file if file is not None else sys.stdout
    isatty = getattr(stream, "isatty", None)
    if callable(isatty) and isatty():
        prefix, message = _ansi(prefix, fg, dim), _ansi(message, fg, dim)
    stream.write(f"{prefix} {message}" + ("\n" if nl else ""))
    stream.flush()


class _Switches:
    """Hold the process-wide output switches, so that no function rebinds a global."""

    def __init__(self) -> None:
        self.writer: Writer = default_writer
        self.stdout_to_stderr = False


_state = _Switches()


def set_writer(writer: Writer) -> None:
    """Install the writer that every output function uses."""
    _state.writer = writer


def get_writer() -> Writer:
    """Return the installed writer."""
    return _state.writer


@contextmanager
def stdout_to_stderr() -> Generator[None, None, None]:
    """Send the stdout-bound messages to stderr until the block ends."""
    previous = _state.stdout_to_stderr
    _state.stdout_to_stderr = True
    try:
        yield
    finally:
        _state.stdout_to_stderr = previous


def _output(
    message: str,
    style: str,
    *,
    file: TextIO | None = None,
    nl: bool = True,
    dry_run: bool = False,
) -> None:
    """Internal helper for styled output."""
    # Add dry-run prefix if enabled
    if dry_run:
        message = f"[DRY RUN] {message}"

    style_kwargs = _STYLES[style]
    fg = str(style_kwargs["fg"]) if "fg" in style_kwargs else None
    dim = bool(style_kwargs.get("dim", False))

    if file is None and _state.stdout_to_stderr:
        file = sys.stderr

    # Thread-safe output to prevent interleaving from concurrent threads
    with _output_lock:
        _state.writer(_PREFIXES[style], message, fg=fg, dim=dim, file=file, nl=nl)


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
        >>> detail("Would process chunk", dry_run=True)
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
    """Print a progress message showing file N of M."""
    # Extract just the filename, not the full path
    name = filename.name if isinstance(filename, Path) else Path(filename).name

    # Build the message
    if context:
        message = f"{context} ({current} of {total}): {name}"
    else:
        message = f"Processing file {current} of {total}: {name}"

    _output(message, "info", file=file, nl=True, dry_run=False)
