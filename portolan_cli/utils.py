"""Shared utility functions.

Small helpers used across multiple modules.
"""

from __future__ import annotations

import posixpath
from pathlib import Path, PurePath
from typing import Any


def href_root(path: Path) -> str:
    """The root string to hand pystac's ``normalize_hrefs``.

    ``normalize_hrefs`` tells a file from a directory by looking for a dot in
    the final path component, so a directory named ``tmp.XXXXXX`` reads as a
    file and the catalog lands in its parent (issue #401). A trailing slash is
    what settles it.

    The slash alone is not enough. pystac absolutizes a relative root against
    the working directory and drops the trailing slash on the way, which put the
    heuristic back in play for ``init``'s ``"."`` default (issue #731).
    Resolving first keeps the slash meaningful.

    This is the single writer for that string. Every ``normalize_hrefs`` call
    takes its root from here, and ``tests/unit/test_href_root.py`` fails if one
    builds it by hand instead.

    Args:
        path: Directory the catalog or collection is written to.

    Returns:
        The absolute directory path with exactly one trailing slash.
    """
    return f"{path.resolve()}/"


def relative_href(from_dir: PurePath, to_file: PurePath) -> str:
    """The POSIX href from a directory to a STAC file.

    A STAC href is a relative URL reference, so its separator is ``/`` on every
    platform. ``os.path.relpath`` returns the *native* one, and on Windows that
    shipped ``..\\catalog.json`` — not a Windows spelling of a parent link but a
    filename containing backslashes, which resolves nowhere (rashid
    ``PTL-LNK-006``). Normalizing both sides to POSIX first keeps the
    computation itself platform-independent.

    Both link ends flow through here so that ``root`` and ``parent`` are derived
    separately rather than sharing one href. Reusing the root href for ``parent``
    is what made a nested collection point past its own intermediate catalog
    (issue #711).

    Args:
        from_dir: Directory the link will live in.
        to_file: File the link points at.

    Returns:
        A relative POSIX href, e.g. ``../catalog.json``.
    """
    return posixpath.relpath(
        PurePath(to_file).as_posix(),
        PurePath(from_dir).as_posix(),
    )


def get_dict(data: dict[str, Any], key: str) -> dict[str, Any]:
    """Safely get a dict value, returning empty dict if not a dict.

    Args:
        data: Dictionary to read from.
        key: Key to look up.

    Returns:
        The value if it's a dict, otherwise an empty dict.
    """
    value = data.get(key, {})
    return value if isinstance(value, dict) else {}


def get_list(data: dict[str, Any], key: str) -> list[Any]:
    """Safely get a list value, returning empty list if not a list.

    Args:
        data: Dictionary to read from.
        key: Key to look up.

    Returns:
        The value if it's a list, otherwise an empty list.
    """
    value = data.get(key, [])
    return value if isinstance(value, list) else []


# =============================================================================
# Config Value Parsing Helpers
# =============================================================================


def parse_bool(value: Any, default: bool) -> bool:
    """Parse config value as bool, returning default if invalid."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return default


def parse_int(value: Any, default: int) -> int:
    """Parse config value as int, returning default if invalid."""
    if value is None:
        return default
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return default


def parse_positive_int(value: Any, default: int) -> int:
    """Parse config value as positive int, returning default if invalid."""
    if value is None:
        return default
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    return default


def parse_bounded_int(value: Any, default: int, lo: int, hi: int) -> int:
    """Parse config value as int in [lo, hi], returning default if invalid."""
    if value is None:
        return default
    if isinstance(value, int) and not isinstance(value, bool) and lo <= value <= hi:
        return value
    return default


def parse_str(value: Any, default: str) -> str:
    """Parse config value as string, returning default if invalid."""
    if value is None:
        return default
    if isinstance(value, str):
        return value
    return default


def parse_float(value: Any, default: float) -> float:
    """Parse config value as float, returning default if invalid."""
    if value is None:
        return default
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return default


def parse_bounded_float(value: Any, default: float, lo: float, hi: float) -> float:
    """Parse config value as float in [lo, hi], returning default if invalid."""
    if value is None:
        return default
    if isinstance(value, (int, float)) and not isinstance(value, bool) and lo <= value <= hi:
        return float(value)
    return default
