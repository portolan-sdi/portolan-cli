"""Shared utility functions.

Small helpers used across multiple modules.
"""

from __future__ import annotations

import posixpath
from pathlib import PurePath
from typing import Any


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
