"""Shared utility functions.

Small helpers used across multiple modules.
"""

from __future__ import annotations

from typing import Any


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
