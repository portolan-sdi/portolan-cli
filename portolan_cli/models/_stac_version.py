"""Shared STAC version accessor to avoid circular imports.

This module provides a lazy accessor for STAC_VERSION that can be used
in dataclass default_factory without causing circular import issues.
"""

from __future__ import annotations


def get_stac_version() -> str:
    """Get STAC_VERSION constant (avoids circular import).

    Returns:
        The current STAC version string (e.g., "1.1.0").
    """
    from portolan_cli.stac import STAC_VERSION

    return STAC_VERSION
