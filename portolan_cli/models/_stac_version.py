"""Shared STAC version accessor for the model dataclasses.

The models use this accessor in a dataclass ``default_factory``. The constant
lives in ``constants.py``, which loads neither pystac nor ``stac.py``.
"""

from __future__ import annotations

from portolan_cli.constants import STAC_VERSION


def get_stac_version() -> str:
    """Get the STAC_VERSION constant.

    Returns:
        The current STAC version string (e.g., "1.1.0").
    """
    return STAC_VERSION
