"""Shared STAC version accessor for the model dataclasses."""

from __future__ import annotations

from portolan_core.constants import STAC_VERSION


def get_stac_version() -> str:
    """Get the STAC_VERSION constant."""
    return STAC_VERSION
