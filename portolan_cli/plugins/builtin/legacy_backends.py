"""Backends from the deprecated ``portolan.backends`` entry point group (#947).

v0.8.0 and v1.0.0b0 found plugin backends in this group. The
``portolan_backends`` hook replaces it. This module reads the group for one
more minor release and warns for each backend it finds there.
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any

from portolan_cli.hookspecs import hookimpl

if TYPE_CHECKING:
    from importlib.metadata import EntryPoint

    from portolan_cli.hookspecs import BackendFactory


@hookimpl
def portolan_backends() -> dict[str, BackendFactory] | None:
    """Register each backend in the ``portolan.backends`` group, with a warning."""
    from portolan_cli.plugins.manager import (
        LEGACY_BACKENDS_ENTRY_POINT_GROUP,
        PluginWarning,
        enabled_entry_points,
    )

    factories: dict[str, BackendFactory] = {}
    for ep in enabled_entry_points(LEGACY_BACKENDS_ENTRY_POINT_GROUP):
        dist = getattr(ep, "dist", None)
        source = dist.name if dist is not None else "an unknown distribution"
        warnings.warn(
            f"Backend '{ep.name}' from {source} uses the deprecated "
            f"'{LEGACY_BACKENDS_ENTRY_POINT_GROUP}' "
            "entry point group. Register it with the portolan_backends hook. A future "
            "minor release stops reading the group.",
            PluginWarning,
            stacklevel=2,
        )
        factories[ep.name] = _factory(ep)
    return factories or None


def _factory(ep: EntryPoint) -> BackendFactory:
    """Import the backend class only when ``get_backend`` asks for it."""

    def _create(**kwargs: Any) -> Any:
        return ep.load()(**kwargs)

    return _create
