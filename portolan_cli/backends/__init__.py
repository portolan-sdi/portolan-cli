"""Versioning backends for portolan-cli.

This module provides the plugin discovery mechanism for versioning backends.
The MVP uses JsonFileBackend (versions.json), while external plugins like
portolake can provide enterprise backends (Iceberg/Icechunk).

See ADR-0015 (Two-Tier Versioning Architecture) for architectural context.
See ADR-0003 (Plugin Architecture) for plugin patterns.

Usage:
    from portolan_cli.backends import get_backend

    # Get the default file-based backend
    backend = get_backend()

    # Get a specific backend (if plugin is installed)
    backend = get_backend("iceberg")

Plugin registration (in plugin's pyproject.toml):
    [project.entry-points."portolan.backends"]
    iceberg = "portolake:IcebergBackend"
"""

from __future__ import annotations

from importlib.metadata import entry_points
from typing import cast

from portolan_cli.backends.protocol import VersioningBackend

__all__ = ["VersioningBackend", "get_backend"]


def get_backend(name: str = "file") -> VersioningBackend:
    """Get a versioning backend by name.

    Discovers backends through two mechanisms:
    1. Built-in "file" backend (JsonFileBackend using versions.json)
    2. External plugins registered via "portolan.backends" entry point

    Args:
        name: Backend name. "file" for built-in, or plugin name (e.g., "iceberg").

    Returns:
        VersioningBackend instance.

    Raises:
        ValueError: If backend not found. Message includes available backends.

    Example:
        >>> backend = get_backend()  # Default file backend
        >>> backend = get_backend("file")  # Explicit file backend
        >>> backend = get_backend("iceberg")  # Plugin backend (requires portolake)
    """
    # Built-in file backend
    if name == "file":
        from portolan_cli.backends.json_file import JsonFileBackend

        return JsonFileBackend()

    # Discover plugin backends via entry points
    eps = entry_points(group="portolan.backends")
    for ep in eps:
        if ep.name == name:
            backend_class = ep.load()
            # Entry points are dynamically loaded; cast for type safety
            return cast(VersioningBackend, backend_class())

    # Build helpful error message
    plugin_names = [ep.name for ep in eps]
    available = ["file"] + plugin_names
    raise ValueError(f"Unknown backend: {name}. Available: {', '.join(available)}")
