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

import logging
from importlib.metadata import EntryPoint, entry_points

from portolan_cli.backends.protocol import DriftReport, SchemaFingerprint, VersioningBackend

__all__ = ["DriftReport", "SchemaFingerprint", "VersioningBackend", "get_backend"]

logger = logging.getLogger(__name__)


def get_backend(name: str = "file") -> VersioningBackend:
    """Get a versioning backend by name.

    Discovers backends through two mechanisms:
    1. Built-in "file" backend (JsonFileBackend using versions.json)
    2. External plugins registered via "portolan.backends" entry point

    Note: Creates a NEW instance on each call. This function does not implement
    singleton semantics; each call returns a fresh backend instance.

    Args:
        name: Backend name. "file" for built-in, or plugin name (e.g., "iceberg").

    Returns:
        VersioningBackend instance.

    Raises:
        ValueError: If backend not found, plugin fails to load, plugin fails
            to instantiate, or plugin doesn't implement VersioningBackend.
            Message includes available backends and error details.

    Example:
        >>> backend = get_backend()  # Default file backend
        >>> backend = get_backend("file")  # Explicit file backend
        >>> backend = get_backend("iceberg")  # Plugin backend (requires portolake)
    """
    # Built-in file backend
    if name == "file":
        from portolan_cli.backends.json_file import JsonFileBackend

        logger.debug("Creating JsonFileBackend instance")
        return JsonFileBackend()

    # Discover plugin backends via entry points
    eps = entry_points(group="portolan.backends")
    for ep in eps:
        logger.debug("Found backend plugin: %s", ep.name)
        if ep.name == name:
            return _load_plugin_backend(ep, name)

    # Build helpful error message
    plugin_names = [ep.name for ep in eps]
    available = ["file"] + plugin_names
    raise ValueError(f"Unknown backend: {name}. Available: {', '.join(available)}")


def _load_plugin_backend(ep: EntryPoint, name: str) -> VersioningBackend:
    """Load and validate a plugin backend from an entry point.

    Args:
        ep: Entry point object with load() method.
        name: Backend name for error messages.

    Returns:
        Validated VersioningBackend instance.

    Raises:
        ValueError: If loading fails, instantiation fails, or protocol not implemented.
    """
    # Load the backend class from the entry point
    try:
        logger.debug("Loading backend class from entry point: %s", name)
        backend_class = ep.load()
    except Exception as e:
        msg = f"Failed to load backend '{name}': {e}"
        logger.error(msg)
        raise ValueError(msg) from e

    # Instantiate the backend
    try:
        logger.debug("Instantiating backend: %s", name)
        backend = backend_class()
    except Exception as e:
        msg = f"Failed to instantiate backend '{name}': {e}"
        logger.error(msg)
        raise ValueError(msg) from e

    # Validate protocol compliance
    if not isinstance(backend, VersioningBackend):
        msg = f"Backend '{name}' does not implement VersioningBackend protocol"
        logger.error(msg)
        raise ValueError(msg)

    logger.debug("Successfully loaded backend: %s", name)
    return backend
