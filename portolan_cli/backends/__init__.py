"""Versioning backends for portolan-cli.

This module provides the discovery mechanism for versioning backends.
The MVP uses JsonFileBackend (versions.json), while the optional [iceberg]
extra provides the enterprise IcebergBackend (ACID transactions, rollback).

Built-in backends:
    - "file" — JsonFileBackend (versions.json, always available)
    - "iceberg" — IcebergBackend (requires: pip install portolan-cli[iceberg])

Third-party plugins can still register additional backends via the
"portolan.backends" entry point group.

See ADR-0015 (Two-Tier Versioning Architecture) for architectural context.
See ADR-0041 (Iceberg as Optional Extra) for the merge decision.

Usage:
    from portolan_cli.backends import get_backend

    # Get the default file-based backend
    backend = get_backend()

    # Get the Iceberg backend (requires [iceberg] extra)
    backend = get_backend("iceberg")
"""

from __future__ import annotations

import logging
from importlib.metadata import EntryPoint, entry_points
from pathlib import Path

from portolan_cli.backends.protocol import DriftReport, SchemaFingerprint, VersioningBackend

__all__ = ["DriftReport", "SchemaFingerprint", "VersioningBackend", "get_backend"]

logger = logging.getLogger(__name__)


def get_backend(name: str = "file", catalog_root: Path | None = None) -> VersioningBackend:
    """Get a versioning backend by name.

    Discovers backends through three mechanisms:
    1. Built-in "file" backend (JsonFileBackend using versions.json)
    2. Built-in "iceberg" backend (requires [iceberg] extra)
    3. External plugins registered via "portolan.backends" entry point

    Note: Creates a NEW instance on each call. This function does not implement
    singleton semantics; each call returns a fresh backend instance.

    Args:
        name: Backend name. "file" for built-in, "iceberg" for Iceberg
            (requires [iceberg] extra), or a plugin name.

    Returns:
        VersioningBackend instance.

    Raises:
        ValueError: If backend not found, dependencies missing, plugin fails to
            load/instantiate, or plugin doesn't implement VersioningBackend.
            Message includes available backends and error details.

    Example:
        >>> backend = get_backend()  # Default file backend
        >>> backend = get_backend("file")  # Explicit file backend
        >>> backend = get_backend("iceberg")  # Iceberg backend (requires [iceberg] extra)
    """
    # Built-in file backend
    if name == "file":
        from portolan_cli.backends.json_file import JsonFileBackend

        logger.debug("Creating JsonFileBackend instance")
        return JsonFileBackend(catalog_root=catalog_root)

    # Built-in iceberg backend (optional extra)
    if name == "iceberg":
        try:
            from portolan_cli.backends.iceberg import IcebergBackend
        except ImportError as e:
            raise ValueError(
                "The 'iceberg' backend requires the iceberg extra. "
                "Install it with: pip install portolan-cli[iceberg]"
            ) from e
        logger.debug("Creating IcebergBackend instance")
        return IcebergBackend(catalog_root=catalog_root)

    # Discover plugin backends via entry points
    eps = entry_points(group="portolan.backends")
    for ep in eps:
        logger.debug("Found backend plugin: %s", ep.name)
        if ep.name == name:
            return _load_plugin_backend(ep, name, catalog_root=catalog_root)

    # Build helpful error message
    plugin_names = [ep.name for ep in eps]
    available = ["file", "iceberg"] + plugin_names
    raise ValueError(f"Unknown backend: {name}. Available: {', '.join(available)}")


def _load_plugin_backend(
    ep: EntryPoint, name: str, catalog_root: Path | None = None
) -> VersioningBackend:
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
        backend = backend_class(catalog_root=catalog_root)
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
