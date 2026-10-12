"""Versioning backends for portolan-cli.

Backends come from the ``portolan_backends`` hook (#947). The MVP uses
JsonFileBackend (versions.json). The optional [iceberg] extra provides
IcebergBackend (ACID transactions, rollback).

Built-in backends:
    - "file" — JsonFileBackend (versions.json, always available)
    - "iceberg" — IcebergBackend (requires: pip install portolan-cli[iceberg])

A plugin package adds a backend with a ``portolan_backends`` hook
implementation. See ``portolan_cli.hookspecs``.

Usage:
    from portolan_cli.backends import get_backend

    # Get the default file-based backend
    backend = get_backend()

    # Get the Iceberg backend (requires [iceberg] extra)
    backend = get_backend("iceberg")
"""

from __future__ import annotations

import logging
import warnings
from typing import TYPE_CHECKING, Any

from portolan_cli.backends.protocol import DriftReport, SchemaFingerprint, VersioningBackend

if TYPE_CHECKING:
    from pathlib import Path

__all__ = ["DriftReport", "SchemaFingerprint", "VersioningBackend", "get_backend"]

logger = logging.getLogger(__name__)

# Built-in until the Iceberg backend moves to its own hook module.
_ICEBERG = "iceberg"


def _registered_factories() -> dict[str, Any]:
    """Collect backend factories from every ``portolan_backends`` hook.

    ``call_each`` lists the built-ins first, so a built-in name keeps its
    built-in factory. A plugin that registers that name again produces a
    warning. A result that is not a mapping produces a warning and adds nothing.
    """
    from collections.abc import Mapping

    from portolan_cli.plugins.manager import PluginWarning, call_each

    factories: dict[str, Any] = {}
    owners: dict[str, str] = {}
    for plugin_name, result in call_each("portolan_backends"):
        if not isinstance(result, Mapping):
            warnings.warn(
                f"Plugin '{plugin_name}' returned {type(result).__name__} from "
                "portolan_backends, which is not a mapping of names to factories.",
                PluginWarning,
                stacklevel=3,
            )
            continue
        for name, factory in result.items():
            if name in factories or name == _ICEBERG:
                warnings.warn(
                    f"Plugin '{plugin_name}' registers backend '{name}', which "
                    f"'{owners.get(name, 'builtin:iceberg')}' already provides. "
                    "The first registration stays.",
                    PluginWarning,
                    stacklevel=3,
                )
                continue
            factories[name] = factory
            owners[name] = plugin_name
    return factories


def get_backend(name: str = "file", catalog_root: Path | None = None) -> VersioningBackend:
    """Get a versioning backend by name.

    Note: Creates a NEW instance on each call. This function does not implement
    singleton semantics; each call returns a fresh backend instance.

    Args:
        name: Backend name. "file" for built-in, "iceberg" for Iceberg
            (requires [iceberg] extra), or a name a plugin registers.
        catalog_root: Catalog root the backend reads and writes. None lets the
            backend resolve the root itself.

    Returns:
        VersioningBackend instance.

    Raises:
        ValueError: If the backend is not found, its dependencies are missing,
            its factory fails, or it does not implement VersioningBackend.
            The message names the backend and the available backends.

    Example:
        >>> backend = get_backend()  # Default file backend
        >>> backend = get_backend("file")  # Explicit file backend
        >>> backend = get_backend("iceberg")  # Iceberg backend (requires [iceberg] extra)
    """
    # Built-in iceberg backend (optional extra)
    if name == _ICEBERG:
        try:
            from portolan_cli.backends.iceberg import IcebergBackend
        except ImportError as e:
            raise ValueError(
                "The 'iceberg' backend requires the iceberg extra. "
                "Install it with: pip install portolan-cli[iceberg]"
            ) from e
        logger.debug("Creating IcebergBackend instance")
        return IcebergBackend(catalog_root=catalog_root)

    factories = _registered_factories()
    factory = factories.get(name)
    if factory is None:
        available = ", ".join(sorted([*factories, _ICEBERG]))
        raise ValueError(f"Unknown backend: {name}. Available: {available}")

    try:
        backend = factory(catalog_root=catalog_root)
    except Exception as e:
        msg = f"Failed to instantiate backend '{name}': {e}"
        logger.error(msg)
        raise ValueError(msg) from e

    if not isinstance(backend, VersioningBackend):
        msg = f"Backend '{name}' does not implement VersioningBackend protocol"
        logger.error(msg)
        raise ValueError(msg)

    logger.debug("Created backend: %s", name)
    return backend
