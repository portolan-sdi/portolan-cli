"""Version operations middle layer.

Bridges CLI commands and versioning backends. All imports of
portolan_cli.backends and portolan_cli.config are lazy (inside function
bodies) to keep the module lightweight and avoid circular imports.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from portolan_cli.backends.protocol import SchemaFingerprint
    from portolan_cli.versions import Version


def _resolve_backend_name(
    cli_backend: str | None,
    catalog_root: Path | None,
) -> str:
    """Resolve backend name: CLI flag > config > default ('file')."""
    if cli_backend is not None:
        return cli_backend

    from portolan_cli.config import get_setting

    resolved: str | None = get_setting("backend", catalog_path=catalog_root)
    if resolved is not None:
        return resolved

    return "file"


def get_current_version(
    collection: str,
    *,
    backend_name: str | None = None,
    catalog_root: Path | None = None,
) -> Version:
    """Get the current version of a collection."""
    from portolan_cli.backends import get_backend

    name = _resolve_backend_name(backend_name, catalog_root)
    backend = get_backend(name, catalog_root=catalog_root)
    return backend.get_current_version(collection)


def list_versions(
    collection: str,
    *,
    backend_name: str | None = None,
    catalog_root: Path | None = None,
) -> list[Version]:
    """List all versions of a collection."""
    from portolan_cli.backends import get_backend

    name = _resolve_backend_name(backend_name, catalog_root)
    backend = get_backend(name, catalog_root=catalog_root)
    return backend.list_versions(collection)


def publish_version(
    collection: str,
    *,
    assets: dict[str, str],
    schema: SchemaFingerprint | None = None,
    breaking: bool = False,
    message: str = "",
    removed: set[str] | None = None,
    backend_name: str | None = None,
    catalog_root: Path | None = None,
) -> Version:
    """Publish a new version of a collection."""
    from portolan_cli.backends import get_backend

    name = _resolve_backend_name(backend_name, catalog_root)
    backend = get_backend(name, catalog_root=catalog_root)
    schema = schema or {"columns": [], "types": {}, "hash": "unknown"}
    return backend.publish(collection, assets, schema, breaking, message, removed=removed)


def rollback_version(
    collection: str,
    target_version: str,
    *,
    backend_name: str | None = None,
    catalog_root: Path | None = None,
) -> Version:
    """Rollback a collection to a previous version."""
    from portolan_cli.backends import get_backend

    name = _resolve_backend_name(backend_name, catalog_root)
    backend = get_backend(name, catalog_root=catalog_root)
    return backend.rollback(collection, target_version)


def prune_versions(
    collection: str,
    *,
    keep: int,
    dry_run: bool,
    backend_name: str | None = None,
    catalog_root: Path | None = None,
) -> list[Version]:
    """Prune old versions, keeping the N most recent."""
    from portolan_cli.backends import get_backend

    name = _resolve_backend_name(backend_name, catalog_root)
    backend = get_backend(name, catalog_root=catalog_root)
    return backend.prune(collection, keep=keep, dry_run=dry_run)
