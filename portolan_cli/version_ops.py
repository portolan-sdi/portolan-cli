"""Version operations middle layer.

Bridges CLI commands and versioning backends. All imports of
portolan_cli.backends and portolan_cli.config are lazy (inside function
bodies) to keep the module lightweight and avoid circular imports.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from portolan_cli.backends.protocol import SchemaFingerprint, VersioningBackend
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


# =============================================================================
# Backend routing helpers (used by CLI for push/pull/version commands)
# =============================================================================


class PushBlockedResult:
    """Result from check_backend_supports_push when push is blocked."""

    def __init__(self, message: str) -> None:
        self.blocked = True
        self.message = message


def check_backend_supports_push(
    catalog_path: Path,
    collection: str | None = None,
) -> PushBlockedResult | None:
    """Check if the active backend supports file-based push.

    Returns None if push is allowed, or a PushBlockedResult with the
    blocking message if push is not supported.
    """
    from portolan_cli.backends import get_backend
    from portolan_cli.config import get_setting

    active_backend = get_setting("backend", catalog_path=catalog_path, collection=collection)
    if active_backend is None or active_backend == "file":
        return None

    backend = get_backend(active_backend, catalog_root=catalog_path)
    if not (hasattr(backend, "supports_push") and not backend.supports_push()):
        return None

    remote = get_setting("remote", catalog_path=catalog_path, collection=collection)
    if hasattr(backend, "push_blocked_message"):
        msg = backend.push_blocked_message(remote)
    else:
        msg = f"Push is not supported with the '{active_backend}' backend."

    return PushBlockedResult(msg)


class BackendPullResult:
    """Result from try_backend_pull."""

    def __init__(
        self,
        handled: bool,
        success: bool = True,
        files_downloaded: int = 0,
        files_skipped: int = 0,
        local_version: str | None = None,
        remote_version: str | None = None,
        up_to_date: bool = False,
    ) -> None:
        self.handled = handled
        self.success = success
        self.files_downloaded = files_downloaded
        self.files_skipped = files_skipped
        self.local_version = local_version
        self.remote_version = remote_version
        self.up_to_date = up_to_date


def try_backend_pull(
    catalog_path: Path,
    remote_url: str,
    collection: str | None,
    dry_run: bool,
) -> BackendPullResult:
    """Attempt a pull via the active non-file backend.

    Returns a BackendPullResult with handled=True if the backend handled the pull,
    or handled=False if the file-based pull should proceed as normal.
    """
    from portolan_cli.backends import get_backend
    from portolan_cli.config import get_setting

    active_backend = get_setting("backend", catalog_path=catalog_path, collection=collection)
    if active_backend is None or active_backend == "file":
        return BackendPullResult(handled=False)

    backend = get_backend(active_backend, catalog_root=catalog_path)
    if not hasattr(backend, "pull"):
        return BackendPullResult(handled=False)

    result = backend.pull(
        remote_url=remote_url,
        local_root=catalog_path,
        collection=collection,
        dry_run=dry_run,
    )

    return BackendPullResult(
        handled=True,
        success=result.success,
        files_downloaded=result.files_downloaded,
        files_skipped=result.files_skipped,
        local_version=result.local_version,
        remote_version=result.remote_version,
        up_to_date=getattr(result, "up_to_date", False),
    )


class BackendRequiredError(Exception):
    """Raised when a required backend is not configured."""

    def __init__(self, command_name: str, required: str, current: str | None) -> None:
        self.command_name = command_name
        self.required = required
        self.current = current or "file"
        super().__init__(
            f"'portolan version {command_name}' requires the '{required}' backend. "
            f"Current backend: '{self.current}'"
        )


def require_iceberg_backend(
    catalog_path: Path,
    command_name: str,
) -> VersioningBackend:
    """Load the iceberg backend or raise BackendRequiredError.

    Args:
        catalog_path: Path to catalog root.
        command_name: Name of the command (for error messages).

    Returns:
        The iceberg VersioningBackend instance.

    Raises:
        BackendRequiredError: If the current backend is not 'iceberg'.
    """
    from portolan_cli.backends import get_backend
    from portolan_cli.config import get_setting

    backend_name = get_setting("backend", catalog_path=catalog_path)
    if backend_name != "iceberg":
        raise BackendRequiredError(command_name, "iceberg", backend_name)

    return get_backend("iceberg", catalog_root=catalog_path)
