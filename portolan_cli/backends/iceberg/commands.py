"""Library functions of ``portolan version rollback`` and ``portolan version prune``."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from portolan_cli import api

if TYPE_CHECKING:
    from pathlib import Path

    from portolan_cli.api import Version, VersioningBackend


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


class BackendUnavailableError(Exception):
    """Raised when the Iceberg backend cannot start, for example without the extra.

    The message is the message of the ``ValueError`` that ``get_backend`` raised.
    """


def require_iceberg_backend(catalog_path: Path, command_name: str) -> VersioningBackend:
    """Load the iceberg backend or raise BackendRequiredError."""
    backend_name = api.get_setting("backend", catalog_path=catalog_path)
    if backend_name != "iceberg":
        raise BackendRequiredError(command_name, "iceberg", backend_name)
    try:
        backend: VersioningBackend = api.get_backend("iceberg", catalog_root=catalog_path)
    except ValueError as e:
        raise BackendUnavailableError(str(e)) from e
    return backend


@dataclass(frozen=True)
class RollbackResult:
    """The result of ``portolan version rollback``."""

    collection: str
    restored: Version

    def to_dict(self) -> dict[str, Any]:
        """Return the envelope data of ``portolan version rollback``."""
        return {
            "collection": self.collection,
            "restored_version": self.restored.version,
            "created": self.restored.created.isoformat(),
        }


def rollback_collection(
    collection: str, target_version: str, *, catalog_root: Path
) -> RollbackResult:
    """Set the current snapshot of ``collection`` back to ``target_version``."""
    backend = require_iceberg_backend(catalog_root, "rollback")
    return RollbackResult(collection, backend.rollback(collection, target_version))


@dataclass(frozen=True)
class PruneResult:
    """The result of ``portolan version prune``."""

    collection: str
    pruned: list[Version]
    keep: int
    dry_run: bool

    def to_dict(self) -> dict[str, Any]:
        """Return the envelope data of ``portolan version prune``."""
        return {
            "collection": self.collection,
            "pruned": [
                {"version": v.version, "created": v.created.isoformat()} for v in self.pruned
            ],
            "kept": self.keep,
            "dry_run": self.dry_run,
        }


def prune_collection(
    collection: str, *, keep: int = 5, dry_run: bool = False, catalog_root: Path
) -> PruneResult:
    """Remove the old versions of ``collection``, keeping the ``keep`` most recent."""
    backend = require_iceberg_backend(catalog_root, "prune")
    pruned = backend.prune(collection, keep=keep, dry_run=dry_run)
    return PruneResult(collection, pruned, keep, dry_run)
