"""Library functions of ``portolan version rollback`` and ``portolan version prune``.

``backends/iceberg/cli.py`` is the Click layer. It resolves the catalog root,
calls a function here, and emits ``result.to_dict()`` as the envelope data.
This module imports ``portolan_cli.api`` only, and no Click or pyiceberg, so
the help path can load the command module.
"""

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
    """Load the iceberg backend or raise BackendRequiredError.

    Args:
        catalog_path: Path to catalog root.
        command_name: Name of the command (for error messages).

    Returns:
        The iceberg VersioningBackend instance.

    Raises:
        BackendRequiredError: If the current backend is not 'iceberg'.
        BackendUnavailableError: If the iceberg backend cannot start.
    """
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
    """Set the current snapshot of ``collection`` back to ``target_version``.

    Raises:
        BackendRequiredError: If the catalog does not use the iceberg backend.
        BackendUnavailableError: If the iceberg backend cannot start.
        ValueError: If the collection does not hold ``target_version``.
    """
    backend = require_iceberg_backend(catalog_root, "rollback")
    return RollbackResult(collection, backend.rollback(collection, target_version))
