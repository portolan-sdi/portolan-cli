"""File-based versioning backend using versions.json.

This is the MVP implementation of VersioningBackend that stores version history
in a JSON file. It assumes single-writer access (see ADR-0015).

For multi-user concurrent access, use the portolake plugin with IcebergBackend.

Note: Methods currently raise NotImplementedError as stubs.
Implementation will wire to existing functions in portolan_cli.versions.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from portolan_cli.versions import Version


class JsonFileBackend:
    """MVP versioning backend using versions.json.

    This backend uses the versions.json file as the single source of truth
    for version history, sync state, and integrity checksums (ADR-0005).

    Limitations:
        - Single-writer access only (no concurrent safety)
        - No distributed locking
        - Conflict detection via drift checking, not prevention

    For enterprise deployments requiring concurrent access, ACID transactions,
    or distributed locking, use the portolake plugin with IcebergBackend.
    """

    def get_current_version(self, collection: str) -> Version:
        """Get the current (latest) version of a collection.

        Args:
            collection: Collection identifier/path.

        Returns:
            The current Version object.

        Raises:
            NotImplementedError: Method is a stub, pending wiring to versions.py.
        """
        raise NotImplementedError("Wire to versions.py")

    def list_versions(self, collection: str) -> list[Version]:
        """List all versions of a collection, oldest first.

        Args:
            collection: Collection identifier/path.

        Returns:
            List of Version objects, ordered oldest to newest.

        Raises:
            NotImplementedError: Method is a stub, pending wiring to versions.py.
        """
        raise NotImplementedError("Wire to versions.py")

    def publish(
        self,
        collection: str,
        assets: dict[str, Any],
        schema: dict[str, Any],
        breaking: bool,
        message: str,
    ) -> Version:
        """Publish a new version of a collection.

        Args:
            collection: Collection identifier/path.
            assets: Mapping of asset names to asset metadata.
            schema: Schema fingerprint for change detection.
            breaking: Whether this is a breaking change.
            message: Human-readable description of the change.

        Returns:
            The newly created Version object.

        Raises:
            NotImplementedError: Method is a stub, pending wiring to versions.py.
        """
        raise NotImplementedError("Wire to versions.py")

    def rollback(self, collection: str, target_version: str) -> Version:
        """Rollback to a previous version.

        Args:
            collection: Collection identifier/path.
            target_version: Semantic version string to roll back to.

        Returns:
            The newly created Version object (representing the rollback).

        Raises:
            NotImplementedError: Method is a stub, pending wiring to versions.py.
        """
        raise NotImplementedError("Wire to versions.py")

    def prune(self, collection: str, keep: int, dry_run: bool) -> list[Version]:
        """Remove old versions, keeping the N most recent.

        Args:
            collection: Collection identifier/path.
            keep: Number of recent versions to keep.
            dry_run: If True, don't actually delete, just report what would be deleted.

        Returns:
            List of Version objects that were (or would be) deleted.

        Raises:
            NotImplementedError: Method is a stub, pending wiring to versions.py.
        """
        raise NotImplementedError("Wire to versions.py")

    def check_drift(self, collection: str) -> dict[str, Any]:
        """Check for drift between local and remote state.

        Args:
            collection: Collection identifier/path.

        Returns:
            DriftReport dict with drift status and details.

        Raises:
            NotImplementedError: Method is a stub, pending wiring to versions.py.
        """
        raise NotImplementedError("Wire to versions.py")
