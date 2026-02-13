"""VersioningBackend protocol for pluggable version storage.

This module defines the VersioningBackend Protocol that allows external backends
(like portolake's Iceberg implementation) to integrate with portolan-cli.

See ADR-0015 (Two-Tier Versioning Architecture) for architectural context.
See ADR-0003 (Plugin Architecture) for plugin discovery patterns.

Thread Safety:
    The MVP assumes single-writer access. Backends do NOT provide thread-safety
    guarantees. For concurrent multi-user access, use the portolake plugin with
    IcebergBackend which provides ACID transactions and distributed locking.

Example usage for plugin authors:
    # In portolake/pyproject.toml:
    [project.entry-points."portolan.backends"]
    iceberg = "portolake:IcebergBackend"

    # In portolake/__init__.py:
    class IcebergBackend:
        def get_current_version(self, collection: str) -> Version: ...
        # ... implement all protocol methods
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, TypedDict, runtime_checkable

if TYPE_CHECKING:
    from portolan_cli.versions import Version


class DriftReport(TypedDict):
    """Report from drift detection between local and remote state.

    Attributes:
        has_drift: True if local and remote versions differ.
        local_version: Local version string, or None if no local version exists.
        remote_version: Remote version string, or None if no remote version exists.
        message: Human-readable description of the drift status.
    """

    has_drift: bool
    local_version: str | None
    remote_version: str | None
    message: str


class SchemaFingerprint(TypedDict):
    """Schema fingerprint for detecting breaking changes between versions.

    Attributes:
        columns: Ordered list of column names.
        types: Mapping of column names to their data types (e.g., "int64", "geometry").
        hash: Hash of the schema for quick comparison.
    """

    columns: list[str]
    types: dict[str, str]
    hash: str


@runtime_checkable
class VersioningBackend(Protocol):
    """Protocol for version storage and management backends.

    This protocol defines the interface that all versioning backends must implement.
    The MVP uses JsonFileBackend (versions.json), while enterprise deployments can
    use IcebergBackend via the portolake plugin for ACID transactions.

    All methods take a `collection` parameter identifying which collection/dataset
    to operate on. The collection path is relative to the catalog root.

    Thread Safety:
        This protocol assumes single-writer access. Implementations are NOT required
        to be thread-safe. Use drift checking to detect concurrent modifications.
        For true concurrent access, use a backend with distributed locking (e.g.,
        IcebergBackend via portolake).
    """

    def get_current_version(self, collection: str) -> Version:
        """Get the current (latest) version of a collection.

        Args:
            collection: Collection identifier/path.

        Returns:
            The current Version object.

        Raises:
            FileNotFoundError: If the collection has no versions.
        """
        ...

    def list_versions(self, collection: str) -> list[Version]:
        """List all versions of a collection, oldest first.

        Args:
            collection: Collection identifier/path.

        Returns:
            List of Version objects, ordered oldest to newest.
        """
        ...

    def publish(
        self,
        collection: str,
        assets: dict[str, str],
        schema: SchemaFingerprint,
        breaking: bool,
        message: str,
    ) -> Version:
        """Publish a new version of a collection.

        Args:
            collection: Collection identifier/path.
            assets: Mapping of asset names to asset paths/URIs.
            schema: Schema fingerprint for change detection.
            breaking: Whether this is a breaking change.
            message: Human-readable description of the change.

        Returns:
            The newly created Version object.
        """
        ...

    def rollback(self, collection: str, target_version: str) -> Version:
        """Rollback to a previous version.

        This creates a NEW version with the contents of the target version,
        preserving full history. It does not delete intermediate versions.

        Args:
            collection: Collection identifier/path.
            target_version: Semantic version string to roll back to.

        Returns:
            The newly created Version object (representing the rollback).

        Raises:
            ValueError: If target_version doesn't exist.
        """
        ...

    def prune(self, collection: str, keep: int, dry_run: bool) -> list[Version]:
        """Remove old versions, keeping the N most recent.

        Args:
            collection: Collection identifier/path.
            keep: Number of recent versions to keep.
            dry_run: If True, don't actually delete, just report what would be deleted.

        Returns:
            List of Version objects that were (or would be) deleted.
        """
        ...

    def check_drift(self, collection: str) -> DriftReport:
        """Check for drift between local and remote state.

        Detects if another process has modified the remote versions.json
        without going through this backend. Used to catch accidental
        concurrent access in the MVP single-writer model.

        Args:
            collection: Collection identifier/path.

        Returns:
            DriftReport with drift status and details.
        """
        ...
