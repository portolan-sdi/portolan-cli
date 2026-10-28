"""VersioningBackend protocol for pluggable version storage.

This module defines the VersioningBackend Protocol. A backend can come from
portolan-cli itself ("file", "iceberg") or from a plugin package.

Thread Safety:
    The MVP assumes single-writer access. Backends do NOT provide thread-safety
    guarantees. For concurrent multi-user access, use the IcebergBackend from
    the [iceberg] extra, which provides ACID transactions.

Example usage for plugin authors (#947):
    # In example/pyproject.toml:
    [project.entry-points."portolan"]
    example = "portolan_example.hooks"

    # In portolan_example/hooks.py:
    from portolan_cli.hookspecs import hookimpl

    @hookimpl
    def portolan_backends():
        from portolan_example.backend import ExampleBackend

        return {"example": ExampleBackend}

    # ExampleBackend(catalog_root=...) implements every protocol method.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, TypedDict, runtime_checkable

if TYPE_CHECKING:
    from pathlib import Path

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


class PostAddItemContext(TypedDict):
    """Per-item context within PostAddContext.items list.

    Attributes:
        item_id: The STAC item identifier.
        item_dir: Path to the item directory containing assets.
        asset_files: Dict mapping filename to (path, checksum, size) tuples.
    """

    item_id: str
    item_dir: Path
    asset_files: dict[str, tuple[Path, str, int]]


class PostAddContext(TypedDict):
    """Context passed to :meth:`VersioningBackend.on_post_add`.

    After add() completes local processing, portolan-cli calls the active
    backend's ``on_post_add`` with this context.

    Note:
        The ``collection`` field is typed as Any to avoid a hard dependency on pystac.
        At runtime, this will be a pystac.Collection instance.
    """

    catalog_root: Path
    collection_id: str
    collection_dir: Path
    collection: Any  # pystac.Collection (typed as Any to avoid import)
    item_id: str
    item_dir: Path
    asset_files: dict[str, tuple[Path, str, int]]
    remote: str | None
    items: list[PostAddItemContext]


class PullOutcome(Protocol):
    """What a backend pull reports. ``portolan_cli.api.PullResult`` has this shape.

    The protocol names the fields, not the class, so this module imports
    nothing from the sync layer.
    """

    success: bool
    files_downloaded: int
    files_skipped: int
    local_version: str | None
    remote_version: str | None
    up_to_date: bool


# Generated methods become extra protocol requirements: https://github.com/boxed/mutmut/issues/590
@runtime_checkable
class VersioningBackend(Protocol):  # pragma: no mutate block
    """Protocol for version storage and management backends.

    This protocol defines the interface that all versioning backends must implement.
    The MVP uses JsonFileBackend (versions.json), while enterprise deployments can
    use IcebergBackend via the portolake plugin for ACID transactions.

    All methods take a `collection` parameter identifying which collection
    to operate on. The collection path is relative to the catalog root.

    Thread Safety:
        This protocol assumes single-writer access. Implementations are NOT required
        to be thread-safe. Use drift checking to detect concurrent modifications.
        For true concurrent access, use a backend with distributed locking (e.g.,
        IcebergBackend via portolake).

    Lifecycle hooks:
        ``on_post_add``, ``pull``, ``supports_push`` and ``push_blocked_message``
        are protocol methods, so mypy checks every call against them (#977).
        ``JsonFileBackend`` shows the neutral behavior: no post-add work, no
        backend pull, and push allowed.
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
        removed: set[str] | None = None,
        version: str | None = None,
    ) -> Version:
        """Publish a new version of a collection.

        Args:
            collection: Collection identifier/path.
            assets: Mapping of asset names to asset paths/URIs.
            schema: Schema fingerprint for change detection.
            breaking: Whether this is a breaking change.
            message: Human-readable description of the change.
            removed: Set of asset keys to remove from the version.
            version: Explicit version string. If None, auto-compute next version.

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

    def on_post_add(self, context: PostAddContext) -> None:
        """Run backend work after ``add`` published a version of a collection.

        A failure is logged and does not fail the add, because the version is
        already published.

        Args:
            context: The collection and the items the add wrote.
        """
        ...

    def pull(
        self,
        remote_url: str,
        local_root: Path,
        collection: str,
        *,
        dry_run: bool = False,
    ) -> PullOutcome | None:
        """Pull one collection through the backend.

        ``portolan pull`` without ``--collection`` calls this once per
        collection of the catalog.

        Args:
            remote_url: The remote catalog URL.
            local_root: The local catalog root.
            collection: Collection identifier/path.
            dry_run: Report what would be pulled, and download nothing.

        Returns:
            The pull result, or None when the file-based pull must run instead.
        """
        ...

    def supports_push(self) -> bool:
        """Whether ``portolan push`` may upload a catalog on this backend."""
        ...

    def push_blocked_message(self, remote: str | None) -> str:
        """The message ``push`` prints when :meth:`supports_push` is False.

        Args:
            remote: The configured remote, or None.
        """
        ...
