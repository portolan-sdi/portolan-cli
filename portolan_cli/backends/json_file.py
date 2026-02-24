"""File-based versioning backend using versions.json.

This is the MVP implementation of VersioningBackend that stores version history
in a JSON file. It assumes single-writer access (see ADR-0015).

For multi-user concurrent access, use the portolake plugin with IcebergBackend.

Implementation Status (MINOR #9, #10):
    - get_current_version: Implemented
    - list_versions: Implemented
    - publish: Implemented (with schema and message support per ADR-0005)
    - check_drift: Stub (deferred to sync implementation)
    - rollback: Deferred to portolake plugin for enterprise use
    - prune: Deferred to portolake plugin for enterprise use

Deferred Features:
    rollback and prune are intentionally not implemented in the MVP file backend.
    These operations require careful handling of remote state and are better suited
    for the enterprise portolake plugin with Iceberg's transactional guarantees.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from portolan_cli.backends.protocol import DriftReport, SchemaFingerprint
from portolan_cli.versions import (
    Asset,
    SchemaInfo,
    VersionsFile,
    add_version,
    parse_version,
    read_versions,
    write_versions,
)

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
        - rollback and prune are deferred to enterprise portolake plugin

    For enterprise deployments requiring concurrent access, ACID transactions,
    or distributed locking, use the portolake plugin with IcebergBackend.
    """

    def __init__(self, catalog_root: Path | None = None) -> None:
        """Initialize the backend.

        Args:
            catalog_root: Root directory of the catalog. If None, uses current directory.
        """
        self._catalog_root = catalog_root or Path.cwd()

    def _versions_path(self, collection: str) -> Path:
        """Get the path to versions.json for a collection.

        Args:
            collection: Collection identifier/path.

        Returns:
            Path to the collection's versions.json file.

        Raises:
            ValueError: If collection name is empty.
        """
        if not collection or not collection.strip():
            raise ValueError("Collection name cannot be empty")
        # Normalize path to prevent directory traversal (MAJOR #8)
        safe_collection = Path(collection).name
        # Explicitly reject traversal attempts that survive Path.name
        if safe_collection in ("", ".", ".."):
            raise ValueError(f"Invalid collection name: {collection!r}")
        return self._catalog_root / ".portolan" / "collections" / safe_collection / "versions.json"

    def get_current_version(self, collection: str) -> Version:
        """Get the current (latest) version of a collection.

        Args:
            collection: Collection identifier/path.

        Returns:
            The current Version object.

        Raises:
            FileNotFoundError: If the collection has no versions.json.
            ValueError: If collection name is empty or versions.json is invalid.
        """
        versions_file = read_versions(self._versions_path(collection))
        if not versions_file.versions:
            raise FileNotFoundError(f"No versions found for collection: {collection}")
        return versions_file.versions[-1]

    def list_versions(self, collection: str) -> list[Version]:
        """List all versions of a collection, oldest first.

        Args:
            collection: Collection identifier/path.

        Returns:
            List of Version objects, ordered oldest to newest.
            Returns empty list if versions.json doesn't exist.
        """
        try:
            versions_file = read_versions(self._versions_path(collection))
            return versions_file.versions
        except FileNotFoundError:
            return []

    def publish(
        self,
        collection: str,
        assets: dict[str, str],
        schema: SchemaFingerprint,
        breaking: bool,
        message: str,
    ) -> Version:
        """Publish a new version of a collection.

        Stores schema fingerprint and message as per ADR-0005.
        Uses atomic write to prevent corruption (CRITICAL #2).

        Args:
            collection: Collection identifier/path.
            assets: Mapping of asset names to asset paths/URIs.
            schema: Schema fingerprint for change detection (CRITICAL #1).
            breaking: Whether this is a breaking change.
            message: Human-readable description of the change (MAJOR #6).

        Returns:
            The newly created Version object.
        """
        from portolan_cli.dataset import compute_checksum
        from portolan_cli.versions import SPEC_VERSION

        versions_path = self._versions_path(collection)

        # Load or create versions file
        try:
            versions_file = read_versions(versions_path)
        except FileNotFoundError:
            versions_file = VersionsFile(
                spec_version=SPEC_VERSION,
                current_version=None,
                versions=[],
            )

        # Compute next version
        next_version = self._compute_next_version(versions_file, breaking)

        # Build asset objects with checksums
        asset_objects: dict[str, Asset] = {}
        for name, path_str in assets.items():
            asset_path = Path(path_str)
            if asset_path.exists():
                checksum = compute_checksum(asset_path)
                size_bytes = asset_path.stat().st_size
            else:
                # Remote asset - no local checksum available
                checksum = ""
                size_bytes = 0
            asset_objects[name] = Asset(
                sha256=checksum,
                size_bytes=size_bytes,
                href=path_str,
            )

        # Convert protocol SchemaFingerprint to internal SchemaInfo (CRITICAL #1)
        schema_info = SchemaInfo(
            type=schema.get("hash", "unknown"),  # Use hash as type identifier
            fingerprint={
                "columns": schema.get("columns", []),
                "types": schema.get("types", {}),
            },
        )

        # Add version with schema and message (CRITICAL #1, MAJOR #6)
        updated = add_version(
            versions_file,
            version=next_version,
            assets=asset_objects,
            breaking=breaking,
            schema=schema_info,
            message=message,
        )

        # Atomic write (CRITICAL #2)
        write_versions(versions_path, updated)

        return updated.versions[-1]

    def _compute_next_version(self, versions_file: VersionsFile, breaking: bool) -> str:
        """Compute the next semantic version.

        Args:
            versions_file: Current versions file.
            breaking: Whether this is a breaking change.

        Returns:
            Next version string (e.g., "1.1.0" or "2.0.0").
        """
        if not versions_file.versions:
            return "1.0.0"

        current = versions_file.current_version or "0.0.0"
        major, minor, patch = parse_version(current)

        if breaking:
            return f"{major + 1}.0.0"
        else:
            return f"{major}.{minor + 1}.0"

    def rollback(self, collection: str, target_version: str) -> Version:
        """Rollback to a previous version.

        Note: This feature is deferred to the enterprise portolake plugin.
        The MVP file backend does not implement rollback because it requires
        careful handling of remote state and transactional guarantees.

        Args:
            collection: Collection identifier/path.
            target_version: Semantic version string to roll back to.

        Returns:
            The newly created Version object (representing the rollback).

        Raises:
            NotImplementedError: Rollback is deferred to portolake plugin.
        """
        raise NotImplementedError(
            "Rollback is deferred to the portolake plugin for enterprise use. "
            "See ADR-0015 for details on the two-tier versioning architecture."
        )

    def prune(self, collection: str, keep: int, dry_run: bool) -> list[Version]:
        """Remove old versions, keeping the N most recent.

        Note: This feature is deferred to the enterprise portolake plugin.
        The MVP file backend does not implement prune because it requires
        careful handling of remote state and transactional guarantees.

        Args:
            collection: Collection identifier/path.
            keep: Number of recent versions to keep.
            dry_run: If True, don't actually delete, just report what would be deleted.

        Returns:
            List of Version objects that were (or would be) deleted.

        Raises:
            NotImplementedError: Prune is deferred to portolake plugin.
        """
        raise NotImplementedError(
            "Prune is deferred to the portolake plugin for enterprise use. "
            "See ADR-0015 for details on the two-tier versioning architecture."
        )

    def check_drift(self, collection: str) -> DriftReport:
        """Check for drift between local and remote state.

        Note: Full drift detection requires comparing against remote storage,
        which is part of the sync implementation. This method currently provides
        a stub that reports no drift.

        Args:
            collection: Collection identifier/path.

        Returns:
            DriftReport with drift status and details.
        """
        try:
            versions_file = read_versions(self._versions_path(collection))
            current = versions_file.current_version
        except FileNotFoundError:
            current = None

        # Stub: actual drift detection requires remote comparison
        return DriftReport(
            has_drift=False,
            local_version=current,
            remote_version=current,  # Stub: would query remote
            message="Drift detection pending sync implementation",
        )
