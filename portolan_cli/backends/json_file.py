"""File-based versioning backend using versions.json."""

from __future__ import annotations

from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import TYPE_CHECKING

from portolan_cli.backends.protocol import (
    DriftReport,
    PostAddContext,
    PullOutcome,
    SchemaFingerprint,
)
from portolan_core.versions import (
    Asset,
    SchemaInfo,
    VersionsFile,
    add_version,
    parse_version,
    read_versions,
    write_versions,
)

if TYPE_CHECKING:
    from portolan_core.versions import Version


class JsonFileBackend:
    """MVP versioning backend using versions.json."""

    def __init__(self, catalog_root: Path | None = None) -> None:
        """Initialize the backend."""
        self._catalog_root = catalog_root or Path.cwd()

    def _versions_path(self, collection: str) -> Path:
        """Get the path to versions.json for a collection."""
        if not collection or not collection.strip():
            raise ValueError("Collection name cannot be empty")
        # Validate the nested id lexically, not with resolve(), so a symlinked root stays verbatim.
        # Check both POSIX and Windows flavors, because each flavor misses the other's escapes.
        candidate = Path(collection)
        flavors = (PurePosixPath(collection), PureWindowsPath(collection))
        if not candidate.parts or any(
            f.is_absolute() or f.root or f.drive or ".." in f.parts for f in flavors
        ):
            raise ValueError(f"Invalid collection name: {collection!r}")
        # versions.json at collection root
        return self._catalog_root.joinpath(*candidate.parts) / "versions.json"

    def get_current_version(self, collection: str) -> Version:
        """Get the current (latest) version of a collection."""
        versions_file = read_versions(self._versions_path(collection))
        if not versions_file.versions:
            raise FileNotFoundError(f"No versions found for collection: {collection}")
        return versions_file.versions[-1]

    def list_versions(self, collection: str) -> list[Version]:
        """List all versions of a collection, oldest first."""
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
        removed: set[str] | None = None,
        version: str | None = None,
    ) -> Version:
        """Publish a new version of a collection."""
        from portolan_core.checksums import compute_checksum
        from portolan_core.run_state import stat_matches
        from portolan_core.versions import SPEC_VERSION

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

        # Use explicit version if provided, otherwise auto-compute
        next_version = version or self._compute_next_version(versions_file, breaking)

        # An asset whose size and mtime match the latest record keeps its checksum, unhashed.
        previous = versions_file.versions[-1].assets if versions_file.versions else {}
        asset_objects: dict[str, Asset] = {}
        for name, path_str in assets.items():
            asset_path = Path(path_str)
            if asset_path.exists():
                stat = asset_path.stat()
                prior = previous.get(name)
                if (
                    prior is not None
                    and asset_path.resolve() == (self._catalog_root / prior.href).resolve()
                    and stat_matches(stat, prior.mtime, prior.size_bytes)
                ):
                    checksum = prior.sha256
                else:
                    checksum = compute_checksum(asset_path)
                size_bytes = stat.st_size if asset_path.is_file() else 0
                mtime = stat.st_mtime
            else:
                # Remote asset - no local checksum available
                checksum = ""
                size_bytes = 0
                mtime = None
            asset_objects[name] = Asset(
                sha256=checksum,
                size_bytes=size_bytes,
                href=f"{collection}/{name}",
                mtime=mtime,
            )

        # Convert protocol SchemaFingerprint to internal SchemaInfo
        schema_info = SchemaInfo(
            type=schema.get("hash", "unknown"),  # Use hash as type identifier
            fingerprint={
                "columns": schema.get("columns", []),
                "types": schema.get("types", {}),
            },
        )

        # Add version with schema and message
        updated = add_version(
            versions_file,
            version=next_version,
            assets=asset_objects,
            breaking=breaking,
            schema=schema_info,
            message=message,
            removed=removed,
        )

        # Atomic write: temp file, then rename
        write_versions(versions_path, updated)

        return updated.versions[-1]

    def _compute_next_version(self, versions_file: VersionsFile, breaking: bool) -> str:
        """Compute the next semantic version."""
        if not versions_file.versions:
            return "1.0.0"

        current = versions_file.current_version or "0.0.0"
        major, minor, _patch = parse_version(current)

        if breaking:
            return f"{major + 1}.0.0"
        return f"{major}.{minor + 1}.0"

    def rollback(self, collection: str, target_version: str) -> Version:
        """Rollback to a previous version."""
        raise NotImplementedError("Rollback is deferred to enterprise plugin backends. ")

    def prune(self, collection: str, keep: int, dry_run: bool) -> list[Version]:
        """Remove old versions, keeping the N most recent."""
        raise NotImplementedError("Prune is deferred to enterprise plugin backends. ")

    def check_drift(self, collection: str) -> DriftReport:
        """Check for drift between local and remote state."""
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

    def on_post_add(self, context: PostAddContext) -> None:  # the protocol fixes it
        """Do nothing: versions.json is already written when add finishes."""

    def pull(
        self,
        remote_url: str,  # noqa: ARG002 - the protocol fixes it
        local_root: Path,  # noqa: ARG002 - the protocol fixes it
        collection: str,  # noqa: ARG002 - the protocol fixes it
        *,
        dry_run: bool = False,  # noqa: ARG002 - the protocol fixes it
    ) -> PullOutcome | None:
        """Return None, so the file-based pull of ``sync.pull`` runs."""
        return None

    def supports_push(self) -> bool:
        """Allow push: the file backend publishes by uploading the catalog."""
        return True

    def push_blocked_message(self, remote: str | None) -> str:  # noqa: ARG002 - the protocol fixes it
        """Name the backend. ``push`` never shows it, as push is allowed."""
        return "Push is not supported with the 'file' backend."
