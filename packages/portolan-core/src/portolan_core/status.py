"""Status module - shows local vs remote version state."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from portolan_core.constants import MANAGED_FILES
from portolan_core.run_state import RunState
from portolan_core.stac_links import catalog_collections, versioned_collections

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from portolan_core.versions import VersionsFile


@dataclass
class CollectionStatus:
    """Status of a single collection."""

    collection: str
    local_version: str | None
    remote_version: str | None
    modified_files: list[str] = field(default_factory=list)
    untracked_files: list[str] = field(default_factory=list)
    deleted_files: list[str] = field(default_factory=list)

    @property
    def sync_state(self) -> str:
        """Compute sync state relative to remote."""
        if self.remote_version is None:
            return "unknown"

        if self.local_version is None:
            return "behind" if self.remote_version else "unknown"

        if self.local_version == self.remote_version:
            return "in_sync"

        # Simple semver comparison for common cases
        local_parts = self._parse_version(self.local_version)
        remote_parts = self._parse_version(self.remote_version)

        if local_parts > remote_parts:
            return "ahead"
        if local_parts < remote_parts:
            return "behind"
        return "diverged"

    @staticmethod
    def _parse_version(version: str) -> tuple[int, int, int]:
        """Parse semver string to comparable tuple."""
        try:
            parts = version.split("-", maxsplit=1)[0].split("+", maxsplit=1)[0].split(".")
            return (int(parts[0]), int(parts[1]), int(parts[2]))
        except (IndexError, ValueError):
            return (0, 0, 0)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dict for JSON output."""
        return {
            "collection": self.collection,
            "local_version": self.local_version,
            "remote_version": self.remote_version,
            "sync_state": self.sync_state,
            "modified_files": self.modified_files,
            "untracked_files": self.untracked_files,
            "deleted_files": self.deleted_files,
        }


def detect_modified_files(
    collection_path: Path,
    versions_file: VersionsFile,
    *,
    state: RunState | None = None,
) -> list[str]:
    """Detect files modified since last versioned state."""
    if not versions_file.versions:
        return []

    if state is None:
        state = RunState.open(collection_path.parent)
    modified = []
    for asset_name, asset in versions_file.versions[-1].assets.items():
        file_path = collection_path / asset_name
        if not file_path.exists():
            # Missing files are handled by detect_deleted_files
            continue
        try:
            if not state.unchanged(file_path, asset):
                modified.append(asset_name)
        except (OSError, ValueError):
            # Can't compute checksum, treat as modified
            modified.append(asset_name)

    return sorted(modified)


def detect_deleted_files(
    collection_path: Path,
    versions_file: VersionsFile,
) -> list[str]:
    """Detect files deleted since last versioned state."""
    if not versions_file.versions:
        return []

    current_version = versions_file.versions[-1]
    deleted = []

    for asset_name in current_version.assets:
        file_path = collection_path / asset_name
        if not file_path.exists():
            deleted.append(asset_name)

    return sorted(deleted)


def status_collections(catalog_root: Path) -> list[str]:
    """The collections ``status`` reports: the ``check`` set and the ``push`` set."""
    return sorted({*catalog_collections(catalog_root), *versioned_collections(catalog_root)})


def _is_stac_item(file_path: Path) -> bool:
    """Check if a JSON file is a STAC item (has type: Feature)."""
    if file_path.suffix != ".json":
        return False
    import json

    try:
        data = json.loads(file_path.read_text(encoding="utf-8"))
        return bool(data.get("type") == "Feature")
    except (json.JSONDecodeError, OSError, KeyError):
        return False


def detect_untracked_files(
    collection_path: Path,
    versions_file: VersionsFile,
) -> list[str]:
    """Detect data files on disk not tracked in versions.json."""
    if not versions_file.versions:
        # No versions = everything is untracked
        tracked: set[str] = set()
    else:
        tracked = set(versions_file.versions[-1].assets.keys())

    untracked = []
    for file_path in collection_path.iterdir():
        if not file_path.is_file():
            continue

        name = file_path.name

        # Skip Portolan-managed files
        if name in MANAGED_FILES:
            continue

        # Skip STAC item files (*.json with type: Feature)
        if _is_stac_item(file_path):
            continue

        # Report as untracked if not in versions.json
        if name not in tracked:
            untracked.append(name)

    return sorted(untracked)


def get_collection_status(
    catalog_root: Path,
    collection: str,
    *,
    offline: bool = False,
    remote_url: str | None = None,
    fetch_remote_versions: Callable[[str, str], VersionsFile] | None = None,
    state: RunState | None = None,
) -> CollectionStatus:
    """Get status for a single collection."""
    collection_path = catalog_root / collection
    versions_path = collection_path / "versions.json"
    if state is None:
        state = RunState.open(catalog_root)

    # Read local versions.json
    local_version: str | None = None
    versions_file: VersionsFile | None = None

    if versions_path.exists():
        try:
            versions_file = state.versions(collection_path)
            local_version = versions_file.current_version
        except (ValueError, FileNotFoundError):
            pass

    # Detect file changes
    modified_files: list[str] = []
    deleted_files: list[str] = []
    untracked_files: list[str] = []

    if versions_file is not None:
        modified_files = detect_modified_files(collection_path, versions_file, state=state)
        deleted_files = detect_deleted_files(collection_path, versions_file)
        untracked_files = detect_untracked_files(collection_path, versions_file)

    # Fetch remote version (unless offline)
    remote_version: str | None = None
    if not offline and remote_url and fetch_remote_versions is not None:
        remote_version = _fetch_remote_version(remote_url, collection, fetch_remote_versions)

    return CollectionStatus(
        collection=collection,
        local_version=local_version,
        remote_version=remote_version,
        modified_files=modified_files,
        untracked_files=untracked_files,
        deleted_files=deleted_files,
    )


@dataclass(frozen=True)
class CatalogStatus:
    """The status of each collection that ``status`` reports."""

    collections: list[CollectionStatus]

    def to_dict(self) -> dict[str, Any]:
        """Serialize the result as the ``status`` envelope data."""
        return {"collections": [s.to_dict() for s in self.collections]}


def get_catalog_status(
    catalog_root: Path,
    collection: str | None = None,
    *,
    offline: bool = False,
    remote_url: str | None = None,
    fetch_remote_versions: Callable[[str, str], VersionsFile] | None = None,
) -> CatalogStatus:
    """Get the status of one collection, or of each collection in the catalog."""
    collections = [collection] if collection else status_collections(catalog_root)
    return CatalogStatus(
        [
            get_collection_status(
                catalog_root=catalog_root,
                collection=coll,
                offline=offline,
                remote_url=remote_url,
                fetch_remote_versions=fetch_remote_versions,
            )
            for coll in collections
        ]
    )


def _fetch_remote_version(
    remote_url: str,
    collection: str,
    fetch_remote_versions: Callable[[str, str], VersionsFile],
) -> str | None:
    """Fetch current version from remote versions.json."""
    try:
        return fetch_remote_versions(remote_url, collection).current_version
    except Exception:
        return None
