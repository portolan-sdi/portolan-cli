"""Version operations middle layer."""

from __future__ import annotations

import logging
import re
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from functools import partial
from typing import TYPE_CHECKING, Any

from portolan_core.trace import count

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

    from portolan_cli.backends.protocol import SchemaFingerprint
    from portolan_core.run_state import RunState, StagedVersion
    from portolan_core.versions import Version

logger = logging.getLogger(__name__)


def _resolve_backend_name(
    cli_backend: str | None,
    catalog_root: Path | None,
    state: RunState | None = None,
    *,
    collection: str | None = None,
) -> str:
    """Resolve backend name: CLI flag > config > default ('file')."""
    if cli_backend is not None:
        return cli_backend

    if state is not None:
        directory = state.collection_dir(collection) if collection is not None else None
        resolved: str | None = state.setting("backend", collection_dir=directory)
    elif collection is not None:
        from pathlib import Path

        # Backend factories use cwd when no root is supplied. Resolve the same
        # collection's configuration rather than silently selecting file.
        resolved = _collection_setting("backend", catalog_root or Path.cwd(), collection, None)
    else:
        from portolan_core.config import get_setting

        resolved = get_setting("backend", catalog_path=catalog_root, collection=collection)
    if resolved is not None:
        return resolved

    return "file"


def _collection_setting(
    key: str, catalog_path: Path, collection: str | None, state: RunState | None
) -> Any | None:
    """Read a setting for a collection through the run state."""
    if state is None:
        from portolan_core.run_state import RunState

        state = RunState.open(catalog_path)
    collection_dir = state.collection_dir(collection) if collection else None
    return state.setting(key, collection_dir=collection_dir)


def get_current_version(
    collection: str,
    *,
    backend_name: str | None = None,
    catalog_root: Path | None = None,
    state: RunState | None = None,
) -> Version:
    """Get the current version of a collection."""
    from portolan_cli.backends import get_backend

    name = _resolve_backend_name(backend_name, catalog_root, state, collection=collection)
    backend = get_backend(name, catalog_root=state.catalog_root if state else catalog_root)
    return backend.get_current_version(collection)


def _version_dict(ver: Version) -> dict[str, Any]:
    """Summarize a version for the JSON envelope: an ISO timestamp and an asset count."""
    return {
        "version": ver.version,
        "created": ver.created.isoformat(),
        "breaking": ver.breaking,
        "message": ver.message,
        "assets": len(ver.assets),
        "changes": ver.changes,
    }


@dataclass(frozen=True)
class CurrentVersionResult:
    """The current version of a collection, the result of ``portolan version current``."""

    collection: str
    version: Version

    def to_dict(self) -> dict[str, Any]:
        """Return the envelope data of ``portolan version current``."""
        return {"collection": self.collection, **_version_dict(self.version)}


def describe_current_version(
    collection: str, *, catalog_root: Path | None = None
) -> CurrentVersionResult:
    """Return the current version of ``collection`` through the active backend."""
    return CurrentVersionResult(
        collection, get_current_version(collection, catalog_root=catalog_root)
    )


def list_versions(
    collection: str,
    *,
    backend_name: str | None = None,
    catalog_root: Path | None = None,
    state: RunState | None = None,
) -> list[Version]:
    """List all versions of a collection."""
    from portolan_cli.backends import get_backend

    name = _resolve_backend_name(backend_name, catalog_root, state, collection=collection)
    backend = get_backend(name, catalog_root=state.catalog_root if state else catalog_root)
    return backend.list_versions(collection)


@dataclass(frozen=True)
class VersionHistoryResult:
    """Each version of a collection, oldest first, the result of ``portolan version list``."""

    collection: str
    versions: list[Version]

    def to_dict(self) -> dict[str, Any]:
        """Return the envelope data of ``portolan version list``."""
        return {
            "collection": self.collection,
            "versions": [_version_dict(v) for v in self.versions],
        }


def describe_version_history(
    collection: str, *, catalog_root: Path | None = None
) -> VersionHistoryResult:
    """Return each version of ``collection`` through the active backend."""
    return VersionHistoryResult(collection, list_versions(collection, catalog_root=catalog_root))


def publish_version(
    collection: str,
    *,
    assets: dict[str, str],
    schema: SchemaFingerprint | None = None,
    breaking: bool = False,
    message: str = "",
    removed: set[str] | None = None,
    version: str | None = None,
    backend_name: str | None = None,
    catalog_root: Path | None = None,
    state: RunState | None = None,
) -> Version:
    """Publish a new version of a collection through the active backend."""
    from portolan_cli.backends import get_backend

    name = _resolve_backend_name(backend_name, catalog_root, state, collection=collection)
    backend = get_backend(name, catalog_root=state.catalog_root if state else catalog_root)
    schema = schema or {"columns": [], "types": {}, "hash": "unknown"}
    published = backend.publish(
        collection, assets, schema, breaking, message, removed=removed, version=version
    )
    # The backend persisted a version. add_version counts nothing, so a
    # projection of a staged change never counts as a publish.
    count("versions.published")
    return published


_SEMVER = re.compile(r"^\d+\.\d+\.\d+(-[a-zA-Z0-9.-]+)?(\+[a-zA-Z0-9.-]+)?$")


def semver_error(version: str) -> str | None:
    """Return the message that rejects ``version``, or None for a valid semver string."""
    if _SEMVER.match(version):
        return None
    return f"Invalid semver: '{version}'. Expected format: MAJOR.MINOR.PATCH (e.g., '1.2.0')"


@dataclass(frozen=True)
class VersionBumpResult:
    """The outcome of ``portolan version bump``."""

    collection: str
    previous_version: str | None = None
    modified: list[str] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)
    published: Version | None = None
    error: tuple[str, str] | None = None

    @property
    def changed(self) -> bool:
        """True when a file of the collection changed since the current version."""
        return bool(self.modified or self.deleted)

    def to_dict(self) -> dict[str, Any]:
        """Return the envelope data of ``portolan version bump``."""
        if self.error is not None or (self.published is None and self.changed):
            raise ValueError(f"A failed or declined bump of {self.collection} has no envelope data")
        if self.published is None:
            return {
                "collection": self.collection,
                "message": "No changes detected",
                "created": False,
            }
        return {
            "collection": self.collection,
            "version": self.published.version,
            "previous_version": self.previous_version,
            "created": self.published.created.isoformat(),
            "breaking": self.published.breaking,
            "message": self.published.message,
            "modified_files": self.modified,
            "deleted_files": self.deleted,
        }


def bump_collection_version(
    collection: str,
    new_version: str,
    *,
    catalog_root: Path,
    notes: str | None = None,
    breaking: bool = False,
    confirm: Callable[[VersionBumpResult], bool] | None = None,
) -> VersionBumpResult:
    """Publish ``new_version`` of ``collection`` from the files on disk."""
    from portolan_core.status import detect_deleted_files, detect_modified_files
    from portolan_core.versions import read_versions

    def failed(error_type: str, message: str) -> VersionBumpResult:
        return VersionBumpResult(collection, error=(error_type, message))

    invalid = semver_error(new_version)
    if invalid is not None:
        return failed("ValidationError", invalid)
    collection_path = catalog_root / collection
    versions_path = collection_path / "versions.json"
    if not versions_path.exists():
        return failed("NotFoundError", f"No versions.json in {collection}")
    try:
        versions_file = read_versions(versions_path)
    except ValueError as e:
        return failed("ParseError", f"Invalid versions.json: {e}")
    if new_version in {v.version for v in versions_file.versions}:
        return failed("DuplicateVersionError", f"Version '{new_version}' already exists")

    plan = VersionBumpResult(
        collection,
        versions_file.current_version,
        detect_modified_files(collection_path, versions_file),
        detect_deleted_files(collection_path, versions_file),
    )
    if not plan.changed or (confirm is not None and not confirm(plan)):
        return plan
    assets = {
        name: str(collection_path / name)
        for name in plan.modified
        if (collection_path / name).exists()
    }
    try:
        published = publish_version(
            collection,
            assets=assets,
            breaking=breaking,
            message=notes or "",
            removed=set(plan.deleted) if plan.deleted else None,
            version=new_version,
            catalog_root=catalog_root,
        )
    # A plugin backend can raise any exception, and the result reports each one.
    except Exception as e:
        return replace(plan, error=(type(e).__name__, f"Failed to create version: {e}"))
    return replace(plan, published=published)


def _schema_fingerprint(staged: StagedVersion) -> SchemaFingerprint | None:
    if staged.schema is None:
        return None
    fingerprint = staged.schema.fingerprint
    return {
        "columns": fingerprint.get("columns", []),
        "types": fingerprint.get("types", {}),
        "hash": staged.schema.type,
    }


def _commit_to_plugin(
    state: RunState, staged: dict[str, StagedVersion], backend_name: str
) -> dict[str, Version]:
    """Publish one version per staged collection through a plugin backend.

    The protocol takes asset paths, so the plugin computes its own checksums.
    """
    published: dict[str, Version] = {}
    for collection_id, change in sorted(staged.items()):
        published[collection_id] = publish_version(
            collection_id,
            assets={key: str(state.catalog_root / a.href) for key, a in change.assets.items()},
            schema=_schema_fingerprint(change),
            breaking=change.breaking,
            message=change.message or "",
            removed=change.removed or None,
            backend_name=backend_name,
            catalog_root=state.catalog_root,
            state=state,
        )
        if change.on_publish is not None:
            change.on_publish()
    return published


def commit_run(state: RunState) -> dict[str, Version]:
    """Write the versions that a run staged, one version per collection."""
    from portolan_cli.catalog import CollectionSummary, collection_summaries, update_catalog_index
    from portolan_core.versions import write_versions

    pending = state.take_staged()
    reconcile_index = not pending
    published: dict[str, Version] = {}
    summaries: dict[str, CollectionSummary] = {}
    try:
        by_backend: dict[str, dict[str, StagedVersion]] = {}
        for collection_id, change in pending.items():
            backend = state.backend(state.collection_dir(collection_id))
            by_backend.setdefault(backend, {})[collection_id] = change
        for backend, changes in sorted(by_backend.items()):
            if backend != "file":
                for collection_id, change in sorted(changes.items()):
                    published.update(_commit_to_plugin(state, {collection_id: change}, backend))
                    pending.pop(collection_id)
        for collection_id, change in sorted(by_backend.get("file", {}).items()):
            collection_dir = state.collection_dir(collection_id)
            disk = state.versions(collection_dir, include_staged=False)
            updated = change.apply(disk)
            if updated is not disk:
                versions_path = collection_dir / "versions.json"
                write_versions(versions_path, updated)
                state.invalidate(versions_path)
                published[collection_id] = updated.versions[-1]
                if len(updated.versions) > len(disk.versions):
                    # An amend rewrites the latest version and publishes none.
                    count("versions.published")
            if updated.versions:
                summaries[collection_id] = CollectionSummary.of(updated.versions[-1])
            pending.pop(collection_id)
    finally:
        # A later collection can fail after earlier writes reached disk. Keep
        # its stage retryable, and index the writes that already succeeded.
        state.restore_staged(pending)
        try:
            # The recovery reads under the index lock. A summary read before it
            # could overwrite the newer entry of another add.
            update_catalog_index(
                state.catalog_root,
                partial(collection_summaries, state) if reconcile_index else summaries,
            )
        except Exception:
            logger.warning(
                "Failed to update catalog-level versions.json. "
                "The collection versions were written, but the catalog-level view may be stale.",
                exc_info=True,
            )
    return published


@contextmanager
def owned_run(catalog_root: Path, state: RunState | None = None) -> Iterator[RunState]:
    """Yield the run state of a public entry point, and commit it if it is new."""
    if state is not None:
        yield state
        return
    from portolan_core.run_state import RunState

    own = RunState.open(catalog_root)
    yield own
    commit_run(own)


class PushBlockedResult:
    """Result from check_backend_supports_push when push is blocked."""

    def __init__(self, message: str) -> None:
        self.message = message


def check_backend_supports_push(
    catalog_path: Path,
    collection: str | None = None,
    *,
    state: RunState | None = None,
) -> PushBlockedResult | None:
    """Check if the active backend supports file-based push."""
    from portolan_cli.backends import get_backend
    from portolan_core.run_state import RunState
    from portolan_core.stac_links import catalog_collections, versioned_collections

    if state is None:
        state = RunState.open(catalog_path)
    if collection is None:
        targets = sorted(
            set(catalog_collections(catalog_path)) | set(versioned_collections(catalog_path))
        )
        for target in targets:
            blocked = check_backend_supports_push(catalog_path, target, state=state)
            if blocked is not None:
                return blocked
        if targets:
            return None

    active_backend = _collection_setting("backend", catalog_path, collection, state)
    if active_backend is None or active_backend == "file":
        return None

    backend = get_backend(active_backend, catalog_root=catalog_path)
    if backend.supports_push():
        return None

    remote = _collection_setting("remote", catalog_path, collection, state)
    return PushBlockedResult(backend.push_blocked_message(remote))


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
    *,
    state: RunState | None = None,
) -> BackendPullResult:
    """Attempt a pull via the active non-file backend."""
    from portolan_cli.backends import get_backend
    from portolan_core.run_state import RunState
    from portolan_core.stac_links import catalog_collections

    if state is None:
        state = RunState.open(catalog_path)
    targets = [collection] if collection is not None else catalog_collections(catalog_path)
    if not targets:
        active_backend = _collection_setting("backend", catalog_path, None, state)
        return BackendPullResult(handled=active_backend not in (None, "file"), up_to_date=True)
    names = [
        _resolve_backend_name(None, catalog_path, state, collection=target) for target in targets
    ]
    # A mixed catalog is handled collection by collection by pull_all_collections.
    # Do not pull any collection here before returning an unhandled result.
    if any(name in (None, "file") for name in names):
        return BackendPullResult(handled=False)
    results = []
    for target, name in zip(targets, names, strict=True):
        backend = get_backend(name, catalog_root=catalog_path)
        result = backend.pull(remote_url, catalog_path, target, dry_run=dry_run)
        if result is None:
            return BackendPullResult(handled=False)
        results.append(result)

    return BackendPullResult(
        handled=True,
        success=all(r.success for r in results),
        files_downloaded=sum(r.files_downloaded for r in results),
        files_skipped=sum(r.files_skipped for r in results),
        local_version=results[0].local_version if len(results) == 1 else None,
        remote_version=results[0].remote_version if len(results) == 1 else None,
        up_to_date=all(r.up_to_date for r in results),
    )
