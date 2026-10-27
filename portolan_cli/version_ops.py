"""Version operations middle layer.

Bridges CLI commands and versioning backends. All imports of
portolan_cli.backends and portolan_cli.config are lazy (inside function
bodies) to keep the module lightweight and avoid circular imports.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from portolan_cli.backends.protocol import SchemaFingerprint
    from portolan_cli.run_state import RunState, StagedVersion
    from portolan_cli.versions import Version

logger = logging.getLogger(__name__)


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
    version: str | None = None,
    backend_name: str | None = None,
    catalog_root: Path | None = None,
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
        backend_name: Backend to use. If None, resolved from config.
        catalog_root: Path to catalog root for config lookup.

    Returns:
        The newly created Version object.
    """
    from portolan_cli.backends import get_backend

    name = _resolve_backend_name(backend_name, catalog_root)
    backend = get_backend(name, catalog_root=catalog_root)
    schema = schema or {"columns": [], "types": {}, "hash": "unknown"}
    return backend.publish(
        collection, assets, schema, breaking, message, removed=removed, version=version
    )


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
        )
    return published


def commit_run(state: RunState) -> dict[str, Version]:
    """Write the versions that a run staged, one version per collection.

    The file backend gets one ``add_version`` and one ``write_versions`` for
    each staged collection. The staged assets carry their checksums, so nothing
    is hashed again. A collection whose stage changes nothing gets no write.
    Then one update of the catalog-level versions.json covers every staged
    collection, under the catalog lock. A failure of that index update is
    logged, because the collection records are already on disk.

    A plugin backend gets one ``publish_version`` for each staged collection.

    The staged changes are cleared, so a second call commits nothing.

    Args:
        state: The state of the run.

    Returns:
        Collection id to the version written, for each collection that got one.
    """
    from portolan_cli.catalog import CollectionSummary, update_catalog_index
    from portolan_cli.versions import write_versions

    staged = state.take_staged()
    if not staged:
        return {}
    backend_name = _resolve_backend_name(None, state.catalog_root)
    if backend_name != "file":
        return _commit_to_plugin(state, staged, backend_name)

    published: dict[str, Version] = {}
    summaries: dict[str, CollectionSummary] = {}
    for collection_id, change in sorted(staged.items()):
        collection_dir = state.collection_dir(collection_id)
        disk = state.versions(collection_dir, include_staged=False)
        updated = change.apply(disk)
        if updated is not disk:
            versions_path = collection_dir / "versions.json"
            write_versions(versions_path, updated)
            state.invalidate(versions_path)
            published[collection_id] = updated.versions[-1]
        if updated.versions:
            summaries[collection_id] = CollectionSummary.of(updated.versions[-1])
    try:
        update_catalog_index(state.catalog_root, summaries)
    except Exception:
        logger.warning(
            "Failed to update catalog-level versions.json. "
            "The collection versions were written, but the catalog-level view may be stale.",
            exc_info=True,
        )
    return published


@contextmanager
def owned_run(catalog_root: Path, state: RunState | None = None) -> Iterator[RunState]:
    """Yield the run state of a public entry point, and commit it if it is new.

    A caller that passes its own ``state`` commits it itself, so this yields it
    and commits nothing. With no ``state``, this opens one and commits it when
    the block ends without an exception. An exception commits nothing.

    Args:
        catalog_root: Root directory of the catalog.
        state: The caller's state, or None.

    Yields:
        The state to pass down.
    """
    if state is not None:
        yield state
        return
    from portolan_cli.run_state import RunState

    own = RunState.open(catalog_root)
    yield own
    commit_run(own)


# =============================================================================
# Backend routing helpers (used by CLI for push/pull/version commands)
# =============================================================================


class PushBlockedResult:
    """Result from check_backend_supports_push when push is blocked."""

    def __init__(self, message: str) -> None:
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
    if backend.supports_push():
        return None

    remote = get_setting("remote", catalog_path=catalog_path, collection=collection)
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
) -> BackendPullResult:
    """Attempt a pull via the active non-file backend.

    Returns a BackendPullResult with handled=True if the backend handled the pull,
    or handled=False if the file-based pull should proceed as normal.

    Without ``collection`` the backend pulls each collection of the catalog in
    turn, and the result sums them (issue #977).
    """
    from portolan_cli.backends import get_backend
    from portolan_cli.config import get_setting
    from portolan_cli.stac_links import catalog_collections

    active_backend = get_setting("backend", catalog_path=catalog_path, collection=collection)
    if active_backend is None or active_backend == "file":
        return BackendPullResult(handled=False)

    backend = get_backend(active_backend, catalog_root=catalog_path)
    targets = [collection] if collection is not None else catalog_collections(catalog_path)
    results = []
    for target in targets:
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
