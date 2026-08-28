"""Remove items and files from a Portolan catalog."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

from portolan_cli.collection_id import resolve_collection_id
from portolan_cli.discovery import get_sidecars
from portolan_cli.versions import (
    Version,
    read_versions,
)

logger = logging.getLogger(__name__)


def _gather_removable_files(path: Path) -> list[Path]:
    """Expand a removal target into the concrete files it covers.

    Directories expand to every file beneath them; single files are paired with
    their sidecars (e.g. ``.dbf``/``.shx``/``.prj``).

    Args:
        path: A file or directory passed to ``remove_files``.

    Returns:
        List of candidate file paths to untrack/delete.
    """
    if path.is_dir():
        return [f for f in path.rglob("*") if f.is_file()]

    sidecars = get_sidecars(path) if path.exists() else []
    return [path, *sidecars]


def _resolve_collection_dir(file_path: Path, catalog_root: Path) -> Path | None:
    """Find the collection directory that owns ``file_path``.

    Walks up from the file to the catalog root and returns the nearest ancestor
    carrying a ``collection.json`` or a ``versions.json``. That covers every
    layout in one pass: a collection-level asset (the file's own parent), an
    item or Hive-partition asset (one level further up), and a collection nested
    under a subcatalog, where the id spans several path segments.

    ``resolve_collection_id`` returns only the *first* component, so deriving the
    directory from it pointed ``rm`` at ``{catalog_root}/climate/`` for a file in
    ``climate/hittekaart/``. No versions.json sits there, so the removal silently
    skipped untracking (issue #723).

    Args:
        file_path: File being removed.
        catalog_root: Root directory of the catalog.

    Returns:
        The collection directory, or None if no marker is found below the root.
    """
    root = catalog_root.resolve()
    current = file_path.resolve().parent
    while current != root:
        if (current / "collection.json").exists() or (current / "versions.json").exists():
            return current
        parent = current.parent
        if parent == current:
            # Reached the filesystem root without passing through catalog_root.
            return None
        current = parent
    return None


def _remove_one_file(
    file_path: Path,
    *,
    catalog_root: Path,
    keep: bool,
    dry_run: bool,
) -> bool:
    """Untrack (and optionally delete) a single file.

    A file that is gone from disk is still untracked, so ``rm`` can drop a
    tracked asset that a user already deleted (issue #803). The delete step has
    nothing to delete, so it is skipped. A path that is neither on disk nor
    tracked is a no-op and reports as skipped.

    Args:
        file_path: File to remove.
        catalog_root: Root directory of the catalog.
        keep: If True, preserve the file on disk (only untrack).
        dry_run: If True, do not mutate anything.

    Returns:
        True if the file was removed (or would be, in dry-run), False if skipped.
    """
    file_missing = not file_path.exists()

    # Refuse to delete symlinks - they might point outside the catalog and
    # deleting them could have unintended consequences. Users should resolve
    # symlinks manually or use --keep to just untrack. A missing path is never a
    # symlink, so this guard only affects files present on disk.
    if file_path.is_symlink() and not keep:
        return False

    # Reject files outside the catalog before touching anything.
    try:
        coll_id = resolve_collection_id(file_path, catalog_root)
    except ValueError:
        return False

    # The owning collection is found by walking up from the file, because a
    # collection id can span several path segments and `resolve_collection_id`
    # returns only the first (#723). Fall back to that first component when no
    # marker is found; nothing is tracked there either way.
    collection_dir = _resolve_collection_dir(file_path, catalog_root) or catalog_root / coll_id
    versions_path = collection_dir / "versions.json"

    if dry_run:
        # A file that is gone from disk is only removable when it is still
        # tracked. Otherwise the preview reports a phantom removal (#803).
        if file_missing:
            return _file_is_tracked(file_path, versions_path)
        return True

    # Remove from versions.json.
    untracked = _remove_from_versions(file_path, versions_path)

    # A path that is gone from disk untracks only. There is nothing to delete,
    # so a path that is neither on disk nor tracked is a no-op we report as
    # skipped (#803).
    if file_missing:
        return untracked

    # Remove STAC item and files (unless --keep)
    if not keep:
        # The STAC item lives in the file's PARENT directory (issue #602): a Hive
        # partition dir (`kdtree_cell=…/`) or a nested item dir whose name is the
        # parent, NOT the file stem — mirroring `add`, which sets
        # `item_dir = path.parent` in `_derive_item_id_and_asset_level`.
        # Reconstructing from `file_path.stem` only matched the degenerate case and
        # orphaned partition/item dirs (and their `item.json`) on disk.
        #
        # Guard the collection-level case: a file sitting directly in the
        # collection dir (the dir holding `collection.json`) has no item subdir to
        # remove, so only the file and its sidecars are deleted.
        item_dir = file_path.parent
        if not (item_dir / "collection.json").exists() and item_dir.is_dir():
            # Item-level asset: drop the whole item/partition dir, which owns
            # item.json and every sibling asset.
            shutil.rmtree(item_dir)
        else:
            # Delete file from disk. missing_ok=True handles race conditions where
            # another process deletes the file between exists() and unlink().
            file_path.unlink(missing_ok=True)
            for sidecar in get_sidecars(file_path):
                sidecar.unlink(missing_ok=True)

    return True


def remove_files(
    *,
    paths: list[Path],
    catalog_root: Path,
    keep: bool = False,
    dry_run: bool = False,
) -> tuple[list[Path], list[Path]]:
    """Remove files from Portolan catalog tracking.

    This is the main entry point for the `portolan rm` command.
    By default, deletes the file AND removes from tracking (git-style).
    With keep=True, removes from tracking but preserves the file.

    Args:
        paths: List of paths to remove (files or directories).
        catalog_root: Root directory of the catalog.
        keep: If True, preserve file on disk (only untrack).
        dry_run: If True, preview what would be removed without actually removing.

    Returns:
        Tuple of (removed_paths, skipped_paths).
        removed_paths: Paths that were removed from tracking.
        skipped_paths: Paths that were skipped (not in catalog, errors).
    """
    removed: list[Path] = []
    skipped: list[Path] = []

    for path in paths:
        for file_path in _gather_removable_files(path):
            was_removed = _remove_one_file(
                file_path, catalog_root=catalog_root, keep=keep, dry_run=dry_run
            )
            (removed if was_removed else skipped).append(file_path)

    return removed, skipped


def _versions_catalog_root(versions_path: Path) -> Path:
    """Resolve the catalog root that hrefs in ``versions_path`` are relative to.

    Falls back to the collection's grandparent when no ``.portolan`` sentinel is
    found (e.g. a bare test catalog without ``catalog.json``).
    """
    from portolan_cli.catalog import find_catalog_root

    catalog_root = find_catalog_root(versions_path.parent)
    if catalog_root is None:
        catalog_root = versions_path.parent.parent
    return catalog_root


def _matching_asset_keys(file_path: Path, version: Version, catalog_root: Path) -> set[str]:
    """Return the asset keys in ``version`` that track ``file_path``.

    Match on the tracked asset's href, not a key reconstructed from the file
    stem. Hrefs are catalog-root-relative and POSIX and already encode the
    true item directory that add._batch_update_versions used as the key prefix
    ("{collection_id}/{item_id}/{filename}", or "{collection_id}/{filename}"
    for collection-level assets). Reconstructing "{stem}/{filename}" is wrong
    for every layout where the item dir name != the file stem — every real
    Hive partition ("kdtree_cell=.../data.parquet") and nested item dir
    (issue #589). Href matching is also unique per file, so it can't
    over-match a sibling item that happens to share a filename.
    """
    try:
        target_href = file_path.resolve().relative_to(catalog_root.resolve()).as_posix()
    except ValueError:
        # File lives outside the catalog; nothing tracked here can match it.
        return set()

    keys = {name for name, asset in version.assets.items() if asset.href == target_href}

    # Convert-on-add: a non-cloud-native source (e.g. roads.shp) is tracked as
    # its converted roads.parquet. Removing the source must still untrack the
    # parquet, or it becomes a phantom entry.
    if not keys and file_path.suffix and file_path.suffix != ".parquet":
        parquet_href = (
            file_path.resolve()
            .relative_to(catalog_root.resolve())
            .with_suffix(".parquet")
            .as_posix()
        )
        keys = {name for name, asset in version.assets.items() if asset.href == parquet_href}

    return keys


def _file_is_tracked(file_path: Path, versions_path: Path) -> bool:
    """Report whether ``file_path`` matches a tracked asset in ``versions_path``."""
    if not versions_path.exists():
        return False

    versions_file = read_versions(versions_path)
    if not versions_file.versions:
        return False

    catalog_root = _versions_catalog_root(versions_path)
    return bool(_matching_asset_keys(file_path, versions_file.versions[-1], catalog_root))


def _remove_from_versions(file_path: Path, versions_path: Path) -> bool:
    """Remove a file from version tracking via the active backend.

    This creates a new version entry without the specified file.

    Args:
        file_path: Path to the file to untrack.
        versions_path: Path to the versions.json file.

    Returns:
        True if the file was tracked and a removal version was published.
        False if nothing matched (no new version).
    """
    if not versions_path.exists():
        return False

    versions_file = read_versions(versions_path)
    if not versions_file.versions:
        return False

    catalog_root = _versions_catalog_root(versions_path)
    removed_keys = _matching_asset_keys(file_path, versions_file.versions[-1], catalog_root)
    if not removed_keys:
        # File wasn't tracked, nothing to do
        return False

    from portolan_cli.version_ops import publish_version

    collection_id = versions_path.parent.relative_to(catalog_root).as_posix()

    publish_version(
        collection_id,
        assets={},
        removed=removed_keys,
        message=f"Removed {file_path.name}",
        catalog_root=catalog_root,
    )
    return True
