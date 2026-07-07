"""Remove items and files from a Portolan catalog."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

from portolan_cli.collection_id import resolve_collection_id
from portolan_cli.discovery import get_sidecars
from portolan_cli.versions import (
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
        if not path.exists():
            return []
        return [f for f in path.rglob("*") if f.is_file()]

    sidecars = get_sidecars(path) if path.exists() else []
    return [path, *sidecars]


def _remove_one_file(
    file_path: Path,
    *,
    catalog_root: Path,
    keep: bool,
    dry_run: bool,
) -> bool:
    """Untrack (and optionally delete) a single file.

    Args:
        file_path: File to remove.
        catalog_root: Root directory of the catalog.
        keep: If True, preserve the file on disk (only untrack).
        dry_run: If True, do not mutate anything.

    Returns:
        True if the file was removed (or would be, in dry-run), False if skipped.
    """
    if not file_path.exists() and not keep:
        return False

    # Refuse to delete symlinks - they might point outside the catalog and
    # deleting them could have unintended consequences. Users should resolve
    # symlinks manually or use --keep to just untrack.
    if file_path.is_symlink() and not keep:
        return False

    # Determine collection ID (raises if the file is outside the catalog).
    try:
        coll_id = resolve_collection_id(file_path, catalog_root)
    except ValueError:
        return False

    if dry_run:
        return True

    # Remove from versions.json
    versions_path = catalog_root / coll_id / "versions.json"
    if versions_path.exists():
        _remove_from_versions(file_path, versions_path)

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
            # item.json and every sibling asset (ADR-0028).
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


def _remove_from_versions(file_path: Path, versions_path: Path) -> None:
    """Remove a file from version tracking via the active backend.

    This creates a new version entry without the specified file.

    Args:
        file_path: Path to the file to untrack.
        versions_path: Path to the versions.json file.
    """
    if not versions_path.exists():
        return

    versions_file = read_versions(versions_path)
    if not versions_file.versions:
        return

    from portolan_cli.catalog import find_catalog_root

    catalog_root = find_catalog_root(versions_path.parent)
    if catalog_root is None:
        catalog_root = versions_path.parent.parent

    # Match on the tracked asset's href, not a key reconstructed from the file
    # stem. Hrefs are catalog-root-relative and POSIX and already encode the
    # true item directory that add._batch_update_versions used as the key prefix
    # ("{collection_id}/{item_id}/{filename}", or "{collection_id}/{filename}"
    # for collection-level assets). Reconstructing "{stem}/{filename}" is wrong
    # for every layout where the item dir name != the file stem — every real
    # Hive partition ("kdtree_cell=.../data.parquet") and nested item dir
    # (issue #589). Href matching is also unique per file, so it can't
    # over-match a sibling item that happens to share a filename.
    try:
        target_href = file_path.resolve().relative_to(catalog_root.resolve()).as_posix()
    except ValueError:
        # File lives outside the catalog; nothing tracked here can match it.
        return

    current = versions_file.versions[-1]
    removed_keys = {name for name, asset in current.assets.items() if asset.href == target_href}

    # Convert-on-add: a non-cloud-native source (e.g. roads.shp) is tracked as
    # its converted roads.parquet. Removing the source must still untrack the
    # parquet, or it becomes a phantom entry.
    if not removed_keys and file_path.suffix and file_path.suffix != ".parquet":
        parquet_href = (
            file_path.resolve()
            .relative_to(catalog_root.resolve())
            .with_suffix(".parquet")
            .as_posix()
        )
        removed_keys = {
            name for name, asset in current.assets.items() if asset.href == parquet_href
        }

    if not removed_keys:
        # File wasn't tracked, nothing to do
        return

    from portolan_cli.version_ops import publish_version

    collection_id = versions_path.parent.relative_to(catalog_root).as_posix()

    publish_version(
        collection_id,
        assets={},
        removed=removed_keys,
        message=f"Removed {file_path.name}",
        catalog_root=catalog_root,
    )
