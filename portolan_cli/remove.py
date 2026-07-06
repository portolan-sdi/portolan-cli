"""Remove items and files from a Portolan catalog."""

from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path

from portolan_cli.collection_id import resolve_collection_id
from portolan_cli.discovery import get_sidecars
from portolan_cli.versions import (
    read_versions,
)

logger = logging.getLogger(__name__)


def remove_item(
    catalog_root: Path,
    stac_id: str,
    *,
    remove_collection: bool = False,
) -> None:
    """Remove an item from a Portolan catalog.

    Args:
        catalog_root: Root directory of the catalog.
        stac_id: STAC identifier in format "collection/item" or just "collection".
        remove_collection: If True, remove entire collection.

    Raises:
        KeyError: If the item doesn't exist.
    """
    # STAC at root level (per ADR-0023)
    if remove_collection or "/" not in stac_id:
        # Remove entire collection
        collection_id = stac_id.split("/")[0]
        collection_dir = catalog_root / collection_id

        if not collection_dir.exists():
            raise KeyError(f"Item not found: {stac_id}")

        # Remove collection directory
        shutil.rmtree(collection_dir)

        # Update catalog links
        catalog_path = catalog_root / "catalog.json"
        if catalog_path.exists():
            catalog_data = json.loads(catalog_path.read_text(encoding="utf-8"))
            catalog_data["links"] = [
                link
                for link in catalog_data.get("links", [])
                if not link.get("href", "").endswith(f"/{collection_id}/collection.json")
            ]
            catalog_path.write_text(json.dumps(catalog_data, indent=2), encoding="utf-8")
    else:
        # Remove single item
        collection_id, item_id = stac_id.split("/", 1)
        item_dir = catalog_root / collection_id / item_id

        if not item_dir.exists():
            raise KeyError(f"Item not found: {stac_id}")

        # Remove item directory
        shutil.rmtree(item_dir)

        # Update collection links
        collection_path = catalog_root / collection_id / "collection.json"
        if collection_path.exists():
            collection_data = json.loads(collection_path.read_text(encoding="utf-8"))
            collection_data["links"] = [
                link
                for link in collection_data.get("links", [])
                if not link.get("href", "").startswith(f"./{item_id}/")
            ]
            collection_path.write_text(json.dumps(collection_data, indent=2), encoding="utf-8")


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
        item_dir = catalog_root / coll_id / file_path.stem
        if item_dir.exists() and item_dir.is_dir():
            shutil.rmtree(item_dir)

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

    # Check if the file is tracked under any key
    current = versions_file.versions[-1]
    filename = file_path.name
    parquet_name = f"{file_path.stem}.parquet"

    removed_keys = {name for name in current.assets if name == filename or name == parquet_name}

    if not removed_keys:
        # File wasn't tracked, nothing to do
        return

    from portolan_cli.catalog import find_catalog_root
    from portolan_cli.version_ops import publish_version

    catalog_root = find_catalog_root(versions_path.parent)
    if catalog_root is None:
        catalog_root = versions_path.parent.parent
    collection_id = versions_path.parent.relative_to(catalog_root).as_posix()

    publish_version(
        collection_id,
        assets={},
        removed=removed_keys,
        message=f"Removed {filename}",
        catalog_root=catalog_root,
    )
