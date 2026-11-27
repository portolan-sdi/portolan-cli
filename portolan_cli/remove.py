"""Remove items and files from a Portolan catalog."""

from __future__ import annotations

import json
import logging
import shutil
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from portolan_cli.collection_id import resolve_collection_id
from portolan_cli.discovery import get_sidecars
from portolan_core.json_io import write_json_atomic
from portolan_core.versions import (
    Version,
    read_versions,
)

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

logger = logging.getLogger(__name__)


class SafetyError(Exception):
    """A removal that deletes files and has no ``force``."""


def require_force(*, keep: bool, dry_run: bool, force: bool) -> None:
    """Refuse a removal that deletes files when ``force`` is not set."""
    if not (keep or dry_run or force):
        raise SafetyError(
            "Destructive rm requires --force flag. "
            "Use --keep to preserve files, or --dry-run to preview."
        )


def _gather_removable_files(path: Path) -> list[Path]:
    """Expand a removal target into the concrete files it covers."""
    if path.is_dir():
        return [f for f in path.rglob("*") if f.is_file()]

    sidecars = get_sidecars(path) if path.exists() else []
    return [path, *sidecars]


def _resolve_collection_dir(file_path: Path, catalog_root: Path) -> Path | None:
    """Find the collection directory that owns ``file_path``."""
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
    """Untrack (and optionally delete) a single file."""
    file_missing = not file_path.exists()

    # Refuse to delete a symlink: it may point outside the catalog.
    if file_path.is_symlink() and not keep:
        return False

    # Reject files outside the catalog before touching anything.
    try:
        coll_id = resolve_collection_id(file_path, catalog_root)
    except ValueError:
        return False

    collection_dir = _resolve_collection_dir(file_path, catalog_root) or catalog_root / coll_id
    versions_path = collection_dir / "versions.json"

    if dry_run:
        if file_missing:
            return _file_is_tracked(file_path, versions_path)
        return True

    item_dir = file_path.parent
    drops_item_dir = (
        not keep
        and not file_missing
        and item_dir.is_dir()
        and not (item_dir / "collection.json").exists()
    )
    # Deleting the item dir deletes every file in it, so the version that
    # untracks the file untracks them too.
    siblings = [f for f in item_dir.rglob("*") if f.is_file()] if drops_item_dir else []
    untracked = _remove_from_versions(file_path, versions_path, siblings=siblings)

    # A file gone from disk leaves the collection assets with the files.
    if (file_missing or not keep) and not drops_item_dir:
        _drop_collection_assets(collection_dir, [file_path, *get_sidecars(file_path)])

    if file_missing:
        return untracked

    # Remove STAC item and files (unless --keep)
    if not keep:
        if drops_item_dir:
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


def _drop_collection_assets(collection_dir: Path, files: list[Path]) -> None:
    """Drop each collection-level asset whose local href names one of ``files``."""
    path = collection_dir / "collection.json"
    if not path.is_file():
        return
    data = json.loads(path.read_text(encoding="utf-8"))
    assets = data.get("assets")
    if not isinstance(assets, dict):
        return
    gone = {file.resolve() for file in files}
    kept = {
        key: asset
        for key, asset in assets.items()
        if not (
            isinstance(asset, dict)
            and isinstance(asset.get("href"), str)
            and "://" not in asset["href"]
            and (collection_dir / asset["href"]).resolve() in gone
        )
    }
    if kept != assets:
        data["assets"] = kept
        write_json_atomic(path, data)


def remove_files(
    *,
    paths: list[Path],
    catalog_root: Path,
    keep: bool = False,
    dry_run: bool = False,
) -> tuple[list[Path], list[Path]]:
    """Remove files from Portolan catalog tracking."""
    removed: list[Path] = []
    skipped: list[Path] = []

    for path in paths:
        for file_path in _gather_removable_files(path):
            was_removed = _remove_one_file(
                file_path, catalog_root=catalog_root, keep=keep, dry_run=dry_run
            )
            (removed if was_removed else skipped).append(file_path)

    if removed and not dry_run:
        from portolan_cli.validation.fixers import repair_written_items

        for result in repair_written_items(catalog_root, removed, dry_run=False):
            if not result.success:
                logger.warning("%s: %s", result.file_path, result.message)

    return removed, skipped


@dataclass(frozen=True)
class RemoveResult:
    """The outcome of ``portolan rm``."""

    removed: list[Path]
    skipped: list[Path]
    kept_on_disk: bool
    dry_run: bool

    def to_dict(self) -> dict[str, Any]:
        """Serialize the result as the ``rm`` envelope data."""
        return {
            "removed": [str(p) for p in self.removed],
            "skipped": [str(p) for p in self.skipped],
            "kept_on_disk": self.kept_on_disk,
            "dry_run": self.dry_run,
        }


def remove_path(
    path: Path,
    *,
    catalog_root: Path,
    keep: bool = False,
    dry_run: bool = False,
    force: bool = False,
) -> RemoveResult:
    """Remove one file or directory from the catalog."""
    require_force(keep=keep, dry_run=dry_run, force=force)
    removed, skipped = remove_files(
        paths=[path.resolve()], catalog_root=catalog_root, keep=keep, dry_run=dry_run
    )
    return RemoveResult(removed, skipped, kept_on_disk=keep, dry_run=dry_run)


def _versions_catalog_root(versions_path: Path) -> Path:
    """Resolve the catalog root that hrefs in ``versions_path`` are relative to."""
    from portolan_cli.catalog import find_catalog_root

    catalog_root = find_catalog_root(versions_path.parent)
    if catalog_root is None:
        catalog_root = versions_path.parent.parent
    return catalog_root


def _matching_asset_keys(file_path: Path, version: Version, catalog_root: Path) -> set[str]:
    """Return the asset keys in ``version`` that track ``file_path``."""
    try:
        target_href = file_path.resolve().relative_to(catalog_root.resolve()).as_posix()
    except ValueError:
        # File lives outside the catalog; nothing tracked here can match it.
        return set()

    keys = {name for name, asset in version.assets.items() if asset.href == target_href}

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


def _remove_from_versions(
    file_path: Path, versions_path: Path, *, siblings: Iterable[Path] = ()
) -> bool:
    """Remove a file from version tracking via the active backend."""
    if not versions_path.exists():
        return False

    versions_file = read_versions(versions_path)
    if not versions_file.versions:
        return False

    catalog_root = _versions_catalog_root(versions_path)
    latest = versions_file.versions[-1]
    removed_keys = set().union(
        *(_matching_asset_keys(path, latest, catalog_root) for path in (file_path, *siblings))
    )
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
