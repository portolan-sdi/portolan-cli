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
        if path.is_dir():
            # Remove all files in directory
            files = list(path.rglob("*")) if path.exists() else []
            files = [f for f in files if f.is_file()]
        else:
            # Include sidecars for single file removal
            sidecars = get_sidecars(path) if path.exists() else []
            files = [path] + sidecars

        for file_path in files:
            if not file_path.exists() and not keep:
                skipped.append(file_path)
                continue

            # Refuse to delete symlinks - they might point outside the catalog
            # and deleting them could have unintended consequences. Users should
            # resolve symlinks manually or use --keep to just untrack.
            if file_path.is_symlink() and not keep:
                skipped.append(file_path)
                continue

            # Determine collection ID
            try:
                coll_id = resolve_collection_id(file_path, catalog_root)
            except ValueError:
                # File is outside catalog - skip with warning
                skipped.append(file_path)
                continue

            # In dry-run mode, just record what would be removed
            if dry_run:
                removed.append(file_path)
                continue

            # Remove from versions.json
            versions_path = catalog_root / coll_id / "versions.json"
            if versions_path.exists():
                _remove_from_versions(file_path, versions_path)

            # Remove STAC item and files (unless --keep)
            if not keep:
                item_id = file_path.stem
                item_dir = catalog_root / coll_id / item_id
                if item_dir.exists() and item_dir.is_dir():
                    shutil.rmtree(item_dir)

                # Delete file from disk (missing_ok handles race conditions)
                file_path.unlink(missing_ok=True)

                # Also delete sidecars if this is the primary file
                # Use missing_ok=True to handle race conditions where another
                # process might delete the file between exists() and unlink()
                for sidecar in get_sidecars(file_path):
                    sidecar.unlink(missing_ok=True)

            removed.append(file_path)

    return removed, skipped


def _increment_version(version: str) -> str:
    """Safely increment a semantic version string.

    Handles standard semver (1.2.3) and pre-release versions (1.0.0-beta.1).

    Args:
        version: Current version string.

    Returns:
        Incremented version string.
    """
    if not version:
        return "0.0.1"

    # Handle pre-release versions (e.g., 1.0.0-beta.1)
    if "-" in version:
        base, prerelease = version.split("-", 1)
        # Try to increment the prerelease number
        prerelease_parts = prerelease.rsplit(".", 1)
        if len(prerelease_parts) == 2 and prerelease_parts[1].isdigit():
            prerelease_parts[1] = str(int(prerelease_parts[1]) + 1)
            return f"{base}-{'.'.join(prerelease_parts)}"
        else:
            # No numeric suffix: 1.0.0-beta → 1.0.0-beta.1
            # Preserve the prerelease tag by appending .1
            return f"{base}-{prerelease}.1"

    # Standard semver: increment patch
    parts = version.split(".")
    if len(parts) >= 3 and parts[-1].isdigit():
        parts[-1] = str(int(parts[-1]) + 1)
    elif len(parts) < 3:
        # Pad to 3 parts if needed
        while len(parts) < 3:
            parts.append("0")
        parts[-1] = "1"
    return ".".join(parts)


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
