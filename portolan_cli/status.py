"""Status module for tracking state detection.

This module provides functions to detect tracking states (untracked, tracked,
modified, deleted) by comparing filesystem contents against versions.json.

Per ADR-0007, this is the library layer - CLI wraps these functions.
Per ADR-0022, status shows: untracked, modified, deleted files.
Per ADR-0023, item files live in collection/{item_id}/ subdirectories and
    versions.json is at collection/versions.json (not .portolan/versions.json).
Per issue #133, ALL files in item directories are tracked (not just geo files).
Per issue #137, uninitialized directories (no collection.json) that contain
    geo-assets are treated as potential collections and their files reported
    as untracked. This fixes the chicken-and-egg problem where files were
    never shown as untracked before `portolan add` was run.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

from portolan_cli.constants import GEOSPATIAL_EXTENSIONS
from portolan_cli.dataset import is_current
from portolan_cli.versions import read_versions

logger = logging.getLogger(__name__)

# Files that are always excluded from tracking regardless of location.
# These are OS/tool artifacts that provide no value as catalog assets.
IGNORED_FILES: frozenset[str] = frozenset(
    {
        ".DS_Store",
        "Thumbs.db",
        ".gitkeep",
    }
)

# STAC metadata filenames that live inside item directories but are not
# user data assets and should not appear in status output.
_STAC_METADATA_FILES: frozenset[str] = frozenset({"item.json"})

# Maximum depth to scan for geo-assets in uninitialized directories.
# Supports hive-partitioned datasets (e.g., year=2024/month=01/data.parquet).
_MAX_GEO_SCAN_DEPTH: int = 5


def _has_geo_assets(directory: Path, *, max_depth: int = _MAX_GEO_SCAN_DEPTH) -> bool:
    """Check whether a directory subtree contains at least one geospatial file.

    Used to determine if an uninitialized directory (one without collection.json)
    should be treated as a potential collection for status reporting purposes.

    Recursively scans the directory tree up to max_depth levels to find any file
    with a known geospatial extension. This supports hive-partitioned datasets
    where geo files may be nested deeply (e.g., collection/item/year=2024/data.parquet).

    Hidden directories (names starting with '.') are skipped. Symlinks are followed
    but broken symlinks are logged and skipped. FileGDB directories (.gdb) are
    detected as geo-assets.

    Args:
        directory: Path to the candidate collection directory.
        max_depth: Maximum recursion depth (default: 5). Prevents runaway recursion.

    Returns:
        True if any geospatial file or .gdb directory is found within the subtree.
    """
    if max_depth <= 0:
        return False

    try:
        for entry in directory.iterdir():
            name = entry.name

            # Skip hidden entries
            if name.startswith("."):
                continue

            # Skip ignored files
            if name in IGNORED_FILES:
                continue

            # Check for .gdb directories (FileGDB is a directory, not a file)
            if entry.suffix.lower() == ".gdb":
                try:
                    if entry.is_dir():
                        return True
                except OSError as e:
                    logger.debug("Cannot access %s: %s", entry, e)
                    continue

            # Check if it's a file with a geo extension
            try:
                if entry.is_file():
                    if entry.suffix.lower() in GEOSPATIAL_EXTENSIONS:
                        return True
                elif entry.is_dir():
                    # Recurse into subdirectories
                    if _has_geo_assets(entry, max_depth=max_depth - 1):
                        return True
            except OSError as e:
                # Handle broken symlinks, permission errors, etc.
                logger.debug("Cannot access %s: %s", entry, e)
                continue

    except PermissionError as e:
        logger.warning("Permission denied scanning %s: %s", directory, e)
    except OSError as e:
        logger.warning("Error scanning %s: %s", directory, e)

    return False


def _is_stac_item_metadata(filename: str, item_id: str) -> bool:
    """Check if a file is a STAC item metadata file.

    STAC item metadata can be named either "item.json" or "{item_id}.json".

    Args:
        filename: The filename to check.
        item_id: The item directory name.

    Returns:
        True if the file is STAC item metadata.
    """
    if filename in _STAC_METADATA_FILES:
        return True
    # Also match {item_id}.json pattern (e.g., census.json in census/ directory)
    if filename == f"{item_id}.json":
        return True
    return False


@dataclass(frozen=True)
class FileStatus:
    """Represents a file's tracking status.

    Attributes:
        collection_id: The collection containing this file.
        item_id: The item subdirectory (per ADR-0023 hierarchy).
        filename: The filename within the item directory.
    """

    collection_id: str
    item_id: str
    filename: str

    @property
    def path(self) -> str:
        """Return the full relative path (collection_id/item_id/filename)."""
        return f"{self.collection_id}/{self.item_id}/{self.filename}"


@dataclass
class StatusResult:
    """Result of a catalog status check.

    Attributes:
        untracked: Files in item dirs but not in versions.json.
        modified: Files that exist but have changed since last tracked.
        deleted: Files in versions.json but missing from disk.
    """

    untracked: list[FileStatus]
    modified: list[FileStatus]
    deleted: list[FileStatus]

    def is_clean(self) -> bool:
        """Return True if there are no changes to report."""
        return not self.untracked and not self.modified and not self.deleted


def _get_tracked_assets(versions_path: Path) -> set[str]:
    """Read tracked asset keys from versions.json.

    Args:
        versions_path: Path to versions.json file.

    Returns:
        Set of asset keys from the current version, or empty set if
        versions.json doesn't exist or is corrupt.
    """
    if not versions_path.exists():
        return set()
    try:
        versions_file = read_versions(versions_path)
        if versions_file.versions:
            current_version = versions_file.versions[-1]
            return set(current_version.assets.keys())
    except (ValueError, json.JSONDecodeError):
        pass
    return set()


def _scan_item_dir_recursive(
    base_dir: Path,
    item_id: str,
    current_dir: Path,
    collection_id: str,
    versions_path: Path,
    tracked_assets: set[str],
    untracked: list[FileStatus],
    modified: list[FileStatus],
    seen_keys: set[str],
    *,
    max_depth: int = _MAX_GEO_SCAN_DEPTH,
) -> None:
    """Recursively scan an item directory for status changes.

    Supports hive-partitioned datasets where geo files are nested
    in subdirectories (e.g., year=2024/month=01/data.parquet).

    Args:
        base_dir: Base item directory (used for relative path calculation).
        item_id: ID of the item (base directory name).
        current_dir: Current directory being scanned.
        collection_id: ID of the containing collection.
        versions_path: Path to versions.json.
        tracked_assets: Set of tracked asset keys.
        untracked: List to append untracked files to.
        modified: List to append modified files to.
        seen_keys: Set to add seen asset keys to.
        max_depth: Maximum recursion depth to prevent runaway recursion.
    """
    if max_depth <= 0:
        return

    try:
        entries = sorted(current_dir.iterdir())
    except OSError as e:
        logger.debug("Cannot scan %s: %s", current_dir, e)
        return

    for entry in entries:
        name = entry.name

        # Skip hidden entries
        if name.startswith("."):
            continue

        # Skip ignored files
        if name in IGNORED_FILES:
            continue

        try:
            if entry.is_file():
                # Skip STAC metadata files
                if _is_stac_item_metadata(name, item_id):
                    continue

                # Calculate relative path from base item directory
                relative_path = entry.relative_to(base_dir)
                relative_key = f"{item_id}/{relative_path}"
                seen_keys.add(relative_key)

                # For deeply nested files, we report with the item_id as the container
                # and the full relative path as the filename (for display purposes)
                if relative_key not in tracked_assets:
                    untracked.append(FileStatus(collection_id, item_id, str(relative_path)))
                elif not is_current(entry, versions_path, asset_key=relative_key):
                    modified.append(FileStatus(collection_id, item_id, str(relative_path)))

            elif entry.is_dir():
                # Recurse into subdirectories (for hive partitions)
                _scan_item_dir_recursive(
                    base_dir=base_dir,
                    item_id=item_id,
                    current_dir=entry,
                    collection_id=collection_id,
                    versions_path=versions_path,
                    tracked_assets=tracked_assets,
                    untracked=untracked,
                    modified=modified,
                    seen_keys=seen_keys,
                    max_depth=max_depth - 1,
                )
        except OSError as e:
            logger.debug("Cannot access %s: %s", entry, e)
            continue


def _scan_item_dir_status(
    item_dir: Path,
    collection_id: str,
    versions_path: Path,
    tracked_assets: set[str],
) -> tuple[list[FileStatus], list[FileStatus], set[str]]:
    """Scan a single item directory for status changes.

    Recursively scans the item directory to support hive-partitioned
    datasets where geo files are nested in subdirectories.

    Args:
        item_dir: Path to the item directory.
        collection_id: ID of the containing collection.
        versions_path: Path to versions.json.
        tracked_assets: Set of tracked asset keys.

    Returns:
        Tuple of (untracked, modified, seen_keys).
    """
    untracked: list[FileStatus] = []
    modified: list[FileStatus] = []
    seen_keys: set[str] = set()
    item_id = item_dir.name

    # Use recursive scanning to support hive partitions
    _scan_item_dir_recursive(
        base_dir=item_dir,
        item_id=item_id,
        current_dir=item_dir,
        collection_id=collection_id,
        versions_path=versions_path,
        tracked_assets=tracked_assets,
        untracked=untracked,
        modified=modified,
        seen_keys=seen_keys,
    )

    return untracked, modified, seen_keys


def get_catalog_status(catalog_root: Path) -> StatusResult:
    """Get the tracking status of all files in a catalog.

    Compares filesystem contents against versions.json for each collection
    to determine which files are untracked, modified, or deleted.

    Scans item subdirectories (collection/{item_id}/) for ALL files, not
    just geospatial ones.  STAC metadata files (item.json) and files in
    IGNORED_FILES are excluded from reporting.

    Per ADR-0023, versions.json lives at the collection root
    (collection/versions.json), NOT inside .portolan/.

    Args:
        catalog_root: Root directory of the catalog (contains catalog.json).

    Returns:
        StatusResult with lists of untracked, modified, and deleted files.

    Raises:
        FileNotFoundError: If catalog.json doesn't exist.
    """
    catalog_path = catalog_root / "catalog.json"
    if not catalog_path.exists():
        raise FileNotFoundError(f"catalog.json not found: {catalog_path}")

    untracked: list[FileStatus] = []
    modified: list[FileStatus] = []
    deleted: list[FileStatus] = []

    for col_dir in sorted(catalog_root.iterdir()):
        if not col_dir.is_dir() or col_dir.name.startswith("."):
            continue

        # Include directories that have collection.json (initialized collections)
        # OR directories that contain geo-assets (uninitialized potential collections).
        # This fixes issue #137: without this, files in un-added directories never
        # showed up as untracked, creating a chicken-and-egg problem.
        is_initialized = (col_dir / "collection.json").exists()
        if not is_initialized and not _has_geo_assets(col_dir):
            continue

        collection_id = col_dir.name
        versions_path = col_dir / "versions.json"
        tracked_assets = _get_tracked_assets(versions_path)
        seen_relative_paths: set[str] = set()

        # Scan item subdirectories
        for item_dir in sorted(col_dir.iterdir()):
            if not item_dir.is_dir() or item_dir.name.startswith("."):
                continue

            item_untracked, item_modified, item_seen = _scan_item_dir_status(
                item_dir, collection_id, versions_path, tracked_assets
            )
            untracked.extend(item_untracked)
            modified.extend(item_modified)
            seen_relative_paths.update(item_seen)

        # Check for deleted files
        for tracked_key in tracked_assets:
            if tracked_key in seen_relative_paths:
                continue
            if not (col_dir / tracked_key).exists():
                parts = tracked_key.split("/", 1)
                if len(parts) == 2:
                    deleted.append(FileStatus(collection_id, parts[0], parts[1]))
                else:
                    deleted.append(FileStatus(collection_id, "", tracked_key))

    # Sort for consistent output
    untracked.sort(key=lambda f: f.path)
    modified.sort(key=lambda f: f.path)
    deleted.sort(key=lambda f: f.path)

    return StatusResult(untracked=untracked, modified=modified, deleted=deleted)
