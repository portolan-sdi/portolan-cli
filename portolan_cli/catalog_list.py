"""Catalog listing with status indicators.

This module provides functions to list all files in a catalog with their
tracking status (tracked, untracked, modified, deleted).

Per issue #210: The list command shows ALL files with status indicators.
Everything in a catalog is tracked unless excluded by ignored_files config.
"""

from __future__ import annotations

import fnmatch
import json
import logging
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING

from portolan_cli.config import get_ignored_files
from portolan_cli.formats import FORMAT_DISPLAY_NAMES
from portolan_cli.versions import read_versions

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class AssetStatus(Enum):
    """Tracking status for an asset."""

    TRACKED = "tracked"  # In versions.json, unchanged
    UNTRACKED = "untracked"  # On disk, not in versions.json
    MODIFIED = "modified"  # In versions.json, but changed
    DELETED = "deleted"  # In versions.json, but missing from disk


@dataclass
class AssetInfo:
    """Information about a single asset with its status."""

    path: str  # Relative path within item (e.g., "data.parquet")
    status: AssetStatus
    size_bytes: int | None = None
    format_name: str | None = None  # Human-readable format (e.g., "GeoParquet")


@dataclass
class ItemInfo:
    """Information about an item with all its assets."""

    item_id: str
    collection_id: str
    assets: list[AssetInfo] = field(default_factory=list)

    @property
    def tracked_count(self) -> int:
        """Count of tracked assets."""
        return sum(1 for a in self.assets if a.status == AssetStatus.TRACKED)

    @property
    def untracked_count(self) -> int:
        """Count of untracked assets."""
        return sum(1 for a in self.assets if a.status == AssetStatus.UNTRACKED)

    @property
    def modified_count(self) -> int:
        """Count of modified assets."""
        return sum(1 for a in self.assets if a.status == AssetStatus.MODIFIED)

    @property
    def deleted_count(self) -> int:
        """Count of deleted assets."""
        return sum(1 for a in self.assets if a.status == AssetStatus.DELETED)


@dataclass
class CollectionInfo:
    """Information about a collection with all its items."""

    collection_id: str
    is_initialized: bool  # Has collection.json
    items: list[ItemInfo] = field(default_factory=list)


@dataclass
class CatalogListResult:
    """Result of listing catalog contents with status."""

    collections: list[CollectionInfo] = field(default_factory=list)

    @property
    def total_tracked(self) -> int:
        """Total tracked assets across all collections."""
        return sum(item.tracked_count for col in self.collections for item in col.items)

    @property
    def total_untracked(self) -> int:
        """Total untracked assets across all collections."""
        return sum(item.untracked_count for col in self.collections for item in col.items)

    @property
    def total_modified(self) -> int:
        """Total modified assets across all collections."""
        return sum(item.modified_count for col in self.collections for item in col.items)

    @property
    def total_deleted(self) -> int:
        """Total deleted assets across all collections."""
        return sum(item.deleted_count for col in self.collections for item in col.items)

    def is_empty(self) -> bool:
        """Return True if there are no collections or items."""
        return not self.collections or all(not col.items for col in self.collections)


# Files that are always excluded regardless of config
_ALWAYS_IGNORED: frozenset[str] = frozenset(
    {
        ".DS_Store",
        "Thumbs.db",
        ".gitkeep",
    }
)

# STAC metadata files to exclude
_STAC_METADATA_FILES: frozenset[str] = frozenset(
    {
        "item.json",
        "collection.json",
        "catalog.json",
    }
)


def _is_ignored(filename: str, item_id: str, ignored_patterns: list[str]) -> bool:
    """Check if a file should be ignored.

    Args:
        filename: The filename to check.
        item_id: The item directory name (for STAC metadata matching).
        ignored_patterns: List of glob patterns to ignore.

    Returns:
        True if the file should be ignored.
    """
    # Always ignore certain files
    if filename in _ALWAYS_IGNORED:
        return True

    # Ignore hidden files
    if filename.startswith("."):
        return True

    # Ignore STAC metadata files
    if filename in _STAC_METADATA_FILES:
        return True
    if filename == f"{item_id}.json":
        return True

    # Check against ignored patterns
    for pattern in ignored_patterns:
        if fnmatch.fnmatch(filename, pattern):
            return True

    return False


def _get_format_display_name(filename: str) -> str:
    """Get human-readable format name for a file based on extension.

    Args:
        filename: The filename to check.

    Returns:
        Human-readable format name (e.g., "GeoParquet", "PNG").
    """
    ext = Path(filename).suffix.lower()
    if ext in FORMAT_DISPLAY_NAMES:
        return FORMAT_DISPLAY_NAMES[ext]
    if ext:
        return ext.upper().lstrip(".")
    return "Unknown"


def _get_tracked_assets(versions_path: Path) -> set[str]:
    """Read tracked asset keys from versions.json.

    Args:
        versions_path: Path to versions.json file.

    Returns:
        Set of asset keys that are tracked, or empty set if
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


def _scan_item_directory(
    item_dir: Path,
    collection_id: str,
    versions_path: Path,
    tracked_assets: set[str],
    ignored_patterns: list[str],
) -> ItemInfo:
    """Scan an item directory and return all files with status.

    Args:
        item_dir: Path to the item directory.
        collection_id: ID of the parent collection.
        versions_path: Path to versions.json.
        tracked_assets: Dict of tracked asset keys to metadata.
        ignored_patterns: List of glob patterns to ignore.

    Returns:
        ItemInfo with all assets and their status.
    """
    item_id = item_dir.name
    assets: list[AssetInfo] = []
    seen_keys: set[str] = set()

    # Scan files on disk
    try:
        for entry in sorted(item_dir.iterdir()):
            if entry.is_dir():
                # Skip directories (could extend to support nested later)
                continue

            filename = entry.name

            # Check if ignored
            if _is_ignored(filename, item_id, ignored_patterns):
                continue

            asset_key = f"{item_id}/{filename}"
            seen_keys.add(asset_key)

            # Determine status based on versions.json presence
            # Simplified logic: file exists + in versions.json = TRACKED
            # Modified detection (hash comparison) deferred to future enhancement
            if asset_key in tracked_assets:
                status = AssetStatus.TRACKED
            else:
                status = AssetStatus.UNTRACKED

            # Get file size
            try:
                size_bytes = entry.stat().st_size
            except OSError:
                size_bytes = None

            assets.append(
                AssetInfo(
                    path=filename,
                    status=status,
                    size_bytes=size_bytes,
                    format_name=_get_format_display_name(filename),
                )
            )
    except OSError as e:
        logger.debug("Cannot scan item directory %s: %s", item_dir, e)

    # Check for deleted files (in versions.json but not on disk)
    for asset_key in tracked_assets:
        if asset_key.startswith(f"{item_id}/") and asset_key not in seen_keys:
            # Extract filename from key
            filename = asset_key.split("/", 1)[1] if "/" in asset_key else asset_key
            assets.append(
                AssetInfo(
                    path=filename,
                    status=AssetStatus.DELETED,
                    size_bytes=None,
                    format_name=_get_format_display_name(filename),
                )
            )

    return ItemInfo(
        item_id=item_id,
        collection_id=collection_id,
        assets=assets,
    )


def _scan_collection_directory(
    col_dir: Path,
    ignored_patterns: list[str],
) -> CollectionInfo:
    """Scan a collection directory and return all items with status.

    Args:
        col_dir: Path to the collection directory.
        ignored_patterns: List of glob patterns to ignore.

    Returns:
        CollectionInfo with all items and their assets.
    """
    collection_id = col_dir.name
    is_initialized = (col_dir / "collection.json").exists()
    versions_path = col_dir / "versions.json"
    tracked_assets = _get_tracked_assets(versions_path)

    items: list[ItemInfo] = []

    # Scan subdirectories as items
    try:
        for entry in sorted(col_dir.iterdir()):
            if not entry.is_dir():
                continue

            # Skip hidden directories
            if entry.name.startswith("."):
                continue

            item_info = _scan_item_directory(
                entry,
                collection_id,
                versions_path,
                tracked_assets,
                ignored_patterns,
            )

            # Only include items that have assets
            if item_info.assets:
                items.append(item_info)
    except OSError as e:
        logger.debug("Cannot scan collection directory %s: %s", col_dir, e)

    return CollectionInfo(
        collection_id=collection_id,
        is_initialized=is_initialized,
        items=items,
    )


def list_catalog_contents(
    catalog_root: Path,
    collection_id: str | None = None,
) -> CatalogListResult:
    """List all contents of a catalog with tracking status.

    Scans ALL subdirectories in the catalog root (not just initialized
    collections) and returns all files with their status.

    Args:
        catalog_root: Root directory of the catalog.
        collection_id: Optional collection to filter by.

    Returns:
        CatalogListResult with all collections, items, and assets.
    """
    # Verify catalog exists
    catalog_path = catalog_root / "catalog.json"
    if not catalog_path.exists():
        return CatalogListResult()

    # Get ignored patterns
    ignored_patterns = get_ignored_files(catalog_root)

    collections: list[CollectionInfo] = []

    # Scan all subdirectories
    try:
        for entry in sorted(catalog_root.iterdir()):
            if not entry.is_dir():
                continue

            # Skip hidden directories
            if entry.name.startswith("."):
                continue

            # Filter by collection if specified
            if collection_id and entry.name != collection_id:
                continue

            col_info = _scan_collection_directory(entry, ignored_patterns)

            # Include collection if it has items or is initialized
            if col_info.items or col_info.is_initialized:
                collections.append(col_info)
    except OSError as e:
        logger.debug("Cannot scan catalog root %s: %s", catalog_root, e)

    return CatalogListResult(collections=collections)
