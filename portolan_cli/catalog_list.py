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

from portolan_cli.config import get_ignored_files
from portolan_cli.format_types import FORMAT_DISPLAY_NAMES, FormatType, _detect_json_type
from portolan_cli.stac_links import catalog_collections
from portolan_cli.versions import read_versions

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
    """Information about a collection with all its items.

    ``assets`` holds the files directly in the collection directory, such as a
    collection-level GeoParquet that ``portolan add`` writes.
    """

    collection_id: str
    is_initialized: bool  # Has collection.json
    items: list[ItemInfo] = field(default_factory=list)
    assets: list[AssetInfo] = field(default_factory=list)

    def _count(self, status: AssetStatus) -> int:
        item_total = sum(1 for item in self.items for a in item.assets if a.status == status)
        return item_total + sum(1 for a in self.assets if a.status == status)


@dataclass
class CatalogListResult:
    """Result of listing catalog contents with status."""

    collections: list[CollectionInfo] = field(default_factory=list)

    @property
    def total_tracked(self) -> int:
        """Total tracked assets across all collections."""
        return sum(col._count(AssetStatus.TRACKED) for col in self.collections)

    @property
    def total_untracked(self) -> int:
        """Total untracked assets across all collections."""
        return sum(col._count(AssetStatus.UNTRACKED) for col in self.collections)

    @property
    def total_modified(self) -> int:
        """Total modified assets across all collections."""
        return sum(col._count(AssetStatus.MODIFIED) for col in self.collections)

    @property
    def total_deleted(self) -> int:
        """Total deleted assets across all collections."""
        return sum(col._count(AssetStatus.DELETED) for col in self.collections)

    def is_empty(self) -> bool:
        """Return True if there are no collections, items, or collection files."""
        return all(not col.items and not col.assets for col in self.collections)


# Files that are always excluded regardless of config
_ALWAYS_IGNORED: frozenset[str] = frozenset(
    {
        ".DS_Store",
        "Thumbs.db",
        ".gitkeep",
        ".env",  # Credentials - must never be tracked/pushed (Issue #356)
    }
)

# STAC metadata files to exclude
_STAC_METADATA_FILES: frozenset[str] = frozenset(
    {
        "item.json",
        "collection.json",
        "catalog.json",
        "versions.json",  # Portolan internal version tracking
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

    # Ignore STAC metadata files (by name)
    if filename in _STAC_METADATA_FILES:
        return True

    # Ignore STAC item JSON files named {item_id}.json
    # These are STAC metadata, not user data files
    if item_id and filename == f"{item_id}.json":
        return True

    # Check against ignored patterns
    return any(fnmatch.fnmatch(filename, pattern) for pattern in ignored_patterns)


def _get_format_display_name(filename: str, file_path: Path | None = None) -> str:
    """Get human-readable format name for a file.

    Uses extension-based detection by default, with content inspection
    for ambiguous formats like .json (which may be GeoJSON or plain JSON).

    Args:
        filename: The filename to check.
        file_path: Optional full path for content inspection. If provided and
            the file exists, content inspection is used for .json files.

    Returns:
        Human-readable format name (e.g., "GeoParquet", "GeoJSON", "JSON").
    """
    ext = Path(filename).suffix.lower()

    # Special case: .json files need content inspection to distinguish
    # GeoJSON from plain JSON (PR #261)
    if ext == ".json" and file_path is not None and file_path.exists():
        if _detect_json_type(file_path) == FormatType.VECTOR:
            return "GeoJSON"
        return "JSON"

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


def _scan_files(
    directory: Path,
    key_prefix: str,
    item_id: str,
    tracked_assets: set[str],
    ignored_patterns: list[str],
) -> list[AssetInfo]:
    """List the files directly in ``directory`` with their tracking status.

    Args:
        directory: The item or collection directory.
        key_prefix: The versions.json key prefix of a file here. An item uses
            ``"{item_id}/"``. The collection directory uses ``""``.
        item_id: The item ID, to skip the ``{item_id}.json`` STAC file. Empty
            for the collection directory.
        tracked_assets: Asset keys in the current version of versions.json.
        ignored_patterns: List of glob patterns to ignore.

    Returns:
        The files on disk, then the tracked files missing from disk.
    """
    assets: list[AssetInfo] = []
    seen_keys: set[str] = set()

    try:
        for entry in sorted(directory.iterdir()):
            if entry.is_dir():
                continue

            filename = entry.name
            if _is_ignored(filename, item_id, ignored_patterns):
                continue

            asset_key = f"{key_prefix}{filename}"
            seen_keys.add(asset_key)

            # Simplified logic: file exists + in versions.json = TRACKED
            status = AssetStatus.TRACKED if asset_key in tracked_assets else AssetStatus.UNTRACKED

            try:
                size_bytes: int | None = entry.stat().st_size
            except OSError:
                size_bytes = None

            assets.append(
                AssetInfo(
                    path=filename,
                    status=status,
                    size_bytes=size_bytes,
                    format_name=_get_format_display_name(filename, file_path=entry),
                )
            )
    except OSError as e:
        logger.debug("Cannot scan directory %s: %s", directory, e)

    # Tracked files missing from disk. A key directly in this directory holds no
    # further "/" after the prefix.
    for asset_key in sorted(tracked_assets):
        if not asset_key.startswith(key_prefix) or asset_key in seen_keys:
            continue
        filename = asset_key[len(key_prefix) :]
        if "/" in filename:
            continue
        assets.append(
            AssetInfo(
                path=filename,
                status=AssetStatus.DELETED,
                size_bytes=None,
                format_name=_get_format_display_name(filename),
            )
        )

    return assets


def _scan_item_directory(
    item_dir: Path,
    collection_id: str,
    tracked_assets: set[str],
    ignored_patterns: list[str],
) -> ItemInfo:
    """Scan an item directory and return all files with status.

    Args:
        item_dir: Path to the item directory.
        collection_id: ID of the parent collection.
        tracked_assets: Asset keys in the current version of versions.json.
        ignored_patterns: List of glob patterns to ignore.

    Returns:
        ItemInfo with all assets and their status.
    """
    item_id = item_dir.name
    return ItemInfo(
        item_id=item_id,
        collection_id=collection_id,
        assets=_scan_files(item_dir, f"{item_id}/", item_id, tracked_assets, ignored_patterns),
    )


def _scan_collection_directory(
    col_dir: Path,
    collection_id: str,
    ignored_patterns: list[str],
) -> CollectionInfo:
    """Scan a collection directory and return its files and items with status.

    A subdirectory that holds its own ``collection.json`` is a nested
    collection. It is listed on its own, not as an item of this collection.

    Args:
        col_dir: Path to the collection directory.
        collection_id: The collection ID, a path below the catalog root.
        ignored_patterns: List of glob patterns to ignore.

    Returns:
        CollectionInfo with all items and their assets.
    """
    is_initialized = (col_dir / "collection.json").exists()
    tracked_assets = _get_tracked_assets(col_dir / "versions.json")

    items: list[ItemInfo] = []
    try:
        for entry in sorted(col_dir.iterdir()):
            if not entry.is_dir() or entry.name.startswith("."):
                continue
            if (entry / "collection.json").exists():
                continue

            item_info = _scan_item_directory(
                entry,
                collection_id,
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
        assets=_scan_files(col_dir, "", "", tracked_assets, ignored_patterns),
    )


def list_catalog_contents(
    catalog_root: Path,
    collection_id: str | None = None,
) -> CatalogListResult:
    """List all contents of a catalog with tracking status.

    The collections come from ``catalog_collections``, the walk ``check`` also
    applies, so a nested or unlinked collection appears (issue #944). A
    top-level directory without ``collection.json`` appears too when it holds
    item files, so ``list`` still shows data that is not yet added.

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

    ignored_patterns = get_ignored_files(catalog_root)

    ids = set(catalog_collections(catalog_root))
    try:
        ids.update(
            entry.name
            for entry in catalog_root.iterdir()
            if entry.is_dir() and not entry.name.startswith(".")
        )
    except OSError as e:
        logger.debug("Cannot scan catalog root %s: %s", catalog_root, e)

    collections: list[CollectionInfo] = []
    for col_id in sorted(ids):
        if collection_id and col_id != collection_id:
            continue

        col_info = _scan_collection_directory(catalog_root / col_id, col_id, ignored_patterns)

        # An uninitialized directory appears only when it holds item files
        if col_info.items or col_info.is_initialized:
            collections.append(col_info)

    return CatalogListResult(collections=collections)
