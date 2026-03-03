"""Status module for tracking state detection.

This module provides functions to detect tracking states (untracked, tracked,
modified, deleted) by comparing filesystem contents against versions.json.

Per ADR-0007, this is the library layer - CLI wraps these functions.
Per ADR-0022, status shows: untracked, modified, deleted files.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from portolan_cli.constants import GEOSPATIAL_EXTENSIONS
from portolan_cli.dataset import is_current
from portolan_cli.versions import read_versions


@dataclass(frozen=True)
class FileStatus:
    """Represents a file's tracking status.

    Attributes:
        collection_id: The collection containing this file.
        filename: The filename within the collection.
    """

    collection_id: str
    filename: str

    @property
    def path(self) -> str:
        """Return the full relative path (collection_id/filename)."""
        return f"{self.collection_id}/{self.filename}"


@dataclass
class StatusResult:
    """Result of a catalog status check.

    Attributes:
        untracked: Files in catalog dirs but not in versions.json.
        modified: Files that exist but have changed since last tracked.
        deleted: Files in versions.json but missing from disk.
    """

    untracked: list[FileStatus]
    modified: list[FileStatus]
    deleted: list[FileStatus]

    def is_clean(self) -> bool:
        """Return True if there are no changes to report."""
        return not self.untracked and not self.modified and not self.deleted


def get_catalog_status(catalog_root: Path) -> StatusResult:
    """Get the tracking status of all files in a catalog.

    Compares filesystem contents against versions.json for each collection
    to determine which files are untracked, modified, or deleted.

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

    # Scan root-level directories for collections (per ADR-0023)
    for col_dir in catalog_root.iterdir():
        if not col_dir.is_dir():
            continue

        # Skip .portolan and hidden directories
        if col_dir.name.startswith("."):
            continue

        collection_path = col_dir / "collection.json"
        if not collection_path.exists():
            continue

        collection_id = col_dir.name
        versions_path = col_dir / ".portolan" / "versions.json"

        # Get tracked assets from versions.json
        tracked_assets: set[str] = set()
        if versions_path.exists():
            try:
                versions_file = read_versions(versions_path)
                if versions_file.versions:
                    current_version = versions_file.versions[-1]
                    tracked_assets = set(current_version.assets.keys())
            except (ValueError, json.JSONDecodeError):
                # If versions.json is corrupt, treat all files as untracked
                pass

        # Scan for geospatial files in collection directory
        for item in col_dir.iterdir():
            if not item.is_file():
                continue

            # Skip non-geospatial files
            if item.suffix.lower() not in GEOSPATIAL_EXTENSIONS:
                continue

            filename = item.name

            if filename not in tracked_assets:
                # File exists on disk but not in versions.json -> untracked
                untracked.append(FileStatus(collection_id, filename))
            elif not is_current(item, versions_path):
                # File exists but has changed -> modified
                modified.append(FileStatus(collection_id, filename))
            # else: file is tracked and unchanged, don't report

        # Check for deleted files (in versions.json but not on disk)
        for tracked_filename in tracked_assets:
            file_path = col_dir / tracked_filename
            if not file_path.exists():
                deleted.append(FileStatus(collection_id, tracked_filename))

    # Sort results for consistent output
    untracked.sort(key=lambda f: f.path)
    modified.sort(key=lambda f: f.path)
    deleted.sort(key=lambda f: f.path)

    return StatusResult(untracked=untracked, modified=modified, deleted=deleted)
