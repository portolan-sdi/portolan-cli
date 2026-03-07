"""Clean command for removing Portolan metadata while preserving data files.

This module provides functionality to remove all Portolan-generated metadata
from a catalog while preserving user data files. Essential for testing
workflows where you want to reset a catalog to a "clean slate" state.

Files removed (metadata only):
- .portolan/ directory (entire thing)
- catalog.json (with "type": "Catalog")
- */collection.json (with "type": "Collection")
- */*/item.json or similar with "type": "Feature" (STAC items)
- */versions.json (Portolan-specific schema)

Files preserved (all data):
- .parquet, .tif, .gpkg, .geojson, .shp, etc.
- ANY file that isn't STAC metadata
- Non-STAC JSON files (checked by "type" field)
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from portolan_cli.constants import PORTOLAN_DIR


def is_stac_metadata(path: Path) -> bool:
    """Check if a JSON file is STAC metadata (safe to remove).

    STAC metadata files have a "type" field with one of:
    - "Catalog" (root catalog.json)
    - "Collection" (collection.json)
    - "Feature" (item.json - STAC items are GeoJSON Features)

    Args:
        path: Path to the file to check.

    Returns:
        True if the file is STAC metadata, False otherwise.

    Examples:
        >>> is_stac_metadata(Path("catalog.json"))  # has type=Catalog
        True
        >>> is_stac_metadata(Path("style.json"))  # non-STAC JSON
        False
        >>> is_stac_metadata(Path("data.parquet"))
        False
    """
    # Only check JSON files
    if path.suffix.lower() != ".json":
        return False

    try:
        data = json.loads(path.read_text())
        if not isinstance(data, dict):
            return False
        stac_type = data.get("type")
        return stac_type in ("Catalog", "Collection", "Feature")
    except (json.JSONDecodeError, OSError):
        return False


def is_versions_json(path: Path) -> bool:
    """Check if a file is versions.json (Portolan-specific, safe to remove).

    Args:
        path: Path to the file to check.

    Returns:
        True if the file is named versions.json, False otherwise.
    """
    return path.name == "versions.json"


def collect_files_to_remove(catalog_root: Path) -> tuple[list[Path], list[Path]]:
    """Collect all metadata files and directories to remove from a catalog.

    Walks the catalog structure and identifies:
    1. .portolan/ directory (always removed entirely)
    2. STAC metadata files (catalog.json, collection.json, item.json with proper type)
    3. versions.json files (Portolan-specific)

    Args:
        catalog_root: Path to the catalog root directory.

    Returns:
        Tuple of (files_to_remove, directories_to_remove).
        Files are sorted for deterministic output.
    """
    files_to_remove: list[Path] = []
    directories_to_remove: list[Path] = []

    # Always remove .portolan directory if it exists
    portolan_dir = catalog_root / PORTOLAN_DIR
    if portolan_dir.exists() and portolan_dir.is_dir():
        directories_to_remove.append(portolan_dir)

    # Walk the catalog and collect files
    for path in catalog_root.rglob("*"):
        # Skip .portolan directory contents (we're removing the whole dir)
        if PORTOLAN_DIR in path.parts:
            continue

        if path.is_file():
            # Check for STAC metadata
            if is_stac_metadata(path):
                files_to_remove.append(path)
            # Check for versions.json
            elif is_versions_json(path):
                files_to_remove.append(path)

    # Sort files for deterministic output
    files_to_remove.sort()

    return files_to_remove, directories_to_remove


def remove_empty_directories(catalog_root: Path, removed_files: list[Path]) -> list[Path]:
    """Remove directories that became empty after file removal.

    Walks bottom-up from the deepest directories and removes any that are empty.
    Stops at the catalog root.

    Args:
        catalog_root: Path to the catalog root directory.
        removed_files: List of files that were removed.

    Returns:
        List of directories that were removed.
    """
    directories_removed: list[Path] = []

    # Get unique parent directories of removed files, sorted deepest first
    parent_dirs = set()
    for file_path in removed_files:
        parent = file_path.parent
        while parent != catalog_root and parent != catalog_root.parent:
            parent_dirs.add(parent)
            parent = parent.parent

    # Sort by depth (deepest first)
    sorted_dirs = sorted(parent_dirs, key=lambda p: len(p.parts), reverse=True)

    for dir_path in sorted_dirs:
        if dir_path.exists() and dir_path.is_dir():
            # Check if directory is empty using lazy iteration (no list allocation)
            if next(dir_path.iterdir(), None) is None:
                try:
                    dir_path.rmdir()
                except OSError:
                    # On Windows, rmdir fails if the directory is the process CWD.
                    # This is a best-effort cleanup, so skip and continue.
                    continue
                directories_removed.append(dir_path)

    return directories_removed


def clean_catalog(
    catalog_root: Path,
    *,
    dry_run: bool = False,
) -> tuple[list[Path], list[Path], int]:
    """Remove all Portolan metadata from a catalog while preserving data.

    Args:
        catalog_root: Path to the catalog root directory.
        dry_run: If True, only report what would be removed without deleting.

    Returns:
        Tuple of (files_removed, directories_removed, data_files_preserved).

    Raises:
        OSError: If file deletion fails (permission errors, etc.).
    """
    files_to_remove, directories_to_remove = collect_files_to_remove(catalog_root)

    # Count data files (files that are NOT being removed)
    # Use a set for O(1) membership checks instead of O(n) list scan
    files_to_remove_set = set(files_to_remove)
    data_files = 0
    for path in catalog_root.rglob("*"):
        if path.is_file() and PORTOLAN_DIR not in path.parts:
            if path not in files_to_remove_set:
                data_files += 1

    if dry_run:
        return files_to_remove, directories_to_remove, data_files

    # Actually remove files
    files_removed: list[Path] = []
    for file_path in files_to_remove:
        if file_path.exists():
            file_path.unlink()
            files_removed.append(file_path)

    # Remove .portolan directory
    directories_removed: list[Path] = []
    for dir_path in directories_to_remove:
        if dir_path.exists():
            shutil.rmtree(dir_path)
            directories_removed.append(dir_path)

    # Clean up empty directories
    empty_dirs_removed = remove_empty_directories(catalog_root, files_removed)
    directories_removed.extend(empty_dirs_removed)

    return files_removed, directories_removed, data_files
