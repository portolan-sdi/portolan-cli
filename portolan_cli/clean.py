"""Clean command for removing Portolan metadata while preserving data files."""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from portolan_core.constants import PORTOLAN_DIR

if TYPE_CHECKING:
    from pathlib import Path


def is_stac_metadata(path: Path) -> bool:
    """Check if a JSON file is STAC metadata (safe to remove)."""
    # Only check JSON files
    if path.suffix.lower() != ".json":
        return False

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return False
        stac_type = data.get("type")
        return stac_type in ("Catalog", "Collection", "Feature")
    except (json.JSONDecodeError, OSError):
        return False


def is_versions_json(path: Path) -> bool:
    """Check if a file is versions.json (Portolan-specific, safe to remove)."""
    return path.name == "versions.json"


def collect_files_to_remove(catalog_root: Path) -> tuple[list[Path], list[Path]]:
    """Collect all metadata files and directories to remove from a catalog."""
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

        # Check for STAC metadata
        if path.is_file() and (is_stac_metadata(path) or is_versions_json(path)):
            files_to_remove.append(path)

    # Sort files for deterministic output
    files_to_remove.sort()

    return files_to_remove, directories_to_remove


def remove_empty_directories(catalog_root: Path, removed_files: list[Path]) -> list[Path]:
    """Remove directories that became empty after file removal."""
    directories_removed: list[Path] = []

    # Get unique parent directories of removed files, sorted deepest first
    parent_dirs = set()
    for file_path in removed_files:
        parent = file_path.parent
        while parent not in (catalog_root, catalog_root.parent):
            parent_dirs.add(parent)
            parent = parent.parent

    # Sort by depth (deepest first)
    sorted_dirs = sorted(parent_dirs, key=lambda p: len(p.parts), reverse=True)

    for dir_path in sorted_dirs:
        # Check if directory is empty using lazy iteration (no list allocation)
        if dir_path.is_dir() and next(dir_path.iterdir(), None) is None:
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
    """Remove all Portolan metadata from a catalog while preserving data."""
    files_to_remove, directories_to_remove = collect_files_to_remove(catalog_root)

    # Count data files (files that are NOT being removed)
    # Use a set for O(1) membership checks instead of O(n) list scan
    files_to_remove_set = set(files_to_remove)
    data_files = 0
    for path in catalog_root.rglob("*"):
        if path.is_file() and PORTOLAN_DIR not in path.parts and path not in files_to_remove_set:
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


@dataclass(frozen=True)
class CleanResult:
    """The outcome of ``portolan clean``."""

    catalog_root: Path
    files: list[Path]
    directories: list[Path]
    data_files_preserved: int
    dry_run: bool

    def to_dict(self) -> dict[str, Any]:
        """Serialize the outcome as the ``clean`` envelope data."""
        files = [str(f.relative_to(self.catalog_root)) for f in self.files]
        directories = [str(d.relative_to(self.catalog_root)) for d in self.directories]
        if self.dry_run:
            return {
                "would_remove_files": files,
                "would_remove_directories": directories,
                "data_files_preserved": self.data_files_preserved,
                "dry_run": True,
            }
        return {
            "files_removed": files,
            "directories_removed": directories,
            "data_files_preserved": self.data_files_preserved,
        }


def clean_metadata(catalog_root: Path, *, dry_run: bool = False) -> CleanResult:
    """Remove all Portolan metadata from a catalog while preserving data."""
    files, directories, data_files = clean_catalog(catalog_root, dry_run=dry_run)
    return CleanResult(catalog_root, files, directories, data_files, dry_run)
