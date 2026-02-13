"""Special format detection for portolan scan.

This module detects special format structures:
- FileGDB directories (.gdb) and archives (.gdb.zip)
- Hive-partitioned datasets (key=value/ directories)
- Existing STAC catalogs (catalog.json, collection.json)
- Dual-format datasets (same basename, different formats)

Functions:
    is_filegdb: Check if a path is a FileGDB directory.
    is_filegdb_archive: Check if a path is a zipped FileGDB.
    detect_filegdb: Detect FileGDB and return SpecialFormat.
    is_hive_partition_dir: Check if directory name matches Hive pattern.
    detect_hive_partitions: Detect Hive-partitioned datasets.
    detect_stac_catalogs: Detect existing STAC catalogs.
    detect_dual_formats: Detect files with same basename, different formats.
"""

from __future__ import annotations

import os
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from portolan_cli.scan import ScannedFile

# Hive partition pattern: key=value where key starts with letter/underscore
HIVE_PARTITION_PATTERN = re.compile(r"^([a-zA-Z_][a-zA-Z0-9_]*)=(.+)$")

# FileGDB internal file extensions
FILEGDB_INTERNAL_EXTENSIONS: frozenset[str] = frozenset(
    {".gdbtable", ".gdbtablx", ".gdbindexes", ".atx", ".spx", ".freelist"}
)

# FileGDB lock file patterns
FILEGDB_LOCK_PATTERNS: tuple[str, ...] = (".lck", "lockfile", ".lock")


@dataclass(frozen=True)
class SpecialFormat:
    """A detected special format structure."""

    path: Path
    relative_path: str
    format_type: str  # "filegdb", "filegdb_archive", "hive_partition", etc.
    details: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "path": str(self.path),
            "relative_path": self.relative_path,
            "format_type": self.format_type,
            "details": self.details,
        }


@dataclass(frozen=True)
class DualFormatPair:
    """Two files representing same dataset in different formats."""

    basename: str
    files: tuple[Path, Path]
    format_types: tuple[str, str]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "basename": self.basename,
            "files": [str(p) for p in self.files],
            "format_types": list(self.format_types),
        }


# =============================================================================
# FileGDB Detection
# =============================================================================


def is_filegdb(path: Path) -> bool:
    """Check if a path is a FileGDB directory.

    FileGDB is detected by:
    1. Directory ending with .gdb
    2. Contains internal .gdbtable files OR 'gdb' marker file

    Args:
        path: Path to check.

    Returns:
        True if path is a FileGDB directory.
    """
    # Must be a directory
    if not path.is_dir():
        return False

    # Must end with .gdb
    if not path.name.lower().endswith(".gdb"):
        return False

    # Check for FileGDB internal structure
    try:
        for entry in os.scandir(path):
            name = entry.name.lower()
            # Check for .gdbtable files
            if name.endswith(".gdbtable"):
                return True
            # Check for 'gdb' marker file
            if name == "gdb" and entry.is_file():
                return True
    except OSError:
        pass

    return False


def is_filegdb_archive(path: Path) -> bool:
    """Check if a path is a zipped FileGDB archive.

    Args:
        path: Path to check.

    Returns:
        True if path ends with .gdb.zip.
    """
    # Must be a file, not directory
    if not path.is_file():
        return False

    # Check for .gdb.zip extension
    name_lower = path.name.lower()
    return name_lower.endswith(".gdb.zip")


def _get_relative_path(path: Path, root: Path) -> str:
    """Get path relative to root as string."""
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def detect_filegdb(path: Path, root: Path) -> SpecialFormat | None:
    """Detect FileGDB and return SpecialFormat.

    Args:
        path: Path to check.
        root: Root directory for relative path calculation.

    Returns:
        SpecialFormat if FileGDB detected, None otherwise.
    """
    # Check for .gdb.zip archive first
    if is_filegdb_archive(path):
        return SpecialFormat(
            path=path,
            relative_path=_get_relative_path(path, root),
            format_type="filegdb_archive",
            details={"archive": True},
        )

    # Check for .gdb directory
    if not is_filegdb(path):
        return None

    # Count internal files and check for lock files
    gdbtable_count = 0
    lock_files_present = False

    try:
        for entry in os.scandir(path):
            name_lower = entry.name.lower()
            if name_lower.endswith(".gdbtable"):
                gdbtable_count += 1
            # Check for lock files
            for lock_pattern in FILEGDB_LOCK_PATTERNS:
                if lock_pattern in name_lower:
                    lock_files_present = True
                    break
    except OSError:
        pass

    return SpecialFormat(
        path=path,
        relative_path=_get_relative_path(path, root),
        format_type="filegdb",
        details={
            "gdbtable_count": gdbtable_count,
            "lock_files_present": lock_files_present,
        },
    )


# =============================================================================
# Hive Partition Detection
# =============================================================================


def is_hive_partition_dir(name: str) -> tuple[str, str] | None:
    """Check if directory name is a Hive partition.

    Args:
        name: Directory name to check.

    Returns:
        (key, value) tuple if matches Hive pattern, None otherwise.
    """
    match = HIVE_PARTITION_PATTERN.match(name)
    if match:
        key, value = match.groups()
        # Require non-empty value
        if value:
            return (key, value)
    return None


def detect_hive_partitions(root: Path) -> list[SpecialFormat]:
    """Detect Hive-partitioned datasets under root.

    A Hive-partitioned dataset is detected when there are directories
    following the key=value pattern at any level.

    Args:
        root: Root directory to scan.

    Returns:
        List of SpecialFormat objects for each partitioned dataset.
    """
    results: list[SpecialFormat] = []
    partition_roots: set[Path] = set()

    # Walk the directory tree looking for Hive partition patterns
    for dirpath, dirnames, _filenames in os.walk(root):
        current = Path(dirpath)

        # Check each subdirectory name for Hive pattern
        for dirname in dirnames:
            partition_info = is_hive_partition_dir(dirname)
            if partition_info:
                # Found a Hive partition - the parent is the dataset root
                if current not in partition_roots:
                    partition_roots.add(current)

                    # Collect partition keys
                    keys: list[str] = []
                    for d in dirnames:
                        info = is_hive_partition_dir(d)
                        if info:
                            keys.append(info[0])

                    results.append(
                        SpecialFormat(
                            path=current,
                            relative_path=_get_relative_path(current, root),
                            format_type="hive_partition",
                            details={
                                "partition_keys": sorted(set(keys)),
                            },
                        )
                    )

    return results


# =============================================================================
# STAC Catalog Detection
# =============================================================================


def detect_stac_catalogs(root: Path) -> list[SpecialFormat]:
    """Detect existing STAC catalogs and collections.

    Looks for:
    - catalog.json files (STAC catalog)
    - collection.json files (STAC collection)

    Args:
        root: Root directory to scan.

    Returns:
        List of SpecialFormat objects for detected catalogs/collections.
    """
    results: list[SpecialFormat] = []

    for dirpath, _dirnames, filenames in os.walk(root):
        current = Path(dirpath)

        for filename in filenames:
            filename_lower = filename.lower()

            if filename_lower == "catalog.json":
                results.append(
                    SpecialFormat(
                        path=current / filename,
                        relative_path=_get_relative_path(current / filename, root),
                        format_type="stac_catalog",
                        details={},
                    )
                )
            elif filename_lower == "collection.json":
                results.append(
                    SpecialFormat(
                        path=current / filename,
                        relative_path=_get_relative_path(current / filename, root),
                        format_type="stac_collection",
                        details={},
                    )
                )

    return results


# =============================================================================
# Dual Format Detection
# =============================================================================


def detect_dual_formats(files: list[ScannedFile]) -> list[DualFormatPair]:
    """Detect pairs of files representing same dataset in different formats.

    Only pairs within the same format type (vector/vector or raster/raster)
    are flagged. Cross-type pairs (raster/vector) are intentional.

    Args:
        files: List of scanned geo-asset files.

    Returns:
        List of DualFormatPair objects.
    """
    # Group files by (directory, stem, format_type)
    groups: dict[tuple[Path, str, str], list[ScannedFile]] = defaultdict(list)

    for f in files:
        stem = f.path.stem
        parent = f.path.parent
        format_type = f.format_type.value
        groups[(parent, stem, format_type)].append(f)

    results: list[DualFormatPair] = []

    # Find groups with multiple files (same stem, same format type, same dir)
    for (_parent, stem, _format_type), group_files in groups.items():
        if len(group_files) >= 2:
            # Sort by extension for consistent ordering
            sorted_files = sorted(group_files, key=lambda x: x.extension)
            # Create pairs from first two files
            results.append(
                DualFormatPair(
                    basename=stem,
                    files=(sorted_files[0].path, sorted_files[1].path),
                    format_types=(sorted_files[0].extension, sorted_files[1].extension),
                )
            )

    return results
