"""File discovery: iterate geospatial files and their sidecars."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path

from portolan_cli.constants import (
    GEOSPATIAL_EXTENSIONS,
    SIDECAR_PATTERNS,
)
from portolan_cli.scan_detect import is_filegdb

logger = logging.getLogger(__name__)


def _is_geospatial_file(item: Path) -> bool:
    """Return True for a regular file with a recognized geospatial extension."""
    return item.suffix.lower() in GEOSPATIAL_EXTENSIONS and item.is_file()


def _iter_recursive(directory: Path) -> Iterator[Path]:
    """Yield geospatial assets under ``directory``, pruning FileGDB subtrees.

    FileGDB (``.gdb``) directories are single geospatial assets: they are
    yielded as-is and their internals are *never* enumerated (issue #590),
    avoiding a wasted walk over the thousands of files a FileGDB contains.
    """
    for item in directory.iterdir():
        if item.is_dir():
            if is_filegdb(item):
                yield item  # single asset; do not descend into its internals
            else:
                yield from _iter_recursive(item)
        elif _is_geospatial_file(item):
            yield item


def iter_geospatial_files(
    path: Path,
    *,
    recursive: bool = True,
) -> list[Path]:
    """Iterate over geospatial files in a directory.

    Includes both regular files and FileGDB directories (.gdb).
    FileGDB directories are treated as single geospatial assets.

    Args:
        path: Directory to scan.
        recursive: If True, scan subdirectories recursively.

    Returns:
        List of paths to geospatial files (including FileGDB directories).
    """
    # Special case: if path itself is a FileGDB, return it directly
    if is_filegdb(path):
        return [path]

    if not path.is_dir():
        return []

    files: list[Path] = []

    if recursive:
        files.extend(_iter_recursive(path))
    else:
        for item in path.iterdir():
            # FileGDB directories are single assets; other files match by extension.
            if item.is_dir():
                if is_filegdb(item):
                    files.append(item)
            elif _is_geospatial_file(item):
                files.append(item)

    return sorted(files)


def get_sidecars(path: Path) -> list[Path]:
    """Detect sidecar files for a given primary file.

    Automatically finds associated files like .dbf/.shx/.prj for shapefiles,
    or .tfw/.xml for GeoTIFFs.

    Args:
        path: Path to the primary file.

    Returns:
        List of existing sidecar file paths (may be empty).
    """
    suffix_lower = path.suffix.lower()
    patterns = SIDECAR_PATTERNS.get(suffix_lower, [])

    sidecars: list[Path] = []
    stem = path.stem
    parent = path.parent

    for ext in patterns:
        sidecar_path = parent / f"{stem}{ext}"
        if sidecar_path.exists():
            sidecars.append(sidecar_path)

    return sidecars


def iter_files_with_sidecars(path: Path, *, recursive: bool = True) -> list[Path]:
    """Iterate over geospatial files in a directory (including their sidecars).

    Returns geospatial files and their associated sidecars (e.g., .dbf/.shx for shapefiles).
    FileGDB directories (.gdb) are treated as single geospatial assets (which have no
    sidecars). Discovery and FileGDB handling are delegated to iter_geospatial_files.

    Args:
        path: Directory to scan.
        recursive: If True, scan subdirectories recursively.

    Returns:
        List of geospatial file paths (including FileGDB directories) and their sidecars.
    """
    files: list[Path] = []
    seen: set[Path] = set()

    for geo_file in iter_geospatial_files(path, recursive=recursive):
        if geo_file not in seen:
            files.append(geo_file)
            seen.add(geo_file)

        # Include any sidecars for this file (FileGDB dirs yield none).
        for sidecar in get_sidecars(geo_file):
            if sidecar not in seen:
                files.append(sidecar)
                seen.add(sidecar)

    return sorted(files)
