"""File discovery: iterate geospatial files and their sidecars."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from portolan_cli.scan.detect import is_filegdb
from portolan_core.constants import (
    GEOSPATIAL_EXTENSIONS,
    SIDECAR_PATTERNS,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

logger = logging.getLogger(__name__)


def _is_geospatial_file(item: Path) -> bool:
    """Return True for a regular file with a recognized geospatial extension."""
    from portolan_cli.plugins.formats import plugin_extensions

    extension = item.suffix.lower()
    return (extension in GEOSPATIAL_EXTENSIONS or extension in plugin_extensions()) and (
        item.is_file()
    )


def _iter_recursive(directory: Path, visited: set[Path] | None = None) -> Iterator[Path]:
    """Yield geospatial assets under ``directory``, pruning FileGDB subtrees."""
    if visited is None:
        visited = set()
    real = directory.resolve()
    if real in visited:
        return
    visited.add(real)
    for item in sorted(directory.iterdir()):
        if item.is_dir():
            if is_filegdb(item):
                yield item  # single asset; do not descend into its internals
            else:
                yield from _iter_recursive(item, visited)
        elif _is_geospatial_file(item):
            yield item


def iter_geospatial_files(
    path: Path,
    *,
    recursive: bool = True,
) -> list[Path]:
    """Iterate over geospatial files in a directory."""
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
    """Detect sidecar files for a given primary file."""
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
    """Iterate over geospatial files in a directory (including their sidecars)."""
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
