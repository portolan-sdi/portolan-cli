"""Checksum and size helpers for catalog assets."""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def compute_checksum(path: Path) -> str:
    """Compute SHA-256 checksum of a file securely.

    Security: Validates the resolved path is a regular file to prevent
    symlink attacks (MAJOR #5 - symlink security vulnerability).

    Args:
        path: Path to the file.

    Returns:
        Hex-encoded SHA-256 checksum.

    Raises:
        ValueError: If path is not a regular file (e.g., symlink to directory,
            device file, or other non-regular file).
        FileNotFoundError: If path does not exist.
    """
    # Resolve symlinks and check it's a regular file (MAJOR #5)
    resolved = path.resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"File not found: {path}")
    if not resolved.is_file():
        raise ValueError(f"Not a regular file: {path} (resolves to {resolved})")

    sha256 = hashlib.sha256()
    with open(resolved, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def compute_dir_checksum(path: Path) -> str:
    """Compute a stable fingerprint for a directory by hashing its contents' metadata.

    Used for directory-format assets such as FileGDB (.gdb). Rather than reading
    all bytes (expensive for large catalogs), hashes the sorted list of
    (relative_path, size, mtime) tuples for every file inside the directory.
    This detects file additions, removals, and modifications within the directory.

    Directories are not checksummed by content — the fingerprint is based on the
    metadata of all contained files (recursively). This is consistent with how
    ``is_current()`` uses mtime as a fast-path gate before falling back to this
    checksum.

    Args:
        path: Path to the directory.

    Returns:
        Hex-encoded SHA-256 fingerprint of the directory contents.

    Raises:
        ValueError: If path is not a directory.
        FileNotFoundError: If path does not exist.
    """
    resolved = path.resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"Directory not found: {path}")
    if not resolved.is_dir():
        raise ValueError(f"Not a directory: {path} (resolves to {resolved})")

    sha256 = hashlib.sha256()
    # Collect (relative_path, size, mtime) for all files, sorted for determinism.
    entries: list[tuple[str, int, float]] = []
    try:
        for fpath in sorted(resolved.rglob("*")):
            if not fpath.is_file():
                continue
            rel_path = fpath.relative_to(resolved).as_posix()
            try:
                stat = fpath.stat()
                entries.append((rel_path, stat.st_size, stat.st_mtime))
            except OSError:
                # Skip files we can't stat (e.g., broken symlinks inside .gdb)
                entries.append((rel_path, -1, -1.0))
    except OSError as exc:
        raise ValueError(f"Cannot read directory contents: {path}") from exc

    for rel_path, size, mtime in entries:
        sha256.update(f"{rel_path}\x00{size}\x00{mtime:.6f}\n".encode())
    return sha256.hexdigest()


def compute_dir_size(path: Path) -> int:
    """Compute total size of all files in a directory.

    Used for directory-format assets such as FileGDB (.gdb) to populate
    the STAC file:size field.

    Args:
        path: Path to the directory.

    Returns:
        Total size in bytes of all files in the directory.

    Raises:
        ValueError: If path is not a directory.
        FileNotFoundError: If path does not exist.
    """
    resolved = path.resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"Directory not found: {path}")
    if not resolved.is_dir():
        raise ValueError(f"Not a directory: {path} (resolves to {resolved})")

    total_size = 0
    try:
        for fpath in resolved.rglob("*"):
            if fpath.is_file():
                try:
                    total_size += fpath.stat().st_size
                except OSError:
                    pass
    except OSError as exc:
        raise ValueError(f"Cannot read directory contents: {path}") from exc

    return total_size
