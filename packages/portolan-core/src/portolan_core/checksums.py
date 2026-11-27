"""Checksum and size helpers for catalog assets."""

from __future__ import annotations

import contextlib
import hashlib
import logging
from pathlib import Path
from typing import Any

from rashid_core.api import SHA2_256, encode_multihash

from portolan_core.trace import count

_SHA256_HEX_LEN = 64

logger = logging.getLogger(__name__)

#: Read size of :func:`compute_checksum`, the one file hasher in Portolan.
CHUNK_SIZE = 1024 * 1024


def multihash_sha256(digest_hex: str) -> str:
    """Encode a hex SHA-256 digest as the hex multihash that STAC ``file:checksum`` needs.

    Raises ``ValueError`` if ``digest_hex`` is not a 64-character hex string.
    """
    if len(digest_hex) != _SHA256_HEX_LEN:
        raise ValueError(f"Not a 64-character SHA-256 hex digest: {digest_hex!r}")
    try:
        digest = bytes.fromhex(digest_hex)
    except ValueError as exc:
        raise ValueError(f"Not a 64-character SHA-256 hex digest: {digest_hex!r}") from exc
    return encode_multihash(SHA2_256, digest)


def compute_checksum(path: Path) -> str:
    """Compute the SHA-256 checksum of a file in 1 MiB chunks.

    Args:
        path: Path to the file.

    Returns:
        Hex-encoded SHA-256 checksum.

    Raises:
        ValueError: If path does not resolve to a regular file.
        FileNotFoundError: If path does not exist.
    """
    # Security: hash only a resolved regular file, so a symlink cannot point at a device.
    resolved = path.resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"File not found: {path}")
    if not resolved.is_file():
        raise ValueError(f"Not a regular file: {path} (resolves to {resolved})")

    sha256 = hashlib.sha256()
    size = 0
    with Path(resolved).open("rb") as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            sha256.update(chunk)
            size += len(chunk)
    count("checksum.files")
    count("checksum.bytes", size)
    return sha256.hexdigest()


def file_fields_from(digest_hex: str, size: int) -> dict[str, Any]:
    """Build ``file:size`` and ``file:checksum`` from a measured digest and size.

    Takes no path, because a FileGDB asset is a directory that :func:`compute_checksum` rejects.
    """
    return {"file:size": size, "file:checksum": multihash_sha256(digest_hex)}


def file_fields(path: Path) -> dict[str, Any]:
    """Build ``file:size`` and ``file:checksum`` from the bytes of a regular file as it stands.

    PORTO-CORE-030 makes these fields a claim about the current bytes, so never carry them forward.
    """
    return file_fields_from(compute_checksum(path), path.stat().st_size)


def compute_dir_checksum(path: Path) -> str:
    """Fingerprint a directory asset (FileGDB) from the sorted path, size, and mtime of its files.

    It reads no file bytes, so it is fast on large directories.
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
    """Return the total byte size of all files in a directory asset, for ``file:size``."""
    resolved = path.resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"Directory not found: {path}")
    if not resolved.is_dir():
        raise ValueError(f"Not a directory: {path} (resolves to {resolved})")

    total_size = 0
    try:
        for fpath in resolved.rglob("*"):
            if fpath.is_file():
                with contextlib.suppress(OSError):
                    total_size += fpath.stat().st_size
    except OSError as exc:
        raise ValueError(f"Cannot read directory contents: {path}") from exc

    return total_size
