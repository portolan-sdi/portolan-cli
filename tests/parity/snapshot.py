"""A byte snapshot of a catalog, for parity tests of a command that must write nothing."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def tree(root: Path) -> dict[str, bytes]:
    """Return the bytes of each file below ``root``, by relative path."""
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in root.rglob("*") if p.is_file()}
