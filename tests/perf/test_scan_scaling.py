"""``scan`` does linear work in the number of directories.
Linear work gives ``c(4n) - c(2n) == 2 * (c(2n) - c(n))`` in executed lines; a loop over pairs breaks it.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

import portolan_cli.scan
from portolan_cli.scan.core import IssueType, scan_directory

if TYPE_CHECKING:
    from types import FrameType

pytestmark = pytest.mark.integration

SCAN_PACKAGE = str(Path(portolan_cli.scan.__file__).parent)


def _many_dirs(base: Path, count: int) -> Path:
    """Build the EuroSAT layout: one file in each of ``count`` directories."""
    base.mkdir()
    for i in range(count):
        scene = base / f"scene_{i:05d}"
        scene.mkdir()
        (scene / "data.tif").write_bytes(b"dummy tif content")
    return base


def scan_lines(root: Path) -> int:
    """Return the number of ``portolan_cli.scan`` source lines one scan of ``root`` runs."""
    lines = 0

    def local(frame: FrameType, event: str, arg: Any) -> Any:
        nonlocal lines
        if event == "line":
            lines += 1
        return local

    def tracer(frame: FrameType, event: str, arg: Any) -> Any:
        return local if frame.f_code.co_filename.startswith(SCAN_PACKAGE) else None

    previous = sys.gettrace()
    sys.settrace(tracer)
    try:
        result = scan_directory(root)
    finally:
        sys.settrace(previous)
    assert [i for i in result.issues if i.issue_type == IssueType.MIXED_FLAT_MULTIITEM] == []
    return lines


def test_scan_work_is_linear_in_directories(tmp_path: Path) -> None:
    # A first scan imports modules, which runs their top-level lines once.
    scan_lines(_many_dirs(tmp_path / "warm", 2))

    c1, c2, c4 = (scan_lines(_many_dirs(tmp_path / str(n), n)) for n in (100, 200, 400))

    assert c2 > c1
    assert c4 - c2 == 2 * (c2 - c1), (c1, c2, c4)
