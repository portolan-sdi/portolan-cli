"""``list`` orders files the same way on Windows and POSIX.

A Windows ``Path`` compares without case. ``sorted`` on paths therefore put
``data.parquet`` before ``README.md`` on Windows and after it on POSIX. The
``list --json`` asset order then differed between platforms.
"""

from __future__ import annotations

from pathlib import PurePosixPath, PureWindowsPath

import pytest

from portolan_cli.catalog_list import _by_name

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("path_type", [PurePosixPath, PureWindowsPath])
def test_files_sort_by_name_on_every_platform(
    path_type: type[PurePosixPath | PureWindowsPath],
) -> None:
    """``README.md`` sorts before ``data.parquet`` with Windows and POSIX paths."""
    paths = [path_type("roads/data.parquet"), path_type("roads/README.md")]

    assert [p.name for p in sorted(paths, key=_by_name)] == ["README.md", "data.parquet"]
