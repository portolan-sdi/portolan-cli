"""Discovery walks each real directory once, whatever symlinks point at it.

A directory symlink that points back at an ancestor makes a cycle. Discovery
used to follow it until the OS refused the path. `portolan add` then found the
same file about 40 times and converted it each time.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import pytest

from portolan_cli.discovery import iter_files_with_sidecars, iter_geospatial_files

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = [
    pytest.mark.unit,
    pytest.mark.skipif(sys.platform == "win32", reason="directory symlinks need privileges"),
]


def _points(directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "points.geojson"
    path.write_text('{"type": "FeatureCollection", "features": []}', encoding="utf-8")
    return path


def test_symlink_to_an_ancestor_is_not_followed_again(tmp_path: Path) -> None:
    """A link back to the root adds no second copy of any file."""
    data = _points(tmp_path / "data")
    (tmp_path / "loop").symlink_to(tmp_path)

    assert iter_geospatial_files(tmp_path) == [data]
    assert iter_files_with_sidecars(tmp_path) == [data]


def test_walk_through_a_symlinked_root_finds_each_file_once(tmp_path: Path) -> None:
    """The root argument itself may be a symlink, as in `portolan add <link>`."""
    catalog = tmp_path / "catalog"
    _points(catalog / "data")
    (catalog / "loop").symlink_to(catalog)
    link = tmp_path / "link"
    link.symlink_to(catalog)

    assert iter_geospatial_files(link) == [link / "data" / "points.geojson"]


def test_symlink_to_an_outside_directory_is_followed_once(tmp_path: Path) -> None:
    """A link to data outside the catalog still contributes its files."""
    outside = _points(tmp_path / "outside")
    catalog = tmp_path / "catalog"
    catalog.mkdir()
    (catalog / "linked").symlink_to(outside.parent)
    (catalog / "linked_again").symlink_to(outside.parent)

    assert iter_geospatial_files(catalog) == [catalog / "linked" / "points.geojson"]
