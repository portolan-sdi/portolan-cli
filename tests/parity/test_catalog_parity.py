"""Command parity for the local catalog commands: list, status, init, rm, config, clean.

`info` has its own module, `test_info_parity.py`.
"""

from __future__ import annotations

import shutil
from typing import TYPE_CHECKING

import pytest

from portolan_cli.catalog_list import list_catalog_contents

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from tests.catalog_invariants import RunCli
    from tests.parity.harness import Parity

pytestmark = pytest.mark.integration


@pytest.fixture
def points_catalog(
    build_conformant_catalog: Callable[..., Path],
    run_cli: RunCli,
    valid_points_geojson: Path,
) -> Callable[[Path], None]:
    """Build a catalog with one collection, `points`, and one untracked file in it."""

    def build(root: Path) -> None:
        build_conformant_catalog(root)
        (root / "points").mkdir()
        shutil.copy(valid_points_geojson, root / "points" / "points.geojson")
        run_cli("add", "--portolan-dir", str(root), str(root / "points" / "points.geojson"))
        (root / "points" / "notes.txt").write_text("not tracked\n", encoding="utf-8")

    return build


@pytest.mark.parametrize(
    ("args", "tracked_only", "untracked_only"),
    [([], False, False), (["--tracked-only"], True, False), (["--untracked-only"], False, True)],
)
def test_list(
    parity: Parity,
    points_catalog: Callable[[Path], None],
    args: list[str],
    tracked_only: bool,
    untracked_only: bool,
) -> None:
    """`list` reports the files of each collection, with the status filter applied."""
    parity(
        "list",
        args,
        catalog=points_catalog,
        call=lambda root: list_catalog_contents(
            root, tracked_only=tracked_only, untracked_only=untracked_only
        ),
    )


def test_list_collection(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`list --collection` reports one collection."""
    parity(
        "list",
        ["--collection", "points"],
        catalog=points_catalog,
        call=lambda root: list_catalog_contents(root, collection_id="points"),
    )
