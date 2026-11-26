"""Command parity for the local catalog commands: list, status, init, rm, config, clean.

`info` has its own module, `test_info_parity.py`.
"""

from __future__ import annotations

import shutil
from typing import TYPE_CHECKING

import pytest

from portolan_cli.catalog_list import list_catalog_contents
from portolan_cli.status import get_catalog_status

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


@pytest.mark.parametrize(
    ("args", "collection", "offline"),
    [([], None, False), (["--offline"], None, True), (["-c", "points"], "points", False)],
)
def test_status(
    parity: Parity,
    points_catalog: Callable[[Path], None],
    args: list[str],
    collection: str | None,
    offline: bool,
) -> None:
    """`status` reports each collection. The catalog has no remote, so none is read."""
    parity(
        "status",
        args,
        catalog=points_catalog,
        call=lambda root: get_catalog_status(root, collection, offline=offline),
    )


def test_status_no_collections(
    parity: Parity, build_conformant_catalog: Callable[..., Path]
) -> None:
    """`status` on a catalog with no collection reports an empty list."""
    parity(
        "status",
        [],
        catalog=build_conformant_catalog,
        call=lambda root: get_catalog_status(root),
    )
