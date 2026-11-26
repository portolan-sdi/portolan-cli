"""Command parity for `info`: each target kind against its inspect function."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from portolan_cli.inspect import inspect_catalog, inspect_collection, inspect_file

if TYPE_CHECKING:
    from collections.abc import Callable

    from tests.catalog_invariants import RunCli
    from tests.parity.harness import Parity

pytestmark = pytest.mark.integration


@pytest.fixture
def points_catalog(
    build_conformant_catalog: Callable[..., Path],
    run_cli: RunCli,
    valid_points_geojson: Path,
) -> Callable[[Path], None]:
    """Build a catalog with one collection, `points`, that holds one GeoParquet item."""

    def build(root: Path) -> None:
        build_conformant_catalog(root)
        (root / "points").mkdir()
        shutil.copy(valid_points_geojson, root / "points" / "points.geojson")
        run_cli("add", "--portolan-dir", str(root), str(root / "points" / "points.geojson"))

    return build


def test_info_catalog(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`info` with no target reports the catalog at `--catalog`, which defaults to `.`."""
    parity("info", [], catalog=points_catalog, call=lambda root: inspect_catalog(Path()))


def test_info_collection(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`info <collection dir>` reports the collection."""
    parity(
        "info",
        ["points"],
        catalog=points_catalog,
        call=lambda root: inspect_collection(Path("points")),
    )


def test_info_file(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`info <file>` reports the file, with its version from the catalog at `.`."""
    target = Path("points") / "points.parquet"
    parity(
        "info",
        [str(target)],
        catalog=points_catalog,
        call=lambda root: inspect_file(target, catalog_root=Path()),
    )
