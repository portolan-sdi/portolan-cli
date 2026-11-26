"""Command parity for `add` and `add-external`."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from portolan_cli.add import run_add
from portolan_cli.errors import MissingLicenseError
from portolan_cli.external import add_external

if TYPE_CHECKING:
    from collections.abc import Callable

    from tests.parity.harness import Parity

pytestmark = pytest.mark.integration

EXTERNAL_URL = "https://example.org/data/buildings.parquet"


@pytest.fixture
def points_source(
    build_conformant_catalog: Callable[..., Path], valid_points_geojson: Path
) -> Callable[[Path], None]:
    """Build a catalog with one untracked GeoJSON file in the collection dir `points`."""

    def build(root: Path) -> None:
        build_conformant_catalog(root)
        (root / "points").mkdir()
        shutil.copy(valid_points_geojson, root / "points" / "points.geojson")

    return build


def test_add_file(parity: Parity, points_source: Callable[[Path], None]) -> None:
    """`add <file>` tracks the file and reports the new item."""
    source = Path("points") / "points.geojson"
    parity(
        "add",
        [str(source)],
        catalog=points_source,
        call=lambda root: run_add([(root / source).resolve()], root),
    )


def test_add_unlicensed(parity: Parity, points_source: Callable[[Path], None]) -> None:
    """A catalog without a license fails both sides with the same error."""

    def unlicensed(root: Path) -> None:
        points_source(root)
        (root / ".portolan" / "metadata.yaml").unlink()

    source = Path("points") / "points.geojson"
    parity.error(
        "add",
        [str(source)],
        catalog=unlicensed,
        call=lambda root: run_add([(root / source).resolve()], root),
        raises=MissingLicenseError,
    )


def test_add_external(parity: Parity, build_conformant_catalog: Callable[..., Path]) -> None:
    """`add-external <url>` registers the remote asset as a collection."""
    parity(
        "add-external",
        [EXTERNAL_URL, "--license", "CC-BY-4.0"],
        catalog=build_conformant_catalog,
        call=lambda root: add_external(catalog_root=root, url=EXTERNAL_URL, license="CC-BY-4.0"),
    )


def test_add_external_exists(parity: Parity, build_conformant_catalog: Callable[..., Path]) -> None:
    """A collection that exists already fails both sides with the same error."""

    def with_collection(root: Path) -> None:
        build_conformant_catalog(root)
        add_external(catalog_root=root, url=EXTERNAL_URL, license="CC-BY-4.0")

    parity.error(
        "add-external",
        [EXTERNAL_URL, "--license", "CC-BY-4.0"],
        catalog=with_collection,
        call=lambda root: add_external(catalog_root=root, url=EXTERNAL_URL, license="CC-BY-4.0"),
        raises=FileExistsError,
    )
