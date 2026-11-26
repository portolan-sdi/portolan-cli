"""Command parity for the local catalog commands: list, status, init, rm, config, clean.

`info` has its own module, `test_info_parity.py`.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from portolan_cli.catalog_list import list_catalog_contents
from portolan_cli.errors import CatalogAlreadyExistsError
from portolan_cli.inspect import initialize_catalog
from portolan_cli.remove import remove_path
from portolan_cli.status import get_catalog_status

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


def _empty_directory_with_logo(root: Path) -> None:
    root.mkdir()
    (root / "brand.svg").write_text(
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1 1"/>\n', encoding="utf-8"
    )


def test_init(parity: Parity) -> None:
    """`init --auto` creates the catalog and reports its id, logo, and warnings."""
    parity(
        "init",
        ["--auto", "--id", "parity", "--license", "CC-BY-4.0", "--logo", "brand.svg"],
        catalog=_empty_directory_with_logo,
        call=lambda root: initialize_catalog(
            Path(), catalog_id="parity", license_id="CC-BY-4.0", logo="brand.svg"
        ),
    )


def test_init_derived_id(parity: Parity) -> None:
    """`init` with no --id derives the id from the directory name."""
    parity(
        "init",
        ["--auto", "--license", "CC-BY-4.0"],
        catalog=lambda root: root.mkdir(),
        call=lambda root: initialize_catalog(Path(), license_id="CC-BY-4.0"),
    )


def test_init_existing_catalog(
    parity: Parity, build_conformant_catalog: Callable[..., Path]
) -> None:
    """`init` in a Portolan catalog reports CatalogAlreadyExistsError."""
    parity.error(
        "init",
        ["--auto", "--license", "CC-BY-4.0"],
        catalog=build_conformant_catalog,
        call=lambda root: initialize_catalog(Path(), license_id="CC-BY-4.0"),
        raises=CatalogAlreadyExistsError,
    )


@pytest.mark.parametrize(
    ("args", "keep", "dry_run"),
    [(["--keep"], True, False), (["--dry-run"], False, True), (["--force"], False, False)],
)
def test_rm(
    parity: Parity,
    points_catalog: Callable[[Path], None],
    args: list[str],
    keep: bool,
    dry_run: bool,
) -> None:
    """`rm` untracks the file, and deletes it unless --keep or --dry-run is given."""
    target = Path("points") / "points.geojson"
    parity(
        "rm",
        [*args, str(target)],
        catalog=points_catalog,
        call=lambda root: remove_path(target, catalog_root=root, keep=keep, dry_run=dry_run),
    )


def test_rm_corrupt_versions(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`rm` reports the ValueError of a versions.json that is not JSON."""

    def catalog(root: Path) -> None:
        points_catalog(root)
        (root / "points" / "versions.json").write_text("{", encoding="utf-8")

    target = Path("points") / "points.geojson"
    parity.error(
        "rm",
        ["--keep", str(target)],
        catalog=catalog,
        call=lambda root: remove_path(target, catalog_root=root, keep=True),
        raises=ValueError,
    )
