"""Command parity for `scan`: the command against `run_scan`."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from portolan_cli.scan.workflow import run_scan

if TYPE_CHECKING:
    from collections.abc import Callable

    from tests.parity.harness import Parity

pytestmark = pytest.mark.integration


@pytest.fixture
def raw_catalog(
    build_conformant_catalog: Callable[..., Path], valid_points_geojson: Path
) -> Callable[[Path], None]:
    """Build a catalog with an untracked GeoJSON file whose name holds a space."""

    def build(root: Path) -> None:
        build_conformant_catalog(root)
        (root / "points").mkdir()
        shutil.copy(valid_points_geojson, root / "points" / "my points.geojson")

    return build


def test_scan(parity: Parity, raw_catalog: Callable[[Path], None]) -> None:
    """`scan` reports the files, the issues, and the fix commands."""
    parity("scan", [], catalog=raw_catalog, call=lambda root: run_scan(Path()))


def test_scan_fix_with_suggestions(parity: Parity, raw_catalog: Callable[[Path], None]) -> None:
    """`scan --fix --suggest-collections` renames the file and suggests collections."""
    parity(
        "scan",
        [".", "--fix", "--suggest-collections"],
        catalog=raw_catalog,
        call=lambda root: run_scan(Path(), fix=True, suggest_collections=True),
    )


def test_scan_fix_dry_run(parity: Parity, raw_catalog: Callable[[Path], None]) -> None:
    """`scan --fix --dry-run` proposes the rename and applies nothing."""
    parity(
        "scan",
        ["--fix", "--dry-run"],
        catalog=raw_catalog,
        call=lambda root: run_scan(Path(), fix=True, dry_run=True),
    )


def test_scan_of_a_file_fails(parity: Parity, raw_catalog: Callable[[Path], None]) -> None:
    """`scan <file>` fails with the error that `run_scan` raises."""
    target = Path("points") / "my points.geojson"
    parity.error(
        "scan",
        [str(target)],
        catalog=raw_catalog,
        call=lambda root: run_scan(target),
        raises=NotADirectoryError,
    )
