"""Command parity for `metadata init`, `metadata validate`, and `readme`."""

from __future__ import annotations

import shutil
from typing import TYPE_CHECKING

import pytest

from portolan_cli.input_hardening import InputValidationError
from portolan_cli.metadata.yaml_tree import init_metadata_templates

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
    """Build a catalog with one collection, `points`, that holds one GeoParquet item."""

    def build(root: Path) -> None:
        build_conformant_catalog(root)
        (root / "points").mkdir()
        shutil.copy(valid_points_geojson, root / "points" / "points.geojson")
        run_cli("add", "--portolan-dir", str(root), str(root / "points" / "points.geojson"))

    return build


def test_metadata_init_recursive(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`metadata init` walks the catalog and reports what it created and skipped."""
    parity(
        "metadata init",
        ["--force"],
        catalog=points_catalog,
        call=lambda root: init_metadata_templates(root, force=True),
    )


def test_metadata_init_single(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`metadata init <path> --no-recursive` writes one template."""
    parity(
        "metadata init",
        ["points", "--no-recursive", "--force"],
        catalog=points_catalog,
        call=lambda root: init_metadata_templates(root, "points", force=True, recursive=False),
    )


def test_metadata_init_traversal(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """A PATH that escapes the catalog fails both sides the same way."""
    parity.error(
        "metadata init",
        ["../escape"],
        catalog=points_catalog,
        call=lambda root: init_metadata_templates(root, "../escape"),
        raises=InputValidationError,
    )
