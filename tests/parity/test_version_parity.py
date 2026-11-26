"""Command parity for `version current`, `version list`, and `version bump`."""

from __future__ import annotations

import shutil
from typing import TYPE_CHECKING

import pytest

from portolan_cli.version_ops import describe_current_version

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
    """Build a catalog with one collection, `points`, at version 1.0.0."""

    def build(root: Path) -> None:
        build_conformant_catalog(root)
        (root / "points").mkdir()
        shutil.copy(valid_points_geojson, root / "points" / "points.geojson")
        run_cli("add", "--portolan-dir", str(root), str(root / "points" / "points.geojson"))

    return build


def test_version_current(parity: Parity, points_catalog: Callable[[Path], None]) -> None:
    """`version current` reports the latest version. Each build has its own timestamp."""
    parity(
        "version current",
        ["points"],
        catalog=points_catalog,
        call=lambda root: describe_current_version("points", catalog_root=root),
        ignore=["created"],
    )


def test_version_current_missing_collection(
    parity: Parity, points_catalog: Callable[[Path], None]
) -> None:
    """A collection with no versions.json fails both sides with the same error."""
    parity.error(
        "version current",
        ["missing"],
        catalog=points_catalog,
        call=lambda root: describe_current_version("missing", catalog_root=root),
        raises=FileNotFoundError,
    )
