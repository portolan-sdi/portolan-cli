"""Command parity for `stac-geoparquet`: one collection, every collection, and a dry run."""

from __future__ import annotations

import shutil
from typing import TYPE_CHECKING

import pytest

from portolan_cli.stac_parquet import generate_stac_geoparquet

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from tests.catalog_invariants import RunCli
    from tests.parity.harness import Parity

pytestmark = pytest.mark.integration


@pytest.fixture
def raster_catalog(
    build_conformant_catalog: Callable[..., Path],
    run_cli: RunCli,
    valid_singleband_cog: Path,
) -> Callable[[Path], None]:
    """Build a catalog with one collection, `rasters`, that holds one COG item."""

    def build(root: Path) -> None:
        build_conformant_catalog(root)
        item_dir = root / "rasters" / "scene"
        item_dir.mkdir(parents=True)
        shutil.copy(valid_singleband_cog, item_dir / "scene.tif")
        run_cli("add", "--portolan-dir", str(root), str(item_dir / "scene.tif"))

    return build


def test_stac_geoparquet_collection(parity: Parity, raster_catalog: Callable[[Path], None]) -> None:
    """`stac-geoparquet -c` writes the mirror of one collection."""
    parity(
        "stac-geoparquet",
        ["-c", "rasters"],
        catalog=raster_catalog,
        call=lambda root: generate_stac_geoparquet(root, collection="rasters"),
    )


def test_stac_geoparquet_all(parity: Parity, raster_catalog: Callable[[Path], None]) -> None:
    """`stac-geoparquet` with no collection writes the mirror of each item-bearing one."""
    parity(
        "stac-geoparquet",
        [],
        catalog=raster_catalog,
        call=lambda root: generate_stac_geoparquet(root),
    )


def test_stac_geoparquet_dry_run(parity: Parity, raster_catalog: Callable[[Path], None]) -> None:
    """`stac-geoparquet --dry-run` reports the mirror and writes nothing."""
    parity(
        "stac-geoparquet",
        ["-c", "rasters", "--dry-run"],
        catalog=raster_catalog,
        call=lambda root: generate_stac_geoparquet(root, collection="rasters", dry_run=True),
    )
