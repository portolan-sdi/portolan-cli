"""Command parity for `stac-geoparquet`: one collection, every collection, and a dry run.
The harness removes the command catalog, so each test reruns the command to check its output.
"""

from __future__ import annotations

import json
import shutil
from typing import TYPE_CHECKING

import pytest

from portolan_cli.stac_parquet import generate_stac_geoparquet
from tests.parity.snapshot import tree

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from portolan_cli.stac_parquet import StacGeoparquetResult
    from tests.catalog_invariants import RunCli
    from tests.parity.harness import Parity

pytestmark = pytest.mark.integration


def assert_no_mirror(root: Path) -> None:
    """Fail unless ``rasters`` has no mirror file, no mirror asset, and no mirror version."""
    collection = json.loads((root / "rasters" / "collection.json").read_text())
    versions = json.loads((root / "rasters" / "versions.json").read_text())
    assert not (root / "rasters" / "items.parquet").exists()
    assert all("collection-mirror" not in a.get("roles", []) for a in collection["assets"].values())
    assert "items.parquet" not in versions["versions"][-1]["assets"]


def assert_mirror(root: Path) -> None:
    """Fail unless ``rasters`` has the mirror file, registers it, and tracks it."""
    collection = json.loads((root / "rasters" / "collection.json").read_text())
    versions = json.loads((root / "rasters" / "versions.json").read_text())
    assert (root / "rasters" / "items.parquet").is_file()
    mirrors = [a for a in collection["assets"].values() if "collection-mirror" in a["roles"]]
    assert [a["href"].removeprefix("./") for a in mirrors] == ["items.parquet"]
    assert "items.parquet" in versions["versions"][-1]["assets"]


@pytest.fixture
def raster_catalog(
    build_conformant_catalog: Callable[..., Path],
    run_cli: RunCli,
    valid_singleband_cog: Path,
) -> Callable[[Path], None]:
    """Build a catalog with one collection, `rasters`, that holds one COG item and no mirror."""

    def build(root: Path) -> None:
        build_conformant_catalog(root)
        run_cli("config", "set", "parquet.enabled", "false", cwd=root)
        item_dir = root / "rasters" / "scene"
        item_dir.mkdir(parents=True)
        shutil.copy(valid_singleband_cog, item_dir / "scene.tif")
        run_cli("add", "--portolan-dir", str(root), str(item_dir / "scene.tif"))
        assert_no_mirror(root)

    return build


def mirrored(root: Path, result: StacGeoparquetResult) -> StacGeoparquetResult:
    """Check the library side wrote the mirror, then return its result."""
    assert_mirror(root)
    return result


@pytest.mark.parametrize(
    ("args", "collection"), [(["-c", "rasters"], "rasters"), ([], None)], ids=["one", "all"]
)
def test_stac_geoparquet_writes_the_mirror(
    parity: Parity,
    raster_catalog: Callable[[Path], None],
    run_cli: RunCli,
    tmp_path: Path,
    args: list[str],
    collection: str | None,
) -> None:
    """`stac-geoparquet` writes, registers, and tracks the mirror of the collection."""
    parity(
        "stac-geoparquet",
        args,
        catalog=raster_catalog,
        call=lambda root: mirrored(root, generate_stac_geoparquet(root, collection=collection)),
    )

    command_root = tmp_path / "command-side"
    raster_catalog(command_root)
    run_cli("stac-geoparquet", *args, cwd=command_root)
    assert_mirror(command_root)


def test_stac_geoparquet_dry_run(
    parity: Parity, raster_catalog: Callable[[Path], None], run_cli: RunCli, tmp_path: Path
) -> None:
    """`stac-geoparquet --dry-run` reports the mirror and writes nothing."""

    def dry_run(root: Path) -> StacGeoparquetResult:
        before = tree(root)
        result = generate_stac_geoparquet(root, collection="rasters", dry_run=True)
        assert tree(root) == before
        return result

    parity(
        "stac-geoparquet",
        ["-c", "rasters", "--dry-run"],
        catalog=raster_catalog,
        call=dry_run,
    )

    command_root = tmp_path / "command-side"
    raster_catalog(command_root)
    before = tree(command_root)
    run_cli("stac-geoparquet", "-c", "rasters", "--dry-run", cwd=command_root)
    assert tree(command_root) == before
