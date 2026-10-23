"""End-to-end Bands rendering on a real 4-band COG (issue #713).

`portolan add` writes the unified ``bands`` array onto the item's data asset.
`portolan readme` has to walk the collection's ``rel="item"`` links to reach it.
This test drives both commands over ``tests/fixtures/realdata/rapidai4eo-sample.tif``
and asserts the generated README carries a row per band.
"""

from __future__ import annotations

import json
import shutil
from typing import TYPE_CHECKING

import pytest
import yaml

if TYPE_CHECKING:
    from pathlib import Path

    from tests.catalog_invariants import RunCli


@pytest.fixture
def raster_catalog(tmp_path: Path, run_cli: RunCli) -> Path:
    """A catalog from ``init`` with statistics on, so the bands carry statistics."""
    catalog_root = tmp_path / "catalog"
    run_cli("init", str(catalog_root), "--auto", "--license", "CC-BY-4.0")
    config_path = catalog_root / ".portolan" / "config.yaml"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    config["statistics"] = {"enabled": True}
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return catalog_root


def _add_and_render(catalog_root: Path, raster: Path, run_cli: RunCli) -> str:
    """Add a raster, regenerate READMEs, and return the collection README text."""
    item_dir = catalog_root / "imagery" / "scene-a"
    item_dir.mkdir(parents=True)
    shutil.copy(raster, item_dir / "scene-a.tif")

    run_cli("add", "--portolan-dir", str(catalog_root), str(item_dir / "scene-a.tif"))
    # `portolan readme` resolves the catalog from the working directory.
    run_cli("readme", cwd=catalog_root)

    readme_path = catalog_root / "imagery" / "README.md"
    assert readme_path.exists(), f"Expected {readme_path}"
    return readme_path.read_text(encoding="utf-8")


@pytest.mark.integration
@pytest.mark.realdata
class TestReadmeBandsRealData:
    """The Bands section must render from the bands `add` actually wrote."""

    def test_readme_has_a_bands_section(
        self, raster_catalog: Path, rapidai4eo_path: Path, run_cli: RunCli
    ) -> None:
        """Before #713 this heading never appeared for any raster collection."""
        readme = _add_and_render(raster_catalog, rapidai4eo_path, run_cli)
        assert "## Bands" in readme

    def test_every_band_gets_a_row(
        self, raster_catalog: Path, rapidai4eo_path: Path, run_cli: RunCli
    ) -> None:
        """rapidai4eo-sample.tif is 4-band int16, so four numbered rows."""
        readme = _add_and_render(raster_catalog, rapidai4eo_path, run_cli)
        block = readme.split("## Bands", 1)[1].split("\n## ", 1)[0]

        # The separator line starts with "|-", so this counts header + 4 bands.
        rows = [line for line in block.splitlines() if line.startswith("| ")]
        assert len(rows) == 5, block
        for i in range(1, 5):
            assert f"| {i} | band_{i} | int16 |" in block

    def test_band_statistics_reach_the_readme(
        self, raster_catalog: Path, rapidai4eo_path: Path, run_cli: RunCli
    ) -> None:
        """Statistics on the asset must survive into the rendered table."""
        readme = _add_and_render(raster_catalog, rapidai4eo_path, run_cli)
        block = readme.split("## Bands", 1)[1].split("\n## ", 1)[0]

        item = json.loads(
            (raster_catalog / "imagery" / "scene-a" / "scene-a.json").read_text(encoding="utf-8")
        )
        stats = item["assets"]["data"]["bands"][0]["statistics"]

        assert "| Min | Max | Mean | Std Dev |" in block
        assert f"| 1 | band_1 | int16 | {stats['minimum']} | {stats['maximum']} |" in block

    def test_cog_reaches_the_files_table(
        self, raster_catalog: Path, rapidai4eo_path: Path, run_cli: RunCli
    ) -> None:
        """The same dead item walk kept the data file out of Files entirely."""
        readme = _add_and_render(raster_catalog, rapidai4eo_path, run_cli)
        assert "| scene-a/scene-a.tif |" in readme
