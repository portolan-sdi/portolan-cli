"""End-to-end Bands rendering on a real 4-band COG (issue #713).

`portolan add` writes the unified ``bands`` array onto the item's data asset.
`portolan readme` has to walk the collection's ``rel="item"`` links to reach it.
This test drives both commands over ``tests/fixtures/realdata/rapidai4eo-sample.tif``
and asserts the generated README carries a row per band.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from portolan_cli.cli import cli


@pytest.fixture
def raster_catalog(tmp_path: Path) -> Path:
    """Catalog with statistics enabled, so emitted bands carry a statistics dict."""
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()
    (catalog_root / "catalog.json").write_text(
        json.dumps(
            {
                "type": "Catalog",
                "stac_version": "1.1.0",
                "id": "test-catalog",
                "title": "Test Catalog",
                "description": "Test catalog",
                "links": [],
            }
        ),
        encoding="utf-8",
    )
    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text(
        yaml.dump({"version": 1, "statistics": {"enabled": True}}), encoding="utf-8"
    )
    # `portolan add` refuses a collection with no license (PRTLN-VAL004), so the
    # catalog needs one before the raster can land. Matches the fixture in
    # tests/integration/test_raster_bands_placement.py.
    (portolan_dir / "metadata.yaml").write_text('license: "CC-BY-4.0"\n', encoding="utf-8")
    (catalog_root / "imagery").mkdir()
    return catalog_root


def _add_and_render(catalog_root: Path, raster: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    """Add a raster, regenerate READMEs, and return the collection README text."""
    item_dir = catalog_root / "imagery" / "scene-a"
    item_dir.mkdir(parents=True)
    shutil.copy(raster, item_dir / "scene-a.tif")

    runner = CliRunner()
    added = runner.invoke(
        cli,
        ["add", "--portolan-dir", str(catalog_root), str(item_dir / "scene-a.tif")],
        catch_exceptions=False,
    )
    assert added.exit_code == 0, added.output

    # `portolan readme` resolves the catalog from the working directory.
    monkeypatch.chdir(catalog_root)
    rendered = runner.invoke(cli, ["readme"], catch_exceptions=False)
    assert rendered.exit_code == 0, rendered.output

    readme_path = catalog_root / "imagery" / "README.md"
    assert readme_path.exists(), f"Expected {readme_path}"
    return readme_path.read_text(encoding="utf-8")


@pytest.mark.integration
@pytest.mark.realdata
class TestReadmeBandsRealData:
    """The Bands section must render from the bands `add` actually wrote."""

    def test_readme_has_a_bands_section(
        self, raster_catalog: Path, rapidai4eo_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Before #713 this heading never appeared for any raster collection."""
        readme = _add_and_render(raster_catalog, rapidai4eo_path, monkeypatch)
        assert "## Bands" in readme

    def test_every_band_gets_a_row(
        self, raster_catalog: Path, rapidai4eo_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """rapidai4eo-sample.tif is 4-band int16, so four numbered rows."""
        readme = _add_and_render(raster_catalog, rapidai4eo_path, monkeypatch)
        block = readme.split("## Bands", 1)[1].split("\n## ", 1)[0]

        # The separator line starts with "|-", so this counts header + 4 bands.
        rows = [line for line in block.splitlines() if line.startswith("| ")]
        assert len(rows) == 5, block
        for i in range(1, 5):
            assert f"| {i} | band_{i} | int16 |" in block

    def test_band_statistics_reach_the_readme(
        self, raster_catalog: Path, rapidai4eo_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Statistics on the asset must survive into the rendered table."""
        readme = _add_and_render(raster_catalog, rapidai4eo_path, monkeypatch)
        block = readme.split("## Bands", 1)[1].split("\n## ", 1)[0]

        item = json.loads(
            (raster_catalog / "imagery" / "scene-a" / "scene-a.json").read_text(encoding="utf-8")
        )
        stats = item["assets"]["data"]["bands"][0]["statistics"]

        assert "| Min | Max | Mean | Std Dev |" in block
        assert f"| 1 | band_1 | int16 | {stats['minimum']} | {stats['maximum']} |" in block

    def test_cog_reaches_the_files_table(
        self, raster_catalog: Path, rapidai4eo_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The same dead item walk kept the data file out of Files entirely."""
        readme = _add_and_render(raster_catalog, rapidai4eo_path, monkeypatch)
        assert "| scene-a/scene-a.tif |" in readme
