"""Commands that reach the emitting modules print their notices as on f7f8214 (#1133).

``sync`` converts files in its check step. ``rm`` and ``stac-geoparquet``
update the catalog index, which warns about a corrupted file. On f7f8214 each
notice printed through ``portolan_cli.output``, also under ``--quiet``. Without
a report scope the notice goes to the log, which ``--quiet`` hides.
"""

from __future__ import annotations

import shutil
from typing import TYPE_CHECKING

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.integration

CORRUPTED = "⚠ Catalog versions.json is corrupted: "


def test_the_check_step_of_sync_reports_each_conversion_notice(tmp_path: Path) -> None:
    """The sidecar notice of the conversion reaches the report of sync."""
    import geopandas
    from shapely.geometry import Point

    from portolan_cli.sync.core import _step_check

    shp = tmp_path / "roads.shp"
    frame = geopandas.GeoDataFrame(
        {"name": ["Zürich"]}, geometry=[Point(8.5, 47.4)], crs="EPSG:4326"
    )
    frame.to_file(shp, encoding="latin1")
    shp.with_suffix(".cpg").unlink(missing_ok=True)
    lines: list[tuple[str, str]] = []

    _step_check(tmp_path, fix=True, dry_run=False, report=lambda lv, t: lines.append((lv, t)))

    assert lines[:3] == [
        ("info", "Checking cloud-native status..."),
        ("info", "Encoding sidecar roads.cpg written: DBF records are not valid UTF-8"),
        ("success", "Converted 1 file(s) to cloud-native format"),
    ]


@pytest.fixture
def mirrored(conformant_catalog: Path, valid_rgb_cog: Path) -> Path:
    """A catalog with two raster items, an items.parquet mirror, and a corrupted index."""
    root = conformant_catalog
    (root / "imagery" / "scene").mkdir(parents=True)
    shutil.copy(valid_rgb_cog, root / "imagery" / "scene" / "scene.tif")
    # rm refreshes the mirror only while the collection keeps an item.
    (root / "imagery" / "other").mkdir()
    shutil.copy(valid_rgb_cog, root / "imagery" / "other" / "other.tif")
    result = CliRunner().invoke(
        cli, ["add", "--stac-geoparquet", "--portolan-dir", str(root), str(root)]
    )
    assert result.exit_code == 0, result.output
    assert (root / "imagery" / "items.parquet").exists()
    (root / "versions.json").write_text("{not json")
    return root


def test_stac_geoparquet_warns_about_a_corrupted_index_under_quiet(mirrored: Path) -> None:
    result = CliRunner().invoke(
        cli, ["--quiet", "stac-geoparquet", "-c", "imagery", "--catalog", str(mirrored)]
    )

    assert [line for line in result.stderr.splitlines() if line.startswith(CORRUPTED)] == [
        f"{CORRUPTED}Expecting property name enclosed in double quotes: line 1 column 2 "
        f"(char 1). File: {mirrored / 'versions.json'}. Fix manually or delete to reinitialize."
    ]


def test_rm_warns_about_a_corrupted_index_under_quiet(mirrored: Path) -> None:
    target = mirrored / "imagery" / "scene" / "scene.tif"
    result = CliRunner().invoke(
        cli, ["--quiet", "rm", "--force", "--portolan-dir", str(mirrored), str(target)]
    )

    assert [line for line in result.stderr.splitlines() if line.startswith(CORRUPTED)] == [
        f"{CORRUPTED}Expecting property name enclosed in double quotes: line 1 column 2 "
        f"(char 1). File: {mirrored / 'versions.json'}. Fix manually or delete to reinitialize."
    ]
