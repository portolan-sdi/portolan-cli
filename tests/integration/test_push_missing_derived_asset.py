"""The Issue #735 reproduction, end to end on real data.

An ``init`` plus an ``add`` on one COG records three assets in version 1.0.0.
They are the raster the user supplied, the ``.thumb.jpg`` Portolan drew from it,
and the ``items.parquet`` mirror. A user who deletes either derived artifact
once made push raise ``FileNotFoundError``. The rest of the catalog was fine.

The roles here are the ones a real ``add`` emits, not a fixture's guess. That is
what the classifier reads, per specs/portolan/core.md, section Assets.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.derived_assets import resolve_asset_roles
from portolan_cli.sync.push import _get_assets_to_upload

pytestmark = [pytest.mark.integration, pytest.mark.realdata]


@pytest.fixture(scope="module")
def raster_catalog(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A catalog with one added COG. Built once for this module."""
    catalog_root = tmp_path_factory.mktemp("issue-735")
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["init", str(catalog_root), "--auto", "--license", "CC-BY-4.0", "--title", "Repro 735"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"Init failed: {result.output}"

    fixture = Path(__file__).parent.parent / "fixtures" / "realdata" / "rapidai4eo-sample.tif"
    scene_dir = catalog_root / "imagery" / "scene1"
    scene_dir.mkdir(parents=True)
    shutil.copy(fixture, scene_dir / "scene1.tif")

    result = runner.invoke(
        cli,
        [
            "add",
            "--portolan-dir",
            str(catalog_root),
            str(catalog_root / "imagery"),
            "--datetime",
            "2024-01-01",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"Add failed: {result.output}"
    return catalog_root


@pytest.fixture
def catalog(raster_catalog: Path, tmp_path: Path) -> Path:
    """A writable copy of the built catalog, one per test."""
    destination = tmp_path / "catalog"
    shutil.copytree(raster_catalog, destination)
    return destination


def _push_assets(catalog_root: Path) -> list[Path]:
    """Drive the raise site the way ``push`` does, without a remote."""
    versions_data = json.loads((catalog_root / "imagery" / "versions.json").read_text())
    return _get_assets_to_upload(catalog_root, versions_data, ["1.0.0"], None)


def test_add_records_both_derived_artifacts(catalog: Path) -> None:
    """The premise of the bug: version 1.0.0 records artifacts add generated."""
    versions_data = json.loads((catalog / "imagery" / "versions.json").read_text())
    hrefs = {asset["href"] for asset in versions_data["versions"][0]["assets"].values()}

    assert "imagery/scene1/scene1.tif" in hrefs
    assert "imagery/scene1/scene1.thumb.jpg" in hrefs
    assert "imagery/items.parquet" in hrefs


def test_add_emits_the_roles_the_classifier_reads(catalog: Path) -> None:
    """The emitted STAC gives each tracked file its spec role."""
    assert resolve_asset_roles(catalog, "imagery/scene1/scene1.tif") == {"data"}
    assert resolve_asset_roles(catalog, "imagery/scene1/scene1.thumb.jpg") == {"thumbnail"}
    assert resolve_asset_roles(catalog, "imagery/items.parquet") == {"collection-mirror"}


def test_missing_cog_thumbnail_is_skipped(
    catalog: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The exact repro: delete the COG thumbnail, then push the collection."""
    (catalog / "imagery" / "scene1" / "scene1.thumb.jpg").unlink()

    hrefs = [path.relative_to(catalog).as_posix() for path in _push_assets(catalog)]

    # Sorted because the order is not a contract. push.py iterates the assets
    # dict in versions.json, so the order follows what add recorded, which
    # follows filesystem enumeration and differs per platform.
    assert sorted(hrefs) == ["imagery/items.parquet", "imagery/scene1/scene1.tif"]
    assert "imagery/scene1/scene1.thumb.jpg" in capsys.readouterr().err


def test_missing_items_parquet_is_skipped(
    catalog: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The second path in the issue, where the item mirror is gone."""
    (catalog / "imagery" / "items.parquet").unlink()

    hrefs = [path.relative_to(catalog).as_posix() for path in _push_assets(catalog)]

    assert sorted(hrefs) == [
        "imagery/scene1/scene1.thumb.jpg",
        "imagery/scene1/scene1.tif",
    ]
    assert "imagery/items.parquet" in capsys.readouterr().err


def test_missing_source_raster_still_raises(catalog: Path) -> None:
    """The user's COG is not derived, so its absence stays a hard failure."""
    (catalog / "imagery" / "scene1" / "scene1.tif").unlink()

    with pytest.raises(FileNotFoundError, match=r"imagery/scene1/scene1\.tif"):
        _push_assets(catalog)
