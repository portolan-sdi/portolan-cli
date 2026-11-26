"""`run_check_workflow` reports each fix as it happens, and prints nothing itself."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest

from portolan_cli.validation.workflow import run_check_workflow

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

pytestmark = pytest.mark.integration


def test_fix_reports_reach_the_callback_before_a_failed_commit(
    build_conformant_catalog: Callable[..., Path], tmp_path: Path
) -> None:
    """A commit that raises leaves the user the report of what the fix already wrote."""
    root = build_conformant_catalog(tmp_path / "catalog")
    catalog_file = root / "catalog.json"
    catalog = json.loads(catalog_file.read_text())
    del catalog["title"]
    catalog_file.write_text(json.dumps(catalog, indent=2))
    reports: list[tuple[Any, Any]] = []

    with (
        patch("portolan_cli.version_ops.commit_run", side_effect=OSError("disk full")),
        pytest.raises(OSError, match="disk full"),
    ):
        run_check_workflow(
            root,
            fix=True,
            on_fix_report=lambda report, conversion: reports.append((report, conversion)),
        )

    # The freshness and conversion pass first, then the registry, as the CLI printed them.
    assert len(reports) == 2
    fixer_report, conversion = reports[1]
    assert conversion is None
    assert [(r.file_path, r.success) for r in fixer_report.results] == [(catalog_file, True)]


def test_geo_asset_fix_prints_nothing(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """A shapefile with Latin-1 records and no .cpg gets a sidecar, and the library stays quiet."""
    import geopandas as gpd
    from shapely.geometry import Point

    shp = tmp_path / "roads.shp"
    frame = gpd.GeoDataFrame({"name": ["Zürich"]}, geometry=[Point(8.5, 47.4)], crs="EPSG:4326")
    frame.to_file(shp, encoding="latin1")
    shp.with_suffix(".cpg").unlink(missing_ok=True)
    capsys.readouterr()

    run_check_workflow(tmp_path, fix=True, geo_assets=True, workers=1)

    assert shp.with_suffix(".cpg").read_text() == "ISO-8859-1"
    printed = capsys.readouterr()
    assert (printed.out, printed.err) == ("", "")


def test_check_fix_command_prints_the_sidecar_notice(tmp_path: Path) -> None:
    """The command prints the encoding sidecar notice that the library reports."""
    import geopandas as gpd
    from click.testing import CliRunner
    from shapely.geometry import Point

    from portolan_cli.cli import cli

    shp = tmp_path / "roads.shp"
    frame = gpd.GeoDataFrame({"name": ["Zürich"]}, geometry=[Point(8.5, 47.4)], crs="EPSG:4326")
    frame.to_file(shp, encoding="latin1")
    shp.with_suffix(".cpg").unlink(missing_ok=True)

    result = CliRunner().invoke(cli, ["check", str(tmp_path), "--fix", "--geo-assets"])

    assert shp.with_suffix(".cpg").read_text() == "ISO-8859-1"
    assert "Encoding sidecar roads.cpg written: DBF records are not valid UTF-8" in result.output
