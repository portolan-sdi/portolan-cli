"""The terminal output of `portolan add`."""

from __future__ import annotations

import json
import shutil
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.integration

REWRITE = "→ Rewriting open-buildings.parquet (142.4KB): it carries no bbox covering column\n"
ADDED = "✓ Added 1 file to 1 collection\n"
TIPPECANOE = (
    "✗ [PRTLN-PMT002] tippecanoe not found in PATH. PMTiles generation requires tippecanoe. "
    "Install: brew install tippecanoe (macOS) or apt install tippecanoe (Ubuntu)\n"
)


@pytest.fixture
def buildings(conformant_catalog: Path, open_buildings_path: Path) -> Path:
    """A catalog with a GeoParquet file that has no bbox covering column, so add rewrites it."""
    (conformant_catalog / "buildings").mkdir()
    shutil.copy(open_buildings_path, conformant_catalog / "buildings")
    return conformant_catalog


@pytest.fixture
def imagery(conformant_catalog: Path, valid_rgb_cog: Path, valid_points_geojson: Path) -> Path:
    """A catalog with one raster item and one file that ignored_files excludes."""
    root = conformant_catalog
    (root / "imagery" / "scene").mkdir(parents=True)
    shutil.copy(valid_rgb_cog, root / "imagery" / "scene" / "scene.tif")
    shutil.copy(valid_points_geojson, root / "imagery" / "ignored.geojson")
    config = root / ".portolan" / "config.yaml"
    config.write_text(config.read_text() + 'ignored_files:\n  - "ignored*"\n')
    return root


@pytest.fixture
def points(conformant_catalog: Path, valid_points_geojson: Path) -> Path:
    """A catalog with one untracked GeoJSON file in the collection dir `points`."""
    (conformant_catalog / "points").mkdir()
    shutil.copy(valid_points_geojson, conformant_catalog / "points" / "points.geojson")
    return conformant_catalog


def _invoke(*args: str) -> tuple[int, str, str, BaseException | None]:
    result = CliRunner().invoke(cli, list(args))
    error = None if isinstance(result.exception, SystemExit) else result.exception
    return result.exit_code, result.stdout, result.stderr, error


@pytest.mark.realdata
@pytest.mark.parametrize(
    ("flags", "stdout", "stderr"),
    [
        ([], REWRITE + ADDED, ""),
        (["--quiet"], REWRITE + ADDED, ""),
        (
            ["-v"],
            "→ Adding: open-buildings.parquet\n"
            + REWRITE
            + "  Rendered thumbnail for buildings: open-buildings.thumb.jpg\n"
            "→ Adding 1 file to buildings\n"
            "✓   + open-buildings\n" + ADDED,
            "",
        ),
    ],
    ids=["default", "quiet", "verbose"],
)
def test_the_rewrite_notice_in_text_mode(
    buildings: Path, flags: list[str], stdout: str, stderr: str
) -> None:
    """The rewrite notice is an arrow line on stdout, also under --quiet."""
    root = buildings
    root_flags = [f for f in flags if f == "--quiet"]
    add_flags = [f for f in flags if f != "--quiet"]
    code, out, err, error = _invoke(
        *root_flags, "add", *add_flags, "--portolan-dir", str(root), str(root / "buildings")
    )

    assert (code, error) == (0, None)
    assert (out, err) == (stdout, stderr)


@pytest.mark.realdata
def test_the_rewrite_notice_in_json_mode(buildings: Path) -> None:
    """In JSON mode the notice goes to stderr and stdout holds only the envelope."""
    root = buildings
    code, out, err, error = _invoke(
        "add", "--json", "--portolan-dir", str(root), str(root / "buildings")
    )

    assert (code, error, err) == (0, None, REWRITE)
    assert json.loads(out)["data"]["added"][0]["item_id"] == "open-buildings"


def test_a_side_step_error_keeps_the_lines_before_it(imagery: Path) -> None:
    """A thumbnail error raises after the skip warning and the mirror line have printed."""
    root = imagery
    with patch(
        "portolan_cli.collection_thumbnail.ensure_collection_thumbnails",
        side_effect=RuntimeError("thumbnail broke"),
    ):
        code, out, err, error = _invoke(
            "add", "-v", "--stac-geoparquet", "--portolan-dir", str(root), str(root)
        )

    assert code == 1
    assert repr(error) == "RuntimeError('thumbnail broke')"
    assert out == "→ Adding: scene.tif\n→ Generated items.parquet for 'imagery'\n"
    assert err == "⚠ Skipping ignored.geojson: it matches ignored_files pattern 'ignored*'\n"


@pytest.mark.parametrize("json_flag", [[], ["--json"]], ids=["text", "json"])
def test_a_side_step_value_error_is_no_add_error(imagery: Path, json_flag: list[str]) -> None:
    """Only an error from adding the files becomes an error envelope. A mirror error raises."""
    root = imagery
    with patch(
        "portolan_cli.stac_parquet.generate_items_parquet",
        side_effect=ValueError("mirror broke"),
    ):
        code, out, _err, error = _invoke(
            "add", *json_flag, "--stac-geoparquet", "--portolan-dir", str(root), str(root)
        )

    assert code == 1
    assert repr(error) == "ValueError('mirror broke')"
    if json_flag:
        assert out == ""


def test_an_add_files_value_error_is_an_add_error(points: Path) -> None:
    """An error from adding the files is an error envelope that names the path."""
    root = points
    source = root / "points" / "points.geojson"
    with patch("portolan_cli.add.add_files", side_effect=ValueError("add broke")):
        code, out, err, error = _invoke("add", "--json", "--portolan-dir", str(root), str(source))

    assert (code, error, err) == (1, None, "")
    assert json.loads(out)["errors"] == [
        {"type": "ValueError", "message": f"{source.resolve()}: add broke"}
    ]


@pytest.mark.parametrize("flag", ["--pmtiles", "--force-pmtiles"])
@pytest.mark.parametrize(
    ("json_flag", "stderr"), [([], TIPPECANOE), (["--json"], "")], ids=["text", "json"]
)
def test_an_explicit_pmtiles_request_that_fails_exits_1(
    points: Path, flag: str, json_flag: list[str], stderr: str
) -> None:
    """A missing tippecanoe fails an explicit request. JSON mode prints no envelope."""
    from portolan_cli.viz.pmtiles import TippecanoeNotFoundError

    root = points
    with patch(
        "portolan_cli.viz.pmtiles.generate_pmtiles_for_collection",
        side_effect=TippecanoeNotFoundError(),
    ):
        code, out, err, error = _invoke(
            "add", flag, *json_flag, "--portolan-dir", str(root), str(root / "points")
        )

    assert (code, error) == (1, None)
    assert (out, err) == ("", stderr)
