"""The example plugin works through the public plugin API alone (#947).

These tests need the example plugin installed:

    uv pip install ./tests/plugins/portolan_example
    uv run --no-sync pytest tests/plugins -m plugin
"""

from __future__ import annotations

import importlib
import json
import shutil
from importlib.metadata import entry_points
from typing import TYPE_CHECKING, Any

import pytest
from click.testing import CliRunner

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

pytest.importorskip("portolan_example", reason="the example plugin is not installed")

pytestmark = pytest.mark.plugin

SCI = "https://stac-extensions.github.io/scientific/v1.0.0/schema.json"


@pytest.fixture(autouse=True)
def fresh_manager(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Build the plugin manager from the real installed entry points."""
    from portolan_cli.plugins import manager

    monkeypatch.delenv(manager.ENV_VAR, raising=False)
    manager.reset_plugin_manager()
    yield
    manager.reset_plugin_manager()


def _cli() -> Any:
    return importlib.import_module("portolan_cli.cli").cli


def _read(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return data


def test_entry_points_are_installed() -> None:
    hooks = {ep.name: ep.value for ep in entry_points(group="portolan")}
    commands = {ep.name: ep.value for ep in entry_points(group="portolan.commands")}

    assert hooks["example"] == "portolan_example.hooks"
    assert commands["example"] == "portolan_example.cli:group"


def test_help_lists_the_plugin_command() -> None:
    result = CliRunner().invoke(_cli(), ["--help"])

    assert result.exit_code == 0
    assert "(plugin: portolan-example)" in result.output


def test_plugin_command_runs(tmp_path: Path) -> None:
    help_result = CliRunner().invoke(_cli(), ["example", "--help"])
    run_result = CliRunner().invoke(
        _cli(), ["example", "versions", "roads", "--catalog", str(tmp_path)]
    )

    assert help_result.exit_code == 0
    assert "Commands from the example plugin." in help_result.output
    assert run_result.exit_code == 0
    assert run_result.output == "→ No versions for roads.\n"


def test_none_hides_the_plugin(monkeypatch: pytest.MonkeyPatch) -> None:
    from portolan_cli.plugins import manager

    monkeypatch.setenv(manager.ENV_VAR, "none")

    result = CliRunner().invoke(_cli(), ["--help"])

    assert "example" not in result.output


# --8<-- [start:check-backend]
def test_backend_conforms(tmp_path: Path) -> None:
    from portolan_example.backend import ExampleBackend

    from portolan_cli.api import get_backend
    from portolan_cli.testing import check_backend

    check_backend(ExampleBackend, tmp_path / "direct")

    assert type(get_backend("example", catalog_root=tmp_path)) is ExampleBackend
    # --8<-- [end:check-backend]


def test_add_runs_every_hook(conformant_catalog: Path, fixtures_dir: Path) -> None:
    """`add` with `backend: example` enriches the STAC and versions through the plugin."""
    from portolan_cli.add import add_files

    config = conformant_catalog / ".portolan" / "config.yaml"
    config.write_text(config.read_text(encoding="utf-8") + "backend: example\n")
    item_dir = conformant_catalog / "imagery" / "scene-a"
    item_dir.mkdir(parents=True)
    raster = item_dir / "scene-a.tif"
    shutil.copy(fixtures_dir / "raster" / "valid" / "singleband.tif", raster)

    _, _, failures = add_files(
        paths=[raster], catalog_root=conformant_catalog, collection_id="imagery"
    )

    assert failures == []
    item = _read(item_dir / "scene-a.json")
    collection = _read(conformant_catalog / "imagery" / "collection.json")
    assert item["properties"]["sci:doi"] == "10.1000/portolan-example/imagery/scene-a"
    assert SCI in item["stac_extensions"]
    assert collection["sci:citation"].startswith("Example Agency (2026).")
    result = CliRunner().invoke(
        _cli(), ["example", "versions", "imagery", "--catalog", str(conformant_catalog)]
    )
    assert result.output.startswith("✓ 1.0.0  ")


_XYZ = "-70.0 -30.0 12.5\n-69.5 -29.8 14.0\n-69.9 -29.9 13.1\n"


def test_xyz_handler_conforms(tmp_path: Path) -> None:
    from portolan_example.formats import XyzHandler

    from portolan_cli.testing import check_format_handler

    sample = tmp_path / "points.xyz"
    sample.write_text(_XYZ, encoding="utf-8")

    check_format_handler(XyzHandler(), sample, tmp_path)


def test_add_converts_an_xyz_file(conformant_catalog: Path) -> None:
    """`add` routes `.xyz` to the plugin handler and tracks the GeoParquet."""
    from portolan_cli.add import add_files

    collection_dir = conformant_catalog / "heights"
    collection_dir.mkdir()
    source = collection_dir / "survey.xyz"
    source.write_text(_XYZ, encoding="utf-8")

    _, _, failures = add_files(
        paths=[collection_dir], catalog_root=conformant_catalog, collection_id="heights"
    )

    assert failures == []
    assert (collection_dir / "survey.parquet").exists()
    collection = _read(collection_dir / "collection.json")
    assert collection["extent"]["spatial"]["bbox"][0] == [-70.0, -30.0, -69.5, -29.8]
    versions = _read(collection_dir / "versions.json")
    assert "survey.parquet" in versions["versions"][-1]["assets"]


def test_check_fix_converts_an_xyz_file(conformant_catalog: Path) -> None:
    """`check --fix --geo-assets` converts a `.xyz` file with the plugin handler."""
    collection_dir = conformant_catalog / "heights"
    collection_dir.mkdir()
    (collection_dir / "survey.xyz").write_text(_XYZ, encoding="utf-8")

    result = CliRunner().invoke(_cli(), ["check", str(conformant_catalog), "--fix", "--geo-assets"])

    assert "Converted 1 file(s)" in result.output
    assert (collection_dir / "survey.parquet").exists()


@pytest.fixture
def geojson_dir(tmp_path: Path, fixtures_dir: Path) -> Path:
    """A directory with two GeoJSON layers for the example extractor."""
    source = tmp_path / "source"
    source.mkdir()
    for name in ("points", "lines"):
        shutil.copy(
            fixtures_dir / "vector" / "valid" / f"{name}.geojson", source / f"{name}.geojson"
        )
    return source


def test_extract_help_lists_the_example_extractor() -> None:
    result = CliRunner().invoke(_cli(), ["extract", "--help"])

    assert result.exit_code == 0
    assert "  example  Layers from a directory of GeoJSON files.\n" in result.output


def test_example_extractor_builds_a_catalog(geojson_dir: Path, tmp_path: Path) -> None:
    """`portolan extract example` runs the LayerSource through the shared driver."""
    out = tmp_path / "catalog"

    result = CliRunner().invoke(
        _cli(),
        ["extract", "example", str(geojson_dir), str(out), "--auto", "--license", "CC0-1.0"],
    )

    assert result.exit_code == 0, result.output
    assert "Extracted 2/2 layers" in result.output
    collection = _read(out / "points" / "collection.json")
    # The source link is a file:// URI. A local path names the machine that ran
    # the command, so the catalog publishes no via link for it (issue #939).
    vias = [link["href"] for link in collection["links"] if link["rel"] == "via"]
    assert vias == []
    assert collection["sci:citation"].startswith("Example Agency (2026).")


def test_example_extractor_conforms(geojson_dir: Path, tmp_path: Path) -> None:
    from portolan_cli.plugins.extractors import registered_extractors
    from portolan_cli.testing import check_extractor

    extractor = registered_extractors()["example"]

    check_extractor(extractor, str(geojson_dir), tmp_path / "out", license="CC0-1.0")
