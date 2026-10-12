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


def test_backend_conforms(tmp_path: Path) -> None:
    from portolan_example.backend import ExampleBackend

    from portolan_cli.backends import get_backend
    from portolan_cli.testing import check_backend

    check_backend(ExampleBackend, tmp_path / "direct")

    assert type(get_backend("example", catalog_root=tmp_path)) is ExampleBackend


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
