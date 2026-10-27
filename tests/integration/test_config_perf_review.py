"""Regression probes for shared config reads in the check repair workflow."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from portolan_cli import trace
from portolan_cli.run_state import RunState
from portolan_cli.validation.fixers import FIXERS

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("fixer", ["readme", "required_files"])
def test_readme_fix_reuses_the_run_metadata(tmp_path: Path, fixer: str) -> None:
    """Scaffolding must reuse the metadata already read by the title sweep."""
    collection = tmp_path / "roads"
    collection.mkdir()
    (tmp_path / "catalog.json").write_text(
        json.dumps(
            {
                "type": "Catalog",
                "id": "root",
                "description": "Catalog",
                "stac_version": "1.1.0",
                "links": [{"rel": "child", "href": "./roads/collection.json"}],
            }
        )
    )
    (collection / "collection.json").write_text(
        json.dumps(
            {
                "type": "Collection",
                "id": "roads",
                "description": "Roads",
                "stac_version": "1.1.0",
                "license": "CC-BY-4.0",
                "extent": {
                    "spatial": {"bbox": [[0, 0, 1, 1]]},
                    "temporal": {"interval": [[None, None]]},
                },
                "links": [],
            }
        )
    )
    for directory in (tmp_path, collection):
        (directory / ".portolan").mkdir()
        (directory / ".portolan" / "metadata.yaml").write_text("license: CC-BY-4.0\n")

    state = RunState.open(tmp_path)
    with trace.tracing():
        state.metadata(tmp_path)
        state.metadata(collection)
        before = trace.counters()
        results = FIXERS[fixer](tmp_path, False, state)
        after = trace.counters()

    assert all(result.success for result in results)
    assert (tmp_path / "README.md").exists()
    assert (collection / "README.md").exists()
    assert after["config.yaml_parses"] - before["config.yaml_parses"] == 0
    assert after["config.merges"] - before["config.merges"] == 0


def test_check_fix_counts_conversion_worker_work(tmp_path: Path) -> None:
    """The command counters must include reads performed in spawned processes."""
    from tests.perf.gates import measure
    from tests.perf.scenarios import SCENARIOS

    scenario = SCENARIOS["check-fix-geo-assets"]
    serial = measure(scenario, tmp_path, 4)
    parallel = measure(scenario, tmp_path, 4, workers=2)
    assert serial["exit_code"] == parallel["exit_code"] == 0
    assert serial["network"] == parallel["network"] == []
    # The parent parses once, and each worker that converts a file parses once.
    assert 2 <= parallel["counters"]["config.yaml_parses"] <= 3
    assert parallel["counters"]["config.merges"] == parallel["counters"]["config.yaml_parses"]
    for counter in ("parquet.opens", "vector.opens"):
        assert parallel["counters"][counter] == serial["counters"][counter]


@pytest.mark.parametrize("fix", [False, True], ids=["check", "fix"])
def test_preserve_glob_matches_a_symlinked_asset(tmp_path: Path, fix: bool) -> None:
    """A preserve rule names the catalog asset, rather than its symlink target."""
    from portolan_cli.config import save_config
    from portolan_cli.formats import CloudNativeStatus
    from portolan_cli.scan.check import check_directory

    root = tmp_path / "catalog"
    sub = root / "sub"
    sub.mkdir(parents=True)
    save_config(root, {"conversion": {"paths": {"preserve": ["sub/a.geojson"]}}})
    source = tmp_path / "original.geojson"
    source.write_text(
        '{"type":"FeatureCollection","features":[{"type":"Feature",'
        '"properties":{},"geometry":{"type":"Point","coordinates":[0,0]}}]}'
    )
    alias = sub / "a.geojson"
    alias.symlink_to(source)

    report = check_directory(sub, fix=fix, catalog_path=root, state=RunState.open(root), workers=1)

    assert [(file.path, file.status) for file in report.files] == [
        (alias, CloudNativeStatus.CLOUD_NATIVE)
    ]
    assert not (sub / "a.parquet").exists()


@pytest.mark.parametrize("command", ["get", "list"])
def test_config_read_command_parses_the_catalog_file_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command: str
) -> None:
    """Listing all settings must share one file read with the sensitive-key warning."""
    from click.testing import CliRunner

    from portolan_cli.cli import cli
    from portolan_cli.config import save_config
    from tests.conftest import _init_catalog

    _init_catalog(tmp_path)
    save_config(tmp_path, {"backend": "file", "statistics": {"enabled": False}})
    monkeypatch.chdir(tmp_path)
    args = ["--timings", "config", command, "--json"]
    if command == "get":
        args.append("backend")
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    if command == "list":
        settings = payload["data"]["settings"]
        assert settings["backend"] == {"value": "file", "source": "catalog"}
        assert settings["statistics.enabled"] == {"value": False, "source": "catalog"}
    else:
        assert payload["data"] == {"key": "backend", "value": "file", "source": "catalog"}
    assert payload["meta"]["counters"]["config.yaml_parses"] == 1
