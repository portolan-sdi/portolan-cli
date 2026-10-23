"""Run the real ``extract`` command against recorded service responses.

Each scenario under ``tests/fixtures/network/`` holds the response shape that
broke a production extraction: sparse ArcGIS layer IDs, a layer URL, a
FeatureServer with only tables, a token gate, a transient 503, prefixed WFS
names, an empty FeatureCollection, and an API key in the WFS URL. Its README
names the issue and the source of the recording.

``tests/replay_server.py`` serves the scenario on the loopback interface, so
the requests of ``httpx``, ``requests``, and geoparquet-io all reach it. Each
test asserts ``server.unmatched == []`` first. That proves the recording
covers every request that the extraction made. The test then asserts the
exact layer set in the extraction report and the exit code.

A test that fails on ``origin/main`` carries a strict xfail with its issue
number. The fix for that issue removes the marker.
"""

from __future__ import annotations

import json
import re
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from tests.replay_server import ReplayServer

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from click.testing import Result

pytestmark = [pytest.mark.integration]

NETWORK = Path(__file__).resolve().parents[1] / "fixtures" / "network"
EAST_HAMPTON = "/arcgis/rest/services/MapServiceForOpenGov/MapServer"
BOSTON = "/arcgis/rest/services/BaseServices/Open_Space_Public/MapServer"

LayerRow = tuple[Any, str, str, int]
SCENARIOS = sorted(path.parent for path in NETWORK.glob("*/*/routes.json"))


@contextmanager
def _replay(scenario: str) -> Iterator[tuple[str, ReplayServer]]:
    server = ReplayServer(NETWORK / scenario)
    with server as base_url:
        yield base_url, server


def _extract(run_cli: Callable[..., Result], kind: str, url: str, out: Path, *extra: str) -> Result:
    return run_cli(
        "extract", kind, url, str(out), "--auto", "--license", "CC-BY-4.0", "--retries", "1", *extra
    )


def _report(out: Path) -> list[dict[str, Any]]:
    report = json.loads((out / ".portolan" / "extraction-report.json").read_text())
    layers: list[dict[str, Any]] = report["layers"]
    return layers


def _layers(out: Path) -> list[LayerRow]:
    """Return (id, name, status, features) for each layer in the extraction report."""
    return [
        (layer["id"], layer["name"], layer["status"], layer.get("features") or 0)
        for layer in _report(out)
    ]


def _wfs_layers(out: Path) -> list[tuple[str, str, int]]:
    """Return (name, status, features) for each layer. A WFS ID is a list position."""
    return sorted(
        (layer["name"], layer["status"], layer.get("features") or 0) for layer in _report(out)
    )


@pytest.mark.xfail(strict=True, reason="#1003: --layers matches the list position, not the ID")
def test_arcgis_layers_matches_sparse_layer_ids(
    tmp_path: Path, run_cli: Callable[..., Result]
) -> None:
    """``--layers 2`` extracts layer ID 2 of a service whose IDs are 2, 9, 10."""
    out = tmp_path / "out"
    with _replay("arcgis/sparse_ids") as (base_url, server):
        result = _extract(run_cli, "arcgis", base_url + EAST_HAMPTON, out, "--layers", "2")

    assert server.unmatched == []
    assert result.exit_code == 0, result.output
    assert _layers(out) == [(2, "Municipal Boundary Lines", "success", 2)]


@pytest.mark.xfail(
    strict=True, reason="#1003: --exclude-layers matches the list position, not the ID"
)
def test_arcgis_exclude_layers_matches_sparse_layer_ids(
    tmp_path: Path, run_cli: Callable[..., Result]
) -> None:
    """``--exclude-layers 2`` keeps layer IDs 9 and 10."""
    out = tmp_path / "out"
    with _replay("arcgis/sparse_ids") as (base_url, server):
        result = _extract(run_cli, "arcgis", base_url + EAST_HAMPTON, out, "--exclude-layers", "2")

    assert server.unmatched == []
    assert result.exit_code == 0, result.output
    assert sorted(_layers(out)) == [
        (9, "EH Fire Districts", "success", 2),
        (10, "EH School Districts", "success", 2),
    ]


@pytest.mark.xfail(strict=True, reason="#652: a layer URL extracts 0 layers and exits 0")
def test_arcgis_layer_url_extracts_that_layer(
    tmp_path: Path, run_cli: Callable[..., Result]
) -> None:
    """A URL that ends in ``/MapServer/0`` extracts layer 0."""
    out = tmp_path / "out"
    with _replay("arcgis/layer_url") as (base_url, server):
        result = _extract(run_cli, "arcgis", f"{base_url}{BOSTON}/0", out)

    assert server.unmatched == []
    assert result.exit_code == 0, result.output
    assert _layers(out) == [(0, "PRK.open_space", "success", 2)]


@pytest.mark.xfail(strict=True, reason="#812: a tables-only FeatureServer reports 0/0 and exits 0")
def test_arcgis_tables_only_service_fails_and_names_the_tables(
    tmp_path: Path, run_cli: Callable[..., Result]
) -> None:
    """A service that holds one table and no layer exits 1 and says why (#1034)."""
    out = tmp_path / "out"
    url = "/arcgis/rest/services/zoning_descriptions/FeatureServer"
    with _replay("arcgis/tables_only") as (base_url, server):
        result = _extract(run_cli, "arcgis", base_url + url, out)

    assert server.unmatched == []
    assert result.exit_code == 1, result.output
    assert "Extracted 0/0 layers" not in result.output
    assert "table" in result.output.lower()


@pytest.mark.xfail(strict=True, reason="#855: layer discovery sends no token")
def test_arcgis_token_reaches_layer_discovery(
    tmp_path: Path, run_cli: Callable[..., Result]
) -> None:
    """``--token`` reaches every request of a token-gated server."""
    out = tmp_path / "out"
    url = "/arcgis/rest/services/Secured/Open_Space_Public/MapServer"
    with _replay("arcgis/token_gated") as (base_url, server):
        result = _extract(run_cli, "arcgis", base_url + url, out, "--token", "REPLAY-TOKEN")

    assert server.unmatched == []
    assert [r.path for r in server.requests if "token" not in r.query] == []
    assert result.exit_code == 0, result.output
    assert _layers(out) == [(0, "PRK.open_space", "success", 2)]


@pytest.mark.xfail(strict=True, reason="#811: one transient discovery error ends the harvest")
def test_arcgis_discovery_retries_a_transient_error(
    tmp_path: Path, run_cli: Callable[..., Result]
) -> None:
    """A 503 on the first service request is retried, and the layer extracts."""
    out = tmp_path / "out"
    with _replay("arcgis/transient_error") as (base_url, server):
        result = _extract(run_cli, "arcgis", base_url + BOSTON, out, "--retries", "2")

    assert server.unmatched == []
    assert [r.status for r in server.requests if r.path == BOSTON] == [503, 200]
    assert result.exit_code == 0, result.output
    assert _layers(out) == [(0, "PRK.open_space", "success", 2)]


@pytest.mark.xfail(
    strict=True, reason="#768: --exclude-layers compares the leaf with workspace:layer"
)
def test_wfs_exclude_layers_matches_the_leaf_name(
    tmp_path: Path, run_cli: Callable[..., Result]
) -> None:
    """``--exclude-layers states`` excludes ``topp:states``."""
    out = tmp_path / "out"
    with _replay("wfs/workspace_layer") as (base_url, server):
        result = _extract(
            run_cli, "wfs", f"{base_url}/geoserver/wfs", out, "--exclude-layers", "states"
        )

    assert server.unmatched == []
    assert result.exit_code == 0, result.output
    assert _wfs_layers(out) == [("topp:tasmania_roads", "success", 2)]


def test_wfs_layers_matches_the_qualified_name(
    tmp_path: Path, run_cli: Callable[..., Result]
) -> None:
    """``--layers topp:states`` extracts that layer only. This is the control for #768."""
    out = tmp_path / "out"
    with _replay("wfs/workspace_layer") as (base_url, server):
        result = _extract(
            run_cli, "wfs", f"{base_url}/geoserver/wfs", out, "--layers", "topp:states"
        )

    assert server.unmatched == []
    assert result.exit_code == 0, result.output
    assert _wfs_layers(out) == [("topp:states", "success", 2)]


@pytest.mark.xfail(strict=True, reason="#963: a named layer with 0 features exits 0")
def test_wfs_named_layer_with_no_features_fails(
    tmp_path: Path, run_cli: Callable[..., Result]
) -> None:
    """A layer named with ``--layers`` that returns 0 features fails the run (#1034)."""
    out = tmp_path / "out"
    with _replay("wfs/empty_collection") as (base_url, server):
        result = _extract(
            run_cli,
            "wfs",
            f"{base_url}/n2000/wfs",
            out,
            "--layers",
            "N2000_WFS:N2000",
            "--wfs-version",
            "2.0.0",
        )

    assert server.unmatched == []
    assert result.exit_code == 1, result.output
    assert _wfs_layers(out) == [("N2000_WFS:N2000", "failed", 0)]


@pytest.mark.xfail(strict=True, reason="#981: the WFS api_key reaches the published catalog")
def test_wfs_api_key_stays_out_of_the_catalog(
    tmp_path: Path, run_cli: Callable[..., Result]
) -> None:
    """No file under the output root holds the API key of the service URL."""
    out = tmp_path / "out"
    with _replay("wfs/geodata_gov_md") as (base_url, server):
        result = _extract(run_cli, "wfs", f"{base_url}/geoserver/wfs?api_key=SECRET", out)

    assert server.unmatched == []
    assert result.exit_code == 0, result.output
    assert _wfs_layers(out) == [("topp:tasmania_roads", "success", 2)]
    leaks = sorted(
        str(path.relative_to(out))
        for path in out.rglob("*")
        if path.is_file() and b"SECRET" in path.read_bytes()
    )
    assert leaks == []


@pytest.mark.unit
@pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda path: f"{path.parent.name}/{path.name}")
def test_scenario_is_documented_small_and_has_no_stray_file(scenario: Path) -> None:
    """Each scenario names its issue or test, stays under 100 KB, and serves every file."""
    readme = (scenario / "README.md").read_text(encoding="utf-8")
    assert re.search(r"#\d+|`tests/[\w/]+\.py`", readme), scenario
    assert "Source:" in readme, scenario
    assert sum(path.stat().st_size for path in scenario.iterdir()) < 100_000

    bodies = {route.body_file for route in ReplayServer(scenario).routes}
    stray = {path.name for path in scenario.iterdir()} - bodies - {"README.md", "routes.json"}
    assert sorted(stray) == []
