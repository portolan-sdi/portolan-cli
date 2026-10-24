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
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from tests.replay_server import ReplayServer

if TYPE_CHECKING:
    from collections.abc import Callable

    from click.testing import Result

    from tests.catalog_invariants import RunCli

pytestmark = [pytest.mark.integration]

NETWORK = Path(__file__).resolve().parents[1] / "fixtures" / "network"
EAST_HAMPTON = "/arcgis/rest/services/MapServiceForOpenGov/MapServer"
BOSTON = "/arcgis/rest/services/BaseServices/Open_Space_Public/MapServer"

LayerRow = tuple[Any, str, str, int]
SCENARIOS = sorted(path.parent for path in NETWORK.glob("*/*/routes.json"))


@dataclass(frozen=True)
class Extraction:
    """One ``extract`` run against one scenario."""

    scenario: str
    kind: str
    path: str
    args: tuple[str, ...] = ()


EXTRACTIONS = {
    "sparse_ids-layers": Extraction("arcgis/sparse_ids", "arcgis", EAST_HAMPTON, ("--layers", "2")),
    "sparse_ids-exclude": Extraction(
        "arcgis/sparse_ids", "arcgis", EAST_HAMPTON, ("--exclude-layers", "2")
    ),
    "sparse_ids-no_match": Extraction(
        "arcgis/sparse_ids", "arcgis", f"{EAST_HAMPTON}?token=SECRET", ("--layers", "99")
    ),
    "layer_url": Extraction("arcgis/layer_url", "arcgis", f"{BOSTON}/0"),
    "tables_only": Extraction(
        "arcgis/tables_only", "arcgis", "/arcgis/rest/services/zoning_descriptions/FeatureServer"
    ),
    "token_gated": Extraction(
        "arcgis/token_gated",
        "arcgis",
        "/arcgis/rest/services/Secured/Open_Space_Public/MapServer",
        ("--token", "REPLAY-TOKEN"),
    ),
    "transient_error": Extraction("arcgis/transient_error", "arcgis", BOSTON, ("--retries", "2")),
    "workspace_layer-exclude": Extraction(
        "wfs/workspace_layer", "wfs", "/geoserver/wfs", ("--exclude-layers", "states")
    ),
    "workspace_layer-layers": Extraction(
        "wfs/workspace_layer", "wfs", "/geoserver/wfs", ("--layers", "topp:states")
    ),
    "empty_collection": Extraction(
        "wfs/empty_collection",
        "wfs",
        "/n2000/wfs",
        ("--layers", "N2000_WFS:N2000", "--wfs-version", "2.0.0"),
    ),
    "geodata_gov_md": Extraction("wfs/geodata_gov_md", "wfs", "/geoserver/wfs?api_key=SECRET"),
    "geodata_gov_md-no_match": Extraction(
        "wfs/geodata_gov_md", "wfs", "/geoserver/wfs?api_key=SECRET", ("--layers", "nomatch")
    ),
    "phl_zoning": Extraction(
        "carto/phl_zoning",
        "carto",
        "/api/v2/sql",
        ("--tables", "zoning_basedistricts", "--limit", "3"),
    ),
}
# tests/docs/test_v1_examples.py replays arcgis/philadelphia. It asserts
# server.unmatched == [] with no xfail marker.
CHECKED_ELSEWHERE = {"arcgis/philadelphia"}


def _run(
    case: str, tmp_path: Path, run_cli: RunCli, *extra: str, out_name: str = "out"
) -> tuple[Result, ReplayServer, Path]:
    """Run one extraction against its scenario and return the result, server, and output."""
    extraction = EXTRACTIONS[case]
    out = tmp_path / out_name
    server = ReplayServer(NETWORK / extraction.scenario)
    with server as base_url:
        result = run_cli(
            "extract",
            extraction.kind,
            base_url + extraction.path,
            str(out),
            "--auto",
            "--license",
            "CC-BY-4.0",
            "--retries",
            "1",
            *extraction.args,
            *extra,
            expect=None,
        )
    return result, server, out


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


# A case whose output breaks a catalog invariant because of an open bug. The
# reason is an ``invariant`` line in tests/guardrail_exemptions.txt.
DAMAGED: dict[str, str] = {}


@pytest.mark.parametrize("case", sorted(EXTRACTIONS))
def test_recording_answers_every_request(
    case: str,
    tmp_path: Path,
    run_cli: RunCli,
    build_damaged_catalog: Callable[[Path, str], Path],
) -> None:
    """The scenario answers each request of the extraction.

    This test has no xfail marker. A strict xfail on a behavior test also
    swallows a lost route, so this test keeps the recording gate visible.
    """
    if case in DAMAGED:
        build_damaged_catalog(tmp_path / "out", DAMAGED[case])
    _result, server, _out = _run(case, tmp_path, run_cli)

    assert server.unmatched == []


@pytest.mark.unit
def test_every_scenario_has_a_recording_gate() -> None:
    """Each scenario directory has an extraction in ``EXTRACTIONS`` or a gate elsewhere."""
    covered = {extraction.scenario for extraction in EXTRACTIONS.values()} | CHECKED_ELSEWHERE
    on_disk = {f"{path.parent.name}/{path.name}" for path in SCENARIOS}
    assert sorted(covered) == sorted(on_disk)


def test_arcgis_layers_matches_sparse_layer_ids(tmp_path: Path, run_cli: RunCli) -> None:
    """``--layers 2`` extracts layer ID 2 of a service whose IDs are 2, 9, 10."""
    result, server, out = _run("sparse_ids-layers", tmp_path, run_cli)

    assert server.unmatched == []
    assert result.exit_code == 0, result.output
    assert _layers(out) == [(2, "Municipal Boundary Lines", "success", 2)]


def test_arcgis_exclude_layers_matches_sparse_layer_ids(tmp_path: Path, run_cli: RunCli) -> None:
    """``--exclude-layers 2`` keeps layer IDs 9 and 10."""
    result, server, out = _run("sparse_ids-exclude", tmp_path, run_cli)

    assert server.unmatched == []
    assert result.exit_code == 0, result.output
    assert sorted(_layers(out)) == [
        (9, "EH Fire Districts", "success", 2),
        (10, "EH School Districts", "success", 2),
    ]


def test_arcgis_layer_url_extracts_that_layer(tmp_path: Path, run_cli: RunCli) -> None:
    """A URL that ends in ``/MapServer/0`` extracts layer 0."""
    result, server, out = _run("layer_url", tmp_path, run_cli)

    assert server.unmatched == []
    assert result.exit_code == 0, result.output
    assert _layers(out) == [(0, "PRK.open_space", "success", 2)]


def test_arcgis_tables_only_service_fails_and_names_the_tables(
    tmp_path: Path, run_cli: RunCli
) -> None:
    """A service that holds one table and no layer exits 1 and says why (#1034)."""
    result, server, _out = _run("tables_only", tmp_path, run_cli)

    assert server.unmatched == []
    assert result.exit_code == 1, result.output
    assert "Extracted 0/0 layers" not in result.output
    assert (
        "The service lists 1 table and no layers. Pass --include-tables to extract tables."
        in result.output
    )


def test_arcgis_token_reaches_layer_discovery(tmp_path: Path, run_cli: RunCli) -> None:
    """``--token`` reaches every request of a token-gated server."""
    result, server, out = _run("token_gated", tmp_path, run_cli)

    assert server.unmatched == []
    assert [r.path for r in server.requests if "token" not in r.query] == []
    assert result.exit_code == 0, result.output
    assert _layers(out) == [(0, "PRK.open_space", "success", 2)]


def test_arcgis_discovery_retries_a_transient_error(tmp_path: Path, run_cli: RunCli) -> None:
    """A 503 on the first service request is retried, and the layer extracts."""
    result, server, out = _run("transient_error", tmp_path, run_cli)

    assert server.unmatched == []
    assert [r.status for r in server.requests if r.path == BOSTON] == [503, 200]
    assert result.exit_code == 0, result.output
    assert _layers(out) == [(0, "PRK.open_space", "success", 2)]


def test_wfs_exclude_layers_matches_the_leaf_name(tmp_path: Path, run_cli: RunCli) -> None:
    """``--exclude-layers states`` excludes ``topp:states``."""
    result, server, out = _run("workspace_layer-exclude", tmp_path, run_cli)

    assert server.unmatched == []
    assert result.exit_code == 0, result.output
    assert _wfs_layers(out) == [("topp:tasmania_roads", "success", 2)]


def test_wfs_layers_matches_the_qualified_name(tmp_path: Path, run_cli: RunCli) -> None:
    """``--layers topp:states`` extracts that layer only. This is the control for #768."""
    result, server, out = _run("workspace_layer-layers", tmp_path, run_cli)

    assert server.unmatched == []
    assert result.exit_code == 0, result.output
    assert _wfs_layers(out) == [("topp:states", "success", 2)]


def _index_mismatches(out: Path) -> dict[str, tuple[Any, Any]]:
    """Return each collection whose root index entry disagrees with its versions.json."""
    index = json.loads((out / "versions.json").read_text())["collections"]
    mismatches: dict[str, tuple[Any, Any]] = {}
    for versions_path in sorted(out.rglob("*/versions.json")):
        versions = json.loads(versions_path.read_text())
        current = next(
            v for v in versions["versions"] if v["version"] == versions["current_version"]
        )
        collection_id = versions_path.parent.relative_to(out).as_posix()
        entry = index[collection_id]
        actual = (entry["asset_count"], entry["total_size_bytes"])
        expected = (
            len(current["assets"]),
            sum(a["size_bytes"] for a in current["assets"].values()),
        )
        if actual != expected:
            mismatches[collection_id] = (actual, expected)
    return mismatches


def test_wfs_extraction_keeps_the_root_index_in_step(tmp_path: Path, run_cli: RunCli) -> None:
    """The root versions.json counts the assets the side steps add after ``add_files``."""
    result, server, out = _run("workspace_layer-layers", tmp_path, run_cli)

    assert server.unmatched == []
    assert result.exit_code == 0, result.output
    assert _index_mismatches(out) == {}


def test_wfs_named_layer_with_no_features_fails(tmp_path: Path, run_cli: RunCli) -> None:
    """A layer named with ``--layers`` that returns 0 features fails the run (#1034)."""
    result, server, out = _run("empty_collection", tmp_path, run_cli)

    assert server.unmatched == []
    assert result.exit_code == 1, result.output
    assert _wfs_layers(out) == [("N2000_WFS:N2000", "failed", 0)]


def test_wfs_api_key_stays_out_of_the_catalog(tmp_path: Path, run_cli: RunCli) -> None:
    """No file under the output root holds the API key of the service URL."""
    result, server, out = _run("geodata_gov_md", tmp_path, run_cli)

    assert server.unmatched == []
    assert result.exit_code == 0, result.output
    assert _wfs_layers(out) == [("topp:tasmania_roads", "success", 2)]
    leaks = sorted(
        str(path.relative_to(out))
        for path in out.rglob("*")
        if path.is_file() and b"SECRET" in path.read_bytes()
    )
    assert leaks == []


@pytest.mark.parametrize(
    ("case", "service_path"),
    [("geodata_gov_md-no_match", "/geoserver/wfs"), ("sparse_ids-no_match", EAST_HAMPTON)],
)
def test_discovery_failure_names_the_host_and_not_the_credential(
    case: str, service_path: str, tmp_path: Path, run_cli: RunCli
) -> None:
    """A run that selects no layer names the service in text and JSON, with no credential.

    The source URL carries ``SECRET`` as its key or token. A terminal log and
    a CI log keep the message, so the message drops the key, as the catalog
    does (#981).
    """
    text, text_server, _out = _run(case, tmp_path, run_cli)
    as_json, json_server, _out = _run(case, tmp_path, run_cli, "--json", out_name="json")

    assert (text_server.unmatched, json_server.unmatched) == ([], [])
    assert (text.exit_code, as_json.exit_code) == (1, 1)
    streams = {
        "text stdout": text.stdout,
        "text stderr": text.stderr,
        "json stdout": as_json.stdout,
        "json stderr": as_json.stderr,
    }
    assert sorted(name for name, stream in streams.items() if "SECRET" in stream) == []
    service = rf"http://127\.0\.0\.1:\d+{re.escape(service_path)}"
    failure = (
        rf"Extraction failed: \[PRTLN-EXT003\] No layer at {service} matches the layer filters\."
    )
    assert re.search(rf"^✗ {failure}", text.stderr, re.MULTILINE), text.stderr
    envelope = json.loads(as_json.stdout)
    assert re.fullmatch(service, envelope["data"]["url"]), envelope
    assert [error["type"] for error in envelope["errors"]] == ["NoLayersSelectedError"]
    assert re.match(failure, envelope["errors"][0]["message"]), envelope


def test_wfs_layer_that_add_skips_is_named(tmp_path: Path, run_cli: RunCli) -> None:
    """A layer file that ``ignored_files`` matches is skipped with a warning, not in silence."""
    from portolan_cli.catalog import init_catalog
    from portolan_cli.config import set_setting

    init_catalog(tmp_path / "out", catalog_id="recorded", license_id="CC-BY-4.0")
    set_setting(tmp_path / "out", "ignored_files", ["topp_states*"])
    result, server, out = _run("workspace_layer-layers", tmp_path, run_cli)

    assert server.unmatched == []
    assert result.exit_code == 0, result.output
    assert not (out / "topp_states" / "collection.json").exists()
    assert (
        "Skipping topp_states.parquet: it matches ignored_files pattern 'topp_states*'"
        in result.output
    )


@pytest.mark.unit
@pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda path: f"{path.parent.name}/{path.name}")
def test_scenario_is_documented_small_and_has_no_stray_file(scenario: Path) -> None:
    """Each scenario names its issue or test, stays under 100 KB, and serves every file."""
    readme = (scenario / "README.md").read_text(encoding="utf-8")
    assert re.search(r"#\d+|`tests/[\w/]+\.py`", readme), scenario
    assert "\n## Source\n" in readme, scenario
    assert sum(path.stat().st_size for path in scenario.iterdir()) < 100_000

    bodies = {route.body_file for route in ReplayServer(scenario).routes}
    stray = {path.name for path in scenario.iterdir()} - bodies - {"README.md", "routes.json"}
    assert sorted(stray) == []
