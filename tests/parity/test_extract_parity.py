"""Command parity for `extract arcgis`, `extract carto`, and `extract wfs`.

Each test serves a recorded service from ``tests/fixtures/network/`` on the
loopback interface. The command and the library function both read that one
server, so they see the same service. ``tests/replay_server.py`` gives the
reason for the loopback replay.

Equal results alone do not prove an extraction. Two empty reports are equal
too. So each success test also checks what the library side wrote: the layers,
the feature counts, the asset files, and ``versions.json``. It also checks the
requests that reached the server.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path, PurePath
from typing import TYPE_CHECKING, Any

import pytest

from portolan_cli.extract.arcgis.imageserver.discovery import ImageServerDiscoveryError
from portolan_cli.extract.arcgis.imageserver.orchestrator import ImageServerCLIOptions
from portolan_cli.extract.arcgis.orchestrator import ExtractionOptions as ArcGISOptions
from portolan_cli.extract.arcgis.orchestrator import run_extract_arcgis
from portolan_cli.extract.carto.orchestrator import ExtractionOptions as CartoOptions
from portolan_cli.extract.carto.orchestrator import extract_carto_catalog
from portolan_cli.extract.common.report import run_extract_command
from portolan_cli.extract.wfs.orchestrator import ExtractionOptions as WFSOptions
from portolan_cli.extract.wfs.orchestrator import extract_wfs_catalog
from portolan_cli.input_hardening import InputValidationError
from tests.known_upstream import GPIO_WFS_OOM
from tests.replay_server import ReplayServer

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from tests.catalog_invariants import RunCli
    from tests.parity.harness import Parity

pytestmark = pytest.mark.integration

NETWORK = Path(__file__).resolve().parents[1] / "fixtures" / "network"
EAST_HAMPTON = "/arcgis/rest/services/MapServiceForOpenGov/MapServer"
IMAGESERVER = "/arcgis/rest/services/Minimal/ImageServer"
LICENSE = ("--auto", "--license", "CC-BY-4.0")
CARTO_TABLE = ["zoning_basedistricts"]
CARTO_ARGS = ("--tables", "zoning_basedistricts", "--limit", "3")


def _empty(root: Path) -> None:
    root.mkdir(parents=True)


@pytest.fixture
def replay() -> Iterator[Callable[[str | Path], ReplayServer]]:
    """Start the replay server of one scenario and return it."""
    servers: list[ReplayServer] = []

    def start(scenario: str | Path) -> ReplayServer:
        path = scenario if isinstance(scenario, Path) else NETWORK / scenario
        server = ReplayServer(path)
        servers.append(server)
        server.__enter__()
        return server

    yield start
    for server in servers:
        server.__exit__(None, None, None)
        assert server.unmatched == []


def _gets(server: ReplayServer) -> set[str]:
    """Return the path of each GET request that reached the server."""
    return {request.path for request in server.requests if request.method == "GET"}


def _extracted(out: Path) -> list[tuple[Any, str, str, int]]:
    """Return (id, name, status, features) for each layer, and check the files of each.

    Each successful layer has its asset file and a ``versions.json`` in its
    collection directory.
    """
    report = json.loads((out / ".portolan" / "extraction-report.json").read_text())
    for layer in report["layers"]:
        if layer["status"] == "success":
            asset = out / layer["output_path"]
            assert asset.is_file(), asset
            assert (out / PurePath(layer["output_path"]).parent / "versions.json").is_file()
    assert (out / "versions.json").is_file()
    return [
        (layer["id"], layer["name"], layer["status"], layer.get("features") or 0)
        for layer in report["layers"]
    ]


def _carto(url: str, out: Path, **options: Any) -> Any:
    return run_extract_command(
        extract_carto_catalog,
        url,
        out,
        CartoOptions(limit=3, license="CC-BY-4.0", **options),
        layer_filter=CARTO_TABLE,
    )


def test_extract_arcgis_featureserver(
    parity: Parity, replay: Callable[[str | Path], ReplayServer]
) -> None:
    """`extract arcgis` on a MapServer reports the layers the library extracts."""
    server = replay("arcgis/sparse_ids")
    url = server.url + EAST_HAMPTON
    parity(
        "extract arcgis",
        [url, "out", *LICENSE, "--layers", "2"],
        catalog=_empty,
        call=lambda _root: run_extract_arcgis(
            url,
            Path("out"),
            layer_filter=["2"],
            options=ArcGISOptions(license="CC-BY-4.0"),
        ),
    )

    assert _extracted(parity.root / "out") == [(2, "Municipal Boundary Lines", "success", 2)]
    assert _gets(server) == {EAST_HAMPTON, f"{EAST_HAMPTON}/2", f"{EAST_HAMPTON}/2/query"}


def _imageserver_scenario(base: Path, service: dict[str, Any] | None = None) -> Path:
    """Write a one-route ImageServer scenario. The default extent holds one tile."""
    scenario = base / "imageserver"
    scenario.mkdir()
    # The fields that discovery requires.
    service = service or {
        "name": "Minimal_Raster",
        "bandCount": 1,
        "pixelType": "F32",
        "pixelSizeX": 10.0,
        "pixelSizeY": 10.0,
        "extent": {"xmin": 0.0, "ymin": 0.0, "xmax": 100.0, "ymax": 100.0},
    }
    (scenario / "service.json").write_text(json.dumps(service), encoding="utf-8")
    route: dict[str, Any] = {"method": "GET", "path": IMAGESERVER, "body_file": "service.json"}
    (scenario / "routes.json").write_text(json.dumps([route]), encoding="utf-8")
    return scenario


def test_extract_arcgis_imageserver_dry_run(
    parity: Parity, replay: Callable[[str | Path], ReplayServer], tmp_path: Path
) -> None:
    """`extract arcgis` on an ImageServer reports the dry run of the library."""
    server = replay(_imageserver_scenario(tmp_path))
    url = server.url + IMAGESERVER
    parity(
        "extract arcgis",
        [url, "out", "--dry-run"],
        catalog=_empty,
        call=lambda _root: run_extract_arcgis(
            url, Path("out"), imageserver=ImageServerCLIOptions(dry_run=True, timeout=60.0)
        ),
    )

    assert _gets(server) == {IMAGESERVER}
    # The dry run makes empty directories. It writes no file.
    assert [path for path in parity.root.rglob("*") if path.is_file()] == []


def test_extract_arcgis_imageserver_failure(
    run_cli: RunCli, replay: Callable[[str | Path], ReplayServer], tmp_path: Path
) -> None:
    """A service that discovery rejects fails in the command and in the library.

    The command reports every ImageServer error as ``ExtractionError``, as it
    did before #1133. So ``parity.error`` cannot compare it, and the test
    checks each side.
    """
    server = replay(_imageserver_scenario(tmp_path, {"name": "No_Extent"}))
    url = server.url + IMAGESERVER
    root = tmp_path / "command"
    root.mkdir()

    result = run_cli("extract", "arcgis", url, "out", "--json", cwd=root, expect=1)

    assert json.loads(result.stdout)["errors"] == [
        {"type": "ExtractionError", "message": "ImageServer extraction failed"}
    ]
    assert "Missing required field 'bandCount'" in result.stderr
    with pytest.raises(ImageServerDiscoveryError, match="Missing required field 'bandCount'"):
        run_extract_arcgis(url, tmp_path / "library")
    assert [path for path in root.rglob("*") if path.is_file()] == []
    assert [path for path in (tmp_path / "library").rglob("*") if path.is_file()] == []


def test_imageserver_run_logs_no_token(
    replay: Callable[[str | Path], ReplayServer], tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A token in the URL stays out of the log records and the result (Codex review)."""
    caplog.set_level(logging.DEBUG, logger="portolan_cli")
    url = replay(_imageserver_scenario(tmp_path)).url + IMAGESERVER
    messages: list[tuple[str, str]] = []

    result = run_extract_arcgis(
        url + "?token=SECRET",
        tmp_path / "out",
        imageserver=ImageServerCLIOptions(dry_run=True, timeout=60.0),
        on_message=lambda level, text: messages.append((level, text)),
    )

    assert result.to_dict()["source_url"] == url
    assert ("info", f"Discovering ImageServer: {url}") in messages
    assert "SECRET" not in repr(result)
    assert "SECRET" not in json.dumps(result.to_dict())
    assert "SECRET" not in repr(messages)
    assert caplog.records
    assert "SECRET" not in caplog.text


def test_extract_arcgis_list_services(
    run_cli: RunCli, replay: Callable[[str | Path], ReplayServer], tmp_path: Path
) -> None:
    """`extract arcgis --list-services --json` prints the library result.

    This mode prints the bare result, with no envelope, so the ``parity``
    fixture cannot compare it. The test compares the printed JSON instead.
    """
    url = replay("arcgis/philadelphia").url + "/ArcGIS/rest/services"
    result = run_cli("extract", "arcgis", url, "--list-services", "--no-recurse", "--json")
    expected = run_extract_arcgis(
        url, tmp_path / "unused", list_only=True, options=ArcGISOptions(recurse=False)
    )
    assert json.loads(result.stdout) == expected.to_dict()
    total = expected.to_dict()["total_services"]
    assert isinstance(total, int)
    assert total > 0


def test_extract_arcgis_rejects_bad_id(parity: Parity) -> None:
    """A bad --id fails before any request, in the command and in the library."""
    url = "https://example.invalid/arcgis/rest/services/x/MapServer"
    parity.error(
        "extract arcgis",
        [url, "out", "--id", "bad id!", "--auto"],
        catalog=_empty,
        call=lambda root: run_extract_arcgis(
            url, Path("out"), options=ArcGISOptions(catalog_id="bad id!")
        ),
        raises=InputValidationError,
    )


def test_extract_carto(parity: Parity, replay: Callable[[str | Path], ReplayServer]) -> None:
    """`extract carto` reports the tables the library extracts."""
    server = replay("carto/phl_zoning")
    url = server.url + "/api/v2/sql"
    parity(
        "extract carto",
        [url, "out", *LICENSE, *CARTO_ARGS],
        catalog=_empty,
        call=lambda _root: _carto(url, Path("out")),
    )

    assert _extracted(parity.root / "out") == [(0, "zoning_basedistricts", "success", 3)]
    assert _gets(server) == {"/api/v2/sql"}


def test_extract_carto_resume(parity: Parity, replay: Callable[[str | Path], ReplayServer]) -> None:
    """`extract carto --resume` keeps the table that an earlier run extracted."""
    server = replay("carto/phl_zoning")
    url = server.url + "/api/v2/sql"
    # One entry for each factory call: its request count, the request total
    # after it, and the layers of its report.
    first_run: list[tuple[int, int, list[dict[str, Any]]]] = []

    def extracted_once(root: Path) -> None:
        root.mkdir(parents=True)
        before = len(server.requests)
        _carto(url, root / "out")
        report = json.loads((root / "out" / ".portolan" / "extraction-report.json").read_text())
        after = len(server.requests)
        first_run.append((after - before, after, report["layers"]))

    parity(
        "extract carto",
        [url, "out", *LICENSE, *CARTO_ARGS, "--resume"],
        catalog=extracted_once,
        call=lambda _root: _carto(url, Path("out"), resume=True),
    )

    # The last factory call built the library side. The resumed library run follows it.
    full_requests, mark, first_layers = first_run[-1]
    resumed_requests = len(server.requests) - mark
    report = json.loads((parity.root / "out" / ".portolan" / "extraction-report.json").read_text())
    assert report["layers"] == first_layers
    assert [(layer["name"], layer["status"]) for layer in first_layers] == [
        ("zoning_basedistricts", "success")
    ]
    assert resumed_requests < full_requests


def test_extract_carto_rejects_bad_id(parity: Parity) -> None:
    """A bad --id fails before any request, in the command and in the library."""
    url = "https://example.invalid/api/v2/sql"
    parity.error(
        "extract carto",
        [url, "out", "--id", "bad id!", "--auto"],
        catalog=_empty,
        call=lambda root: run_extract_command(
            extract_carto_catalog, url, Path("out"), CartoOptions(catalog_id="bad id!")
        ),
        raises=InputValidationError,
    )


@GPIO_WFS_OOM
def test_extract_wfs(parity: Parity, replay: Callable[[str | Path], ReplayServer]) -> None:
    """`extract wfs` reports the layers the library extracts."""
    server = replay("wfs/workspace_layer")
    url = server.url + "/geoserver/wfs"
    parity(
        "extract wfs",
        [url, "out", *LICENSE, "--layers", "topp:states"],
        catalog=_empty,
        call=lambda _root: run_extract_command(
            extract_wfs_catalog,
            url,
            Path("out"),
            WFSOptions(license="CC-BY-4.0"),
            layer_filter=["topp:states"],
        ),
    )

    assert _extracted(parity.root / "out") == [(1, "topp:states", "success", 2)]
    assert _gets(server) == {"/geoserver/wfs", "/geoserver/wms"}


def test_extract_wfs_rejects_bad_id(parity: Parity) -> None:
    """A bad --id fails before any request, in the command and in the library."""
    url = "https://example.invalid/geoserver/wfs"
    parity.error(
        "extract wfs",
        [url, "out", "--id", "bad id!", "--auto"],
        catalog=_empty,
        call=lambda root: run_extract_command(
            extract_wfs_catalog, url, Path("out"), WFSOptions(catalog_id="bad id!")
        ),
        raises=InputValidationError,
    )
