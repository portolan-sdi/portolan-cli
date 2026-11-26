"""Command parity for `extract arcgis`, `extract carto`, and `extract wfs`.

Each test serves a recorded service from ``tests/fixtures/network/`` on the
loopback interface. The command and the library function both read that one
server, so they see the same service. ``tests/replay_server.py`` gives the
reason for the loopback replay.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from portolan_cli.extract.arcgis.imageserver.orchestrator import ImageServerCLIOptions
from portolan_cli.extract.arcgis.orchestrator import ExtractionOptions as ArcGISOptions
from portolan_cli.extract.arcgis.orchestrator import run_extract_arcgis
from portolan_cli.extract.carto.orchestrator import ExtractionOptions as CartoOptions
from portolan_cli.extract.carto.orchestrator import extract_carto_catalog
from portolan_cli.extract.common.report import run_extract_command
from portolan_cli.extract.wfs.orchestrator import ExtractionOptions as WFSOptions
from portolan_cli.extract.wfs.orchestrator import extract_wfs_catalog
from portolan_cli.input_hardening import InputValidationError
from tests.replay_server import ReplayServer

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from tests.catalog_invariants import RunCli
    from tests.parity.harness import Parity

pytestmark = pytest.mark.integration

NETWORK = Path(__file__).resolve().parents[1] / "fixtures" / "network"
EAST_HAMPTON = "/arcgis/rest/services/MapServiceForOpenGov/MapServer"
LICENSE = ("--auto", "--license", "CC-BY-4.0")


def _empty(root: Path) -> None:
    root.mkdir(parents=True)


@pytest.fixture
def replay() -> Iterator[Callable[[str | Path], str]]:
    """Start the replay server of one scenario and return its base URL."""
    servers: list[ReplayServer] = []

    def start(scenario: str | Path) -> str:
        path = scenario if isinstance(scenario, Path) else NETWORK / scenario
        server = ReplayServer(path)
        servers.append(server)
        return server.__enter__()

    yield start
    for server in servers:
        server.__exit__(None, None, None)
        assert server.unmatched == []


def test_extract_arcgis_featureserver(parity: Parity, replay: Callable[[str | Path], str]) -> None:
    """`extract arcgis` on a MapServer reports the layers the library extracts."""
    url = replay("arcgis/sparse_ids") + EAST_HAMPTON
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


IMAGESERVER = "/arcgis/rest/services/Minimal/ImageServer"


def _imageserver_scenario(base: Path) -> Path:
    """Write a one-route ImageServer scenario. The extent holds one tile."""
    scenario = base / "imageserver"
    scenario.mkdir()
    # The fields that discovery requires.
    service = {
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
    parity: Parity, replay: Callable[[str | Path], str], tmp_path: Path
) -> None:
    """`extract arcgis` on an ImageServer reports the dry run of the library."""
    url = replay(_imageserver_scenario(tmp_path)) + IMAGESERVER
    parity(
        "extract arcgis",
        [url, "out", "--dry-run"],
        catalog=_empty,
        call=lambda _root: run_extract_arcgis(
            url, Path("out"), imageserver=ImageServerCLIOptions(dry_run=True, timeout=60.0)
        ),
    )


def test_imageserver_run_logs_no_token(
    replay: Callable[[str | Path], str], tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A token in the URL stays out of the log records and the result (Codex review)."""
    caplog.set_level(logging.DEBUG, logger="portolan_cli")
    url = replay(_imageserver_scenario(tmp_path)) + IMAGESERVER + "?token=SECRET"
    messages: list[tuple[str, str]] = []

    result = run_extract_arcgis(
        url,
        tmp_path / "out",
        imageserver=ImageServerCLIOptions(dry_run=True, timeout=60.0),
        on_message=lambda level, text: messages.append((level, text)),
    )

    assert result.to_dict()["source_url"] == url.removesuffix("?token=SECRET")
    assert ("info", f"Discovering ImageServer: {url.removesuffix('?token=SECRET')}") in messages
    assert "SECRET" not in repr(result)
    assert "SECRET" not in json.dumps(result.to_dict())
    assert "SECRET" not in repr(messages)
    assert caplog.records
    assert "SECRET" not in caplog.text


def test_extract_arcgis_list_services(
    run_cli: RunCli, replay: Callable[[str | Path], str], tmp_path: Path
) -> None:
    """`extract arcgis --list-services --json` prints the library result.

    This mode prints the bare result, with no envelope, so the ``parity``
    fixture cannot compare it. The test compares the printed JSON instead.
    """
    url = replay("arcgis/philadelphia") + "/ArcGIS/rest/services"
    result = run_cli("extract", "arcgis", url, "--list-services", "--no-recurse", "--json")
    expected = run_extract_arcgis(
        url, tmp_path / "unused", list_only=True, options=ArcGISOptions(recurse=False)
    )
    assert json.loads(result.stdout) == expected.to_dict()


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


def test_extract_carto(parity: Parity, replay: Callable[[str | Path], str]) -> None:
    """`extract carto` reports the tables the library extracts."""
    url = replay("carto/phl_zoning") + "/api/v2/sql"
    parity(
        "extract carto",
        [url, "out", *LICENSE, "--tables", "zoning_basedistricts", "--limit", "3"],
        catalog=_empty,
        call=lambda _root: run_extract_command(
            extract_carto_catalog,
            url,
            Path("out"),
            CartoOptions(limit=3, license="CC-BY-4.0"),
            layer_filter=["zoning_basedistricts"],
        ),
    )


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


def test_extract_wfs(parity: Parity, replay: Callable[[str | Path], str]) -> None:
    """`extract wfs` reports the layers the library extracts."""
    url = replay("wfs/workspace_layer") + "/geoserver/wfs"
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
