"""No extract command lets a credential in its URL reach any output (#1133).

Each case runs one command family against a recorded service, with a
credential in the URL. The credential is the text ``SECRET``. The test runs
the command and its library function. It then checks every place a
credential could go:

- the command stdout, stderr, and JSON envelope
- each file that the run wrote
- the repr and the ``to_dict()`` of the library result, and the repr of
  the options and of the ArcGIS client
- each ``portolan_cli`` log record at DEBUG
- the text of a raised error

The Codex reviews of #1133 found a credential in each of these places. This
test covers all of them at once, so a new leak fails here.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from portolan_cli import logging_setup
from portolan_cli.extract.arcgis import orchestrator as arcgis
from portolan_cli.extract.arcgis.imageserver.orchestrator import ImageServerCLIOptions
from portolan_cli.extract.carto import orchestrator as carto
from portolan_cli.extract.common.report import run_extract_command
from portolan_cli.extract.wfs import orchestrator as wfs
from tests.known_upstream import GPIO_WFS_OOM
from tests.replay_server import ReplayServer

if TYPE_CHECKING:
    from collections.abc import Callable

    from tests.catalog_invariants import RunCli

pytestmark = pytest.mark.integration

NETWORK = Path(__file__).resolve().parents[2] / "fixtures" / "network"
EAST_HAMPTON = "/arcgis/rest/services/MapServiceForOpenGov/MapServer"
IMAGESERVER = "/arcgis/rest/services/Minimal/ImageServer"
LICENSE = ("--license", "CC-BY-4.0")
_IMAGE_SERVICE = {
    "name": "Minimal_Raster",
    "bandCount": 1,
    "pixelType": "F32",
    "pixelSizeX": 10.0,
    "pixelSizeY": 10.0,
    "extent": {"xmin": 0.0, "ymin": 0.0, "xmax": 100.0, "ymax": 100.0},
}


@dataclass(frozen=True)
class Case:
    """One command run with a credential in its URL.

    ``path`` holds the credential as a query parameter. ``userinfo`` puts
    ``user:SECRET@`` in the URL for basic auth.
    """

    family: str
    scenario: str | dict[str, Any]
    path: str
    args: tuple[str, ...]
    exit_code: int
    library: Callable[[str, Path], Any]
    userinfo: bool = False


def _arcgis_layers(layer: str) -> Callable[[str, Path], Any]:
    return lambda url, out: arcgis.run_extract_arcgis(
        url, out, layer_filter=[layer], options=arcgis.ExtractionOptions(license="CC-BY-4.0")
    )


def _imageserver(url: str, out: Path) -> Any:
    return arcgis.run_extract_arcgis(url, out, imageserver=ImageServerCLIOptions(dry_run=True))


def _carto(url: str, out: Path) -> Any:
    return run_extract_command(
        carto.extract_carto_catalog,
        url,
        out,
        carto.ExtractionOptions(limit=3, license="CC-BY-4.0"),
        layer_filter=["zoning_basedistricts"],
    )


def _wfs(layer: str) -> Callable[[str, Path], Any]:
    return lambda url, out: run_extract_command(
        wfs.extract_wfs_catalog,
        url,
        out,
        wfs.ExtractionOptions(license="CC-BY-4.0"),
        layer_filter=[layer],
    )


CASES = {
    "arcgis-success": Case(
        "arcgis",
        "arcgis/sparse_ids",
        f"{EAST_HAMPTON}?token=SECRET",
        ("--layers", "2", *LICENSE),
        0,
        _arcgis_layers("2"),
    ),
    "arcgis-no-match": Case(
        "arcgis",
        "arcgis/sparse_ids",
        f"{EAST_HAMPTON}?token=SECRET",
        ("--layers", "99", *LICENSE),
        1,
        _arcgis_layers("99"),
    ),
    "imageserver-dry-run": Case(
        "arcgis", _IMAGE_SERVICE, f"{IMAGESERVER}?token=SECRET", ("--dry-run",), 0, _imageserver
    ),
    "imageserver-bad-service": Case(
        "arcgis", {"name": "No_Extent"}, f"{IMAGESERVER}?token=SECRET", (), 1, _imageserver
    ),
    "carto-success": Case(
        "carto",
        "carto/phl_zoning",
        "/api/v2/sql?api_key=SECRET",
        ("--tables", "zoning_basedistricts", "--limit", "3", *LICENSE),
        0,
        _carto,
    ),
    "carto-bad-url": Case("carto", "carto/phl_zoning", "/wrong?api_key=SECRET", LICENSE, 1, _carto),
    "wfs-api_key": Case(
        "wfs",
        "wfs/geodata_gov_md",
        "/geoserver/wfs?api_key=SECRET",
        ("--layers", "topp:tasmania_roads", *LICENSE),
        0,
        _wfs("topp:tasmania_roads"),
    ),
    "wfs-no-match": Case(
        "wfs",
        "wfs/geodata_gov_md",
        "/geoserver/wfs?api_key=SECRET",
        ("--layers", "nomatch", *LICENSE),
        1,
        _wfs("nomatch"),
    ),
    **{
        f"wfs-{name}": Case(
            "wfs",
            "wfs/workspace_layer",
            f"/geoserver/wfs?{name}=SECRET",
            ("--layers", "topp:states", *LICENSE),
            0,
            _wfs("topp:states"),
        )
        for name in ("token", "key", "apikey")
    },
    "wfs-basic-auth": Case(
        "wfs",
        "wfs/workspace_layer",
        "/geoserver/wfs",
        ("--layers", "topp:states", *LICENSE),
        0,
        _wfs("topp:states"),
        userinfo=True,
    ),
}


def _scenario(case: Case, base: Path) -> Path:
    if isinstance(case.scenario, str):
        return NETWORK / case.scenario
    scenario = base / "scenario"
    scenario.mkdir()
    (scenario / "service.json").write_text(json.dumps(case.scenario), encoding="utf-8")
    route = {"method": "GET", "path": IMAGESERVER, "body_file": "service.json"}
    (scenario / "routes.json").write_text(json.dumps([route]), encoding="utf-8")
    return scenario


def _url(case: Case, base_url: str) -> str:
    if case.userinfo:
        base_url = base_url.replace("://", "://user:SECRET@", 1)
    return base_url + case.path


def _secret_files(root: Path) -> list[str]:
    """Return each file below ``root`` whose bytes hold the credential."""
    if not root.exists():
        return []
    return sorted(
        str(path.relative_to(root))
        for path in root.rglob("*")
        if path.is_file() and b"SECRET" in path.read_bytes()
    )


def _secret_records(caplog: pytest.LogCaptureFixture) -> list[str]:
    """Return each ``portolan_cli`` or ``portolan_core`` log record that holds the credential."""
    return [
        f"{record.name}: {record.getMessage()}"
        for record in caplog.records
        if record.name.split(".")[0] in logging_setup.PACKAGE_LOGGERS
        and "SECRET" in record.getMessage()
    ]


@pytest.mark.parametrize("output", ["json", "text"])
@pytest.mark.parametrize(
    "name",
    [
        # A WFS run that parses features hits the upstream out-of-memory bug.
        pytest.param(name, marks=GPIO_WFS_OOM)
        if case.family == "wfs" and case.exit_code == 0
        else name
        for name, case in sorted(CASES.items())
    ],
)
def test_no_credential_reaches_any_output(
    name: str,
    output: str,
    run_cli: RunCli,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    case = CASES[name]
    server = ReplayServer(_scenario(case, tmp_path))
    with server as base_url:
        url = _url(case, base_url)
        command_out = tmp_path / "command"
        result = run_cli(
            "-vv",
            *(["--format", "json"] if output == "json" else []),
            "extract",
            case.family,
            url,
            str(command_out),
            "--auto",
            *case.args,
            expect=case.exit_code,
        )
        command_records = _secret_records(caplog)

        logging_setup.reset_logging()
        caplog.clear()
        for package in logging_setup.PACKAGE_LOGGERS:
            caplog.set_level(logging.DEBUG, logger=package)
        library_out = tmp_path / "library"
        try:
            library_result = case.library(url, library_out)
        except Exception as err:  # noqa: BLE001 - any error type must hold no credential
            shown = [str(err), repr(err)]
        else:
            shown = [repr(library_result), json.dumps(library_result.to_dict())]

    assert "SECRET" not in result.stdout
    assert "SECRET" not in result.stderr
    if output == "json":
        assert "SECRET" not in json.dumps(json.loads(result.stdout))
    assert command_records == []
    assert _secret_files(command_out) == []
    assert _secret_records(caplog) == []
    assert _secret_files(library_out) == []
    assert [text for text in shown if "SECRET" in text] == []


def test_no_options_or_client_repr_holds_a_credential() -> None:
    options = [
        arcgis.ExtractionOptions(token="SECRET"),
        arcgis.ExtractionOptions(token="SECRET").client(),
        carto.ExtractionOptions(api_key="SECRET"),
        wfs.ExtractionOptions(),
    ]

    assert [type(item).__name__ for item in options if "SECRET" in repr(item)] == []
