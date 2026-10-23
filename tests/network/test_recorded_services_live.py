"""Check nightly that each recorded public service still answers in the recorded shape.

``tests/integration/test_extract_recorded.py`` trusts the recordings in
``tests/fixtures/network/``. A recording is only worth its shape: sparse layer
IDs, a layer document, prefixed FeatureType names. These tests ask each public
source for the same documents and compare fields and names, not values. A
failure means the recording no longer describes the server, so the recording
needs a refresh before its scenario proves anything.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import httpx
import pytest

from tests.replay_server import load_routes

pytestmark = [pytest.mark.network]

NETWORK = Path(__file__).resolve().parents[1] / "fixtures" / "network"

ARCGIS_ORIGINS = {
    "arcgis/sparse_ids": "https://eh-gis.ehamptonny.gov",
    "arcgis/layer_url": "https://gisportal.boston.gov",
}
WFS_ORIGINS = {
    "wfs/workspace_layer": "https://gs-stable.geo-solutions.it",
    "wfs/empty_collection": "https://geodata.naturvardsverket.se",
}
_TYPE_NAME = re.compile(r"<(?:wfs:)?Name>([^<]+)</(?:wfs:)?Name>")


def _get(url: str, params: dict[str, str]) -> httpx.Response:
    response = httpx.get(url, params=params, timeout=60, follow_redirects=True)
    response.raise_for_status()
    return response


@pytest.mark.parametrize("scenario", sorted(ARCGIS_ORIGINS))
def test_arcgis_documents_keep_the_recorded_fields(scenario: str) -> None:
    """Each recorded service and layer document still has every recorded field."""
    origin = ARCGIS_ORIGINS[scenario]
    documents = [
        route for route in load_routes(NETWORK / scenario) if route.query_subset == {"f": "json"}
    ]
    assert documents != []
    missing: dict[str, list[str]] = {}
    for route in documents:
        assert route.body_file is not None
        recorded: dict[str, Any] = json.loads((NETWORK / scenario / route.body_file).read_text())
        live: dict[str, Any] = _get(origin + route.path, {"f": "json"}).json()
        assert "error" not in live, live
        gone = sorted(set(recorded) - set(live))
        if gone:
            missing[route.path] = gone
    assert missing == {}


def test_east_hampton_layer_ids_still_have_gaps() -> None:
    """The layer IDs of the sparse_ids source are not the list positions (#1003)."""
    path = "/arcgis/rest/services/MapServiceForOpenGov/MapServer"
    live = _get(ARCGIS_ORIGINS["arcgis/sparse_ids"] + path, {"f": "json"}).json()
    ids = [layer["id"] for layer in live["layers"]]
    assert ids[:3] == [2, 9, 10]
    assert ids != list(range(len(ids)))


def test_boston_layer_zero_is_a_feature_layer() -> None:
    """The layer URL of the layer_url source names one feature layer (#652)."""
    path = "/arcgis/rest/services/BaseServices/Open_Space_Public/MapServer/0"
    live = _get(ARCGIS_ORIGINS["arcgis/layer_url"] + path, {"f": "json"}).json()
    assert (live["id"], live["type"]) == (0, "Feature Layer")


@pytest.mark.parametrize("scenario", sorted(WFS_ORIGINS))
def test_wfs_capabilities_keep_the_recorded_type_names(scenario: str) -> None:
    """Each recorded FeatureType name, with its workspace prefix, is still advertised."""
    capabilities = next(
        route
        for route in load_routes(NETWORK / scenario)
        if route.query_subset.get("request") == "GetCapabilities"
    )
    assert capabilities.body_file is not None
    recorded = set(_TYPE_NAME.findall((NETWORK / scenario / capabilities.body_file).read_text()))
    assert recorded != set()
    live = _get(WFS_ORIGINS[scenario] + capabilities.path, capabilities.query_subset).text
    assert sorted(recorded - set(_TYPE_NAME.findall(live))) == []


def test_carto_table_keeps_the_recorded_columns() -> None:
    """The recorded Carto table keeps each recorded column, and the account refuses a listing."""
    scenario = NETWORK / "carto" / "phl_zoning"
    columns = json.loads((scenario / "columns.json").read_text())
    query = 'SELECT * FROM "zoning_basedistricts" LIMIT 0'
    live = _get("https://phl.carto.com/api/v2/sql", {"q": query}).json()
    assert sorted(set(columns["fields"]) - set(live["fields"])) == []

    tables = httpx.get(
        "https://phl.carto.com/api/v2/sql",
        params={"q": "SELECT cdb_usertables AS table_name FROM CDB_UserTables() ORDER BY 1"},
        timeout=60,
    )
    assert tables.status_code == 403
