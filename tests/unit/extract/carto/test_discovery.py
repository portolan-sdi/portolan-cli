"""Unit tests for Carto table discovery."""

from __future__ import annotations

from typing import Any

import pytest

from portolan_cli.extract.carto.discovery import (
    CartoDiscoveryError,
    account_name_from_url,
    discover_carto_tables,
    normalize_sql_api_url,
    table_has_geometry,
    tables_from_names,
)

pytestmark = [pytest.mark.unit]


class _FakeResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self._payload


# A geometry "fields" block (LIMIT 0 returns schema even with zero rows).
_GEOM_FIELDS = {
    "fields": {
        "cartodb_id": {"type": "number"},
        "the_geom": {"type": "geometry", "srid": 4326},
        "name": {"type": "string"},
    },
    "rows": [],
}
# A non-spatial table: a bytea column whose NAME contains "geom" must NOT count.
_NON_GEOM_FIELDS = {
    "fields": {
        "cartodb_id": {"type": "number"},
        "gdb_geomattr_data": {"type": "bytea"},
        "label": {"type": "string"},
    },
    "rows": [],
}


def _route(payload_by_predicate: list[tuple[str, dict[str, Any]]]) -> Any:
    """Build a fake requests.get that routes on a substring of the SQL query."""

    def fake_get(url: str, params: dict[str, str] | None = None, timeout: float = 60.0) -> Any:
        query = (params or {}).get("q", "")
        for needle, payload in payload_by_predicate:
            if needle in query:
                return _FakeResponse(payload)
        raise AssertionError(f"unexpected query: {query}")

    return fake_get


def test_normalize_full_endpoint_unchanged() -> None:
    assert (
        normalize_sql_api_url("https://phl.carto.com/api/v2/sql")
        == "https://phl.carto.com/api/v2/sql"
    )


def test_normalize_bare_domain_appends_endpoint() -> None:
    assert normalize_sql_api_url("https://phl.carto.com") == "https://phl.carto.com/api/v2/sql"


def test_normalize_accepts_v1_and_strips_query() -> None:
    assert (
        normalize_sql_api_url("https://phl.carto.com/api/v1/sql?q=SELECT%201")
        == "https://phl.carto.com/api/v1/sql"
    )


def test_normalize_rejects_missing_scheme() -> None:
    with pytest.raises(CartoDiscoveryError):
        normalize_sql_api_url("phl.carto.com")


def test_account_name_from_url() -> None:
    assert account_name_from_url("https://phl.carto.com/api/v2/sql") == "phl"


def test_table_has_geometry_true_for_geometry_type(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "portolan_cli.extract.carto.discovery.requests.get", _route([("LIMIT 0", _GEOM_FIELDS)])
    )
    assert table_has_geometry("https://x.carto.com/api/v2/sql", "parcels") is True


def test_table_has_geometry_false_for_bytea_named_geom(monkeypatch: pytest.MonkeyPatch) -> None:
    """Detection must key on field type, not a 'geom' substring in the name."""
    monkeypatch.setattr(
        "portolan_cli.extract.carto.discovery.requests.get", _route([("LIMIT 0", _NON_GEOM_FIELDS)])
    )
    assert table_has_geometry("https://x.carto.com/api/v2/sql", "lookup") is False


def test_discover_tables_lists_names_in_one_request(monkeypatch: pytest.MonkeyPatch) -> None:
    """Enumeration is a single CDB_UserTables request; geometry is not probed here."""
    cdb = {"rows": [{"table_name": "parcels"}, {"table_name": "lookup"}], "fields": {}}
    calls: list[str] = []

    def fake_get(url: str, params: dict[str, str] | None = None, timeout: float = 60.0) -> Any:
        query = (params or {}).get("q", "")
        calls.append(query)
        assert "CDB_UserTables" in query, "discovery must not probe individual tables"
        return _FakeResponse(cdb)

    monkeypatch.setattr("portolan_cli.extract.carto.discovery.requests.get", fake_get)

    result = discover_carto_tables("https://phl.carto.com")
    assert result.account_name == "phl"
    assert [(t.name, t.id) for t in result.tables] == [("parcels", 0), ("lookup", 1)]
    assert len(calls) == 1  # no per-table geometry probing


def test_discover_raises_on_sql_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "portolan_cli.extract.carto.discovery.requests.get",
        _route([("CDB_UserTables", {"error": ["relation does not exist"]})]),
    )
    with pytest.raises(CartoDiscoveryError):
        discover_carto_tables("https://phl.carto.com")


def test_tables_from_names_lists_without_network() -> None:
    """The explicit-names fallback enumerates nothing; geometry is resolved later."""
    result = tables_from_names("https://phl.carto.com", ["parcels", "zoning"])
    assert result.service_url == "https://phl.carto.com/api/v2/sql"
    assert [(t.name, t.id) for t in result.tables] == [("parcels", 0), ("zoning", 1)]
