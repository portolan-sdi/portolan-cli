"""Carto table discovery."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from urllib.parse import urlparse, urlunparse

import requests

from portolan_cli.extract.common.urls import redact_credentials


class CartoDiscoveryError(Exception):
    """Raised when Carto discovery fails."""


@dataclass
class CartoTableInfo:
    """Information about a Carto table."""

    name: str
    id: int = 0
    has_geometry: bool = True


@dataclass
class CartoDiscoveryResult:
    """Result of Carto account discovery."""

    service_url: str
    tables: list[CartoTableInfo] = field(default_factory=list)
    account_name: str | None = None


_NETWORK_ERRORS = (
    requests.exceptions.RequestException,
    json.JSONDecodeError,
    OSError,
    TimeoutError,
    ConnectionError,
)


def normalize_sql_api_url(url: str) -> str:
    """Normalize a Carto URL to its ``/api/v2/sql`` endpoint."""
    parsed = urlparse(url)
    if not parsed.scheme:
        raise CartoDiscoveryError(redact_credentials(f"Invalid Carto URL (missing scheme): {url}"))

    # Drop query/fragment, normalize trailing slash.
    path = parsed.path.rstrip("/")
    base = urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))

    if path.endswith(("/api/v2/sql", "/api/v1/sql")):
        return base
    if path == "":
        return f"{base}/api/v2/sql"

    raise CartoDiscoveryError(
        redact_credentials(
            f"Invalid Carto SQL API URL: {url}. "
            "Expected https://account.carto.com or https://account.carto.com/api/v2/sql"
        )
    )


def account_name_from_url(url: str) -> str | None:
    """Extract the account subdomain from a Carto URL (e.g. ``phl`` from phl.carto.com)."""
    netloc = urlparse(url).netloc
    host = netloc.split(":", 1)[0]
    if "." in host:
        return host.split(".", 1)[0]
    return host or None


def carto_sql_request(
    sql_api_url: str,
    query: str,
    *,
    api_key: str | None = None,
    timeout: float = 60.0,
) -> dict[str, object]:
    """Issue a SQL query against the Carto SQL API and return the parsed JSON."""
    params = {"q": query}
    if api_key:
        params["api_key"] = api_key

    try:
        response = requests.get(sql_api_url, params=params, timeout=timeout)
        response.raise_for_status()
        data: dict[str, object] = response.json()
    except _NETWORK_ERRORS as e:
        # A requests error quotes the request URL with its api_key.
        raise CartoDiscoveryError(redact_credentials(f"Carto SQL request failed: {e}")) from e

    if "error" in data:
        raise CartoDiscoveryError(f"Carto SQL error: {data['error']}")

    return data


def quote_table_identifier(name: str) -> str:
    """Quote a table name as a SQL identifier for safe interpolation."""
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def table_has_geometry(
    sql_api_url: str,
    table_name: str,
    *,
    api_key: str | None = None,
    timeout: float = 60.0,
) -> bool:
    """Return True if a Carto table has a geometry column."""
    # An identifier cannot be a bound parameter, so quoting it is the injection defense.
    query = f"SELECT * FROM {quote_table_identifier(table_name)} LIMIT 0"  # noqa: S608 - quoted
    data = carto_sql_request(sql_api_url, query, api_key=api_key, timeout=timeout)
    fields = data.get("fields", {})
    if not isinstance(fields, dict):
        return False
    return any(
        isinstance(meta, dict) and meta.get("type") == "geometry" for meta in fields.values()
    )


def discover_carto_tables(
    url: str,
    *,
    api_key: str | None = None,
    timeout: float = 60.0,
) -> CartoDiscoveryResult:
    """Discover table names in a Carto account via ``CDB_UserTables()``."""
    sql_api_url = normalize_sql_api_url(url)
    data = carto_sql_request(
        sql_api_url,
        "SELECT cdb_usertables AS table_name FROM CDB_UserTables() ORDER BY 1",
        api_key=api_key,
        timeout=timeout,
    )

    rows = data.get("rows", [])
    if not isinstance(rows, list):
        rows = []

    tables = [
        CartoTableInfo(name=row["table_name"], id=i)
        for i, row in enumerate(rows)
        if isinstance(row, dict) and row.get("table_name")
    ]
    return CartoDiscoveryResult(
        service_url=sql_api_url,
        tables=tables,
        account_name=account_name_from_url(sql_api_url),
    )


def tables_from_names(
    url: str,
    names: list[str],
    *,
    api_key: str | None = None,  # noqa: ARG001 - kept for signature parity with discover_carto_tables
    timeout: float = 60.0,  # noqa: ARG001 - signature parity with discover_carto_tables
) -> CartoDiscoveryResult:
    """Build a discovery result from explicit table names (no enumeration)."""
    sql_api_url = normalize_sql_api_url(url)
    tables = [CartoTableInfo(name=name, id=i) for i, name in enumerate(names)]
    return CartoDiscoveryResult(
        service_url=sql_api_url,
        tables=tables,
        account_name=account_name_from_url(sql_api_url),
    )
