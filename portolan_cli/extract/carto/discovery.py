"""Carto table discovery.

geoparquet-io extracts a *single* named Carto table but provides no way to
enumerate the tables in an account. This module fills that gap by issuing
SQL over the Carto SQL API:

- ``CDB_UserTables()`` lists the user's tables.
- A ``LIMIT 0`` probe per table returns a ``fields`` schema; a column whose
  ``type`` is ``"geometry"`` marks the table as spatial.

Terminology is normalized to Portolan's layer-centric model: a Carto *table*
is exposed as a ``CartoTableInfo`` with ``name``/``id`` (so it flows through
the shared ``filter_layers``/resume/report machinery unchanged).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from urllib.parse import urlparse, urlunparse

import requests  # type: ignore[import-untyped]


class CartoDiscoveryError(Exception):
    """Raised when Carto discovery fails."""


@dataclass
class CartoTableInfo:
    """Information about a Carto table.

    Attributes:
        name: Table name (used for filtering and as the collection slug source).
        id: Numeric ID for filter/resume compatibility (auto-assigned).
        has_geometry: Whether the table has a geometry column (spatial table).
    """

    name: str
    id: int = 0
    has_geometry: bool = True

    def to_filter_dict(self) -> dict[str, int | str]:
        """Convert to the dict shape expected by ``filter_layers``."""
        return {"id": self.id, "name": self.name}


@dataclass
class CartoDiscoveryResult:
    """Result of Carto account discovery.

    Attributes:
        service_url: Normalized SQL API endpoint that was queried.
        tables: Discovered tables (spatial and non-spatial).
        account_name: Account/subdomain parsed from the URL, if available.
    """

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
    """Normalize a Carto URL to its ``/api/v2/sql`` endpoint.

    Accepts either a full SQL API endpoint (``.../api/v2/sql`` or
    ``.../api/v1/sql``) or a bare account domain (``https://acct.carto.com``),
    mirroring geoparquet-io's own URL handling. Any query string is dropped.

    Args:
        url: Carto SQL API URL or account domain.

    Returns:
        Normalized URL ending in ``/api/v2/sql``.

    Raises:
        CartoDiscoveryError: If the URL has no scheme or an unexpected path.
    """
    parsed = urlparse(url)
    if not parsed.scheme:
        raise CartoDiscoveryError(f"Invalid Carto URL (missing scheme): {url}")

    # Drop query/fragment, normalize trailing slash.
    path = parsed.path.rstrip("/")
    base = urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))

    if path.endswith("/api/v2/sql") or path.endswith("/api/v1/sql"):
        return base
    if path == "":
        return f"{base}/api/v2/sql"

    raise CartoDiscoveryError(
        f"Invalid Carto SQL API URL: {url}. "
        "Expected https://account.carto.com or https://account.carto.com/api/v2/sql"
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
    """Issue a SQL query against the Carto SQL API and return the parsed JSON.

    Args:
        sql_api_url: Normalized ``/api/v2/sql`` endpoint.
        query: SQL statement to run.
        api_key: Optional API key (sent as the ``api_key`` query param).
        timeout: Request timeout in seconds.

    Returns:
        Parsed JSON response (contains ``rows``, ``fields``, ``total_rows``).

    Raises:
        CartoDiscoveryError: On network failure, non-2xx status, or a
            server-reported SQL error.
    """
    params = {"q": query}
    if api_key:
        params["api_key"] = api_key

    try:
        response = requests.get(sql_api_url, params=params, timeout=timeout)
        response.raise_for_status()
        data: dict[str, object] = response.json()
    except _NETWORK_ERRORS as e:
        raise CartoDiscoveryError(f"Carto SQL request failed: {e}") from e

    if "error" in data:
        raise CartoDiscoveryError(f"Carto SQL error: {data['error']}")

    return data


def _quote_table(name: str) -> str:
    """Quote a table identifier for safe interpolation into SQL."""
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def table_has_geometry(
    sql_api_url: str,
    table_name: str,
    *,
    api_key: str | None = None,
    timeout: float = 60.0,
) -> bool:
    """Return True if a Carto table has a geometry column.

    Issues a ``LIMIT 0`` query — the SQL API returns the full ``fields`` schema
    even with zero rows — and checks for a column whose ``type`` is
    ``"geometry"``. Detection keys on the field *type*, never the column name
    (e.g. ``gdb_geomattr_data`` is a ``bytea``, not geometry).
    """
    query = f"SELECT * FROM {_quote_table(table_name)} LIMIT 0"
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
    """Discover table names in a Carto account via ``CDB_UserTables()``.

    Enumeration is a single request; geometry presence is left unresolved
    (``has_geometry`` defaults True) and should be resolved per table *after*
    filtering — see ``table_has_geometry`` — to avoid an N+1 probe over the
    whole account when only a few tables are wanted.

    Args:
        url: Carto SQL API URL or account domain.
        api_key: Optional API key for authenticated/private accounts.
        timeout: Per-request timeout in seconds.

    Returns:
        CartoDiscoveryResult with one CartoTableInfo per table name.

    Raises:
        CartoDiscoveryError: If the account cannot be enumerated (e.g.
            ``CDB_UserTables()`` is unavailable or access is denied).
    """
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
    timeout: float = 60.0,  # noqa: ARG001
) -> CartoDiscoveryResult:
    """Build a discovery result from explicit table names (no enumeration).

    Fallback for when ``CDB_UserTables()`` is unavailable (e.g. a single public
    table, or an API key without catalog access) but the user named tables
    explicitly. Geometry presence is resolved later, per table.
    """
    sql_api_url = normalize_sql_api_url(url)
    tables = [CartoTableInfo(name=name, id=i) for i, name in enumerate(names)]
    return CartoDiscoveryResult(
        service_url=sql_api_url,
        tables=tables,
        account_name=account_name_from_url(sql_api_url),
    )
