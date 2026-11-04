"""The DuckDB session that writes the Iceberg tables.

DuckDB is the writer. Its ``iceberg`` extension attaches a REST catalog,
reads the data files through an S3 secret, and commits a snapshot per
statement or per transaction. The session opens one connection on first
use, loads ``httpfs``, ``iceberg``, and ``spatial``, creates the catalog
secret and the storage secret, and attaches the catalog as ``lake``. Every
secret value travels as a bound parameter, so no credential lands in SQL
text that a log could carry.

Verified 2026-11-04 on DuckDB 1.5.6 against the Apache REST fixture: a
``CREATE SECRET`` accepts ``?`` parameters for both secret types; ``ATTACH
'<warehouse>' AS lake (TYPE iceberg, ENDPOINT ..., SECRET ...)`` writes;
``LOAD`` of an installed extension needs no network.

Limits of DuckDB's Iceberg writer that the backend works around, each at
its workaround in ``backend.py``:

- Writes are merge-on-read only. ``DELETE`` and ``TRUNCATE`` write a
  deletion vector and keep the old data file live
  (duckdb/duckdb-iceberg#1659). Row counts come from a query, and prune
  deletes the orphaned files itself.
- No custom snapshot summary property (duckdb/duckdb-iceberg#1249). The
  version record is a table property plus a tag, written over REST.
- No ``set-snapshot-ref`` or ``remove-snapshots`` statement
  (duckdb/duckdb-iceberg#1341, #1252). Rollback and prune go over REST.
- A write needs a REST catalog (duckdb/duckdb-iceberg#668). There is no
  local catalog; the compose stack in ``tests/iceberg/e2e`` is the local one.

The minimum DuckDB is 1.5.6: the first release whose Iceberg writer sets
``format-version`` and the typed geometry column this backend needs. The
``pyproject.toml`` pin holds the version; this module refuses an older
interpreter-level import, which a stale environment can produce.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any, Protocol

import duckdb

from portolan_cli.backends.iceberg.ddl import qualified_table, quote_literal

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence

    from portolan_cli.backends.iceberg.auth import TokenProvider
    from portolan_cli.backends.iceberg.settings import IcebergSettings

#: The alias the catalog is attached as. Table names are ``lake."ns"."name"``.
CATALOG_ALIAS = "lake"
#: The oldest DuckDB whose Iceberg writer this backend supports.
MINIMUM_DUCKDB = "1.5.6"
#: The extensions the writer needs, in load order.
EXTENSIONS: tuple[str, ...] = ("httpfs", "iceberg", "spatial")

CATALOG_CREDENTIAL = "portolan_catalog"
WAREHOUSE_CREDENTIAL = "portolan_warehouse"


class SessionError(RuntimeError):
    """The session cannot start: the DuckDB build is too old."""


class Connection(Protocol):
    """The part of ``duckdb.DuckDBPyConnection`` the session uses."""

    def execute(self, sql: str, params: Any = None) -> Any:
        """Run one statement."""
        ...

    def close(self) -> None:
        """Close the connection."""
        ...


def _version_tuple(version: str) -> tuple[int, ...]:
    parts: list[int] = []
    for part in version.split("."):
        digits = "".join(ch for ch in part if ch.isdigit())
        if not digits:
            break
        parts.append(int(digits))
    return tuple(parts)


def check_duckdb_version() -> None:
    """Raise ``SessionError`` when the DuckDB in use is older than the minimum."""
    found = str(duckdb.__version__)
    if _version_tuple(found) < _version_tuple(MINIMUM_DUCKDB):
        raise SessionError(
            f"The iceberg backend needs DuckDB {MINIMUM_DUCKDB} or newer; found {found}."
        )


def load_extensions(connection: Connection) -> None:
    """Load the extensions, installing one only when its load fails.

    An installed extension loads from the cache with no network. The
    install runs only on a failed load, so a machine with the cache never
    reaches extensions.duckdb.org.
    """
    for name in EXTENSIONS:
        try:
            connection.execute(f"LOAD {name}")
        except duckdb.Error:
            connection.execute(f"INSTALL {name}")
            connection.execute(f"LOAD {name}")


def _catalog_secret(auth: TokenProvider) -> tuple[str, list[Any]] | None:
    options = auth.duckdb_secret_options()
    if not options:
        return None
    keys = ", ".join(f"{key} ?" for key in options)
    return f"CREATE SECRET {CATALOG_CREDENTIAL} (TYPE iceberg, {keys})", list(options.values())


def _warehouse_secret(settings: IcebergSettings) -> tuple[str, list[Any]] | None:
    """Return the S3 secret, or None when DuckDB must find the credential itself.

    ``vended`` storage takes the credential from each ``loadTable`` answer,
    which DuckDB's iceberg extension requests on its own. ``gcs`` is
    reserved for a catalog whose data lives on Google Cloud Storage and
    creates no secret yet. An S3 store with no key is a public warehouse.
    """
    storage = settings.storage
    if storage.kind != "s3" or storage.key_id is None or storage.secret is None:
        return None
    options: list[tuple[str, Any]] = [("KEY_ID", storage.key_id), ("SECRET", storage.secret)]
    if storage.session_token is not None:
        options.append(("SESSION_TOKEN", storage.session_token))
    if storage.endpoint is not None:
        options.append(("ENDPOINT", storage.endpoint))
    options.append(("URL_STYLE", storage.url_style))
    options.append(("USE_SSL", storage.use_ssl))
    if storage.region is not None:
        options.append(("REGION", storage.region))
    keys = ", ".join(f"{key} ?" for key, _ in options)
    return f"CREATE SECRET {WAREHOUSE_CREDENTIAL} (TYPE s3, {keys})", [
        value for _, value in options
    ]


def _attach(settings: IcebergSettings, with_secret: bool) -> str:
    credential = f"SECRET {CATALOG_CREDENTIAL}" if with_secret else "AUTHORIZATION_TYPE 'none'"
    warehouse = quote_literal(settings.warehouse or "")
    return (
        f"ATTACH {warehouse} AS {CATALOG_ALIAS} (TYPE iceberg, "
        f"ENDPOINT {quote_literal(settings.uri)}, {credential})"
    )


def prepare_statements(
    settings: IcebergSettings, auth: TokenProvider
) -> list[tuple[str, list[Any] | None]]:
    """Return the statements that make a fresh connection ready to write.

    The extensions are not among them; :func:`load_extensions` runs first.
    Each secret value is a bound parameter, never SQL text.
    """
    statements: list[tuple[str, list[Any] | None]] = []
    catalog_secret = _catalog_secret(auth)
    if catalog_secret is not None:
        statements.append(catalog_secret)
    warehouse_secret = _warehouse_secret(settings)
    if warehouse_secret is not None:
        statements.append(warehouse_secret)
    statements.append((_attach(settings, catalog_secret is not None), None))
    return statements


class Session:
    """One DuckDB connection attached to one REST catalog.

    Args:
        settings: The backend settings.
        auth: The catalog credential.
        connect: Opens a connection. Replaced in tests.
    """

    def __init__(
        self,
        settings: IcebergSettings,
        auth: TokenProvider,
        *,
        connect: Callable[[], Connection] = duckdb.connect,
    ) -> None:
        self._settings = settings
        self._auth = auth
        self._connect = connect
        self._connection: Connection | None = None

    @property
    def connection(self) -> Connection:
        """The connection, opened and prepared on first access."""
        if self._connection is None:
            check_duckdb_version()
            connection = self._connect()
            load_extensions(connection)
            for sql, params in prepare_statements(self._settings, self._auth):
                connection.execute(sql, params)
            self._connection = connection
        return self._connection

    def table(self, name: str) -> str:
        """Return the qualified, quoted name of the collection's table."""
        return qualified_table(CATALOG_ALIAS, self._settings.namespace, name)

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> list[tuple[Any, ...]]:
        """Run one statement and return its rows."""
        result = self.connection.execute(sql, list(params) if params is not None else None)
        rows = result.fetchall()
        return list(rows)

    def fetch_one(self, sql: str, params: Sequence[Any] | None = None) -> tuple[Any, ...] | None:
        """Run one statement and return its first row, or None."""
        rows = self.execute(sql, params)
        return rows[0] if rows else None

    @contextlib.contextmanager
    def transaction(self) -> Iterator[None]:
        """Run the block in one transaction: one commit per table on success."""
        connection = self.connection
        connection.execute("BEGIN")
        try:
            yield
        except BaseException:
            connection.execute("ROLLBACK")
            raise
        connection.execute("COMMIT")

    def close(self) -> None:
        """Close the connection. A later use opens a new one."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
