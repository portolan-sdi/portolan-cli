"""The DuckDB session: one connection, the extensions, two secrets, one ATTACH.

The statements are checked as text against a fake connection, so no test
here reaches a catalog. One test loads the real extensions, which the
network guard installed before the run, to prove the load needs no download.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import duckdb
import pytest

from portolan_cli.backends.iceberg.auth import NoAuth, OAuth2ClientCredentials, StaticBearer
from portolan_cli.backends.iceberg.session import (
    CATALOG_ALIAS,
    MINIMUM_DUCKDB,
    Session,
    SessionError,
    load_extensions,
    prepare_statements,
)
from portolan_cli.backends.iceberg.settings import IcebergSettings, StorageSettings

if TYPE_CHECKING:
    from collections.abc import Sequence

pytestmark = pytest.mark.unit


class FakeConnection:
    """Records every statement. ``fail_on`` makes a statement raise once."""

    def __init__(self, fail_on: Sequence[str] = ()) -> None:
        self.statements: list[tuple[str, list[Any] | None]] = []
        self._fail_on = set(fail_on)
        self.closed = False
        self.rows: list[tuple[Any, ...]] = [(1,)]

    def execute(self, sql: str, params: list[Any] | None = None) -> FakeConnection:
        self.statements.append((sql, params))
        if sql in self._fail_on:
            self._fail_on.discard(sql)
            raise duckdb.Error(f"failed: {sql}")
        return self

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows[0] if self.rows else None

    def close(self) -> None:
        self.closed = True


def _storage(
    *,
    kind: str = "s3",
    endpoint: str | None = "localhost:9000",
    use_ssl: bool = False,
    key_id: str | None = "key",
    session_token: str | None = None,
) -> StorageSettings:
    return StorageSettings(
        kind=kind,  # type: ignore[arg-type]
        endpoint=endpoint,
        use_ssl=use_ssl,
        region="us-east-1",
        url_style="path" if endpoint else "vhost",
        key_id=key_id,
        secret="s3-secret" if key_id else None,  # noqa: S106  # a test value
        session_token=session_token,
    )


def _settings(
    *, warehouse: str | None = "warehouse", storage: StorageSettings | None = None
) -> IcebergSettings:
    return IcebergSettings(
        uri="http://localhost:8181",
        warehouse=warehouse,
        namespace="portolake",
        auth="none",
        oauth2_server_uri=None,
        client_id=None,
        client_secret=None,
        token=None,
        storage=storage or _storage(),
    )


def _sql(statements: list[tuple[str, list[Any] | None]]) -> list[str]:
    return [sql for sql, _ in statements]


# --- the statements ---


def test_no_auth_and_s3_storage_give_one_secret_and_an_anonymous_attach() -> None:
    statements = prepare_statements(_settings(), NoAuth())

    assert statements == [
        (
            "CREATE SECRET portolan_warehouse (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, "
            "URL_STYLE ?, USE_SSL ?, REGION ?)",
            ["key", "s3-secret", "localhost:9000", "path", False, "us-east-1"],
        ),
        (
            "ATTACH 'warehouse' AS lake (TYPE iceberg, ENDPOINT 'http://localhost:8181', "
            "AUTHORIZATION_TYPE 'none')",
            None,
        ),
    ]


def test_oauth2_gives_a_catalog_secret_that_the_attach_names() -> None:
    auth = OAuth2ClientCredentials(
        "https://catalog.example.org/v1/oauth/tokens", client_id="cid", client_secret="csec"
    )
    settings = IcebergSettings(
        uri="https://catalog.example.org",
        warehouse="wh",
        namespace="portolake",
        auth="oauth2",
        oauth2_server_uri="https://catalog.example.org/v1/oauth/tokens",
        client_id="cid",
        client_secret="csec",
        token=None,
        storage=_storage(endpoint="s3.example.org", use_ssl=True),
    )

    statements = prepare_statements(settings, auth)

    assert statements[0] == (
        "CREATE SECRET portolan_catalog (TYPE iceberg, CLIENT_ID ?, CLIENT_SECRET ?, "
        "OAUTH2_SERVER_URI ?)",
        ["cid", "csec", "https://catalog.example.org/v1/oauth/tokens"],
    )
    assert statements[-1] == (
        "ATTACH 'wh' AS lake (TYPE iceberg, ENDPOINT 'https://catalog.example.org', "
        "SECRET portolan_catalog)",
        None,
    )


def test_a_bearer_token_is_a_catalog_secret_too() -> None:
    statements = prepare_statements(_settings(), StaticBearer("tok"))

    assert statements[0] == ("CREATE SECRET portolan_catalog (TYPE iceberg, TOKEN ?)", ["tok"])


def test_no_secret_value_appears_in_the_sql_text() -> None:
    auth = OAuth2ClientCredentials(
        "https://c.example/v1/oauth/tokens", client_id="cid", client_secret="csec"
    )

    for sql, _ in prepare_statements(_settings(), auth):
        assert "csec" not in sql
        assert "s3-secret" not in sql


def test_a_session_token_joins_the_s3_secret() -> None:
    settings = _settings(storage=_storage(session_token="sess"))

    sql, params = prepare_statements(settings, NoAuth())[0]

    assert "SESSION_TOKEN ?" in sql
    assert params == ["key", "s3-secret", "sess", "localhost:9000", "path", False, "us-east-1"]


def test_no_endpoint_means_the_aws_default_in_vhost_style() -> None:
    settings = _settings(storage=_storage(endpoint=None, use_ssl=True))

    sql, params = prepare_statements(settings, NoAuth())[0]

    assert "ENDPOINT" not in sql
    assert params == ["key", "s3-secret", "vhost", True, "us-east-1"]


def test_vended_storage_creates_no_s3_secret() -> None:
    """DuckDB asks the catalog for the credential with each loadTable."""
    settings = _settings(storage=_storage(kind="vended", key_id=None))

    assert _sql(prepare_statements(settings, NoAuth())) == [
        "ATTACH 'warehouse' AS lake (TYPE iceberg, ENDPOINT 'http://localhost:8181', "
        "AUTHORIZATION_TYPE 'none')"
    ]


def test_s3_storage_without_a_key_creates_no_s3_secret() -> None:
    """A public warehouse, or a credential DuckDB finds on its own."""
    settings = _settings(storage=_storage(key_id=None))

    assert len(prepare_statements(settings, NoAuth())) == 1


def test_a_missing_warehouse_attaches_the_empty_string() -> None:
    sql = _sql(prepare_statements(_settings(warehouse=None), NoAuth()))[-1]

    assert sql.startswith("ATTACH '' AS lake ")


def test_a_quote_in_the_warehouse_name_is_escaped() -> None:
    sql = _sql(prepare_statements(_settings(warehouse="it's"), NoAuth()))[-1]

    assert sql.startswith("ATTACH 'it''s' AS lake ")


# --- the connection ---


def test_the_connection_opens_on_first_use_not_at_construction() -> None:
    opened: list[FakeConnection] = []

    def connect() -> FakeConnection:
        opened.append(FakeConnection())
        return opened[-1]

    session = Session(_settings(), NoAuth(), connect=connect)
    assert opened == []

    session.execute("SELECT 1")

    assert len(opened) == 1
    assert _sql(opened[0].statements)[:3] == ["LOAD httpfs", "LOAD iceberg", "LOAD spatial"]
    assert _sql(opened[0].statements)[-1] == "SELECT 1"


def test_the_session_runs_the_prepared_statements_once() -> None:
    connection = FakeConnection()
    session = Session(_settings(), NoAuth(), connect=lambda: connection)

    session.execute("SELECT 1")
    session.execute("SELECT 2")

    attaches = [sql for sql in _sql(connection.statements) if sql.startswith("ATTACH ")]
    assert len(attaches) == 1


def test_a_failed_load_installs_the_extension_and_loads_again() -> None:
    connection = FakeConnection(fail_on=["LOAD iceberg"])
    session = Session(_settings(), NoAuth(), connect=lambda: connection)

    session.execute("SELECT 1")

    assert _sql(connection.statements)[:5] == [
        "LOAD httpfs",
        "LOAD iceberg",
        "INSTALL iceberg",
        "LOAD iceberg",
        "LOAD spatial",
    ]


def test_execute_returns_the_rows() -> None:
    connection = FakeConnection()
    connection.rows = [(3, "a"), (4, "b")]
    session = Session(_settings(), NoAuth(), connect=lambda: connection)

    assert session.execute("SELECT 1") == [(3, "a"), (4, "b")]
    assert session.fetch_one("SELECT 1") == (3, "a")


def test_a_transaction_commits_at_the_end_of_the_block() -> None:
    connection = FakeConnection()
    session = Session(_settings(), NoAuth(), connect=lambda: connection)

    with session.transaction():
        session.execute("INSERT INTO t VALUES (1)")

    assert _sql(connection.statements)[-3:] == ["BEGIN", "INSERT INTO t VALUES (1)", "COMMIT"]


def test_a_transaction_rolls_back_on_an_error_and_re_raises() -> None:
    connection = FakeConnection(fail_on=["INSERT INTO t VALUES (1)"])
    session = Session(_settings(), NoAuth(), connect=lambda: connection)

    with pytest.raises(duckdb.Error), session.transaction():
        session.execute("INSERT INTO t VALUES (1)")

    assert _sql(connection.statements)[-3:] == ["BEGIN", "INSERT INTO t VALUES (1)", "ROLLBACK"]


def test_close_closes_the_connection_and_a_later_use_reopens() -> None:
    connections: list[FakeConnection] = []

    def connect() -> FakeConnection:
        connections.append(FakeConnection())
        return connections[-1]

    session = Session(_settings(), NoAuth(), connect=connect)
    session.execute("SELECT 1")
    session.close()
    session.close()  # idempotent

    assert connections[0].closed is True
    session.execute("SELECT 2")
    assert len(connections) == 2


def test_the_table_name_is_qualified_with_the_alias_and_the_namespace() -> None:
    session = Session(_settings(), NoAuth(), connect=FakeConnection)

    assert session.table("roads") == f'{CATALOG_ALIAS}."portolake"."roads"'
    assert CATALOG_ALIAS == "lake"


def test_an_old_duckdb_is_refused_with_the_minimum_named(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("portolan_cli.backends.iceberg.session.duckdb.__version__", "1.5.5")
    session = Session(_settings(), NoAuth(), connect=FakeConnection)

    with pytest.raises(SessionError) as info:
        session.execute("SELECT 1")

    assert MINIMUM_DUCKDB in str(info.value)
    assert "1.5.5" in str(info.value)


def test_the_real_extensions_load_without_a_download() -> None:
    """The network guard installed them before the run, so LOAD reads the cache."""
    connection = duckdb.connect()
    try:
        load_extensions(connection)
        loaded = {
            row[0]
            for row in connection.execute(
                "SELECT extension_name FROM duckdb_extensions() WHERE loaded"
            ).fetchall()
        }
    finally:
        connection.close()

    assert {"httpfs", "iceberg", "spatial"} <= loaded
