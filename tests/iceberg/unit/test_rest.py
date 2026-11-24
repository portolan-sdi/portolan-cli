"""The Iceberg REST catalog client.

DuckDB writes the data. The client does what DuckDB cannot: read the table
metadata document, and commit the metadata-only updates that record a
version (properties and tags), roll back ``main``, and expire snapshots.
Every test runs against an in-process fake server on ``httpx.MockTransport``.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from portolan_cli.backends.iceberg.auth import NoAuth, StaticBearer
from portolan_cli.backends.iceberg.rest import (
    CommitConflictError,
    NoSuchTableError,
    RestCatalog,
    RestCatalogError,
)
from portolan_cli.extract.common.retry import RetryConfig
from tests.iceberg.fake_lake import FakeCatalog

pytestmark = pytest.mark.unit

BASE = "http://catalog.test"
NO_WAIT = RetryConfig(max_attempts=3, initial_delay=0.0, max_delay=0.0)


class FakeServer(FakeCatalog):
    """The fake catalog, with the vended credential the client tests look for."""

    def __init__(self, *, prefix: str | None = None, prefix_in: str = "overrides") -> None:
        super().__init__(prefix=prefix or "", prefix_in=prefix_in)
        self.table_config = {"s3.access-key-id": "vended-key"}


def _client(server: FakeServer, **kwargs: Any) -> RestCatalog:
    kwargs.setdefault("auth", NoAuth())
    kwargs.setdefault("retry", NO_WAIT)
    return RestCatalog(BASE, http=httpx.Client(transport=httpx.MockTransport(server)), **kwargs)


def _paths(server: FakeServer) -> list[str]:
    return [
        f"{r.method} {r.url.path}{'?' + r.url.query.decode() if r.url.query else ''}"
        for r in server.requests
    ]


# ---------------------------------------------------------------- config and prefix


def test_the_prefix_comes_from_the_config_overrides() -> None:
    server = FakeServer(prefix="ws-one")
    server.add_table("ns", "t", {"format-version": 3})

    client = _client(server, warehouse="my-warehouse")
    client.load_table("ns", "t")

    assert client.prefix == "ws-one"
    assert _paths(server) == [
        "GET /v1/config?warehouse=my-warehouse",
        "GET /v1/ws-one/namespaces/ns/tables/t",
    ]


def test_the_prefix_comes_from_the_defaults_when_there_is_no_override() -> None:
    server = FakeServer(prefix="dflt", prefix_in="defaults")

    assert _client(server).prefix == "dflt"


def test_no_prefix_means_paths_under_v1() -> None:
    server = FakeServer()
    server.add_table("ns", "t", {})

    _client(server).load_table("ns", "t")

    assert _paths(server) == ["GET /v1/config", "GET /v1/namespaces/ns/tables/t"]


def test_the_config_is_read_once() -> None:
    server = FakeServer()
    server.add_table("ns", "t", {})
    client = _client(server)

    client.load_table("ns", "t")
    client.load_table("ns", "t")

    assert _paths(server).count("GET /v1/config") == 1


def test_a_trailing_slash_on_the_base_url_is_fine() -> None:
    server = FakeServer()
    client = RestCatalog(
        BASE + "/", auth=NoAuth(), http=httpx.Client(transport=httpx.MockTransport(server))
    )

    client.config()

    assert _paths(server) == ["GET /v1/config"]


# ---------------------------------------------------------------- auth


def test_the_bearer_header_goes_on_every_request() -> None:
    server = FakeServer()
    server.add_table("ns", "t", {})

    _client(server, auth=StaticBearer("tok")).load_table("ns", "t")

    assert {r.headers["authorization"] for r in server.requests} == {"Bearer tok"}


def test_a_401_invalidates_the_token_and_retries_once() -> None:
    class Rotating:
        def __init__(self) -> None:
            self.n = 0
            self.invalidated = 0

        def headers(self) -> dict[str, str]:
            return {"Authorization": f"Bearer tok-{self.n}"}

        def duckdb_secret_options(self) -> dict[str, str]:
            return {}

        def invalidate(self) -> None:
            self.invalidated += 1
            self.n += 1

    server = FakeServer()
    server.add_table("ns", "t", {})
    auth = Rotating()
    client = _client(server, auth=auth)
    client.config()
    server.failures = [401]

    client.load_table("ns", "t")

    assert auth.invalidated == 1
    assert [r.headers["authorization"] for r in server.requests][-2:] == [
        "Bearer tok-0",
        "Bearer tok-1",
    ]


def test_a_second_401_is_an_error() -> None:
    server = FakeServer()
    server.add_table("ns", "t", {})
    client = _client(server, auth=StaticBearer("tok"))
    client.config()
    server.failures = [401, 401]

    with pytest.raises(RestCatalogError) as info:
        client.load_table("ns", "t")

    assert info.value.status == 401


# ---------------------------------------------------------------- namespaces and tables


def test_namespace_exists_uses_head() -> None:
    server = FakeServer()
    server.namespaces.add("yes")
    client = _client(server)

    assert client.namespace_exists("yes") is True
    assert client.namespace_exists("no") is False
    assert _paths(server)[1:] == ["HEAD /v1/namespaces/yes", "HEAD /v1/namespaces/no"]


def test_create_namespace_posts_and_treats_409_as_created() -> None:
    server = FakeServer()
    client = _client(server)

    client.create_namespace("ns")
    client.create_namespace("ns")

    posts = [r for r in server.requests if r.method == "POST"]
    assert len(posts) == 2
    assert json.loads(posts[0].content) == {"namespace": ["ns"], "properties": {}}
    assert "ns" in server.namespaces


def test_table_exists_uses_head() -> None:
    server = FakeServer()
    server.add_table("ns", "t", {})
    client = _client(server)

    assert client.table_exists("ns", "t") is True
    assert client.table_exists("ns", "missing") is False


def test_load_table_returns_location_metadata_and_config() -> None:
    server = FakeServer()
    server.add_table("ns", "t", {"format-version": 3, "current-snapshot-id": 7})

    loaded = _client(server).load_table("ns", "t")

    assert loaded.metadata_location == "s3://warehouse/ns/t/metadata/v1.metadata.json"
    assert loaded.metadata["format-version"] == 3
    assert loaded.config == {"s3.access-key-id": "vended-key"}


def test_load_table_can_ask_for_vended_credentials() -> None:
    server = FakeServer()
    server.add_table("ns", "t", {})

    _client(server).load_table("ns", "t", vended_credentials=True)

    assert server.requests[-1].headers["x-iceberg-access-delegation"] == "vended-credentials"


def test_load_table_sends_no_delegation_header_by_default() -> None:
    server = FakeServer()
    server.add_table("ns", "t", {})

    _client(server).load_table("ns", "t")

    assert "x-iceberg-access-delegation" not in server.requests[-1].headers


def test_a_missing_table_raises_no_such_table() -> None:
    server = FakeServer()
    server.namespaces.add("ns")

    with pytest.raises(NoSuchTableError, match="ns.missing"):
        _client(server).load_table("ns", "missing")


def test_list_tables_returns_the_names() -> None:
    server = FakeServer()
    server.add_table("ns", "a", {})
    server.add_table("ns", "b", {})

    assert _client(server).list_tables("ns") == ["a", "b"]


def test_drop_table_can_purge() -> None:
    server = FakeServer()
    server.add_table("ns", "t", {})
    client = _client(server)

    client.drop_table("ns", "t", purge=True)

    assert _paths(server)[-1] == "DELETE /v1/namespaces/ns/tables/t?purgeRequested=true"
    assert ("ns", "t") not in server.tables


def test_drop_namespace_sends_delete() -> None:
    server = FakeServer()
    server.namespaces.add("ns")

    _client(server).drop_namespace("ns")

    assert _paths(server)[-1] == "DELETE /v1/namespaces/ns"


def test_namespace_names_are_url_encoded() -> None:
    server = FakeServer()

    _client(server).namespace_exists("a b")

    assert server.requests[-1].url.raw_path == b"/v1/namespaces/a%20b"


# ---------------------------------------------------------------- commits


def test_commit_table_sends_requirements_and_updates_verbatim() -> None:
    server = FakeServer()
    # The requirements below name this uuid and this snapshot; the fake checks them.
    server.add_table("ns", "t", {"table-uuid": "u", "current-snapshot-id": 7})
    requirements: list[dict[str, Any]] = [
        {"type": "assert-table-uuid", "uuid": "u"},
        {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 7},
    ]
    updates: list[dict[str, Any]] = [
        {"action": "set-properties", "updates": {"portolan.version.1.0.0": "{}"}},
        {"action": "set-snapshot-ref", "ref-name": "v1.0.0", "type": "tag", "snapshot-id": 7},
    ]

    result = _client(server).commit_table("ns", "t", requirements=requirements, updates=updates)

    commit = server.requests[-1]
    assert commit.method == "POST"
    assert commit.url.path == "/v1/namespaces/ns/tables/t"
    assert json.loads(commit.content) == {"requirements": requirements, "updates": updates}
    assert result.metadata_location.endswith("v2.metadata.json")


def test_a_409_on_commit_is_a_conflict() -> None:
    server = FakeServer()
    server.add_table("ns", "t", {})
    server.commit_status = 409

    with pytest.raises(CommitConflictError, match="ns.t"):
        _client(server).commit_table("ns", "t", requirements=[], updates=[])


def test_a_commit_is_not_retried_on_a_server_error() -> None:
    """A 5xx after a commit may mean the commit landed. The caller reloads."""
    server = FakeServer()
    server.add_table("ns", "t", {})
    server.commit_status = 500

    with pytest.raises(RestCatalogError) as info:
        _client(server).commit_table("ns", "t", requirements=[], updates=[])

    assert info.value.status == 500
    assert [r.method for r in server.requests].count("POST") == 1


def test_a_commit_on_a_missing_table_raises_no_such_table() -> None:
    server = FakeServer()
    server.namespaces.add("ns")

    with pytest.raises(NoSuchTableError):
        _client(server).commit_table("ns", "gone", requirements=[], updates=[])


# ---------------------------------------------------------------- retries and errors


def test_a_read_is_retried_on_a_transient_status() -> None:
    server = FakeServer()
    server.add_table("ns", "t", {})
    client = _client(server)
    client.config()
    server.failures = [503, 429]

    client.load_table("ns", "t")

    assert _paths(server).count("GET /v1/namespaces/ns/tables/t") == 3


def test_a_read_gives_up_after_the_configured_attempts() -> None:
    server = FakeServer()
    server.add_table("ns", "t", {})
    client = _client(server)
    client.config()
    server.failures = [503, 503, 503]

    with pytest.raises(RestCatalogError) as info:
        client.load_table("ns", "t")

    assert info.value.status == 503
    assert _paths(server).count("GET /v1/namespaces/ns/tables/t") == 3


def test_a_transport_error_is_retried() -> None:
    calls = {"n": 0}
    inner = FakeServer()
    inner.add_table("ns", "t", {})

    def flaky(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 2:
            raise httpx.ConnectError("boom", request=request)
        return inner(request)

    client = RestCatalog(
        BASE, auth=NoAuth(), retry=NO_WAIT, http=httpx.Client(transport=httpx.MockTransport(flaky))
    )

    client.load_table("ns", "t")

    assert calls["n"] == 3


def test_an_error_message_names_method_path_status_and_server_text() -> None:
    server = FakeServer()
    server.add_table("ns", "t", {})
    client = _client(server)
    client.config()
    server.failures = [400]

    with pytest.raises(RestCatalogError) as info:
        client.load_table("ns", "t")

    message = str(info.value)
    assert "GET" in message
    assert "/v1/namespaces/ns/tables/t" in message
    assert "400" in message
    assert "later" in message
    assert info.value.status == 400


def test_an_error_message_never_carries_the_authorization_header() -> None:
    server = FakeServer()
    server.failures = [400]

    with pytest.raises(RestCatalogError) as info:
        _client(server, auth=StaticBearer("super-secret-token")).config()

    assert "super-secret-token" not in str(info.value)
