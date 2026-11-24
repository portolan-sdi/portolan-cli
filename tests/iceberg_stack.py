"""The Iceberg REST stack the e2e tests write to, and the helpers that inspect it.

Lane A is the docker-compose stack in ``tests/iceberg/e2e/docker-compose.yml``:
the Apache REST fixture on port 8181 and RustFS on port 9000. It is
deterministic and needs no secret, so it is the catalog of PR CI.

Lane B is a hosted REST catalog. ``PORTOLAN_TEST_ICEBERG_LANE=hosted`` selects
it, and the lane reads the variables the backend itself reads
(``PORTOLAN_ICEBERG_URI`` and the others in ``docs/guides/iceberg.md``). The
public repository holds no credential for it. A maintainer runs it with the
key pair of a test catalog exported in the shell::

    PORTOLAN_TEST_ICEBERG_LANE=hosted uv run pytest tests/ -m "e2e and not e2e_slow"

Each hosted test namespace is ``portolan_ci_<unix time>_<hex>``. A run that
stops before its teardown leaves its namespace behind, so the next run drops
each such namespace older than ``SWEEP_AFTER_SECONDS``.

This module holds no fixture. ``tests/conftest.py`` declares the fixtures
and calls these functions, so a test in any directory gets the stack
without a second copy of the compose lifecycle. Each test gets its own
namespace, named by ``new_namespace``, and the fixture drops it with its
tables afterwards.

The helpers go through the product's own REST client
(``portolan_cli.backends.iceberg.rest``) and through DuckDB's ``httpfs``, not
through boto3, so the tests exercise the same S3 settings the backend uses
and need no extra dependency.
"""

from __future__ import annotations

import re
import secrets
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb
import httpx
import pytest

from portolan_cli.backends.iceberg.auth import NoAuth, OAuth2ClientCredentials, TokenProvider
from portolan_cli.backends.iceberg.rest import LoadTableResult, RestCatalog

if TYPE_CHECKING:
    from collections.abc import Mapping

COMPOSE_FILE = Path(__file__).parent / "iceberg" / "e2e" / "docker-compose.yml"

REST_URI = "http://localhost:8181"
S3_HOST = "localhost:9000"
S3_KEY = "rustfsadmin"
S3_SECRET = "rustfsadmin"  # noqa: S105  # the fixture's published test credential
WAREHOUSE_BUCKET = "warehouse"

#: Set on the process by the stack fixture. The CLI harness keeps the
#: ``PORTOLAN_ICEBERG_*`` variables when it is set, and strips them otherwise.
STACK_SENTINEL = "PORTOLAN_TEST_ICEBERG_STACK"

#: Selects the lane: ``docker`` (the default) or ``hosted``.
LANE_VARIABLE = "PORTOLAN_TEST_ICEBERG_LANE"
#: The bucket of the hosted warehouse, when its name differs from the warehouse.
BUCKET_VARIABLE = "PORTOLAN_TEST_ICEBERG_BUCKET"
#: The age after which a run drops a hosted test namespace that another run left.
SWEEP_AFTER_SECONDS = 6 * 3600
_HOSTED_NAMESPACE = re.compile(r"^portolan_ci_(\d+)_[0-9a-f]+$")


@dataclass(frozen=True)
class IcebergLane:
    """One REST catalog and the object store behind it.

    Attributes:
        name: ``docker`` for the compose stack, ``hosted`` for a hosted catalog.
        uri: The REST catalog.
        warehouse: The warehouse name for ``GET /v1/config``, or None.
        s3_host: The S3 endpoint host, with an optional port and no scheme.
        s3_use_ssl: Whether the S3 endpoint speaks HTTPS.
        key: The S3 access key.
        secret: The S3 secret key.
        bucket: The bucket that holds the warehouse.
        region: The S3 region.
        client_id: The OAuth2 client id of the catalog, or None for no auth.
        client_secret: The OAuth2 client secret of the catalog.
    """

    name: str
    uri: str
    warehouse: str | None
    s3_host: str
    s3_use_ssl: bool
    key: str = field(repr=False)
    secret: str = field(repr=False)
    bucket: str
    region: str = "us-east-1"
    client_id: str | None = None
    client_secret: str | None = field(default=None, repr=False)

    @property
    def authorization_type(self) -> str:
        """The ``iceberg:authorization_type`` that the backend writes on this lane."""
        return "none" if self.client_id is None else "oauth2"

    @property
    def bucket_url(self) -> str:
        """The ``s3://`` URL of the warehouse bucket, with a trailing slash."""
        return f"s3://{self.bucket}/"

    def published_location(self, location: str) -> str:
        """Return the metadata location as ``collection.json`` publishes it.

        An S3 endpoint over TLS serves the file over HTTPS, so the backend
        writes the https form. The compose stack has no TLS.
        """
        if self.s3_use_ssl and location.startswith(self.bucket_url):
            return f"https://{self.s3_host}/{location.removeprefix('s3://')}"
        return location

    def token_provider(self) -> TokenProvider:
        """Return the catalog credential. The compose stack takes none."""
        if self.client_id is None or self.client_secret is None:
            return NoAuth()
        return OAuth2ClientCredentials(
            f"{self.uri.rstrip('/')}/v1/oauth/tokens",
            client_id=self.client_id,
            client_secret=self.client_secret,
        )


def docker_lane() -> IcebergLane:
    """Return the compose stack as a lane."""
    return IcebergLane(
        name="docker",
        uri=REST_URI,
        warehouse=None,
        s3_host=S3_HOST,
        s3_use_ssl=False,
        key=S3_KEY,
        secret=S3_SECRET,
        bucket=WAREHOUSE_BUCKET,
    )


def hosted_lane(env: Mapping[str, str]) -> IcebergLane:
    """Return the hosted catalog that the backend variables in ``env`` describe.

    Skips the test when a required variable is unset.
    """
    required = (
        "PORTOLAN_ICEBERG_URI",
        "PORTOLAN_ICEBERG_WAREHOUSE",
        "PORTOLAN_S3_ENDPOINT",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
    )
    missing = [name for name in required if not env.get(name)]
    if missing:
        pytest.skip(f"the hosted lane needs {', '.join(missing)}")
    warehouse = env["PORTOLAN_ICEBERG_WAREHOUSE"]
    return IcebergLane(
        name="hosted",
        uri=env["PORTOLAN_ICEBERG_URI"],
        warehouse=warehouse,
        s3_host=env["PORTOLAN_S3_ENDPOINT"],
        s3_use_ssl=env.get("PORTOLAN_S3_USE_SSL", "true").lower() != "false",
        key=env["AWS_ACCESS_KEY_ID"],
        secret=env["AWS_SECRET_ACCESS_KEY"],
        bucket=env.get(BUCKET_VARIABLE) or warehouse,
        region=env.get("AWS_REGION") or "us-east-1",
        client_id=env.get("PORTOLAN_ICEBERG_CLIENT_ID"),
        client_secret=env.get("PORTOLAN_ICEBERG_CLIENT_SECRET"),
    )


def select_lane(env: Mapping[str, str]) -> IcebergLane:
    """Return the lane that ``PORTOLAN_TEST_ICEBERG_LANE`` names in ``env``."""
    name = env.get(LANE_VARIABLE, "docker")
    if name == "docker":
        return docker_lane()
    if name == "hosted":
        return hosted_lane(env)
    raise pytest.UsageError(f"{LANE_VARIABLE} must be docker or hosted, not {name!r}")


def lane_is_hosted(env: Mapping[str, str]) -> bool:
    """Whether ``env`` selects the hosted lane."""
    return env.get(LANE_VARIABLE) == "hosted"


def new_namespace(lane: IcebergLane) -> str:
    """Return a namespace name no other test uses.

    The compose stack gets ``t`` and ten hex digits. A hosted catalog gets
    ``portolan_ci_<unix time>_<hex>``, so a later run can drop a namespace
    that a stopped run left behind.
    """
    if lane.name == "hosted":
        return f"portolan_ci_{int(time.time())}_{secrets.token_hex(5)}"
    return "t" + secrets.token_hex(5)


def stack_environment(lane: IcebergLane, namespace: str) -> dict[str, str]:
    """Return the variables that point the backend at ``lane`` and ``namespace``.

    The backend reads ``PORTOLAN_ICEBERG_*`` and the S3 conventions of
    ``upload_file``. The sentinel tells the CLI harness to keep them.
    """
    env = {
        "PORTOLAN_ICEBERG_URI": lane.uri,
        "PORTOLAN_ICEBERG_NAMESPACE": namespace,
        "PORTOLAN_ICEBERG_AUTH": "none" if lane.client_id is None else "oauth2",
        "PORTOLAN_S3_ENDPOINT": lane.s3_host,
        "PORTOLAN_S3_USE_SSL": "true" if lane.s3_use_ssl else "false",
        "AWS_ACCESS_KEY_ID": lane.key,
        "AWS_SECRET_ACCESS_KEY": lane.secret,
        "AWS_REGION": lane.region,
        STACK_SENTINEL: "1",
    }
    if lane.warehouse:
        env["PORTOLAN_ICEBERG_WAREHOUSE"] = lane.warehouse
    if lane.client_id is not None and lane.client_secret is not None:
        env["PORTOLAN_ICEBERG_CLIENT_ID"] = lane.client_id
        env["PORTOLAN_ICEBERG_CLIENT_SECRET"] = lane.client_secret
    return env


def require_docker() -> None:
    """Skip the test when the docker binary is absent.

    A machine without Docker raised FileNotFoundError from subprocess, which
    reported each e2e test as an error. CI installs Docker, so the e2e jobs
    still run these tests.
    """
    if shutil.which("docker") is None:
        pytest.skip("the e2e tests need the docker binary on PATH")


def wait_for_service(url: str, timeout: int = 60) -> None:
    """Poll a URL until it answers below 400, or raise after ``timeout`` seconds."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            response = urllib.request.urlopen(url, timeout=5)  # noqa: S310  # fixed localhost URL
            if response.status < 400:
                return
        except (urllib.error.URLError, OSError):
            pass
        time.sleep(2)
    raise TimeoutError(f"Service at {url} not ready after {timeout}s")


def compose_up() -> None:
    """Start the compose stack and wait for both services."""
    subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "up", "-d", "--wait"],
        check=True,
        timeout=120,
        capture_output=True,
    )
    wait_for_service(f"{REST_URI}/v1/config", timeout=60)
    wait_for_service(f"http://{S3_HOST}/health/live", timeout=60)


def compose_down() -> None:
    """Stop the compose stack and drop its volumes."""
    subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "down", "-v"],
        check=True,
        timeout=60,
        capture_output=True,
    )


class RestAdmin:
    """Read and clean a lane's catalog through the product's REST client."""

    def __init__(self, lane: IcebergLane) -> None:
        self.lane = lane
        self.auth = lane.token_provider()
        self.catalog = RestCatalog(lane.uri, auth=self.auth, warehouse=lane.warehouse)

    def load_table(self, namespace: str, name: str) -> LoadTableResult:
        """Return the table's metadata location, metadata, and config."""
        return self.catalog.load_table(namespace, name)

    def drop_namespace(self, namespace: str) -> None:
        """Drop every table of the namespace, with its files, then the namespace."""
        if not self.catalog.namespace_exists(namespace):
            return
        for name in self.catalog.list_tables(namespace):
            self.catalog.drop_table(namespace, name, purge=True)
        self.catalog.drop_namespace(namespace)

    def list_namespaces(self) -> list[str]:
        """Return the top-level namespaces of the catalog."""
        prefix = self.catalog.prefix
        path = f"/v1/{prefix}/namespaces" if prefix else "/v1/namespaces"
        response = httpx.get(
            self.lane.uri.rstrip("/") + path, headers=self.auth.headers(), timeout=30
        )
        response.raise_for_status()
        return [".".join(parts) for parts in response.json().get("namespaces", [])]

    def sweep(self, now: float | None = None) -> list[str]:
        """Drop each hosted test namespace older than ``SWEEP_AFTER_SECONDS``.

        Returns:
            The namespaces dropped.
        """
        cutoff = (time.time() if now is None else now) - SWEEP_AFTER_SECONDS
        stale = [
            name
            for name in self.list_namespaces()
            if (match := _HOSTED_NAMESPACE.match(name)) and int(match.group(1)) < cutoff
        ]
        for name in stale:
            self.drop_namespace(name)
        return stale


def _connection(lane: IcebergLane, *, iceberg: bool) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute("LOAD httpfs")
    if iceberg:
        connection.execute("LOAD iceberg")
        connection.execute("LOAD spatial")
    connection.execute(
        "CREATE SECRET lane (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, URL_STYLE 'path', "
        "USE_SSL ?, REGION ?)",
        [lane.key, lane.secret, lane.s3_host, lane.s3_use_ssl, lane.region],
    )
    return connection


def warehouse_objects(lane: IcebergLane, prefix: str) -> list[str]:
    """Return the object keys under ``prefix`` in the lane's warehouse bucket.

    DuckDB's ``httpfs`` lists them with the same S3 settings the backend
    uses, so a test that finds a file here proves the warehouse is readable
    the way DuckDB reads it.
    """
    connection = _connection(lane, iceberg=False)
    try:
        pattern = f"s3://{lane.bucket}/{prefix}**"
        rows = connection.execute("SELECT file FROM glob(?)", [pattern]).fetchall()
    finally:
        connection.close()
    head = f"s3://{lane.bucket}/"
    return sorted(str(row[0]).removeprefix(head) for row in rows)


def scan_rows(
    lane: IcebergLane, metadata_location: str, sql: str = "SELECT count(*) FROM t"
) -> list[tuple[Any, ...]]:
    """Run ``sql`` over the table read from its metadata file, as ``t``.

    This is the reader path a user has: ``iceberg_scan`` over the metadata
    location the collection publishes, with no catalog server.
    """
    connection = _connection(lane, iceberg=True)
    try:
        # A view definition takes no bound parameter; the location is a quoted literal.
        location = metadata_location.replace("'", "''")
        connection.execute(f"CREATE VIEW t AS SELECT * FROM iceberg_scan('{location}')")
        return connection.execute(sql).fetchall()
    finally:
        connection.close()


def scan_row_count(lane: IcebergLane, metadata_location: str) -> int:
    """Return the row count of an Iceberg table read from its metadata file."""
    rows = scan_rows(lane, metadata_location)
    return int(rows[0][0])


@dataclass(frozen=True)
class IcebergStack:
    """One test's view of the stack: the lane, its namespace, and the admin client."""

    lane: IcebergLane
    namespace: str
    admin: RestAdmin

    def load_table(self, name: str) -> LoadTableResult:
        """The ``loadTable`` answer for a collection's table."""
        return self.admin.load_table(self.namespace, name)

    def metadata(self, name: str) -> dict[str, Any]:
        """The metadata document of a collection's table."""
        return self.load_table(name).metadata

    def objects(self, name: str, prefix: str = "") -> list[str]:
        """The warehouse objects of a collection's table, under ``prefix``."""
        return warehouse_objects(self.lane, f"{self.namespace}/{name}/{prefix}")

    def row_count(self, name: str) -> int:
        """The row count of the table as a serverless reader sees it."""
        return scan_row_count(self.lane, self.load_table(name).metadata_location)

    def rows(self, name: str, sql: str) -> list[tuple[Any, ...]]:
        """Run ``sql`` over the table read as ``t`` from its metadata file."""
        return scan_rows(self.lane, self.load_table(name).metadata_location, sql)
