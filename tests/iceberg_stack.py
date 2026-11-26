"""The Iceberg REST stack the e2e tests write to, and the helpers that inspect it.

Lane A is the docker-compose stack in ``tests/iceberg/e2e/docker-compose.yml``:
the Apache REST fixture on port 8181 and RustFS on port 9000. It is
deterministic and needs no secret, so it is the catalog of PR CI. A second
lane, a hosted REST catalog service, reuses the same helpers with its own
endpoint and credential; it arrives with the backend rewrite.

This module holds no fixture. ``tests/iceberg/e2e/conftest.py`` declares the
fixtures and calls these functions, so a test in another directory can do
the same later without a second copy of the compose lifecycle.

The helpers go through the product's own REST client
(``portolan_cli.backends.iceberg.rest``) and through DuckDB's ``httpfs``, not
through boto3, so the tests exercise the same S3 settings the backend will
use and need no extra dependency.
"""

from __future__ import annotations

import shutil
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pytest

from portolan_cli.backends.iceberg.auth import NoAuth, TokenProvider
from portolan_cli.backends.iceberg.rest import LoadTableResult, RestCatalog

COMPOSE_FILE = Path(__file__).parent / "iceberg" / "e2e" / "docker-compose.yml"

REST_URI = "http://localhost:8181"
S3_HOST = "localhost:9000"
S3_KEY = "rustfsadmin"
S3_SECRET = "rustfsadmin"  # noqa: S105  # the fixture's published test credential
WAREHOUSE_BUCKET = "warehouse"


@dataclass(frozen=True)
class IcebergLane:
    """One REST catalog and the object store behind it.

    Attributes:
        name: ``docker`` for the compose stack.
        uri: The REST catalog.
        warehouse: The warehouse name for ``GET /v1/config``, or None.
        s3_host: The S3 endpoint host, with an optional port and no scheme.
        s3_use_ssl: Whether the S3 endpoint speaks HTTPS.
        key: The S3 access key.
        secret: The S3 secret key.
        bucket: The bucket that holds the warehouse.
    """

    name: str
    uri: str
    warehouse: str | None
    s3_host: str
    s3_use_ssl: bool
    key: str
    secret: str
    bucket: str

    def token_provider(self) -> TokenProvider:
        """Return the catalog credential. The compose stack takes none."""
        return NoAuth()


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
        self.catalog = RestCatalog(lane.uri, auth=lane.token_provider(), warehouse=lane.warehouse)

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


def warehouse_objects(lane: IcebergLane, prefix: str) -> list[str]:
    """Return the object keys under ``prefix`` in the lane's warehouse bucket.

    DuckDB's ``httpfs`` lists them with the same S3 settings the backend
    uses, so a test that finds a file here proves the warehouse is readable
    the way DuckDB reads it.
    """
    connection = duckdb.connect()
    try:
        connection.execute("LOAD httpfs")
        connection.execute(
            "CREATE SECRET lane (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, URL_STYLE 'path', "
            "USE_SSL ?, REGION 'us-east-1')",
            [lane.key, lane.secret, lane.s3_host, lane.s3_use_ssl],
        )
        pattern = f"s3://{lane.bucket}/{prefix}**"
        rows = connection.execute("SELECT file FROM glob(?)", [pattern]).fetchall()
    finally:
        connection.close()
    head = f"s3://{lane.bucket}/"
    return sorted(str(row[0]).removeprefix(head) for row in rows)


def scan_row_count(lane: IcebergLane, metadata_location: str) -> int:
    """Return the row count of an Iceberg table read from its metadata file.

    This is the reader path a user has: ``iceberg_scan`` over the metadata
    location the collection publishes, with no catalog server.
    """
    connection = duckdb.connect()
    try:
        connection.execute("LOAD httpfs")
        connection.execute("LOAD iceberg")
        connection.execute(
            "CREATE SECRET lane (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, URL_STYLE 'path', "
            "USE_SSL ?, REGION 'us-east-1')",
            [lane.key, lane.secret, lane.s3_host, lane.s3_use_ssl],
        )
        row = connection.execute(
            "SELECT count(*) FROM iceberg_scan(?)", [metadata_location]
        ).fetchone()
    finally:
        connection.close()
    assert row is not None
    return int(row[0])
