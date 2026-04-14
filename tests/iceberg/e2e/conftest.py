"""E2E test fixtures for iceberg backend with Docker (REST catalog + MinIO).

Session-scoped Docker lifecycle: starts docker-compose before tests,
tears down after. Provides REST catalog and MinIO S3 client fixtures.
"""

import subprocess
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.catalog import load_catalog

from portolan_cli.backends.iceberg.backend import IcebergBackend

COMPOSE_FILE = Path(__file__).parent / "docker-compose.yml"

REST_URI = "http://localhost:8181"
S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"


def _wait_for_service(url: str, timeout: int = 60) -> None:
    """Poll a URL until it responds with 2xx or timeout."""
    import urllib.error
    import urllib.request

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = urllib.request.urlopen(url, timeout=5)
            if resp.status < 400:
                return
        except (urllib.error.URLError, OSError):
            pass
        time.sleep(2)
    raise TimeoutError(f"Service at {url} not ready after {timeout}s")


@pytest.fixture(scope="session")
def docker_services():
    """Start REST catalog + MinIO via docker-compose, tear down after session."""
    subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "up", "-d", "--wait"],
        check=True,
        timeout=120,
        capture_output=True,
    )

    try:
        _wait_for_service(f"{REST_URI}/v1/config", timeout=60)
        _wait_for_service(f"{S3_ENDPOINT}/minio/health/live", timeout=60)
        yield
    finally:
        subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE_FILE), "down", "-v"],
            check=True,
            timeout=60,
            capture_output=True,
        )


@pytest.fixture
def rest_catalog(docker_services):  # noqa: ARG001
    """PyIceberg catalog pointing at the dockerized REST catalog + MinIO."""
    _ = docker_services  # Fixture dependency: ensures Docker services are running
    return load_catalog(
        "e2e-test",
        **{
            "type": "rest",
            "uri": REST_URI,
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": S3_ACCESS_KEY,
            "s3.secret-access-key": S3_SECRET_KEY,
        },
    )


@pytest.fixture
def rest_iceberg_backend(rest_catalog):
    """IcebergBackend with REST catalog injected."""
    return IcebergBackend(catalog=rest_catalog)


@pytest.fixture
def minio_client(docker_services):  # noqa: ARG001
    """boto3 S3 client for direct MinIO verification."""
    _ = docker_services  # Fixture dependency: ensures Docker services are running
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
    )


def write_test_parquet(path: Path, rows: int = 3) -> Path:
    """Write a simple Parquet file for testing. Returns the path."""
    table = pa.table(
        {
            "id": pa.array(range(rows), type=pa.int64()),
            "val": pa.array([f"row_{i}" for i in range(rows)], type=pa.string()),
        }
    )
    pq.write_table(table, path)
    return path
