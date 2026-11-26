"""E2E test fixtures for the Iceberg backend against the compose stack.

The lifecycle and the inspection helpers live in ``tests/iceberg_stack.py``.
This file turns them into fixtures and adds the PyIceberg catalog that the
current backend still takes at construction.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.catalog import load_catalog

from portolan_cli.backends.iceberg.backend import IcebergBackend
from tests.iceberg_stack import (
    S3_HOST,
    IcebergLane,
    RestAdmin,
    compose_down,
    compose_up,
    docker_lane,
    require_docker,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from pyiceberg.catalog import Catalog


@pytest.fixture(scope="session")
def iceberg_lane() -> IcebergLane:
    """The lane every e2e test in this session writes to."""
    return docker_lane()


@pytest.fixture(scope="session")
def docker_services(iceberg_lane: IcebergLane) -> Iterator[None]:
    """Start REST catalog + RustFS via docker-compose, tear down after session."""
    require_docker()
    compose_up()
    try:
        yield
    finally:
        compose_down()


@pytest.fixture(scope="session")
def rest_admin(docker_services: None, iceberg_lane: IcebergLane) -> RestAdmin:
    """The product's REST client, pointed at the running stack."""
    _ = docker_services
    return RestAdmin(iceberg_lane)


@pytest.fixture
def rest_catalog(docker_services: None, iceberg_lane: IcebergLane) -> Catalog:
    """PyIceberg catalog pointing at the dockerized REST catalog + RustFS."""
    _ = docker_services
    return load_catalog(
        "e2e-test",
        **{
            "type": "rest",
            "uri": iceberg_lane.uri,
            "s3.endpoint": f"http://{S3_HOST}",
            "s3.access-key-id": iceberg_lane.key,
            "s3.secret-access-key": iceberg_lane.secret,
        },
    )


@pytest.fixture
def rest_iceberg_backend(rest_catalog: Catalog) -> IcebergBackend:
    """IcebergBackend with REST catalog injected."""
    return IcebergBackend(catalog=rest_catalog)


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
