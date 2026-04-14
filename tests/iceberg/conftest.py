"""Shared test fixtures for iceberg backend tests."""

import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.catalog import load_catalog

# PyIceberg's SQL catalog resolves file:// warehouse URIs incorrectly on Windows,
# producing paths like /C:/Users/... which PyArrow's local filesystem rejects.
# Upstream: https://github.com/apache/iceberg-python/issues/1005 (closed, fix never merged)
# PyIceberg doesn't run CI on Windows: https://github.com/apache/iceberg-python/issues/2477


@pytest.fixture
def iceberg_catalog(tmp_path):
    """Create a temporary Iceberg catalog backed by SQLite."""
    if sys.platform == "win32":
        pytest.skip(
            "PyIceberg SQL catalog warehouse paths broken on Windows"
            " (https://github.com/apache/iceberg-python/issues/1005)"
        )
    warehouse_uri = (tmp_path / "warehouse").as_uri()
    return load_catalog(
        "test",
        **{
            "type": "sql",
            "uri": f"sqlite:///{tmp_path}/catalog.db",
            "warehouse": warehouse_uri,
        },
    )


@pytest.fixture
def iceberg_backend(iceberg_catalog):
    """Create an IcebergBackend using a temporary SQLite catalog."""
    from portolan_cli.backends.iceberg.backend import IcebergBackend

    return IcebergBackend(catalog=iceberg_catalog)


@pytest.fixture
def parquet_file(tmp_path):
    """Create a simple Parquet file with 3 rows for testing."""
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["alpha", "beta", "gamma"], type=pa.string()),
            "value": pa.array([10.5, 20.0, 30.7], type=pa.float64()),
        }
    )
    path = tmp_path / "test_data.parquet"
    pq.write_table(table, path)
    return path


@pytest.fixture
def parquet_file_v2(tmp_path):
    """Create a second Parquet file with 5 rows (different data) for testing updates."""
    table = pa.table(
        {
            "id": pa.array([10, 20, 30, 40, 50], type=pa.int64()),
            "name": pa.array(["one", "two", "three", "four", "five"], type=pa.string()),
            "value": pa.array([1.1, 2.2, 3.3, 4.4, 5.5], type=pa.float64()),
        }
    )
    path = tmp_path / "test_data_v2.parquet"
    pq.write_table(table, path)
    return path
