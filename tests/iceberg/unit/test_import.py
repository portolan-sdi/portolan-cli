"""The Iceberg backend package imports with the core dependencies alone."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit


def test_import_iceberg_backend() -> None:
    from portolan_cli.backends.iceberg import IcebergBackend

    assert IcebergBackend.__name__ == "IcebergBackend"


def test_import_iceberg_version() -> None:
    from portolan_cli.backends.iceberg import __version__

    assert isinstance(__version__, str)


def test_the_writer_and_the_client_are_core_dependencies() -> None:
    """DuckDB writes the tables and httpx talks to the catalog. Neither is an extra."""
    import duckdb
    import httpx

    assert duckdb.__version__
    assert httpx.__version__
