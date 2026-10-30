"""Tests for iceberg backend discoverability as a built-in backend.

After the merge, iceberg is a built-in backend, not a plugin.
These tests verify that the backend is importable and discoverable
via get_backend("iceberg").
"""

from __future__ import annotations

import pytest


@pytest.mark.unit
def test_get_backend_returns_iceberg_backend(tmp_path):
    """get_backend("iceberg") should return an IcebergBackend instance."""
    from portolan_cli.backends import get_backend
    from portolan_cli.backends.iceberg import IcebergBackend

    # A catalog root keeps the SQLite catalog out of the working directory.
    backend = get_backend("iceberg", catalog_root=tmp_path)
    assert isinstance(backend, IcebergBackend)


@pytest.mark.unit
def test_iceberg_backend_satisfies_versioning_protocol():
    """IcebergBackend should satisfy the VersioningBackend protocol."""
    from portolan_cli.backends.iceberg import IcebergBackend
    from portolan_cli.backends.protocol import VersioningBackend

    backend = IcebergBackend.__new__(IcebergBackend)
    assert isinstance(backend, VersioningBackend)
