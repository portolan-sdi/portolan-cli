"""Tests for IcebergBackend.pull() method.

Moved from portolan-cli's test_pull_iceberg.py. The pull method uses
backend.get_current_version() to get asset info, then downloads files
from {remote_url}/{href}.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@pytest.mark.integration
def test_pull_calls_get_current_version(iceberg_backend, parquet_file):
    """pull() should call get_current_version() for asset info."""
    iceberg_backend.publish(
        collection="boundaries",
        assets={"item1/data.parquet": str(parquet_file)},
        schema={"columns": ["id"], "types": {"id": "int64"}, "hash": "x"},
        breaking=False,
        message="test",
    )

    with patch("portolan_cli.sync.download.download_file") as mock_download:
        mock_download.return_value = MagicMock(success=True, files_downloaded=1)
        result = iceberg_backend.pull(
            remote_url="gs://test-bucket/catalog",
            local_root=Path("/tmp/test"),
            collection="boundaries",
        )

    assert result.success is True
    assert result.files_downloaded >= 1


@pytest.mark.integration
def test_pull_downloads_from_remote_plus_href(iceberg_backend, parquet_file):
    """Files should be downloaded from {remote_url}/{asset.href}."""
    iceberg_backend.publish(
        collection="boundaries",
        assets={"item1/data.parquet": str(parquet_file)},
        schema={"columns": ["id"], "types": {"id": "int64"}, "hash": "x"},
        breaking=False,
        message="test",
    )

    with patch("portolan_cli.sync.download.download_file") as mock_download:
        mock_download.return_value = MagicMock(success=True, files_downloaded=1)
        iceberg_backend.pull(
            remote_url="gs://test-bucket/catalog",
            local_root=Path("/tmp/test"),
            collection="boundaries",
        )

    source_arg = mock_download.call_args.kwargs["source"]
    assert source_arg == "gs://test-bucket/catalog/boundaries/item1/data.parquet"


@pytest.mark.integration
def test_pull_saves_to_local_path(iceberg_backend, parquet_file, tmp_path):
    """Downloaded files should be saved to local_root/{href}."""
    iceberg_backend.publish(
        collection="boundaries",
        assets={"item1/data.parquet": str(parquet_file)},
        schema={"columns": ["id"], "types": {"id": "int64"}, "hash": "x"},
        breaking=False,
        message="test",
    )

    local_root = tmp_path / "local_catalog"
    local_root.mkdir()

    with patch("portolan_cli.sync.download.download_file") as mock_download:
        mock_download.return_value = MagicMock(success=True, files_downloaded=1)
        iceberg_backend.pull(
            remote_url="gs://test-bucket/catalog",
            local_root=local_root,
            collection="boundaries",
        )

    dest_arg = mock_download.call_args.kwargs["destination"]
    assert dest_arg == local_root / "boundaries" / "item1" / "data.parquet"


@pytest.mark.integration
def test_pull_no_versions_returns_up_to_date(iceberg_backend):
    """pull() should handle missing collection gracefully."""
    result = iceberg_backend.pull(
        remote_url="gs://test-bucket/catalog",
        local_root=Path("/tmp/test"),
        collection="nonexistent",
    )

    assert result.success is True
    assert result.files_downloaded == 0
    assert result.up_to_date is True


@pytest.mark.integration
def test_pull_dry_run_no_download(iceberg_backend, parquet_file):
    """Dry run should report what would happen without downloading."""
    iceberg_backend.publish(
        collection="boundaries",
        assets={"item1/data.parquet": str(parquet_file)},
        schema={"columns": ["id"], "types": {"id": "int64"}, "hash": "x"},
        breaking=False,
        message="test",
    )

    with patch("portolan_cli.sync.download.download_file") as mock_download:
        result = iceberg_backend.pull(
            remote_url="gs://test-bucket/catalog",
            local_root=Path("/tmp/test"),
            collection="boundaries",
            dry_run=True,
        )

    mock_download.assert_not_called()
    assert result.success is True
    assert result.dry_run is True


@pytest.mark.integration
def test_pull_reports_failed_downloads(iceberg_backend, parquet_file):
    """pull() should report failures when downloads fail."""
    iceberg_backend.publish(
        collection="boundaries",
        assets={"item1/data.parquet": str(parquet_file)},
        schema={"columns": ["id"], "types": {"id": "int64"}, "hash": "x"},
        breaking=False,
        message="test",
    )

    with patch("portolan_cli.sync.download.download_file") as mock_download:
        mock_download.return_value = MagicMock(success=False, files_downloaded=0)
        result = iceberg_backend.pull(
            remote_url="gs://test-bucket/catalog",
            local_root=Path("/tmp/test"),
            collection="boundaries",
        )

    assert result.success is False


@pytest.mark.integration
def test_pull_without_collection_pulls_every_collection(tmp_path, monkeypatch):
    """#977: `portolan pull <remote>` with no --collection exits 0 on Iceberg.

    On main the backend got collection=None and raised "Collection name cannot
    be empty". The catalog holds one collection with no Iceberg table yet, so
    the pull reports it as having no versions.
    """
    import json

    from click.testing import CliRunner

    from portolan_cli.cli import cli

    root = tmp_path / "catalog"
    root.mkdir()
    monkeypatch.chdir(root)
    runner = CliRunner()
    init = runner.invoke(cli, ["init", "--backend", "iceberg", "--license", "CC0-1.0", "--auto"])
    assert init.exit_code == 0, init.output
    (root / "demo").mkdir()
    (root / "demo" / "collection.json").write_text(
        json.dumps({"type": "Collection", "id": "demo"}), encoding="utf-8"
    )

    result = runner.invoke(cli, ["pull", "s3://example-bucket/catalog"])

    assert result.exit_code == 0, result.output
    assert "No versions found for collection 'demo'" in result.output
