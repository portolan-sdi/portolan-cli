"""Tests for pull command with iceberg backend.

Phase 4 of PLAN-portolake-remote-mode: pull should work with iceberg backend
by reading asset info from the backend's get_current_version() and downloading
files from {remote}/{href}.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def catalog_with_iceberg_and_remote(tmp_path: Path) -> Path:
    """Create a catalog with backend=iceberg and remote configured."""
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()
    (catalog_root / "catalog.json").write_text('{"type": "Catalog"}')

    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text(
        "backend: iceberg\nremote: gs://test-bucket/catalog\n"
    )
    return catalog_root


@pytest.mark.unit
def test_pull_iceberg_not_blocked(cli_runner, catalog_with_iceberg_and_remote):
    """Pull with iceberg backend should NOT be blocked anymore."""
    from portolan_cli.cli import cli

    # Mock both the pull function and the backend
    with (
        patch("portolan_cli.pull.pull_iceberg") as mock_pull,
        patch("portolan_cli.backends.get_backend") as mock_get_backend,
    ):
        mock_pull.return_value = MagicMock(
            success=True,
            files_downloaded=0,
            files_skipped=0,
            up_to_date=True,
        )
        mock_get_backend.return_value = MagicMock()
        result = cli_runner.invoke(
            cli,
            [
                "pull",
                "gs://test-bucket/catalog",
                "--collection",
                "boundaries",
                "--catalog",
                str(catalog_with_iceberg_and_remote),
            ],
        )
        # Should NOT get the old "not supported" error
        assert "not supported" not in result.output.lower()


@pytest.mark.unit
def test_pull_iceberg_calls_backend_get_current_version():
    """Iceberg pull should use backend.get_current_version() for asset info."""
    from portolan_cli.pull import pull_iceberg

    mock_backend = MagicMock()
    mock_version = MagicMock()
    mock_version.version = "1.0.0"
    mock_version.assets = {
        "item1/data.parquet": MagicMock(
            href="boundaries/item1/data.parquet",
            sha256="abc123",
            size_bytes=100,
        ),
    }
    mock_backend.get_current_version.return_value = mock_version

    with patch("portolan_cli.pull.download_file") as mock_download:
        mock_download.return_value = MagicMock(success=True, files_downloaded=1)
        pull_iceberg(
            remote_url="gs://test-bucket/catalog",
            local_root=Path("/tmp/test"),
            collection="boundaries",
            backend=mock_backend,
        )

    mock_backend.get_current_version.assert_called_once_with("boundaries")


@pytest.mark.unit
def test_pull_iceberg_downloads_from_remote_plus_href():
    """Files should be downloaded from {remote}/{href}."""
    from portolan_cli.pull import pull_iceberg

    mock_backend = MagicMock()
    mock_version = MagicMock()
    mock_version.version = "1.0.0"
    mock_version.assets = {
        "item1/data.parquet": MagicMock(
            href="boundaries/item1/data.parquet",
            sha256="abc123",
            size_bytes=100,
        ),
    }
    mock_backend.get_current_version.return_value = mock_version

    with patch("portolan_cli.pull.download_file") as mock_download:
        mock_download.return_value = MagicMock(success=True, files_downloaded=1)
        pull_iceberg(
            remote_url="gs://test-bucket/catalog",
            local_root=Path("/tmp/test"),
            collection="boundaries",
            backend=mock_backend,
        )

        # Should download from {remote}/{href}
        source_arg = mock_download.call_args.kwargs["source"]
        assert source_arg == "gs://test-bucket/catalog/boundaries/item1/data.parquet"


@pytest.mark.unit
def test_pull_iceberg_saves_to_local_path():
    """Downloaded files should be saved under local_root/{href}."""
    from portolan_cli.pull import pull_iceberg

    mock_backend = MagicMock()
    mock_version = MagicMock()
    mock_version.version = "1.0.0"
    mock_version.assets = {
        "item1/data.parquet": MagicMock(
            href="boundaries/item1/data.parquet",
            sha256="abc123",
            size_bytes=100,
        ),
    }
    mock_backend.get_current_version.return_value = mock_version

    local_root = Path("/tmp/test")

    with patch("portolan_cli.pull.download_file") as mock_download:
        mock_download.return_value = MagicMock(success=True, files_downloaded=1)
        pull_iceberg(
            remote_url="gs://test-bucket/catalog",
            local_root=local_root,
            collection="boundaries",
            backend=mock_backend,
        )

        dest_arg = mock_download.call_args.kwargs["destination"]
        assert dest_arg == local_root / "boundaries" / "item1" / "data.parquet"


@pytest.mark.unit
def test_pull_iceberg_no_versions_returns_empty():
    """Pull should handle missing collection gracefully."""
    from portolan_cli.pull import pull_iceberg

    mock_backend = MagicMock()
    mock_backend.get_current_version.side_effect = FileNotFoundError("No versions")

    result = pull_iceberg(
        remote_url="gs://test-bucket/catalog",
        local_root=Path("/tmp/test"),
        collection="boundaries",
        backend=mock_backend,
    )
    assert result.files_downloaded == 0
    assert result.success is True
    assert result.up_to_date is True
