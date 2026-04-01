"""Tests for pull command routing with non-file backends.

Verifies that the CLI routes to backend.pull() when the backend
provides a pull method (generic protocol dispatch).
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
def catalog_with_backend_and_remote(tmp_path: Path) -> Path:
    """Create a catalog with a non-file backend and remote configured."""
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
def test_pull_routes_to_backend_pull_method(cli_runner, catalog_with_backend_and_remote):
    """Pull with non-file backend should call backend.pull() if available."""
    from portolan_cli.cli import cli

    mock_backend = MagicMock()
    mock_backend.pull.return_value = MagicMock(
        success=True,
        files_downloaded=0,
        files_skipped=0,
        local_version=None,
        remote_version=None,
        up_to_date=True,
    )

    with patch("portolan_cli.backends.get_backend", return_value=mock_backend):
        result = cli_runner.invoke(
            cli,
            [
                "pull",
                "gs://test-bucket/catalog",
                "--collection",
                "boundaries",
                "--catalog",
                str(catalog_with_backend_and_remote),
            ],
        )

    assert result.exit_code == 0
    assert "not supported" not in result.output.lower()
    mock_backend.pull.assert_called_once()
