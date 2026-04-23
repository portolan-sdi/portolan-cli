"""Tests for push command behavior with non-file backends.

Verifies that the CLI checks backend.supports_push() and shows the
backend's push_blocked_message() when push is not supported.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

# Remote URL for tests - set via env var (Issue #356: sensitive settings)
TEST_REMOTE = "gs://test-bucket/catalog"


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def catalog_with_backend_and_remote(tmp_path: Path) -> Path:
    """Create a catalog with a non-file backend.

    Note: remote must be set via PORTOLAN_REMOTE env var (Issue #356).
    """
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()
    (catalog_root / "catalog.json").write_text('{"type": "Catalog"}')

    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("backend: iceberg\n")
    return catalog_root


@pytest.fixture
def catalog_with_backend_no_remote(tmp_path: Path) -> Path:
    """Create a catalog with a non-file backend but no remote."""
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()
    (catalog_root / "catalog.json").write_text('{"type": "Catalog"}')

    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("backend: iceberg\n")
    return catalog_root


def _mock_backend_no_push(remote: str | None = None) -> MagicMock:
    """Create a mock backend that doesn't support push."""
    backend = MagicMock()
    backend.supports_push.return_value = False
    if remote:
        backend.push_blocked_message.return_value = (
            "Push is not needed with the 'iceberg' backend. "
            "The `add` command already uploads data to the configured remote."
        )
    else:
        backend.push_blocked_message.return_value = (
            "Push is not supported with the 'iceberg' backend. "
            "The iceberg backend manages versions through its catalog."
        )
    return backend


@pytest.mark.unit
def test_push_backend_with_remote_explains_add_uploads(cli_runner, catalog_with_backend_and_remote):
    """Push with backend that doesn't support push should explain add already uploads."""
    from portolan_cli.cli import cli

    mock_backend = _mock_backend_no_push(remote=TEST_REMOTE)

    with (
        patch("portolan_cli.backends.get_backend", return_value=mock_backend),
        patch.dict(os.environ, {"PORTOLAN_REMOTE": TEST_REMOTE}),
    ):
        result = cli_runner.invoke(
            cli,
            [
                "push",
                "--collection",
                "test",
                "--catalog",
                str(catalog_with_backend_and_remote),
            ],
        )
    assert result.exit_code == 1
    assert "add" in result.output.lower()
    assert "already" in result.output.lower() or "not needed" in result.output.lower()


@pytest.mark.unit
def test_push_backend_without_remote_blocked(cli_runner, catalog_with_backend_no_remote):
    """Push with backend that doesn't support push (no remote) should still be blocked."""
    from portolan_cli.cli import cli

    mock_backend = _mock_backend_no_push(remote=None)

    with patch("portolan_cli.backends.get_backend", return_value=mock_backend):
        result = cli_runner.invoke(
            cli,
            [
                "push",
                "--collection",
                "test",
                "--catalog",
                str(catalog_with_backend_no_remote),
            ],
        )
    assert result.exit_code == 1
    assert "iceberg" in result.output.lower()


@pytest.mark.unit
def test_push_backend_json_output(cli_runner, catalog_with_backend_and_remote):
    """Push in JSON mode with unsupported backend should return structured error."""
    from portolan_cli.cli import cli

    mock_backend = _mock_backend_no_push(remote=TEST_REMOTE)

    with (
        patch("portolan_cli.backends.get_backend", return_value=mock_backend),
        patch.dict(os.environ, {"PORTOLAN_REMOTE": TEST_REMOTE}),
    ):
        result = cli_runner.invoke(
            cli,
            [
                "--format",
                "json",
                "push",
                "--collection",
                "test",
                "--catalog",
                str(catalog_with_backend_and_remote),
            ],
        )
    assert result.exit_code == 1
    data = json.loads(result.output)
    assert data["success"] is False
