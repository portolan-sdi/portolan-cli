"""Tests for push command behavior with iceberg backend.

Phase 3 of PLAN-portolake-remote-mode: push should give a clear message
explaining that add already uploads when remote is configured.
"""

from __future__ import annotations

import json
from pathlib import Path

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


@pytest.fixture
def catalog_with_iceberg_no_remote(tmp_path: Path) -> Path:
    """Create a catalog with backend=iceberg but no remote."""
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()
    (catalog_root / "catalog.json").write_text('{"type": "Catalog"}')

    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("backend: iceberg\n")
    return catalog_root


@pytest.mark.unit
def test_push_iceberg_with_remote_explains_add_uploads(cli_runner, catalog_with_iceberg_and_remote):
    """Push with iceberg+remote should tell user that add already uploads."""
    from portolan_cli.cli import cli

    result = cli_runner.invoke(
        cli,
        [
            "push",
            "--collection",
            "test",
            "--catalog",
            str(catalog_with_iceberg_and_remote),
        ],
    )
    assert result.exit_code == 1
    assert "add" in result.output.lower()
    assert "already" in result.output.lower() or "not needed" in result.output.lower()


@pytest.mark.unit
def test_push_iceberg_without_remote_blocked(cli_runner, catalog_with_iceberg_no_remote):
    """Push with iceberg but no remote should still be blocked."""
    from portolan_cli.cli import cli

    result = cli_runner.invoke(
        cli,
        [
            "push",
            "--collection",
            "test",
            "--catalog",
            str(catalog_with_iceberg_no_remote),
        ],
    )
    assert result.exit_code == 1
    assert "iceberg" in result.output.lower()


@pytest.mark.unit
def test_push_iceberg_json_output(cli_runner, catalog_with_iceberg_and_remote):
    """Push with iceberg+remote in JSON mode should return structured error."""
    from portolan_cli.cli import cli

    result = cli_runner.invoke(
        cli,
        [
            "--format",
            "json",
            "push",
            "--collection",
            "test",
            "--catalog",
            str(catalog_with_iceberg_and_remote),
        ],
    )
    assert result.exit_code == 1
    data = json.loads(result.output)
    assert data["success"] is False
