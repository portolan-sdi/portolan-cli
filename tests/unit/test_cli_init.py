"""Tests for `portolan init` CLI command."""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


class TestCliInit:
    """Tests for the `portolan init` CLI command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.unit
    def test_init_creates_catalog_in_current_directory(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """portolan init should create .portolan in the current directory."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["init"])

            assert result.exit_code == 0
            assert Path(".portolan").exists()
            assert Path(".portolan/catalog.json").exists()

    @pytest.mark.unit
    def test_init_prints_success_message(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan init should print a success message."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["init"])

            assert result.exit_code == 0
            assert "Initialized" in result.output or "✓" in result.output

    @pytest.mark.unit
    def test_init_fails_if_catalog_exists(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan init should fail if .portolan already exists."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            Path(".portolan").mkdir()

            result = runner.invoke(cli, ["init"])

            assert result.exit_code != 0
            assert "already exists" in result.output.lower()

    @pytest.mark.unit
    def test_init_accepts_path_argument(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan init PATH should create catalog at specified path."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            target = Path("my-catalog")
            target.mkdir()

            result = runner.invoke(cli, ["init", str(target)])

            assert result.exit_code == 0
            assert (target / ".portolan").exists()
