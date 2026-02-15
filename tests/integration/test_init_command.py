"""Integration tests for `portolan init` command.

User Story 1: Initialize Catalog with Auto-Extracted Metadata

Tests cover:
- Full workflow: `portolan init --auto`
- catalog.json structure verification
- Warning messages for missing fields
- Error handling for existing catalog
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


class TestInitCommandAutoMode:
    """Integration tests for `portolan init --auto` workflow."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_init_auto_creates_catalog_json(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan init --auto should create catalog.json with auto-extracted fields."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["init", "--auto"])

            assert result.exit_code == 0
            catalog_file = Path(".portolan/catalog.json")
            assert catalog_file.exists()

            data = json.loads(catalog_file.read_text())
            assert data["type"] == "Catalog"
            assert data["stac_version"] == "1.0.0"
            assert "id" in data
            assert "description" in data
            assert "created" in data
            assert "updated" in data

    @pytest.mark.integration
    def test_init_auto_extracts_id_from_directory(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan init --auto should extract id from directory name."""
        catalog_dir = tmp_path / "my-cool-catalog"
        catalog_dir.mkdir()

        result = runner.invoke(cli, ["init", "--auto", str(catalog_dir)])

        assert result.exit_code == 0
        catalog_file = catalog_dir / ".portolan" / "catalog.json"
        data = json.loads(catalog_file.read_text())

        # ID should be derived from directory name
        assert data["id"] == "my-cool-catalog"

    @pytest.mark.integration
    def test_init_auto_emits_warnings(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan init --auto should emit warnings for missing best-practice fields."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["init", "--auto"])

            assert result.exit_code == 0
            # Should warn about missing title (best practice)
            output = result.output.lower()
            assert "warning" in output or "title" in output or "missing" in output

    @pytest.mark.integration
    def test_init_auto_does_not_prompt(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan init --auto should complete without waiting for input."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Use a short timeout - if it prompts, it would hang
            result = runner.invoke(cli, ["init", "--auto"], catch_exceptions=False)

            # If we get here without timeout, no prompt was issued
            assert result.exit_code == 0

    @pytest.mark.integration
    def test_init_auto_catalog_timestamps_iso8601(self, runner: CliRunner, tmp_path: Path) -> None:
        """Timestamps in catalog.json should be ISO 8601 format."""
        from datetime import datetime

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["init", "--auto"])

            assert result.exit_code == 0
            catalog_file = Path(".portolan/catalog.json")
            data = json.loads(catalog_file.read_text())

            # Should parse without error
            created = datetime.fromisoformat(data["created"])
            updated = datetime.fromisoformat(data["updated"])

            # updated should equal created on fresh init
            assert created == updated


class TestInitCommandErrors:
    """Integration tests for error handling in portolan init."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_init_fails_if_catalog_exists(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan init should fail with exit code 1 if catalog exists."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            Path(".portolan").mkdir()

            result = runner.invoke(cli, ["init"])

            assert result.exit_code == 1
            assert "already exists" in result.output.lower()

    @pytest.mark.integration
    def test_init_error_has_structured_code(self, runner: CliRunner, tmp_path: Path) -> None:
        """Error should include PRTLN-CAT001 code in JSON output."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            Path(".portolan").mkdir()

            result = runner.invoke(cli, ["--format", "json", "init"])

            assert result.exit_code == 1
            data = json.loads(result.output)
            assert data["success"] is False
            # Error code should be present
            assert "PRTLN-CAT001" in result.output or "CatalogExistsError" in result.output


class TestInitCommandJsonOutput:
    """Integration tests for JSON output mode."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_init_json_output_success(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan --format json init should output JSON envelope."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["--format", "json", "init", "--auto"])

            assert result.exit_code == 0
            data = json.loads(result.output)
            assert data["success"] is True
            assert data["command"] == "init"
            assert "path" in data["data"]

    @pytest.mark.integration
    def test_init_json_output_error(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan --format json init should output JSON error envelope."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            Path(".portolan").mkdir()

            result = runner.invoke(cli, ["--format", "json", "init"])

            assert result.exit_code == 1
            data = json.loads(result.output)
            assert data["success"] is False
            assert len(data["errors"]) > 0
