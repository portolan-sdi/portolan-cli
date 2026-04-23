"""Integration tests for `portolan config` command group.

Tests cover:
- Full workflow: set -> get -> list -> unset
- File creation and persistence
- Precedence: env var overrides config file
- Collection-level config
- JSON output mode

Note: remote/profile/region are sensitive settings and cannot be set via config.yaml (Issue #356).
These tests use non-sensitive settings like 'workers' and 'backend' to test config mechanics.
Use PORTOLAN_* env vars for sensitive settings.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest import mock

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


class TestConfigWorkflow:
    """Integration tests for complete config workflow."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_full_config_workflow(self, runner: CliRunner, tmp_path: Path) -> None:
        """Test set -> get -> list -> unset workflow.

        Note: Uses 'workers' instead of 'remote' since remote is a sensitive setting
        that cannot be set via config.yaml (Issue #356).
        """
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Initialize catalog
            runner.invoke(cli, ["init", "--auto"])

            # 1. Set a value (using non-sensitive setting)
            result = runner.invoke(cli, ["config", "set", "workers", "4"])
            assert result.exit_code == 0

            # 2. Get the value
            result = runner.invoke(cli, ["config", "get", "workers"])
            assert result.exit_code == 0
            assert "4" in result.output

            # 3. List all settings
            result = runner.invoke(cli, ["config", "list"])
            assert result.exit_code == 0
            assert "workers" in result.output
            assert "4" in result.output

            # 4. Unset the value
            result = runner.invoke(cli, ["config", "unset", "workers"])
            assert result.exit_code == 0

            # 5. Verify it's gone
            result = runner.invoke(cli, ["config", "get", "workers"])
            assert result.exit_code == 0
            assert "not set" in result.output.lower()

    @pytest.mark.integration
    def test_config_persists_to_yaml_file(self, runner: CliRunner, tmp_path: Path) -> None:
        """Config should persist to .portolan/config.yaml.

        Note: Uses 'workers' instead of 'remote' since remote is a sensitive setting (Issue #356).
        """
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            runner.invoke(cli, ["config", "set", "workers", "8"])

            # Verify file exists and contains our value
            config_file = Path(".portolan/config.yaml")
            assert config_file.exists()
            content = config_file.read_text()
            assert "workers:" in content
            assert "8" in content

    @pytest.mark.integration
    def test_config_survives_restart(self, runner: CliRunner, tmp_path: Path) -> None:
        """Config should survive CLI restart (read from file).

        Note: Uses 'workers' instead of 'remote' since remote is a sensitive setting (Issue #356).
        """
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            # Set value (using non-sensitive setting)
            runner.invoke(cli, ["config", "set", "workers", "16"])

            # Simulate restart by creating new runner invocation
            result = runner.invoke(cli, ["config", "get", "workers"])
            assert result.exit_code == 0
            assert "16" in result.output


class TestConfigPrecedence:
    """Integration tests for config precedence rules.

    Note: remote/profile are sensitive settings (Issue #356). These tests verify
    that env vars work correctly for sensitive settings, and that non-sensitive
    settings can be set via config file.
    """

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_env_var_overrides_config_file(self, runner: CliRunner, tmp_path: Path) -> None:
        """Environment variable should override config file value.

        Uses non-sensitive setting 'workers' for config file, since remote
        cannot be set in config.yaml (Issue #356).
        """
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            runner.invoke(cli, ["config", "set", "workers", "4"])

            # Set env var and verify it takes precedence
            with mock.patch.dict(os.environ, {"PORTOLAN_WORKERS": "8"}):
                result = runner.invoke(cli, ["config", "get", "workers"])

            assert result.exit_code == 0
            assert "8" in result.output
            assert "env" in result.output.lower()

    @pytest.mark.integration
    def test_env_var_shown_in_list(self, runner: CliRunner, tmp_path: Path) -> None:
        """config list should show env var values with their source.

        Note: Sensitive settings like aws_profile must use env vars (Issue #356).
        """
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            runner.invoke(cli, ["config", "set", "workers", "4"])

            with mock.patch.dict(os.environ, {"PORTOLAN_AWS_PROFILE": "env-profile"}):
                result = runner.invoke(cli, ["config", "list"])

            assert result.exit_code == 0
            assert "aws_profile" in result.output
            assert "env-profile" in result.output
            assert "env" in result.output.lower()


class TestConfigCollection:
    """Integration tests for collection-level config.

    Note: Uses non-sensitive settings since remote/profile cannot be set
    in config.yaml (Issue #356).
    """

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_set_collection_config(self, runner: CliRunner, tmp_path: Path) -> None:
        """config set --collection should set collection-level config."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(
                cli,
                ["config", "set", "workers", "8", "--collection", "demographics"],
            )

            assert result.exit_code == 0

            # Verify it's in the file
            content = Path(".portolan/config.yaml").read_text()
            assert "collections:" in content
            assert "demographics:" in content

    @pytest.mark.integration
    def test_get_collection_config(self, runner: CliRunner, tmp_path: Path) -> None:
        """config get --collection should retrieve collection-level config."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            # Set catalog-level and collection-level (using non-sensitive setting)
            runner.invoke(cli, ["config", "set", "workers", "4"])
            runner.invoke(
                cli,
                ["config", "set", "workers", "16", "--collection", "demographics"],
            )

            # Get collection config
            result = runner.invoke(
                cli, ["config", "get", "workers", "--collection", "demographics"]
            )
            assert result.exit_code == 0
            assert "16" in result.output

    @pytest.mark.integration
    def test_collection_inherits_catalog_config(self, runner: CliRunner, tmp_path: Path) -> None:
        """Collection should inherit unset keys from catalog config."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            # Set only catalog-level workers (using non-sensitive setting)
            runner.invoke(cli, ["config", "set", "workers", "32"])

            # Get workers for a collection that doesn't have it set
            result = runner.invoke(
                cli, ["config", "get", "workers", "--collection", "demographics"]
            )

            assert result.exit_code == 0
            assert "32" in result.output


class TestConfigJsonOutput:
    """Integration tests for JSON output mode.

    Note: Uses non-sensitive settings since remote/profile cannot be set
    in config.yaml (Issue #356).
    """

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_set_json_envelope(self, runner: CliRunner, tmp_path: Path) -> None:
        """config set should output JSON envelope with --format json."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(cli, ["--format", "json", "config", "set", "workers", "8"])

            assert result.exit_code == 0
            data = json.loads(result.output)
            assert data["success"] is True
            assert data["command"] == "config set"
            assert data["data"]["key"] == "workers"
            assert data["data"]["value"] == "8"

    @pytest.mark.integration
    def test_get_json_envelope(self, runner: CliRunner, tmp_path: Path) -> None:
        """config get should output JSON envelope with --format json."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            runner.invoke(cli, ["config", "set", "workers", "8"])

            result = runner.invoke(cli, ["--format", "json", "config", "get", "workers"])

            assert result.exit_code == 0
            data = json.loads(result.output)
            assert data["success"] is True
            assert data["command"] == "config get"
            assert data["data"]["key"] == "workers"
            assert data["data"]["value"] == "8"
            assert data["data"]["source"] == "catalog"

    @pytest.mark.integration
    def test_list_json_envelope(self, runner: CliRunner, tmp_path: Path) -> None:
        """config list should output JSON envelope with --format json."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            runner.invoke(cli, ["config", "set", "workers", "8"])

            result = runner.invoke(cli, ["--format", "json", "config", "list"])

            assert result.exit_code == 0
            data = json.loads(result.output)
            assert data["success"] is True
            assert data["command"] == "config list"
            assert "settings" in data["data"]
            assert "workers" in data["data"]["settings"]

    @pytest.mark.integration
    def test_unset_json_envelope(self, runner: CliRunner, tmp_path: Path) -> None:
        """config unset should output JSON envelope with --format json."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            runner.invoke(cli, ["config", "set", "workers", "8"])

            result = runner.invoke(cli, ["--format", "json", "config", "unset", "workers"])

            assert result.exit_code == 0
            data = json.loads(result.output)
            assert data["success"] is True
            assert data["command"] == "config unset"
            assert data["data"]["key"] == "workers"
            assert data["data"]["removed"] is True


class TestConfigErrors:
    """Integration tests for config error handling.

    Note: Uses non-sensitive settings since remote/profile cannot be set
    in config.yaml (Issue #356).
    """

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_config_fails_outside_catalog(self, runner: CliRunner, tmp_path: Path) -> None:
        """config commands should fail outside a Portolan catalog."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Don't initialize a catalog

            result = runner.invoke(cli, ["config", "set", "workers", "8"])

            assert result.exit_code == 1
            output_lower = result.output.lower()
            assert "catalog" in output_lower or "not found" in output_lower

    @pytest.mark.integration
    def test_config_error_json_envelope(self, runner: CliRunner, tmp_path: Path) -> None:
        """config error should output JSON envelope with --format json."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Don't initialize a catalog

            result = runner.invoke(cli, ["--format", "json", "config", "set", "workers", "8"])

            assert result.exit_code == 1
            data = json.loads(result.output)
            assert data["success"] is False
            assert len(data["errors"]) > 0
            assert data["errors"][0]["type"] == "CatalogNotFoundError"

    @pytest.mark.integration
    def test_config_works_from_subdirectory(self, runner: CliRunner, tmp_path: Path) -> None:
        """config should work from a subdirectory of the catalog."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            # Create and enter a subdirectory
            subdir = Path("data/nested")
            subdir.mkdir(parents=True)
            os.chdir(subdir)

            # Config should still work (using non-sensitive setting)
            result = runner.invoke(cli, ["config", "set", "workers", "8"])
            assert result.exit_code == 0

            result = runner.invoke(cli, ["config", "get", "workers"])
            assert result.exit_code == 0
            assert "8" in result.output
