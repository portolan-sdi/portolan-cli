"""Integration tests for `portolan config` command group.

Tests cover:
- Full workflow: set -> get -> list -> unset
- File creation and persistence
- Precedence: env var overrides config file
- Collection-level config
- JSON output mode
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
        """Test set -> get -> list -> unset workflow."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Initialize catalog
            runner.invoke(cli, ["init", "--auto"])

            # 1. Set a value
            result = runner.invoke(cli, ["config", "set", "remote", "s3://bucket/"])
            assert result.exit_code == 0

            # 2. Get the value
            result = runner.invoke(cli, ["config", "get", "remote"])
            assert result.exit_code == 0
            assert "s3://bucket/" in result.output

            # 3. List all settings
            result = runner.invoke(cli, ["config", "list"])
            assert result.exit_code == 0
            assert "remote" in result.output
            assert "s3://bucket/" in result.output

            # 4. Unset the value
            result = runner.invoke(cli, ["config", "unset", "remote"])
            assert result.exit_code == 0

            # 5. Verify it's gone
            result = runner.invoke(cli, ["config", "get", "remote"])
            assert result.exit_code == 0
            assert "not set" in result.output.lower()

    @pytest.mark.integration
    def test_config_persists_to_yaml_file(self, runner: CliRunner, tmp_path: Path) -> None:
        """Config should persist to .portolan/config.yaml."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            runner.invoke(cli, ["config", "set", "remote", "s3://my-bucket/"])

            # Verify file exists and contains our value
            config_file = Path(".portolan/config.yaml")
            assert config_file.exists()
            content = config_file.read_text()
            assert "remote:" in content
            assert "s3://my-bucket/" in content

    @pytest.mark.integration
    def test_config_survives_restart(self, runner: CliRunner, tmp_path: Path) -> None:
        """Config should survive CLI restart (read from file)."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            # Set value
            runner.invoke(cli, ["config", "set", "remote", "s3://persistent/"])

            # Simulate restart by creating new runner invocation
            result = runner.invoke(cli, ["config", "get", "remote"])
            assert result.exit_code == 0
            assert "s3://persistent/" in result.output


class TestConfigPrecedence:
    """Integration tests for config precedence rules."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_env_var_overrides_config_file(self, runner: CliRunner, tmp_path: Path) -> None:
        """Environment variable should override config file value."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            runner.invoke(cli, ["config", "set", "remote", "s3://from-config/"])

            # Set env var and verify it takes precedence
            with mock.patch.dict(os.environ, {"PORTOLAN_REMOTE": "s3://from-env/"}):
                result = runner.invoke(cli, ["config", "get", "remote"])

            assert result.exit_code == 0
            assert "s3://from-env/" in result.output
            assert "env" in result.output.lower()

    @pytest.mark.integration
    def test_env_var_shown_in_list(self, runner: CliRunner, tmp_path: Path) -> None:
        """config list should show env var values with their source."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            runner.invoke(cli, ["config", "set", "remote", "s3://from-config/"])

            with mock.patch.dict(os.environ, {"PORTOLAN_AWS_PROFILE": "env-profile"}):
                result = runner.invoke(cli, ["config", "list"])

            assert result.exit_code == 0
            assert "aws_profile" in result.output
            assert "env-profile" in result.output
            assert "env" in result.output.lower()


class TestConfigCollection:
    """Integration tests for collection-level config."""

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
                ["config", "set", "remote", "s3://demographics/", "--collection", "demographics"],
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

            # Set catalog-level and collection-level
            runner.invoke(cli, ["config", "set", "remote", "s3://catalog/"])
            runner.invoke(
                cli,
                ["config", "set", "remote", "s3://collection/", "--collection", "demographics"],
            )

            # Get collection config
            result = runner.invoke(cli, ["config", "get", "remote", "--collection", "demographics"])
            assert result.exit_code == 0
            assert "s3://collection/" in result.output

    @pytest.mark.integration
    def test_collection_inherits_catalog_config(self, runner: CliRunner, tmp_path: Path) -> None:
        """Collection should inherit unset keys from catalog config."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            # Set only catalog-level aws_profile
            runner.invoke(cli, ["config", "set", "aws_profile", "catalog-profile"])

            # Get aws_profile for a collection that doesn't have it set
            result = runner.invoke(
                cli, ["config", "get", "aws_profile", "--collection", "demographics"]
            )

            assert result.exit_code == 0
            assert "catalog-profile" in result.output


class TestConfigJsonOutput:
    """Integration tests for JSON output mode."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_set_json_envelope(self, runner: CliRunner, tmp_path: Path) -> None:
        """config set should output JSON envelope with --format json."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(
                cli, ["--format", "json", "config", "set", "remote", "s3://bucket/"]
            )

            assert result.exit_code == 0
            data = json.loads(result.output)
            assert data["success"] is True
            assert data["command"] == "config set"
            assert data["data"]["key"] == "remote"
            assert data["data"]["value"] == "s3://bucket/"

    @pytest.mark.integration
    def test_get_json_envelope(self, runner: CliRunner, tmp_path: Path) -> None:
        """config get should output JSON envelope with --format json."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            runner.invoke(cli, ["config", "set", "remote", "s3://bucket/"])

            result = runner.invoke(cli, ["--format", "json", "config", "get", "remote"])

            assert result.exit_code == 0
            data = json.loads(result.output)
            assert data["success"] is True
            assert data["command"] == "config get"
            assert data["data"]["key"] == "remote"
            assert data["data"]["value"] == "s3://bucket/"
            assert data["data"]["source"] == "catalog"

    @pytest.mark.integration
    def test_list_json_envelope(self, runner: CliRunner, tmp_path: Path) -> None:
        """config list should output JSON envelope with --format json."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            runner.invoke(cli, ["config", "set", "remote", "s3://bucket/"])

            result = runner.invoke(cli, ["--format", "json", "config", "list"])

            assert result.exit_code == 0
            data = json.loads(result.output)
            assert data["success"] is True
            assert data["command"] == "config list"
            assert "settings" in data["data"]
            assert "remote" in data["data"]["settings"]

    @pytest.mark.integration
    def test_unset_json_envelope(self, runner: CliRunner, tmp_path: Path) -> None:
        """config unset should output JSON envelope with --format json."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            runner.invoke(cli, ["config", "set", "remote", "s3://bucket/"])

            result = runner.invoke(cli, ["--format", "json", "config", "unset", "remote"])

            assert result.exit_code == 0
            data = json.loads(result.output)
            assert data["success"] is True
            assert data["command"] == "config unset"
            assert data["data"]["key"] == "remote"
            assert data["data"]["removed"] is True


class TestConfigErrors:
    """Integration tests for config error handling."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_config_fails_outside_catalog(self, runner: CliRunner, tmp_path: Path) -> None:
        """config commands should fail outside a Portolan catalog."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Don't initialize a catalog

            result = runner.invoke(cli, ["config", "set", "remote", "s3://bucket/"])

            assert result.exit_code == 1
            output_lower = result.output.lower()
            assert "catalog" in output_lower or "not found" in output_lower

    @pytest.mark.integration
    def test_config_error_json_envelope(self, runner: CliRunner, tmp_path: Path) -> None:
        """config error should output JSON envelope with --format json."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Don't initialize a catalog

            result = runner.invoke(
                cli, ["--format", "json", "config", "set", "remote", "s3://bucket/"]
            )

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

            # Config should still work
            result = runner.invoke(cli, ["config", "set", "remote", "s3://bucket/"])
            assert result.exit_code == 0

            result = runner.invoke(cli, ["config", "get", "remote"])
            assert result.exit_code == 0
            assert "s3://bucket/" in result.output
