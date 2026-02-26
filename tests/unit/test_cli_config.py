"""Tests for `portolan config` CLI command group.

These tests verify the CLI behavior of config set/get/list/unset commands.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest import mock

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


class TestConfigSet:
    """Tests for `portolan config set` command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.unit
    def test_set_creates_config_file(self, runner: CliRunner, tmp_path: Path) -> None:
        """config set should create .portolan/config.yaml if it doesn't exist."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Initialize catalog first
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(cli, ["config", "set", "remote", "s3://my-bucket/"])

            assert result.exit_code == 0, f"Failed: {result.output}"
            assert Path(".portolan/config.yaml").exists()

    @pytest.mark.unit
    def test_set_writes_value(self, runner: CliRunner, tmp_path: Path) -> None:
        """config set should write the value to config file."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(cli, ["config", "set", "remote", "s3://bucket/path/"])

            assert result.exit_code == 0
            content = Path(".portolan/config.yaml").read_text()
            assert "s3://bucket/path/" in content

    @pytest.mark.unit
    def test_set_outputs_success_message(self, runner: CliRunner, tmp_path: Path) -> None:
        """config set should output a success message."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(cli, ["config", "set", "remote", "s3://bucket/"])

            assert result.exit_code == 0
            # Should have success indicator
            assert "remote" in result.output.lower() or "\u2713" in result.output

    @pytest.mark.unit
    def test_set_json_output(self, runner: CliRunner, tmp_path: Path) -> None:
        """config set should output JSON envelope with --format json."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(
                cli, ["--format", "json", "config", "set", "remote", "s3://bucket/"]
            )

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["success"] is True
            assert output["command"] == "config set"
            assert output["data"]["key"] == "remote"
            assert output["data"]["value"] == "s3://bucket/"

    @pytest.mark.unit
    def test_set_collection_level(self, runner: CliRunner, tmp_path: Path) -> None:
        """config set --collection should set collection-level config."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(
                cli,
                ["config", "set", "remote", "s3://collection/", "--collection", "demographics"],
            )

            assert result.exit_code == 0
            content = Path(".portolan/config.yaml").read_text()
            assert "collections:" in content
            assert "demographics:" in content

    @pytest.mark.unit
    def test_set_fails_outside_catalog(self, runner: CliRunner, tmp_path: Path) -> None:
        """config set should fail outside a Portolan catalog."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Don't init a catalog

            result = runner.invoke(cli, ["config", "set", "remote", "s3://bucket/"])

            assert result.exit_code == 1
            assert "catalog" in result.output.lower() or "not found" in result.output.lower()


class TestConfigGet:
    """Tests for `portolan config get` command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.unit
    def test_get_reads_from_config_file(self, runner: CliRunner, tmp_path: Path) -> None:
        """config get should read value from config file."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            runner.invoke(cli, ["config", "set", "remote", "s3://test-bucket/"])

            result = runner.invoke(cli, ["config", "get", "remote"])

            assert result.exit_code == 0
            assert "s3://test-bucket/" in result.output

    @pytest.mark.unit
    def test_get_shows_source(self, runner: CliRunner, tmp_path: Path) -> None:
        """config get should show the source of the value."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            runner.invoke(cli, ["config", "set", "remote", "s3://bucket/"])

            result = runner.invoke(cli, ["config", "get", "remote"])

            assert result.exit_code == 0
            # Should indicate it's from config file
            assert "catalog" in result.output.lower() or "config" in result.output.lower()

    @pytest.mark.unit
    def test_get_env_var_override(self, runner: CliRunner, tmp_path: Path) -> None:
        """config get should show env var when it overrides config."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            runner.invoke(cli, ["config", "set", "remote", "s3://from-config/"])

            with mock.patch.dict(os.environ, {"PORTOLAN_REMOTE": "s3://from-env/"}):
                result = runner.invoke(cli, ["config", "get", "remote"])

            assert result.exit_code == 0
            assert "s3://from-env/" in result.output
            assert "env" in result.output.lower()

    @pytest.mark.unit
    def test_get_not_set(self, runner: CliRunner, tmp_path: Path) -> None:
        """config get should indicate when a setting is not set."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(cli, ["config", "get", "remote"])

            assert result.exit_code == 0
            # Should indicate not set
            assert "not set" in result.output.lower() or "none" in result.output.lower()

    @pytest.mark.unit
    def test_get_json_output(self, runner: CliRunner, tmp_path: Path) -> None:
        """config get should output JSON envelope with --format json."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            runner.invoke(cli, ["config", "set", "remote", "s3://bucket/"])

            result = runner.invoke(cli, ["--format", "json", "config", "get", "remote"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["success"] is True
            assert output["command"] == "config get"
            assert output["data"]["key"] == "remote"
            assert output["data"]["value"] == "s3://bucket/"
            assert output["data"]["source"] == "catalog"


class TestConfigList:
    """Tests for `portolan config list` command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.unit
    def test_list_shows_all_settings(self, runner: CliRunner, tmp_path: Path) -> None:
        """config list should show all configured settings."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            runner.invoke(cli, ["config", "set", "remote", "s3://bucket/"])
            runner.invoke(cli, ["config", "set", "aws_profile", "prod"])

            result = runner.invoke(cli, ["config", "list"])

            assert result.exit_code == 0
            assert "remote" in result.output
            assert "s3://bucket/" in result.output
            assert "aws_profile" in result.output
            assert "prod" in result.output

    @pytest.mark.unit
    def test_list_shows_sources(self, runner: CliRunner, tmp_path: Path) -> None:
        """config list should show source for each setting."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            runner.invoke(cli, ["config", "set", "remote", "s3://bucket/"])

            with mock.patch.dict(os.environ, {"PORTOLAN_AWS_PROFILE": "from-env"}):
                result = runner.invoke(cli, ["config", "list"])

            assert result.exit_code == 0
            # Both sources should be shown
            assert "catalog" in result.output.lower() or "config" in result.output.lower()
            assert "env" in result.output.lower()

    @pytest.mark.unit
    def test_list_json_output(self, runner: CliRunner, tmp_path: Path) -> None:
        """config list should output JSON envelope with --format json."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            runner.invoke(cli, ["config", "set", "remote", "s3://bucket/"])

            result = runner.invoke(cli, ["--format", "json", "config", "list"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["success"] is True
            assert output["command"] == "config list"
            assert "settings" in output["data"]

    @pytest.mark.unit
    def test_list_empty_config(self, runner: CliRunner, tmp_path: Path) -> None:
        """config list should handle empty config gracefully."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(cli, ["config", "list"])

            assert result.exit_code == 0
            # Should not crash, may show "no settings" or similar


class TestConfigUnset:
    """Tests for `portolan config unset` command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.unit
    def test_unset_removes_setting(self, runner: CliRunner, tmp_path: Path) -> None:
        """config unset should remove setting from config file."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            runner.invoke(cli, ["config", "set", "remote", "s3://bucket/"])

            result = runner.invoke(cli, ["config", "unset", "remote"])

            assert result.exit_code == 0
            content = Path(".portolan/config.yaml").read_text()
            assert "s3://bucket/" not in content

    @pytest.mark.unit
    def test_unset_nonexistent_key(self, runner: CliRunner, tmp_path: Path) -> None:
        """config unset should warn when key doesn't exist."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(cli, ["config", "unset", "nonexistent"])

            # Should succeed but indicate key wasn't set
            assert result.exit_code == 0
            assert "not set" in result.output.lower() or "not found" in result.output.lower()

    @pytest.mark.unit
    def test_unset_json_output(self, runner: CliRunner, tmp_path: Path) -> None:
        """config unset should output JSON envelope with --format json."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            runner.invoke(cli, ["config", "set", "remote", "s3://bucket/"])

            result = runner.invoke(cli, ["--format", "json", "config", "unset", "remote"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["success"] is True
            assert output["command"] == "config unset"
            assert output["data"]["key"] == "remote"
            assert output["data"]["removed"] is True

    @pytest.mark.unit
    def test_unset_collection_level(self, runner: CliRunner, tmp_path: Path) -> None:
        """config unset --collection should remove collection-level setting."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            runner.invoke(
                cli,
                ["config", "set", "remote", "s3://collection/", "--collection", "demographics"],
            )

            result = runner.invoke(
                cli, ["config", "unset", "remote", "--collection", "demographics"]
            )

            assert result.exit_code == 0


class TestConfigErrorMessages:
    """Tests for config command error messages."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.unit
    def test_error_message_suggests_alternatives(self, runner: CliRunner, tmp_path: Path) -> None:
        """Error for missing remote should suggest config/CLI/env alternatives."""
        # This tests the error message format from the issue spec
        # The actual error would be in sync/push commands, not config
        # But we can test that config get shows helpful info
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(cli, ["config", "get", "remote"])

            # Should mention how to set it
            output_lower = result.output.lower()
            assert (
                "config set" in output_lower
                or "portolan_remote" in output_lower
                or "not set" in output_lower
            )
