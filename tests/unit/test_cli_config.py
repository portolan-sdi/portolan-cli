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

            result = runner.invoke(cli, ["config", "set", "backend", "stac"])

            assert result.exit_code == 0, f"Failed: {result.output}"
            assert Path(".portolan/config.yaml").exists()

    @pytest.mark.unit
    def test_set_writes_value(self, runner: CliRunner, tmp_path: Path) -> None:
        """config set should write the value to config file."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(cli, ["config", "set", "backend", "iceberg"])

            assert result.exit_code == 0
            content = Path(".portolan/config.yaml").read_text()
            assert "iceberg" in content

    @pytest.mark.unit
    def test_set_outputs_success_message(self, runner: CliRunner, tmp_path: Path) -> None:
        """config set should output a success message."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(cli, ["config", "set", "backend", "stac"])

            assert result.exit_code == 0
            # Should have success indicator
            assert "backend" in result.output.lower() or "\u2713" in result.output

    @pytest.mark.unit
    def test_set_json_output(self, runner: CliRunner, tmp_path: Path) -> None:
        """config set should output JSON envelope with --format json."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(cli, ["--format", "json", "config", "set", "backend", "stac"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["success"] is True
            assert output["command"] == "config set"
            assert output["data"]["key"] == "backend"
            assert output["data"]["value"] == "stac"

    @pytest.mark.unit
    def test_set_collection_level(self, runner: CliRunner, tmp_path: Path) -> None:
        """config set --collection should set collection-level config."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(
                cli,
                ["config", "set", "backend", "iceberg", "--collection", "demographics"],
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

            result = runner.invoke(cli, ["config", "set", "backend", "stac"])

            assert result.exit_code == 1
            assert "catalog" in result.output.lower() or "not found" in result.output.lower()

    @pytest.mark.unit
    def test_set_shows_collection_in_text_output(self, runner: CliRunner, tmp_path: Path) -> None:
        """config set --collection should mention collection in text output."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(
                cli,
                ["config", "set", "backend", "stac", "--collection", "demo"],
            )

            assert result.exit_code == 0
            assert "demo" in result.output

    @pytest.mark.unit
    def test_set_rejects_sensitive_settings(self, runner: CliRunner, tmp_path: Path) -> None:
        """config set should reject sensitive settings (remote, profile, region)."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            # Test remote
            result = runner.invoke(cli, ["config", "set", "remote", "s3://bucket/"])
            assert result.exit_code == 1
            assert "PORTOLAN_REMOTE" in result.output
            assert ".env" in result.output

            # Test profile
            result = runner.invoke(cli, ["config", "set", "profile", "myprofile"])
            assert result.exit_code == 1
            assert "PORTOLAN_PROFILE" in result.output


class TestConfigGet:
    """Tests for `portolan config get` command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.unit
    def test_get_reads_from_config_file(self, runner: CliRunner, tmp_path: Path) -> None:
        """config get should read value from config file."""
        from portolan_cli.config import save_config

        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            # Use non-sensitive key for testing config read
            save_config(Path("."), {"backend": "iceberg"})

            result = runner.invoke(cli, ["config", "get", "backend"])

            assert result.exit_code == 0
            assert "iceberg" in result.output

    @pytest.mark.unit
    def test_get_shows_source(self, runner: CliRunner, tmp_path: Path) -> None:
        """config get should show the source of the value."""
        from portolan_cli.config import save_config

        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            save_config(Path("."), {"backend": "iceberg"})

            result = runner.invoke(cli, ["config", "get", "backend"])

            assert result.exit_code == 0
            # Should indicate it's from config file
            assert "catalog" in result.output.lower() or "config" in result.output.lower()

    @pytest.mark.unit
    def test_get_env_var_override(self, runner: CliRunner, tmp_path: Path) -> None:
        """config get should show env var when it overrides config."""
        from portolan_cli.config import save_config

        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            save_config(Path("."), {"remote": "s3://from-config/"})

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
    def test_get_fails_outside_catalog(self, runner: CliRunner, tmp_path: Path) -> None:
        """config get should fail outside a Portolan catalog."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Don't init a catalog

            result = runner.invoke(cli, ["config", "get", "remote"])

            assert result.exit_code == 1
            assert "catalog" in result.output.lower() or "not found" in result.output.lower()

    @pytest.mark.unit
    def test_get_json_output(self, runner: CliRunner, tmp_path: Path) -> None:
        """config get should output JSON envelope with --format json."""
        from portolan_cli.config import save_config

        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            save_config(Path("."), {"backend": "iceberg"})

            result = runner.invoke(cli, ["--format", "json", "config", "get", "backend"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["success"] is True
            assert output["command"] == "config get"
            assert output["data"]["key"] == "backend"
            assert output["data"]["value"] == "iceberg"
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
        from portolan_cli.config import save_config

        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            save_config(Path("."), {"backend": "iceberg", "statistics.enabled": True})

            result = runner.invoke(cli, ["config", "list"])

            assert result.exit_code == 0
            assert "backend" in result.output
            assert "iceberg" in result.output

    @pytest.mark.unit
    def test_list_shows_sources(self, runner: CliRunner, tmp_path: Path) -> None:
        """config list should show source for each setting."""
        from portolan_cli.config import save_config

        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            save_config(Path("."), {"backend": "iceberg"})

            with mock.patch.dict(os.environ, {"PORTOLAN_STATISTICS_ENABLED": "true"}):
                result = runner.invoke(cli, ["config", "list"])

            assert result.exit_code == 0
            # Both sources should be shown
            assert "catalog" in result.output.lower() or "config" in result.output.lower()
            assert "env" in result.output.lower()

    @pytest.mark.unit
    def test_list_json_output(self, runner: CliRunner, tmp_path: Path) -> None:
        """config list should output JSON envelope with --format json."""
        from portolan_cli.config import save_config

        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            save_config(Path("."), {"backend": "iceberg"})

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

    @pytest.mark.unit
    def test_list_fails_outside_catalog(self, runner: CliRunner, tmp_path: Path) -> None:
        """config list should fail outside a Portolan catalog."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Don't init a catalog

            result = runner.invoke(cli, ["config", "list"])

            assert result.exit_code == 1
            assert "catalog" in result.output.lower() or "not found" in result.output.lower()

    @pytest.mark.unit
    def test_list_shows_collection_in_text(self, runner: CliRunner, tmp_path: Path) -> None:
        """config list --collection should mention collection in text output."""
        from portolan_cli.config import save_config

        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            save_config(
                Path("."),
                {"collections": {"demo": {"backend": "iceberg"}}},
            )

            result = runner.invoke(cli, ["config", "list", "--collection", "demo"])

            assert result.exit_code == 0
            assert "demo" in result.output


class TestConfigUnset:
    """Tests for `portolan config unset` command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.unit
    def test_unset_removes_setting(self, runner: CliRunner, tmp_path: Path) -> None:
        """config unset should remove setting from config file."""
        from portolan_cli.config import save_config

        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            save_config(Path("."), {"remote": "s3://bucket/"})

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
    def test_unset_fails_outside_catalog(self, runner: CliRunner, tmp_path: Path) -> None:
        """config unset should fail outside a Portolan catalog."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Don't init a catalog

            result = runner.invoke(cli, ["config", "unset", "remote"])

            assert result.exit_code == 1
            assert "catalog" in result.output.lower() or "not found" in result.output.lower()

    @pytest.mark.unit
    def test_unset_json_output(self, runner: CliRunner, tmp_path: Path) -> None:
        """config unset should output JSON envelope with --format json."""
        from portolan_cli.config import save_config

        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            save_config(Path("."), {"remote": "s3://bucket/"})

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
        from portolan_cli.config import save_config

        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            save_config(
                Path("."),
                {"collections": {"demographics": {"remote": "s3://collection/"}}},
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

    @pytest.mark.unit
    def test_get_json_output_not_set(self, runner: CliRunner, tmp_path: Path) -> None:
        """config get should output JSON with value=null when not set."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            result = runner.invoke(cli, ["--format", "json", "config", "get", "remote"])

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["success"] is True
            assert output["data"]["key"] == "remote"
            assert output["data"]["value"] is None

    @pytest.mark.unit
    def test_get_collection_json_output(self, runner: CliRunner, tmp_path: Path) -> None:
        """config get --collection should include collection in JSON output."""
        from portolan_cli.config import save_config

        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            save_config(
                Path("."),
                {"collections": {"demo": {"backend": "iceberg"}}},
            )

            result = runner.invoke(
                cli, ["--format", "json", "config", "get", "backend", "--collection", "demo"]
            )

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["success"] is True
            assert output["data"]["value"] == "iceberg"
            assert output["data"]["collection"] == "demo"

    @pytest.mark.unit
    def test_set_collection_json_output(self, runner: CliRunner, tmp_path: Path) -> None:
        """config set --collection should include collection in JSON output."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            # Use non-sensitive key for testing JSON output format
            result = runner.invoke(
                cli,
                [
                    "--format",
                    "json",
                    "config",
                    "set",
                    "backend",
                    "stac",
                    "--collection",
                    "demo",
                ],
            )

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["success"] is True
            assert output["data"]["collection"] == "demo"

    @pytest.mark.unit
    def test_list_collection_json_output(self, runner: CliRunner, tmp_path: Path) -> None:
        """config list --collection should include collection in JSON output."""
        from portolan_cli.config import save_config

        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            # Use non-sensitive key (backend) to create collection config
            save_config(
                Path("."),
                {"collections": {"demo": {"backend": "iceberg"}}},
            )

            result = runner.invoke(
                cli, ["--format", "json", "config", "list", "--collection", "demo"]
            )

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["success"] is True
            assert output["data"]["collection"] == "demo"

    @pytest.mark.unit
    def test_get_json_error_outside_catalog(self, runner: CliRunner, tmp_path: Path) -> None:
        """config get should return JSON error envelope outside catalog."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Don't init a catalog

            result = runner.invoke(cli, ["--format", "json", "config", "get", "remote"])

            assert result.exit_code == 1
            output = json.loads(result.output)
            assert output["success"] is False
            assert output["errors"][0]["type"] == "CatalogNotFoundError"

    @pytest.mark.unit
    def test_list_json_error_outside_catalog(self, runner: CliRunner, tmp_path: Path) -> None:
        """config list should return JSON error envelope outside catalog."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Don't init a catalog

            result = runner.invoke(cli, ["--format", "json", "config", "list"])

            assert result.exit_code == 1
            output = json.loads(result.output)
            assert output["success"] is False
            assert output["errors"][0]["type"] == "CatalogNotFoundError"

    @pytest.mark.unit
    def test_unset_json_error_outside_catalog(self, runner: CliRunner, tmp_path: Path) -> None:
        """config unset should return JSON error envelope outside catalog."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Don't init a catalog

            result = runner.invoke(cli, ["--format", "json", "config", "unset", "remote"])

            assert result.exit_code == 1
            output = json.loads(result.output)
            assert output["success"] is False
            assert output["errors"][0]["type"] == "CatalogNotFoundError"

    @pytest.mark.unit
    def test_unset_collection_json_output(self, runner: CliRunner, tmp_path: Path) -> None:
        """config unset --collection should include collection in JSON output."""
        from portolan_cli.config import save_config

        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])
            save_config(
                Path("."),
                {"collections": {"demo": {"remote": "s3://col/"}}},
            )

            result = runner.invoke(
                cli, ["--format", "json", "config", "unset", "remote", "--collection", "demo"]
            )

            assert result.exit_code == 0
            output = json.loads(result.output)
            assert output["success"] is True
            assert output["data"]["collection"] == "demo"
            assert output["data"]["removed"] is True
