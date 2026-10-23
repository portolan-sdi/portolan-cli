"""Integration tests for `portolan config` command group.

Tests cover:
- Full workflow: set -> get -> list -> unset
- File creation and persistence
- Precedence: env var overrides config file
- Collection-level config

Note: remote/profile/region are sensitive settings and cannot be set via config.yaml (Issue #356).
These tests use non-sensitive settings like 'pmtiles.precision' and 'backend' to test config mechanics.
Use PORTOLAN_* env vars for sensitive settings.
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from unittest import mock

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


@pytest.fixture
def runner() -> CliRunner:
    """Create a Click test runner."""
    return CliRunner()


class TestConfigWorkflow:
    """Integration tests for complete config workflow."""

    @pytest.mark.integration
    def test_full_config_workflow(self, runner: CliRunner, tmp_path: Path) -> None:
        """Test set -> get -> list -> unset workflow.

        Note: Uses 'pmtiles.min_zoom' instead of 'remote' since remote is a sensitive setting
        that cannot be set via config.yaml (Issue #356).
        """
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Initialize catalog
            runner.invoke(cli, ["init", "--auto", "--license", "CC-BY-4.0"])

            # 1. Set a value (using non-sensitive setting)
            result = runner.invoke(cli, ["config", "set", "pmtiles.min_zoom", "4"])
            assert result.exit_code == 0

            # 2. Get the value
            result = runner.invoke(cli, ["config", "get", "pmtiles.min_zoom"])
            assert result.exit_code == 0
            assert "4" in result.output

            # 3. List all settings
            result = runner.invoke(cli, ["config", "list"])
            assert result.exit_code == 0
            assert "pmtiles.min_zoom" in result.output
            assert "4" in result.output

            # 4. Unset the value
            result = runner.invoke(cli, ["config", "unset", "pmtiles.min_zoom"])
            assert result.exit_code == 0

            # 5. Verify it's gone
            result = runner.invoke(cli, ["config", "get", "pmtiles.min_zoom"])
            assert result.exit_code == 0
            assert "not set" in result.output.lower()

    @pytest.mark.integration
    def test_config_persists_to_yaml_file(self, runner: CliRunner, tmp_path: Path) -> None:
        """Config should persist to .portolan/config.yaml.

        Note: Uses 'pmtiles.precision' instead of 'remote' since remote is a sensitive setting (Issue #356).
        """
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto", "--license", "CC-BY-4.0"])
            runner.invoke(cli, ["config", "set", "pmtiles.precision", "8"])

            # Verify file exists and contains our value
            config_file = Path(".portolan/config.yaml")
            assert config_file.exists()
            content = config_file.read_text()
            assert "precision:" in content
            assert "8" in content

    @pytest.mark.integration
    def test_config_survives_restart(self, runner: CliRunner, tmp_path: Path) -> None:
        """Config should survive CLI restart (read from file).

        Note: Uses 'pmtiles.precision' instead of 'remote' since remote is a sensitive setting (Issue #356).
        """
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto", "--license", "CC-BY-4.0"])

            # Set value (using non-sensitive setting)
            runner.invoke(cli, ["config", "set", "pmtiles.precision", "16"])

            # Simulate restart by creating new runner invocation
            result = runner.invoke(cli, ["config", "get", "pmtiles.precision"])
            assert result.exit_code == 0
            assert "16" in result.output


class TestConfigPrecedence:
    """Integration tests for config precedence rules.

    Note: remote/profile are sensitive settings (Issue #356). These tests verify
    that env vars work correctly for sensitive settings, and that non-sensitive
    settings can be set via config file.
    """

    @pytest.mark.integration
    def test_env_var_overrides_config_file(self, runner: CliRunner, tmp_path: Path) -> None:
        """Environment variable should override config file value.

        Uses non-sensitive setting 'pmtiles.precision' for config file, since remote
        cannot be set in config.yaml (Issue #356).
        """
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto", "--license", "CC-BY-4.0"])
            runner.invoke(cli, ["config", "set", "pmtiles.precision", "4"])

            # Set env var and verify it takes precedence
            with mock.patch.dict(os.environ, {"PORTOLAN_PMTILES_PRECISION": "8"}):
                result = runner.invoke(cli, ["config", "get", "pmtiles.precision"])

            assert result.exit_code == 0
            assert "8" in result.output
            assert "env" in result.output.lower()

    @pytest.mark.integration
    def test_env_var_shown_in_list(self, runner: CliRunner, tmp_path: Path) -> None:
        """config list should show env var values with their source.

        Note: Sensitive settings like aws_profile must use env vars (Issue #356).
        """
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto", "--license", "CC-BY-4.0"])
            runner.invoke(cli, ["config", "set", "pmtiles.precision", "4"])

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

    @pytest.mark.integration
    def test_get_collection_config(self, runner: CliRunner, tmp_path: Path) -> None:
        """config get --collection should retrieve collection-level config."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto", "--license", "CC-BY-4.0"])

            # Set catalog-level and collection-level (using non-sensitive setting)
            runner.invoke(cli, ["config", "set", "pmtiles.precision", "4"])
            runner.invoke(
                cli,
                ["config", "set", "pmtiles.precision", "16", "--collection", "demographics"],
            )

            # Get collection config
            result = runner.invoke(
                cli, ["config", "get", "pmtiles.precision", "--collection", "demographics"]
            )
            assert result.exit_code == 0
            assert "16" in result.output

    @pytest.mark.integration
    def test_collection_inherits_catalog_config(self, runner: CliRunner, tmp_path: Path) -> None:
        """Collection should inherit unset keys from catalog config."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto", "--license", "CC-BY-4.0"])

            # Set only catalog-level pmtiles.precision (using non-sensitive setting)
            runner.invoke(cli, ["config", "set", "pmtiles.precision", "32"])

            # Get pmtiles.precision for a collection that doesn't have it set
            result = runner.invoke(
                cli, ["config", "get", "pmtiles.precision", "--collection", "demographics"]
            )

            assert result.exit_code == 0
            assert "32" in result.output


class TestConfigErrors:
    """Integration tests for config error handling.

    Note: Uses non-sensitive settings since remote/profile cannot be set
    in config.yaml (Issue #356).
    """

    @pytest.mark.integration
    def test_config_fails_outside_catalog(self, runner: CliRunner, tmp_path: Path) -> None:
        """config commands should fail outside a Portolan catalog."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Don't initialize a catalog

            result = runner.invoke(cli, ["config", "set", "pmtiles.precision", "8"])

            assert result.exit_code == 1
            output_lower = result.output.lower()
            assert "catalog" in output_lower or "not found" in output_lower

    @pytest.mark.integration
    def test_config_error_json_envelope(self, runner: CliRunner, tmp_path: Path) -> None:
        """config error should output JSON envelope with --format json."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Don't initialize a catalog

            result = runner.invoke(
                cli, ["--format", "json", "config", "set", "pmtiles.precision", "8"]
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
            runner.invoke(cli, ["init", "--auto", "--license", "CC-BY-4.0"])

            # Create and enter a subdirectory
            subdir = Path("data/nested")
            subdir.mkdir(parents=True)
            os.chdir(subdir)

            # Config should still work (using non-sensitive setting)
            result = runner.invoke(cli, ["config", "set", "pmtiles.precision", "8"])
            assert result.exit_code == 0

            result = runner.invoke(cli, ["config", "get", "pmtiles.precision"])
            assert result.exit_code == 0
            assert "8" in result.output


POINTS_GEOJSON = Path(__file__).parent.parent / "fixtures" / "vector" / "valid" / "points.geojson"


class TestDottedKeysReachTheReaders:
    """`config set` writes what the readers read, and rejects what none reads (#1020)."""

    @pytest.fixture
    def catalog(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(cli, ["init", "--auto", "--license", "CC-BY-4.0"])
        assert result.exit_code == 0, result.output
        return tmp_path

    @pytest.mark.integration
    def test_unknown_key_exits_1_and_names_the_known_keys(self, catalog: Path) -> None:
        before = (catalog / ".portolan" / "config.yaml").read_text()

        result = CliRunner().invoke(cli, ["config", "set", "conversion.vector.compression", "zstd"])

        assert result.exit_code == 1
        assert "Unknown setting 'conversion.vector.compression'" in result.output
        assert "conversion.vector.sort" in result.output
        assert (catalog / ".portolan" / "config.yaml").read_text() == before

    @pytest.mark.integration
    def test_unknown_key_error_envelope(self, catalog: Path) -> None:
        result = CliRunner().invoke(
            cli, ["--format", "json", "config", "set", "conversion.vector.compression", "zstd"]
        )

        assert result.exit_code == 1
        envelope = json.loads(result.output)
        assert envelope["success"] is False
        assert envelope["errors"][0]["type"] == "ConfigError"
        assert envelope["errors"][0]["message"].startswith(
            "[PRTLN-CFG000] Unknown setting 'conversion.vector.compression'. Known settings: "
        )

    @pytest.mark.integration
    @pytest.mark.parametrize(
        ("key", "text", "stored"),
        [
            ("statistics.enabled", "false", False),
            ("push.exclude", '["*.md"]', ["*.md"]),
            ("tabular.convert", "false", False),
        ],
    )
    def test_value_is_stored_as_the_type_of_its_setting(
        self, catalog: Path, key: str, text: str, stored: object
    ) -> None:
        from portolan_cli.config import get_setting

        result = CliRunner().invoke(cli, ["config", "set", key, text])

        assert result.exit_code == 0, result.output
        assert get_setting(key, catalog_path=catalog) == stored
        assert type(get_setting(key, catalog_path=catalog)) is type(stored)

    @pytest.mark.integration
    def test_value_that_does_not_parse_exits_1_and_names_the_type(self, catalog: Path) -> None:
        result = CliRunner().invoke(cli, ["config", "set", "tabular.convert", "maybe"])

        assert result.exit_code == 1
        assert "Setting 'tabular.convert' takes true or false, got 'maybe'" in result.output

    @pytest.mark.integration
    def test_set_get_list_round_trip_through_the_vector_reader(self, catalog: Path) -> None:
        from portolan_cli.conversion_config import get_vector_settings

        runner = CliRunner()
        assert (
            runner.invoke(cli, ["config", "set", "conversion.vector.sort", "none"]).exit_code == 0
        )

        get_result = runner.invoke(cli, ["config", "get", "conversion.vector.sort"])
        list_result = runner.invoke(cli, ["config", "list"])

        assert get_result.output == "✓ conversion.vector.sort=none (from catalog)\n"
        assert "conversion.vector.sort=none (from catalog)" in list_result.output
        assert get_vector_settings(catalog).sort == "none"

    @pytest.mark.integration
    def test_add_applies_the_vector_settings_that_config_set_wrote(self, catalog: Path) -> None:
        import pyarrow.parquet as pq

        runner = CliRunner()
        for key, value in (
            ("conversion.vector.add_bbox", "false"),
            ("conversion.vector.sort", "none"),
        ):
            assert runner.invoke(cli, ["config", "set", key, value]).exit_code == 0
        collection = catalog / "points"
        collection.mkdir()
        shutil.copy(POINTS_GEOJSON, collection / "points.geojson")

        result = runner.invoke(cli, ["add", "points"])

        assert result.exit_code == 0, result.output
        assert "bbox" not in pq.ParquetFile(collection / "points.parquet").schema_arrow.names

    @pytest.mark.integration
    @pytest.mark.xfail(
        strict=True,
        reason="#834: extract arcgis ignores conversion.vector; the fix is in F's orchestrator.py",
    )
    def test_extract_arcgis_applies_the_vector_settings(self, catalog: Path) -> None:
        import threading

        import pyarrow.parquet as pq

        from tests.docs.philadelphia_arcgis_server import create_server

        fixtures = Path(__file__).parent.parent / "fixtures" / "realdata" / "philadelphia-housing"
        server = create_server(fixtures, catalog.parent / "arcgis-requests.jsonl")
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        url = (
            f"http://127.0.0.1:{server.server_address[1]}"
            "/ArcGIS/rest/services/Council_Districts_2024/FeatureServer"
        )
        runner = CliRunner()
        assert (
            runner.invoke(cli, ["config", "set", "conversion.vector.add_bbox", "false"]).exit_code
            == 0
        )
        try:
            result = runner.invoke(
                cli,
                [
                    "extract",
                    "arcgis",
                    url,
                    ".",
                    "--layers",
                    "0",
                    "--license",
                    "CC-BY-4.0",
                    "--auto",
                ],
            )
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        assert result.exit_code == 0, result.output
        output = catalog / "council_districts_2024" / "council_districts_2024.parquet"
        assert "bbox" not in pq.ParquetFile(output).schema_arrow.names
