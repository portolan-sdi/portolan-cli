"""Integration tests for `portolan init` command with new file structure.

The new init command creates (per ADR-0023, updated by issue #290):
- catalog.json at ROOT level (valid STAC catalog via pystac)
- versions.json at ROOT level (consumer-visible catalog-level versioning)
- .portolan/config.yaml -- sentinel file and user config

Note: state.json was removed per issue #290 (config.yaml alone is sufficient).

Error cases:
- MANAGED state: abort with "already a Portolan catalog"
- UNMANAGED_STAC state: abort with "existing STAC catalog found"
"""

from __future__ import annotations

import json
from pathlib import Path

import pystac
import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


class TestInitCreatesRequiredFiles:
    """Tests that init creates all 4 required files."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_init_creates_root_catalog_json(self, runner: CliRunner, tmp_path: Path) -> None:
        """Init should create catalog.json at ROOT level (not inside .portolan)."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["init", "--auto"])

            assert result.exit_code == 0, f"Failed: {result.output}"
            assert Path("catalog.json").exists(), "catalog.json should be at root"

    @pytest.mark.integration
    def test_init_creates_portolan_config(self, runner: CliRunner, tmp_path: Path) -> None:
        """Init should create .portolan/config.yaml (per ADR-0027, serves as sentinel and user config)."""
        import yaml

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["init", "--auto"])

            assert result.exit_code == 0
            config_file = Path(".portolan/config.yaml")
            assert config_file.exists()
            # Config starts as a comment-only file (empty dict when parsed as YAML)
            content = yaml.safe_load(config_file.read_text())
            assert content is None or content == {}  # Empty YAML or comment-only

    # Note: test_init_creates_portolan_state removed per issue #290
    # state.json is no longer created during init

    @pytest.mark.integration
    def test_init_creates_portolan_versions(self, runner: CliRunner, tmp_path: Path) -> None:
        """Init should create versions.json at catalog ROOT with minimal versioning.

        Per ADR-0023: versions.json is consumer-visible metadata and must live
        at the catalog root alongside STAC files, not inside .portolan/.
        """
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["init", "--auto"])

            assert result.exit_code == 0
            # Per ADR-0023: versions.json is at root, NOT inside .portolan/
            versions_file = Path("versions.json")
            assert versions_file.exists(), "versions.json must be at catalog root per ADR-0023"
            assert not Path(".portolan/versions.json").exists(), (
                "versions.json must NOT be inside .portolan/ per ADR-0023"
            )
            data = json.loads(versions_file.read_text())
            # Should have at least a version field
            assert isinstance(data, dict)

    @pytest.mark.integration
    def test_init_creates_all_four_files(self, runner: CliRunner, tmp_path: Path) -> None:
        """Init should create all 4 required files in correct locations (ADR-0023).

        Root level (user-visible STAC + versioning):
          - catalog.json
          - versions.json

        .portolan/ (internal tooling state only):
          - config.yaml

        Note: state.json was removed per issue #290.
        """
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["init", "--auto"])

            assert result.exit_code == 0
            # Root level: STAC catalog + consumer-visible versioning
            assert Path("catalog.json").exists()
            assert Path("versions.json").exists()
            # .portolan/ directory: config.yaml as sentinel (per issue #290)
            assert Path(".portolan/config.yaml").exists()
            # state.json removed per issue #290
            assert not Path(".portolan/state.json").exists()
            # versions.json must NOT be inside .portolan/ (ADR-0023)
            assert not Path(".portolan/versions.json").exists()


class TestCatalogJsonValidity:
    """Tests that catalog.json is a valid STAC catalog."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_catalog_json_is_valid_stac(self, runner: CliRunner, tmp_path: Path) -> None:
        """catalog.json should be loadable by pystac."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            # pystac should be able to read it
            catalog = pystac.Catalog.from_file("catalog.json")
            assert catalog is not None
            # Validate it's actually a Catalog instance (runtime check)
            assert isinstance(catalog, pystac.Catalog)
            assert catalog.to_dict()["type"] == "Catalog"

    @pytest.mark.integration
    def test_catalog_json_has_required_stac_fields(self, runner: CliRunner, tmp_path: Path) -> None:
        """catalog.json must have required STAC Catalog fields."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            data = json.loads(Path("catalog.json").read_text())
            assert data["type"] == "Catalog"
            # pystac uses its default STAC version (1.1.0 as of 2024)
            assert data["stac_version"] in ("1.0.0", "1.1.0")
            assert "id" in data
            assert "description" in data
            assert "links" in data

    @pytest.mark.integration
    def test_catalog_id_derived_from_directory(self, runner: CliRunner, tmp_path: Path) -> None:
        """Catalog ID should be derived from directory name."""
        catalog_dir = tmp_path / "my-geospatial-catalog"
        catalog_dir.mkdir()

        result = runner.invoke(cli, ["init", "--auto", str(catalog_dir)])

        assert result.exit_code == 0
        data = json.loads((catalog_dir / "catalog.json").read_text())
        assert data["id"] == "my-geospatial-catalog"


class TestInitErrorCases:
    """Tests for init error cases based on catalog state."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_init_fails_on_managed_catalog(self, runner: CliRunner, tmp_path: Path) -> None:
        """Init on MANAGED catalog should abort with 'already a Portolan catalog'."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Create managed catalog structure
            portolan = Path(".portolan")
            portolan.mkdir()
            (portolan / "config.yaml").write_text("{}")

            # Use --auto to skip interactive prompts and test error path
            result = runner.invoke(cli, ["init", "--auto"])

            assert result.exit_code == 1
            assert "already" in result.output.lower()

    @pytest.mark.integration
    def test_init_fails_on_unmanaged_stac(self, runner: CliRunner, tmp_path: Path) -> None:
        """Init on UNMANAGED_STAC should abort with 'existing STAC catalog'."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Create unmanaged STAC catalog
            catalog_data = {
                "type": "Catalog",
                "stac_version": "1.0.0",
                "id": "external-catalog",
                "description": "Some external catalog",
                "links": [],
            }
            Path("catalog.json").write_text(json.dumps(catalog_data))

            # Use --auto to skip interactive prompts and test error path
            result = runner.invoke(cli, ["init", "--auto"])

            assert result.exit_code == 1
            output_lower = result.output.lower()
            assert "stac" in output_lower or "catalog" in output_lower
            assert "adopt" in output_lower or "existing" in output_lower


class TestInitPartialState:
    """Tests for init behavior with partial/edge-case states."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_init_succeeds_with_empty_portolan_dir(self, runner: CliRunner, tmp_path: Path) -> None:
        """Empty .portolan directory (no config/state) should allow init.

        This is considered FRESH state - someone created .portolan but never
        completed initialization.
        """
        with runner.isolated_filesystem(temp_dir=tmp_path):
            Path(".portolan").mkdir()

            result = runner.invoke(cli, ["init", "--auto"])

            assert result.exit_code == 0
            assert Path("catalog.json").exists()

    @pytest.mark.integration
    def test_init_fails_with_config_yaml_only(self, runner: CliRunner, tmp_path: Path) -> None:
        """config.yaml alone is now MANAGED state, so init should fail.

        Per issue #290, config.yaml alone is sufficient for MANAGED state.
        """
        with runner.isolated_filesystem(temp_dir=tmp_path):
            portolan = Path(".portolan")
            portolan.mkdir()
            (portolan / "config.yaml").write_text("{}")

            result = runner.invoke(cli, ["init", "--auto"])

            # Should fail because config.yaml exists = MANAGED state
            assert result.exit_code == 1
            assert "already" in result.output.lower()


class TestInitFlags:
    """Tests for --title, --description, and --auto flags."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_title_flag_sets_catalog_title(self, runner: CliRunner, tmp_path: Path) -> None:
        """--title flag should set catalog title."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["init", "--auto", "--title", "My Awesome Catalog"])

            assert result.exit_code == 0
            data = json.loads(Path("catalog.json").read_text())
            assert data.get("title") == "My Awesome Catalog"

    @pytest.mark.integration
    def test_description_flag_sets_catalog_description(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """--description flag should set catalog description."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(
                cli, ["init", "--auto", "--description", "A test catalog for unit tests"]
            )

            assert result.exit_code == 0
            data = json.loads(Path("catalog.json").read_text())
            assert data.get("description") == "A test catalog for unit tests"

    @pytest.mark.integration
    def test_auto_skips_prompts(self, runner: CliRunner, tmp_path: Path) -> None:
        """--auto flag should complete without any prompts."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # If this blocks for input, the test will timeout
            result = runner.invoke(cli, ["init", "--auto"], catch_exceptions=False)

            assert result.exit_code == 0

    @pytest.mark.integration
    def test_flags_work_together(self, runner: CliRunner, tmp_path: Path) -> None:
        """All flags should work together."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(
                cli,
                [
                    "init",
                    "--auto",
                    "--title",
                    "Combined Test",
                    "--description",
                    "Testing all flags",
                ],
            )

            assert result.exit_code == 0
            data = json.loads(Path("catalog.json").read_text())
            assert data.get("title") == "Combined Test"
            assert data.get("description") == "Testing all flags"


class TestInitJsonOutput:
    """Tests for JSON output mode."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_json_output_on_success(self, runner: CliRunner, tmp_path: Path) -> None:
        """JSON output should indicate success with envelope."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["--format", "json", "init", "--auto"])

            assert result.exit_code == 0
            data = json.loads(result.output)
            assert data["success"] is True
            assert data["command"] == "init"

    @pytest.mark.integration
    def test_json_output_on_managed_error(self, runner: CliRunner, tmp_path: Path) -> None:
        """JSON output on MANAGED error should include error details."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            portolan = Path(".portolan")
            portolan.mkdir()
            (portolan / "config.yaml").write_text("{}")

            result = runner.invoke(cli, ["--format", "json", "init"])

            assert result.exit_code == 1
            data = json.loads(result.output)
            assert data["success"] is False
            assert len(data["errors"]) > 0

    @pytest.mark.integration
    def test_json_output_on_unmanaged_stac_error(self, runner: CliRunner, tmp_path: Path) -> None:
        """JSON output on UNMANAGED_STAC error should include error details."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            catalog_data = {
                "type": "Catalog",
                "stac_version": "1.0.0",
                "id": "external",
                "description": "External",
                "links": [],
            }
            Path("catalog.json").write_text(json.dumps(catalog_data))

            result = runner.invoke(cli, ["--format", "json", "init"])

            assert result.exit_code == 1
            data = json.loads(result.output)
            assert data["success"] is False
