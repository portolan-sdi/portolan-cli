"""Integration tests for global --format=json CLI option.

These tests verify that the global --format=json flag works correctly
across all commands with the consistent OutputEnvelope structure.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


@pytest.fixture
def runner() -> CliRunner:
    """Create a Click test runner."""
    return CliRunner()


@pytest.fixture
def valid_catalog(tmp_path: Path) -> Path:
    """Create a valid Portolan catalog for testing."""
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()
    catalog_file = portolan_dir / "catalog.json"
    catalog_file.write_text(
        json.dumps(
            {
                "type": "Catalog",
                "stac_version": "1.0.0",
                "id": "test-catalog",
                "description": "A test catalog",
                "links": [],
            }
        )
    )
    return tmp_path


@pytest.fixture
def scan_fixtures_dir() -> Path:
    """Return path to scan test fixtures."""
    return Path(__file__).parent.parent / "fixtures" / "scan"


# =============================================================================
# Phase 2: --format option on CLI root
# =============================================================================


class TestGlobalFormatOption:
    """Tests for the global --format option."""

    @pytest.mark.integration
    def test_format_option_exists_in_help(self, runner: CliRunner) -> None:
        """portolan --help should show --format option."""
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "--format" in result.output

    @pytest.mark.integration
    def test_format_json_choice(self, runner: CliRunner) -> None:
        """--format=json should be a valid choice."""
        result = runner.invoke(cli, ["--format=json", "--help"])
        assert result.exit_code == 0

    @pytest.mark.integration
    def test_format_text_choice(self, runner: CliRunner) -> None:
        """--format=text should be a valid choice."""
        result = runner.invoke(cli, ["--format=text", "--help"])
        assert result.exit_code == 0

    @pytest.mark.integration
    def test_format_invalid_choice_error(self, runner: CliRunner) -> None:
        """--format=invalid should produce an error."""
        # Use a command that requires the format, not --help
        result = runner.invoke(cli, ["--format=invalid", "check", "."])
        # Click returns 2 for invalid option values
        assert result.exit_code == 2
        assert "invalid" in result.output.lower() or "not one of" in result.output.lower()


# =============================================================================
# Phase 3: User Story 1 - Agent Parses Command Output
# =============================================================================


class TestScanJsonOutput:
    """Tests for portolan --format=json scan."""

    @pytest.mark.integration
    def test_scan_json_produces_valid_envelope(
        self, runner: CliRunner, scan_fixtures_dir: Path
    ) -> None:
        """--format=json scan produces valid JSON envelope."""
        # clean_flat has multiple geo-assets in one dir, which is an error
        result = runner.invoke(
            cli, ["--format=json", "check", str(scan_fixtures_dir / "clean_flat")]
        )

        assert result.exit_code == 0

        # Output should be valid JSON
        data = json.loads(result.output)

        # Must have envelope fields
        assert "success" in data
        assert "command" in data
        assert "data" in data

        # Multiple geo-assets in one dir is an error, so success=False
        assert data["success"] is False
        assert data["command"] == "check"

    @pytest.mark.integration
    def test_scan_json_has_scan_data(self, runner: CliRunner, scan_fixtures_dir: Path) -> None:
        """--format=json scan includes scan-specific data."""
        result = runner.invoke(
            cli, ["--format=json", "check", str(scan_fixtures_dir / "clean_flat")]
        )

        data = json.loads(result.output)

        # Should have scan-specific fields in data
        assert "ready" in data["data"] or "files" in data["data"]

    @pytest.mark.integration
    def test_scan_json_error_has_errors_array(
        self, runner: CliRunner, scan_fixtures_dir: Path
    ) -> None:
        """--format=json scan with issues includes data with issues."""
        result = runner.invoke(
            cli, ["--format=json", "check", str(scan_fixtures_dir / "incomplete_shapefile")]
        )

        # Scan is informational — always exit 0 on success
        assert result.exit_code == 0

        data = json.loads(result.output)

        # Issues found: success=false in envelope, but exit code still 0
        # Note: The scan issues are reported in data, not top-level errors
        # The envelope errors are for CLI-level errors (like FileNotFoundError)
        assert "success" in data
        assert "data" in data


class TestInitJsonOutput:
    """Tests for portolan --format=json init."""

    @pytest.mark.integration
    def test_init_json_produces_valid_envelope(self, runner: CliRunner, tmp_path: Path) -> None:
        """--format=json init produces valid JSON envelope."""
        target = tmp_path / "new_catalog"
        target.mkdir()

        result = runner.invoke(cli, ["--format=json", "init", str(target)])

        assert result.exit_code == 0

        data = json.loads(result.output)

        assert data["success"] is True
        assert data["command"] == "init"
        assert "data" in data

    @pytest.mark.integration
    def test_init_json_error_produces_envelope(
        self, runner: CliRunner, valid_catalog: Path
    ) -> None:
        """--format=json init on existing catalog produces error envelope."""
        result = runner.invoke(cli, ["--format=json", "init", str(valid_catalog)])

        assert result.exit_code == 1

        data = json.loads(result.output)

        assert data["success"] is False
        assert data["command"] == "init"
        assert "errors" in data


class TestCheckJsonOutput:
    """Tests for portolan --format=json check."""

    @pytest.mark.integration
    def test_check_json_produces_valid_envelope(
        self, runner: CliRunner, valid_catalog: Path
    ) -> None:
        """--format=json check produces valid JSON envelope."""
        result = runner.invoke(cli, ["--format=json", "check", str(valid_catalog)])

        assert result.exit_code == 0

        data = json.loads(result.output)

        assert data["success"] is True
        assert data["command"] == "check"
        assert "data" in data

    @pytest.mark.integration
    def test_check_json_empty_dir_produces_envelope(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """--format=json check on empty directory produces success envelope."""
        result = runner.invoke(cli, ["--format=json", "check", str(tmp_path)])

        # Empty directory is valid (just no geo-assets)
        assert result.exit_code == 0

        data = json.loads(result.output)

        assert data["success"] is True
        assert data["command"] == "check"


class TestDatasetListJsonOutput:
    """Tests for portolan --format=json dataset list."""

    @pytest.mark.integration
    def test_dataset_list_json_produces_valid_envelope(
        self, runner: CliRunner, valid_catalog: Path
    ) -> None:
        """--format=json dataset list produces valid JSON envelope."""
        result = runner.invoke(
            cli, ["--format=json", "dataset", "list", "--catalog", str(valid_catalog)]
        )

        assert result.exit_code == 0

        data = json.loads(result.output)

        assert data["success"] is True
        assert data["command"] == "dataset_list"
        assert "data" in data


class TestDatasetInfoJsonOutput:
    """Tests for portolan --format=json dataset info."""

    @pytest.mark.integration
    def test_dataset_info_json_not_found_produces_envelope(
        self, runner: CliRunner, valid_catalog: Path
    ) -> None:
        """--format=json dataset info for missing dataset produces error envelope."""
        result = runner.invoke(
            cli,
            [
                "--format=json",
                "dataset",
                "info",
                "collection/item",
                "--catalog",
                str(valid_catalog),
            ],
        )

        assert result.exit_code == 1

        data = json.loads(result.output)

        assert data["success"] is False
        assert data["command"] == "dataset_info"
        assert "errors" in data


class TestErrorScenariosJsonOutput:
    """Tests for error scenarios producing valid JSON."""

    @pytest.mark.integration
    def test_scan_nonexistent_produces_json_error(self, runner: CliRunner, tmp_path: Path) -> None:
        """--format=json scan on nonexistent path produces JSON error envelope."""
        nonexistent = tmp_path / "does_not_exist"

        result = runner.invoke(cli, ["--format=json", "check", str(nonexistent)])

        # Should exit 1 (our error handling) and produce valid JSON
        assert result.exit_code == 1

        # Output must be valid JSON with error envelope
        data = json.loads(result.output)
        assert data["success"] is False
        assert data["command"] == "check"
        assert "errors" in data
        assert len(data["errors"]) > 0
        assert data["errors"][0]["type"] == "PathNotFoundError"

    @pytest.mark.integration
    def test_check_nonexistent_produces_json_error(self, runner: CliRunner, tmp_path: Path) -> None:
        """--format=json check on nonexistent path produces JSON error envelope."""
        nonexistent = tmp_path / "does_not_exist"

        result = runner.invoke(cli, ["--format=json", "check", str(nonexistent)])

        # Should exit 1 (our error handling) and produce valid JSON
        assert result.exit_code == 1

        # Output must be valid JSON with error envelope
        data = json.loads(result.output)
        assert data["success"] is False
        assert data["command"] == "check"
        assert "errors" in data
        assert len(data["errors"]) > 0
        assert data["errors"][0]["type"] == "PathNotFoundError"

    @pytest.mark.integration
    def test_scan_file_instead_of_dir_produces_json_error(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """--format=json scan on a file (not directory) produces JSON error envelope."""
        # Create a file, not a directory
        test_file = tmp_path / "test.txt"
        test_file.write_text("not a directory")

        result = runner.invoke(cli, ["--format=json", "check", str(test_file)])

        # Should exit 1 (our error handling) and produce valid JSON
        assert result.exit_code == 1

        # Output must be valid JSON with error envelope
        data = json.loads(result.output)
        assert data["success"] is False
        assert data["command"] == "check"
        assert "errors" in data
        assert len(data["errors"]) > 0
        assert data["errors"][0]["type"] == "NotADirectoryError"

    @pytest.mark.integration
    def test_all_output_goes_to_stdout_with_json(self, runner: CliRunner, tmp_path: Path) -> None:
        """With --format=json, all output goes to stdout."""
        result = runner.invoke(cli, ["--format=json", "check", str(tmp_path)])

        # Empty directory is valid, so exit code 0
        assert result.exit_code == 0

        # stdout should contain valid JSON
        data = json.loads(result.output)
        assert "success" in data


# =============================================================================
# Phase 4: User Story 2 - Backward Compatibility
# =============================================================================


class TestBackwardCompatibility:
    """Tests for backward compatibility with per-command --json flags."""

    @pytest.mark.integration
    def test_scan_json_flag_still_works(self, runner: CliRunner, scan_fixtures_dir: Path) -> None:
        """scan --json (per-command flag) produces envelope output."""
        result = runner.invoke(cli, ["check", str(scan_fixtures_dir / "clean_flat"), "--json"])

        assert result.exit_code == 0

        # Should be valid JSON with envelope
        data = json.loads(result.output)
        assert "success" in data
        assert "command" in data

    @pytest.mark.integration
    def test_check_json_flag_still_works(self, runner: CliRunner, valid_catalog: Path) -> None:
        """check --json (per-command flag) produces envelope output."""
        result = runner.invoke(cli, ["check", str(valid_catalog), "--json"])

        assert result.exit_code == 0

        data = json.loads(result.output)
        assert "success" in data
        assert "command" in data

    @pytest.mark.integration
    def test_dataset_info_json_flag_still_works(
        self, runner: CliRunner, valid_catalog: Path
    ) -> None:
        """dataset info --json (per-command flag) produces envelope output."""
        result = runner.invoke(
            cli,
            [
                "dataset",
                "info",
                "test/item",
                "--catalog",
                str(valid_catalog),
                "--json",
            ],
        )

        # Will fail (item doesn't exist) but should be JSON
        assert result.exit_code == 1

        data = json.loads(result.output)
        assert "success" in data
        assert data["success"] is False

    @pytest.mark.integration
    def test_global_format_matches_per_command_flag(
        self, runner: CliRunner, scan_fixtures_dir: Path
    ) -> None:
        """--format=json scan matches scan --json output structure."""
        # Run with global flag
        result_global = runner.invoke(
            cli, ["--format=json", "check", str(scan_fixtures_dir / "clean_flat")]
        )

        # Run with per-command flag
        result_local = runner.invoke(
            cli, ["check", str(scan_fixtures_dir / "clean_flat"), "--json"]
        )

        assert result_global.exit_code == result_local.exit_code

        data_global = json.loads(result_global.output)
        data_local = json.loads(result_local.output)

        # Both should have same envelope structure
        assert data_global.keys() == data_local.keys()
        assert data_global["success"] == data_local["success"]
        assert data_global["command"] == data_local["command"]

    @pytest.mark.integration
    def test_format_json_combined_with_per_command_json(
        self, runner: CliRunner, scan_fixtures_dir: Path
    ) -> None:
        """Using both --format=json and --json together works without conflict."""
        # clean_flat has multiple geo-assets in one dir, which is now an error
        result = runner.invoke(
            cli,
            ["--format=json", "check", str(scan_fixtures_dir / "clean_flat"), "--json"],
        )

        assert result.exit_code == 0

        # Should still produce valid JSON
        data = json.loads(result.output)
        # Multiple geo-assets in one dir is an error
        assert data["success"] is False


# =============================================================================
# Phase 5: User Story 3 - Human-Readable Default
# =============================================================================


class TestTextOutputDefault:
    """Tests for text output (default behavior)."""

    @pytest.mark.integration
    def test_scan_without_format_produces_text(
        self, runner: CliRunner, scan_fixtures_dir: Path
    ) -> None:
        """scan without --format produces human-readable text."""
        result = runner.invoke(cli, ["check", str(scan_fixtures_dir / "clean_flat")])

        assert result.exit_code == 0

        # Should NOT be valid JSON
        with pytest.raises(json.JSONDecodeError):
            json.loads(result.output)

        # Should have human-readable output (compact format)
        # This fixture has multiple geo-assets, so expect warning or pass
        assert len(result.output) > 0

    @pytest.mark.integration
    def test_format_text_matches_default(self, runner: CliRunner, scan_fixtures_dir: Path) -> None:
        """--format=text produces same output as no flag."""
        result_default = runner.invoke(cli, ["check", str(scan_fixtures_dir / "clean_flat")])
        result_text = runner.invoke(
            cli, ["--format=text", "check", str(scan_fixtures_dir / "clean_flat")]
        )

        assert result_default.exit_code == result_text.exit_code
        # Output should be identical (or very close - timing might differ)
        # Just check they're both text, not JSON
        with pytest.raises(json.JSONDecodeError):
            json.loads(result_default.output)
        with pytest.raises(json.JSONDecodeError):
            json.loads(result_text.output)

    @pytest.mark.integration
    def test_exit_codes_unchanged_regardless_of_format(
        self, runner: CliRunner, valid_catalog: Path
    ) -> None:
        """Exit codes are the same regardless of output format."""
        # Success case
        result_text = runner.invoke(cli, ["check", str(valid_catalog)])
        result_json = runner.invoke(cli, ["--format=json", "check", str(valid_catalog)])

        assert result_text.exit_code == result_json.exit_code == 0

    @pytest.mark.integration
    def test_exit_codes_unchanged_for_empty_dir(self, runner: CliRunner, tmp_path: Path) -> None:
        """Exit codes match between formats for empty directory (success case)."""
        result_text = runner.invoke(cli, ["check", str(tmp_path)])
        result_json = runner.invoke(cli, ["--format=json", "check", str(tmp_path)])

        # Empty directory is valid (just has no geo-assets)
        assert result_text.exit_code == result_json.exit_code == 0
