"""Unit tests for CLI list command and format helpers.

Tests the CLI layer for the top-level `portolan list` command.

Note: add/remove were moved to top-level `portolan add` and `portolan rm`
commands (see test_cli_add_rm.py).

Updated for issue #210: list now shows ALL files with status indicators.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI test runner."""
    return CliRunner()


def _write_catalog() -> None:
    """Write an empty catalog and its config sentinel in the current directory."""
    Path("catalog.json").write_text(
        json.dumps(
            {
                "type": "Catalog",
                "stac_version": "1.0.0",
                "id": "test",
                "description": "Test",
                "links": [],
            }
        )
    )
    Path(".portolan").mkdir()
    # config.yaml is the sentinel for catalog detection
    Path(".portolan/config.yaml").write_text("version: 1\n")


def _write_asset(path: str, size: int) -> None:
    """Write a file of the given size and create its parent directories."""
    asset = Path(path)
    asset.parent.mkdir(parents=True, exist_ok=True)
    asset.write_bytes(b"x" * size)


# =============================================================================
# Top-level 'portolan list' command tests
# =============================================================================


class TestTopLevelList:
    """Tests for 'portolan list' top-level command."""

    @pytest.mark.unit
    def test_list_command_exists(self, runner: CliRunner) -> None:
        """portolan list command is available at top level."""
        result = runner.invoke(cli, ["list", "--help"])

        assert result.exit_code == 0
        assert "list" in result.output.lower()

    @pytest.mark.unit
    def test_list_empty_catalog(self, runner: CliRunner) -> None:
        """portolan list shows empty message for catalog with no items."""
        with runner.isolated_filesystem():
            _write_catalog()

            result = runner.invoke(cli, ["list"])

            assert result.exit_code == 0
            assert "no tracked items" in result.output.lower()

    @pytest.mark.unit
    def test_list_empty_shows_guidance_scan(self, runner: CliRunner) -> None:
        """portolan list shows guidance about scan command when empty."""
        with runner.isolated_filesystem():
            _write_catalog()

            result = runner.invoke(cli, ["list"])

            assert result.exit_code == 0
            # Check for scan guidance
            assert "scan" in result.output.lower()

    @pytest.mark.unit
    def test_list_empty_shows_guidance_add(self, runner: CliRunner) -> None:
        """portolan list shows guidance about add command when empty."""
        with runner.isolated_filesystem():
            _write_catalog()

            result = runner.invoke(cli, ["list"])

            assert result.exit_code == 0
            # Check for add guidance
            assert "add" in result.output.lower()

    @pytest.mark.unit
    def test_list_empty_guidance_not_shown_in_json_mode(self, runner: CliRunner) -> None:
        """portolan list --json returns valid empty envelope without guidance text."""
        with runner.isolated_filesystem():
            _write_catalog()

            result = runner.invoke(cli, ["list", "--json"])

            assert result.exit_code == 0
            # Parse JSON and verify it's a valid empty response
            envelope = json.loads(result.output)
            assert envelope["success"] is True
            assert envelope["command"] == "list"
            # Updated for #210: JSON structure has collections array (empty when no items)
            assert envelope["data"]["collections"] == []
            # Summary shows all zeros
            assert envelope["data"]["summary"]["total_tracked"] == 0

    @pytest.mark.unit
    def test_list_empty_guidance_mentions_portolan_commands(self, runner: CliRunner) -> None:
        """portolan list empty guidance mentions full portolan commands."""
        with runner.isolated_filesystem():
            _write_catalog()

            result = runner.invoke(cli, ["list"])

            assert result.exit_code == 0
            # Check for command references
            assert "portolan scan" in result.output or "scan ." in result.output
            assert "portolan add" in result.output or "add <path>" in result.output

    @pytest.mark.unit
    def test_list_shows_tree_view_format(self, runner: CliRunner) -> None:
        """portolan list shows items in tree view format.

        Expected format (updated for #210 with status indicators):
        demographics/
          census/
            + census.parquet (GeoParquet, 4.2MB)  [untracked]
          boundaries/
            + boundaries.parquet (GeoParquet, 1.1MB)  [untracked]
        """
        with runner.isolated_filesystem():
            _write_catalog()
            # Create actual files for scanning
            _write_asset("demographics/census/census.parquet", 4_400_000)  # ~4.2MB
            _write_asset("demographics/boundaries/boundaries.parquet", 1_100_000)  # ~1.1MB

            result = runner.invoke(cli, ["list"])

            assert result.exit_code == 0
            # Check tree structure - collection header
            assert "demographics/" in result.output
            # Check items show with filenames
            assert "census.parquet" in result.output
            assert "boundaries.parquet" in result.output

    @pytest.mark.unit
    def test_list_shows_file_sizes(self, runner: CliRunner) -> None:
        """portolan list displays human-readable file sizes."""
        with runner.isolated_filesystem():
            _write_catalog()
            # Create actual file for scanning
            _write_asset("imagery/large-raster/satellite.tif", 120_000_000)  # ~120MB

            result = runner.invoke(cli, ["list"])

            assert result.exit_code == 0
            # Should show file size in human-readable format
            assert "MB" in result.output or "mb" in result.output.lower()

    @pytest.mark.unit
    def test_list_shows_format_type(self, runner: CliRunner) -> None:
        """portolan list displays format type (GeoParquet, COG)."""
        with runner.isolated_filesystem():
            _write_catalog()
            # Create actual files for scanning
            _write_asset("data/vector-item/data.parquet", 1000)
            _write_asset("imagery/raster-item/data.tif", 1000)

            result = runner.invoke(cli, ["list"])

            assert result.exit_code == 0
            # Should show format types - parquet shows as GeoParquet, tif as COG
            assert "GeoParquet" in result.output or "Parquet" in result.output
            assert "COG" in result.output  # .tif files display as COG format

    @pytest.mark.unit
    def test_list_filter_by_collection(self, runner: CliRunner) -> None:
        """portolan list --collection filters by collection."""
        with runner.isolated_filesystem():
            _write_catalog()
            # Create two collections
            _write_asset("target/item1/data.parquet", 1000)
            _write_asset("other/item2/other.parquet", 1000)

            result = runner.invoke(cli, ["list", "--collection", "target"])

            assert result.exit_code == 0
            # Should show target collection
            assert "target/" in result.output
            assert "data.parquet" in result.output
            # Should NOT show other collection
            assert "other/" not in result.output
            assert "other.parquet" not in result.output

    @pytest.mark.unit
    def test_list_json_output(self, runner: CliRunner) -> None:
        """portolan list --json outputs valid JSON envelope."""
        with runner.isolated_filesystem():
            _write_catalog()
            _write_asset("col1/item1/data.parquet", 1000)

            result = runner.invoke(cli, ["list", "--json"])

            assert result.exit_code == 0
            envelope = json.loads(result.output)
            assert envelope["success"] is True
            assert envelope["command"] == "list"
            # Updated for #210: JSON structure has collections with nested items
            assert "collections" in envelope["data"]
            assert len(envelope["data"]["collections"]) == 1
            assert envelope["data"]["collections"][0]["id"] == "col1"
            # Items nested within collection
            assert len(envelope["data"]["collections"][0]["items"]) == 1


class TestListFormatSize:
    """Tests for format_size helper function."""

    @pytest.mark.unit
    def test_format_size_bytes(self) -> None:
        """format_size handles bytes correctly."""
        from portolan_cli.cli import format_size

        assert format_size(0) == "0B"
        assert format_size(100) == "100B"
        assert format_size(999) == "999B"

    @pytest.mark.unit
    def test_format_size_kilobytes(self) -> None:
        """format_size handles kilobytes correctly."""
        from portolan_cli.cli import format_size

        assert format_size(1024) == "1.0KB"
        assert format_size(1536) == "1.5KB"
        assert format_size(10240) == "10.0KB"

    @pytest.mark.unit
    def test_format_size_megabytes(self) -> None:
        """format_size handles megabytes correctly."""
        from portolan_cli.cli import format_size

        assert format_size(1024 * 1024) == "1.0MB"
        assert format_size(4_400_000) == "4.2MB"

    @pytest.mark.unit
    def test_format_size_gigabytes(self) -> None:
        """format_size handles gigabytes correctly."""
        from portolan_cli.cli import format_size

        assert format_size(1024 * 1024 * 1024) == "1.0GB"
        assert format_size(2 * 1024 * 1024 * 1024) == "2.0GB"
