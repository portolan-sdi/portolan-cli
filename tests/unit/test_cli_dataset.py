"""Unit tests for CLI list command and format helpers.

Tests the CLI layer for the top-level `portolan list` command (ADR-0022).

Note: dataset add/remove were moved to top-level `portolan add` and `portolan rm`
commands (see test_cli_add_rm.py).
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.dataset import DatasetInfo
from portolan_cli.formats import FormatType


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI test runner."""
    return CliRunner()


# =============================================================================
# Top-level 'portolan list' command tests (ADR-0022)
# =============================================================================


class TestTopLevelList:
    """Tests for 'portolan list' top-level command (ADR-0022)."""

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
            # Create catalog structure per ADR-0023
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

            result = runner.invoke(cli, ["list"])

            assert result.exit_code == 0
            assert "no tracked items" in result.output.lower()

    @pytest.mark.unit
    def test_list_empty_shows_guidance_scan(self, runner: CliRunner) -> None:
        """portolan list shows guidance about scan command when empty."""
        with runner.isolated_filesystem():
            # Create catalog structure per ADR-0023
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

            result = runner.invoke(cli, ["list"])

            assert result.exit_code == 0
            # Check for scan guidance
            assert "scan" in result.output.lower()

    @pytest.mark.unit
    def test_list_empty_shows_guidance_add(self, runner: CliRunner) -> None:
        """portolan list shows guidance about add command when empty."""
        with runner.isolated_filesystem():
            # Create catalog structure per ADR-0023
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

            result = runner.invoke(cli, ["list"])

            assert result.exit_code == 0
            # Check for add guidance
            assert "add" in result.output.lower()

    @pytest.mark.unit
    def test_list_empty_guidance_not_shown_in_json_mode(self, runner: CliRunner) -> None:
        """portolan list --json returns valid empty envelope without guidance text."""
        with runner.isolated_filesystem():
            # Create catalog structure per ADR-0023
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

            result = runner.invoke(cli, ["list", "--json"])

            assert result.exit_code == 0
            # Parse JSON and verify it's a valid empty response
            envelope = json.loads(result.output)
            assert envelope["success"] is True
            assert envelope["command"] == "list"
            # Items should be empty list, not contain guidance text
            assert envelope["data"]["count"] == 0
            assert envelope["data"]["items"] == []

    @pytest.mark.unit
    def test_list_empty_guidance_mentions_portolan_commands(self, runner: CliRunner) -> None:
        """portolan list empty guidance mentions full portolan commands."""
        with runner.isolated_filesystem():
            # Create catalog structure per ADR-0023
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

            result = runner.invoke(cli, ["list"])

            assert result.exit_code == 0
            # Check for command references
            assert "portolan scan" in result.output or "scan ." in result.output
            assert "portolan add" in result.output or "add <path>" in result.output

    @pytest.mark.unit
    def test_list_shows_tree_view_format(self, runner: CliRunner) -> None:
        """portolan list shows items in tree view format per ADR-0022.

        Expected format:
        demographics/
          census.parquet (GeoParquet, 4.2MB)
          boundaries.parquet (GeoParquet, 1.1MB)
        """
        with patch("portolan_cli.cli.list_datasets") as mock_list:
            mock_list.return_value = [
                DatasetInfo(
                    item_id="census",
                    collection_id="demographics",
                    format_type=FormatType.VECTOR,
                    bbox=[0, 0, 1, 1],
                    asset_paths=["./census.parquet"],
                ),
                DatasetInfo(
                    item_id="boundaries",
                    collection_id="demographics",
                    format_type=FormatType.VECTOR,
                    bbox=[0, 0, 1, 1],
                    asset_paths=["./boundaries.parquet"],
                ),
            ]

            with runner.isolated_filesystem():
                Path("catalog.json").write_text("{}")
                Path(".portolan").mkdir()
                # Create mock files for size calculation
                Path("demographics").mkdir()
                Path("demographics/census").mkdir()
                census_file = Path("demographics/census/census.parquet")
                census_file.write_bytes(b"x" * 4_400_000)  # ~4.2MB
                Path("demographics/boundaries").mkdir()
                boundaries_file = Path("demographics/boundaries/boundaries.parquet")
                boundaries_file.write_bytes(b"x" * 1_100_000)  # ~1.1MB

                result = runner.invoke(cli, ["list"])

                assert result.exit_code == 0
                # Check tree structure - collection header
                assert "demographics/" in result.output
                # Check items show with filenames (per ADR-0022)
                assert "census.parquet" in result.output
                assert "boundaries.parquet" in result.output

    @pytest.mark.unit
    def test_list_shows_file_sizes(self, runner: CliRunner) -> None:
        """portolan list displays human-readable file sizes."""
        with patch("portolan_cli.cli.list_datasets") as mock_list:
            mock_list.return_value = [
                DatasetInfo(
                    item_id="large-raster",
                    collection_id="imagery",
                    format_type=FormatType.RASTER,
                    bbox=[0, 0, 1, 1],
                    asset_paths=["./satellite.tif"],
                ),
            ]

            with runner.isolated_filesystem():
                Path("catalog.json").write_text("{}")
                Path(".portolan").mkdir()
                # Create mock file
                Path("imagery").mkdir()
                Path("imagery/large-raster").mkdir()
                raster_file = Path("imagery/large-raster/satellite.tif")
                raster_file.write_bytes(b"x" * 120_000_000)  # ~120MB

                result = runner.invoke(cli, ["list"])

                assert result.exit_code == 0
                # Should show file size in human-readable format
                assert "MB" in result.output or "mb" in result.output.lower()

    @pytest.mark.unit
    def test_list_shows_format_type(self, runner: CliRunner) -> None:
        """portolan list displays format type (GeoParquet, COG)."""
        with patch("portolan_cli.cli.list_datasets") as mock_list:
            mock_list.return_value = [
                DatasetInfo(
                    item_id="vector-item",
                    collection_id="data",
                    format_type=FormatType.VECTOR,
                    bbox=[0, 0, 1, 1],
                    asset_paths=["./data.parquet"],
                ),
                DatasetInfo(
                    item_id="raster-item",
                    collection_id="imagery",
                    format_type=FormatType.RASTER,
                    bbox=[0, 0, 1, 1],
                    asset_paths=["./data.tif"],
                ),
            ]

            with runner.isolated_filesystem():
                Path("catalog.json").write_text("{}")
                Path(".portolan").mkdir()
                # Create mock files
                Path("data").mkdir()
                Path("data/vector-item").mkdir()
                Path("data/vector-item/data.parquet").write_bytes(b"x" * 1000)
                Path("imagery").mkdir()
                Path("imagery/raster-item").mkdir()
                Path("imagery/raster-item/data.tif").write_bytes(b"x" * 1000)

                result = runner.invoke(cli, ["list"])

                assert result.exit_code == 0
                # Should show format types
                assert "GeoParquet" in result.output or "vector" in result.output.lower()
                assert "COG" in result.output or "raster" in result.output.lower()

    @pytest.mark.unit
    def test_list_filter_by_collection(self, runner: CliRunner) -> None:
        """portolan list --collection filters by collection."""
        with patch("portolan_cli.cli.list_datasets") as mock_list:
            mock_list.return_value = [
                DatasetInfo(
                    item_id="item1",
                    collection_id="target",
                    format_type=FormatType.VECTOR,
                    bbox=[0, 0, 1, 1],
                    asset_paths=["./item1/data.parquet"],
                ),
            ]

            with runner.isolated_filesystem():
                Path("catalog.json").write_text("{}")
                Path(".portolan").mkdir()
                Path("target").mkdir()
                Path("target/item1").mkdir()
                Path("target/item1/data.parquet").write_bytes(b"x" * 1000)

                result = runner.invoke(cli, ["list", "--collection", "target"])

                assert result.exit_code == 0
                mock_list.assert_called_once()
                call_kwargs = mock_list.call_args.kwargs
                assert call_kwargs.get("collection_id") == "target"

    @pytest.mark.unit
    def test_list_json_output(self, runner: CliRunner) -> None:
        """portolan list --json outputs valid JSON envelope."""
        with patch("portolan_cli.cli.list_datasets") as mock_list:
            mock_list.return_value = [
                DatasetInfo(
                    item_id="item1",
                    collection_id="col1",
                    format_type=FormatType.VECTOR,
                    bbox=[0, 0, 1, 1],
                    asset_paths=["./item1/data.parquet"],
                    title="Test Item",
                ),
            ]

            with runner.isolated_filesystem():
                Path("catalog.json").write_text("{}")
                Path(".portolan").mkdir()
                Path("col1").mkdir()
                Path("col1/item1").mkdir()
                Path("col1/item1/data.parquet").write_bytes(b"x" * 1000)

                result = runner.invoke(cli, ["list", "--json"])

                assert result.exit_code == 0
                envelope = json.loads(result.output)
                assert envelope["success"] is True
                assert envelope["command"] == "list"
                assert "items" in envelope["data"] or "datasets" in envelope["data"]


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
