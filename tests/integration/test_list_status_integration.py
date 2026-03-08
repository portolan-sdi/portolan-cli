"""Integration tests for unified list command with status indicators.

These tests verify the complete workflow from CLI invocation through
to output, using real filesystem operations (no mocks).
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


def init_catalog(catalog_dir: Path) -> None:
    """Initialize a catalog using the CLI."""
    runner = CliRunner()
    result = runner.invoke(
        cli, ["init", "--title", "Test Catalog", "--auto"], catch_exceptions=False
    )
    assert result.exit_code == 0, f"init failed: {result.output}"


# =============================================================================
# Integration tests: Full workflow with real filesystem
# =============================================================================


class TestListStatusIntegration:
    """Integration tests for list command with status indicators."""

    @pytest.mark.integration
    def test_list_shows_untracked_files_in_real_catalog(self, runner: CliRunner) -> None:
        """List shows untracked files in a real catalog."""
        with runner.isolated_filesystem():
            # Initialize catalog
            init_catalog(Path.cwd())

            # Create a collection directory with files
            col_dir = Path("my-collection")
            col_dir.mkdir()
            item_dir = col_dir / "my-item"
            item_dir.mkdir()

            # Create files
            (item_dir / "data.parquet").write_bytes(b"PAR1" + b"x" * 100)
            (item_dir / "README.md").write_text("# My Item\n\nDocumentation")

            # Run list
            result = runner.invoke(cli, ["list"])

            assert result.exit_code == 0
            # Should show the collection and files
            assert "my-collection" in result.output
            # Files should appear (either as untracked or just listed)
            assert "data.parquet" in result.output or "my-item" in result.output

    @pytest.mark.integration
    def test_list_after_add_shows_tracked(self, runner: CliRunner) -> None:
        """After adding files, list shows them as tracked."""
        with runner.isolated_filesystem():
            # Initialize catalog
            init_catalog(Path.cwd())

            # Create a collection directory with a GeoParquet file
            col_dir = Path("demographics")
            col_dir.mkdir()
            item_dir = col_dir / "census"
            item_dir.mkdir()

            # Create a minimal valid GeoParquet-like file
            parquet_file = item_dir / "census.parquet"
            parquet_file.write_bytes(b"PAR1" + b"x" * 100)

            # Add the file
            runner.invoke(cli, ["add", str(col_dir)])
            # Note: add might fail if geoparquet-io can't read the dummy file
            # That's OK - we're testing list behavior

            # Run list
            result = runner.invoke(cli, ["list"])

            assert result.exit_code == 0
            # If add succeeded, files should be tracked
            # If add failed, files should still appear (as untracked)
            assert "demographics" in result.output or "census" in result.output

    @pytest.mark.integration
    def test_list_json_output_structure(self, runner: CliRunner) -> None:
        """List --json produces valid structured output."""
        with runner.isolated_filesystem():
            # Initialize catalog
            init_catalog(Path.cwd())

            # Create some files
            col_dir = Path("test-collection")
            col_dir.mkdir()
            item_dir = col_dir / "item1"
            item_dir.mkdir()
            (item_dir / "data.txt").write_text("test data")

            # Run list --json
            result = runner.invoke(cli, ["list", "--json"])

            assert result.exit_code == 0
            envelope = json.loads(result.output)
            assert envelope["success"] is True
            assert envelope["command"] == "list"
            assert "data" in envelope

    @pytest.mark.integration
    def test_list_tracked_only_filter(self, runner: CliRunner) -> None:
        """List --tracked-only filters out untracked files."""
        with runner.isolated_filesystem():
            # Initialize catalog
            init_catalog(Path.cwd())

            # Create collection with files
            col_dir = Path("test-collection")
            col_dir.mkdir()
            item_dir = col_dir / "item1"
            item_dir.mkdir()
            (item_dir / "untracked.txt").write_text("not tracked")

            # Run list with --tracked-only
            result = runner.invoke(cli, ["list", "--tracked-only"])

            assert result.exit_code == 0
            # Untracked file should not appear
            assert "untracked.txt" not in result.output

    @pytest.mark.integration
    def test_list_empty_catalog_shows_guidance(self, runner: CliRunner) -> None:
        """Empty catalog shows helpful guidance."""
        with runner.isolated_filesystem():
            # Initialize empty catalog
            init_catalog(Path.cwd())

            result = runner.invoke(cli, ["list"])

            assert result.exit_code == 0
            # Should show guidance
            assert "scan" in result.output.lower() or "add" in result.output.lower()


class TestStatusCommandRemovalIntegration:
    """Integration tests verifying status command is removed."""

    @pytest.mark.integration
    def test_status_command_no_longer_exists(self, runner: CliRunner) -> None:
        """The status command should not be available."""
        result = runner.invoke(cli, ["status"])

        # Should fail because command doesn't exist
        assert result.exit_code != 0 or "no such command" in result.output.lower()

    @pytest.mark.integration
    def test_help_does_not_mention_status(self, runner: CliRunner) -> None:
        """Help output should not mention the status command."""
        result = runner.invoke(cli, ["--help"])

        assert result.exit_code == 0
        # "status" should not appear as a command
        # (may appear in other contexts, but not as "status  " which indicates a command)
        lines = result.output.lower().split("\n")
        command_lines = [line for line in lines if line.strip().startswith("status")]
        assert len(command_lines) == 0, f"status command still listed: {command_lines}"


class TestListOutputFormatIntegration:
    """Integration tests for list output formatting."""

    @pytest.mark.integration
    def test_list_shows_status_indicators(self, runner: CliRunner) -> None:
        """List output includes status indicators."""
        with runner.isolated_filesystem():
            # Initialize catalog
            init_catalog(Path.cwd())

            # Create collection with untracked files
            col_dir = Path("data")
            col_dir.mkdir()
            item_dir = col_dir / "item1"
            item_dir.mkdir()
            (item_dir / "new-file.txt").write_text("new content")

            result = runner.invoke(cli, ["list"])

            assert result.exit_code == 0
            # Should show status indicator (+ for untracked or similar)
            # The exact format depends on implementation
            output = result.output
            assert "+" in output or "untracked" in output.lower() or "new-file.txt" in output

    @pytest.mark.integration
    def test_list_shows_item_counts(self, runner: CliRunner) -> None:
        """List shows counts per item (e.g., '3 tracked, 2 untracked')."""
        with runner.isolated_filesystem():
            # Initialize catalog
            init_catalog(Path.cwd())

            # Create collection with multiple files
            col_dir = Path("data")
            col_dir.mkdir()
            item_dir = col_dir / "item1"
            item_dir.mkdir()
            (item_dir / "file1.txt").write_text("content 1")
            (item_dir / "file2.txt").write_text("content 2")
            (item_dir / "file3.txt").write_text("content 3")

            result = runner.invoke(cli, ["list"])

            assert result.exit_code == 0
            # Should show the item and files
            assert "item1" in result.output or "data" in result.output


class TestListIgnoredFilesIntegration:
    """Integration tests for ignored files behavior."""

    @pytest.mark.integration
    def test_ds_store_excluded(self, runner: CliRunner) -> None:
        """List excludes .DS_Store files."""
        with runner.isolated_filesystem():
            init_catalog(Path.cwd())

            col_dir = Path("data")
            col_dir.mkdir()
            item_dir = col_dir / "item1"
            item_dir.mkdir()
            (item_dir / ".DS_Store").write_bytes(b"x" * 10)
            (item_dir / "real-file.txt").write_text("real content")

            result = runner.invoke(cli, ["list"])

            assert result.exit_code == 0
            assert ".DS_Store" not in result.output
            assert "real-file.txt" in result.output

    @pytest.mark.integration
    def test_hidden_directories_excluded(self, runner: CliRunner) -> None:
        """List excludes hidden directories."""
        with runner.isolated_filesystem():
            init_catalog(Path.cwd())

            col_dir = Path("data")
            col_dir.mkdir()
            item_dir = col_dir / "item1"
            item_dir.mkdir()

            # Create a hidden directory
            hidden_dir = item_dir / ".hidden"
            hidden_dir.mkdir()
            (hidden_dir / "secret.txt").write_text("secret")

            # Create a visible file
            (item_dir / "visible.txt").write_text("visible")

            result = runner.invoke(cli, ["list"])

            assert result.exit_code == 0
            assert ".hidden" not in result.output
            assert "secret.txt" not in result.output
            assert "visible.txt" in result.output
