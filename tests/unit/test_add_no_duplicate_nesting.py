"""Unit tests for preventing duplicate directory nesting in add command.

Tests that `portolan add collection/item_dir/FILE.tif` does NOT create
an additional `collection/item_dir/FILE/` subdirectory for the STAC item.

The item JSON should be stored without creating a duplicate nested directory
based on the file stem.

Related issue: portolan add creates duplicate nested subdirectories
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


@pytest.fixture
def mock_cog_file(tmp_path: Path) -> Path:
    """Create a minimal mock COG file for testing.

    This creates a file that passes basic format detection but
    doesn't require actual raster processing.
    """
    # For unit tests, we just need a file with .tif extension
    # Integration tests will use real COG files
    tif_file = tmp_path / "test.tif"
    tif_file.write_bytes(b"x" * 1000)
    return tif_file


class TestNoDuplicateNesting:
    """Tests that add command does not create duplicate nested directories."""

    @pytest.mark.unit
    def test_add_does_not_create_stem_subdirectory(self, runner: CliRunner) -> None:
        """Adding a file should not create a subdirectory named after the file stem.

        Given: catalog/collection/item_dir/FILE.tif
        After add:
          - STAC item metadata should be stored appropriately
          - NO new directory catalog/collection/item_dir/FILE/ should be created

        This tests the bug where adding 26453E204934N.tif created a
        26453E204934N/ subdirectory.
        """
        with runner.isolated_filesystem():
            # Create catalog structure
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
            Path(".portolan/config.yaml").write_text("version: 1\n")

            # Create collection/item structure with a TIF file
            # Simulating: 2025/26453e204934n/26453E204934N.tif
            Path("collection").mkdir()
            Path("collection/item_dir").mkdir()
            tif_file = Path("collection/item_dir/MYFILE.tif")
            tif_file.write_bytes(b"x" * 1000)

            # Get directory contents BEFORE add
            dirs_before = {p for p in Path("collection/item_dir").iterdir() if p.is_dir()}

            # Run add command
            runner.invoke(cli, ["add", str(tif_file), "-v"])

            # The command may fail due to format validation, but we can still
            # check that no duplicate directory was created
            dirs_after = {p for p in Path("collection/item_dir").iterdir() if p.is_dir()}
            new_dirs = dirs_after - dirs_before

            # CRITICAL: No new directory named after the file stem should exist
            stem_dir = Path("collection/item_dir/MYFILE")
            assert not stem_dir.exists(), (
                f"Duplicate nested directory '{stem_dir}' was created. New directories: {new_dirs}"
            )

    @pytest.mark.unit
    def test_add_preserves_flat_structure(self, runner: CliRunner) -> None:
        """Adding a file should preserve the flat item directory structure.

        The item directory (parent of the file) should contain:
        - The original data file(s)
        - STAC metadata (item.json or {item_id}.json)
        - collection.json and versions.json at collection level

        NOT a nested subdirectory named after the file.
        """
        with runner.isolated_filesystem():
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
            Path(".portolan/config.yaml").write_text("version: 1\n")

            # Create structure mimicking real data
            Path("imagery").mkdir()
            Path("imagery/tile_001").mkdir()
            data_file = Path("imagery/tile_001/TILE_001.tif")
            data_file.write_bytes(b"x" * 1000)

            runner.invoke(cli, ["add", str(data_file), "-v"])

            # Check the structure
            item_dir = Path("imagery/tile_001")

            # List all items in the directory
            contents = list(item_dir.iterdir())
            content_names = [p.name for p in contents]

            # There should NOT be a TILE_001/ subdirectory
            assert "TILE_001" not in content_names or not (item_dir / "TILE_001").is_dir(), (
                f"Unexpected nested directory TILE_001/ found. Directory contents: {content_names}"
            )

    @pytest.mark.unit
    def test_item_json_location(self, runner: CliRunner) -> None:
        """STAC item JSON should be in the item directory, not a nested subdir.

        If an item JSON is created, it should be at:
          collection/item_dir/{item_id}.json
        NOT at:
          collection/item_dir/{file_stem}/{file_stem}.json
        """
        with runner.isolated_filesystem():
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
            Path(".portolan/config.yaml").write_text("version: 1\n")

            Path("data").mkdir()
            Path("data/myitem").mkdir()
            data_file = Path("data/myitem/DATAFILE.tif")
            data_file.write_bytes(b"x" * 1000)

            runner.invoke(cli, ["add", str(data_file), "-v"])

            # Check for incorrectly nested item JSON
            wrong_location = Path("data/myitem/DATAFILE/DATAFILE.json")
            assert not wrong_location.exists(), (
                f"Item JSON found at wrong nested location: {wrong_location}"
            )

            # Also check the parent directory doesn't exist
            wrong_dir = Path("data/myitem/DATAFILE")
            assert not wrong_dir.is_dir(), f"Unexpected nested directory created: {wrong_dir}"
