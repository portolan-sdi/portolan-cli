"""Integration tests for preventing duplicate directory nesting in add command.

Tests that `portolan add collection/item_dir/FILE.tif` does NOT create
an additional `collection/item_dir/FILE/` subdirectory for the STAC item.

Uses real COG fixtures to exercise the full add pipeline including
metadata extraction and STAC item creation.

Related issue: portolan add creates duplicate nested subdirectories
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI test runner."""
    return CliRunner()


@pytest.fixture
def cog_fixture() -> Path:
    """Path to a valid COG fixture file."""
    fixture_path = Path(__file__).parent.parent / "fixtures" / "raster" / "valid" / "singleband.tif"
    if not fixture_path.exists():
        pytest.skip(f"COG fixture not found: {fixture_path}")
    return fixture_path


class TestNoDuplicateNestingIntegration:
    """Integration tests that add command does not create duplicate nested directories."""

    @pytest.mark.integration
    def test_add_cog_does_not_create_stem_subdirectory(
        self, runner: CliRunner, cog_fixture: Path
    ) -> None:
        """Adding a COG file should not create a subdirectory named after the file stem.

        Given: catalog/collection/item_dir/FILE.tif (real COG)
        After add:
          - STAC item metadata should be in item_dir, NOT in item_dir/FILE/
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

            # Create collection/item structure
            # Simulating: 2025/26453e204934n/26453E204934N.tif
            Path("imagery").mkdir()
            Path("imagery/tile_001").mkdir()

            # Copy real COG to test location with uppercase name (like the bug)
            tif_file = Path("imagery/tile_001/TILE_001.tif")
            shutil.copy(cog_fixture, tif_file)

            # Get directory contents BEFORE add
            dirs_before = {p.name for p in Path("imagery/tile_001").iterdir() if p.is_dir()}

            # Run add command
            result = runner.invoke(cli, ["add", str(tif_file), "-v"])

            # Check command succeeded
            assert result.exit_code == 0, f"Add failed: {result.output}"

            # Get directory contents AFTER add
            dirs_after = {p.name for p in Path("imagery/tile_001").iterdir() if p.is_dir()}
            new_dirs = dirs_after - dirs_before

            # CRITICAL: No new directory named after the file stem should exist
            stem_dir = Path("imagery/tile_001/TILE_001")
            assert not stem_dir.exists(), (
                f"BUG: Duplicate nested directory '{stem_dir}' was created!\n"
                f"New directories: {new_dirs}\n"
                f"Full contents: {list(Path('imagery/tile_001').iterdir())}"
            )

    @pytest.mark.integration
    def test_add_cog_item_json_not_in_nested_dir(
        self, runner: CliRunner, cog_fixture: Path
    ) -> None:
        """STAC item JSON should NOT be in a nested subdirectory named after the file.

        The item JSON should be at:
          collection/item_dir/{item_id}.json or collection/item_dir/item.json
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

            # Copy real COG with a distinctive name
            tif_file = Path("data/myitem/RASTER_DATA.tif")
            shutil.copy(cog_fixture, tif_file)

            result = runner.invoke(cli, ["add", str(tif_file), "-v"])

            assert result.exit_code == 0, f"Add failed: {result.output}"

            # Check for incorrectly nested item JSON
            wrong_location = Path("data/myitem/RASTER_DATA/RASTER_DATA.json")
            wrong_dir = Path("data/myitem/RASTER_DATA")

            assert not wrong_dir.is_dir(), (
                f"BUG: Unexpected nested directory created: {wrong_dir}\n"
                f"Contents: {list(Path('data/myitem').iterdir())}"
            )

            assert not wrong_location.exists(), (
                f"BUG: Item JSON at wrong nested location: {wrong_location}"
            )

    @pytest.mark.integration
    def test_add_preserves_flat_item_structure(self, runner: CliRunner, cog_fixture: Path) -> None:
        """Adding a file should preserve the flat item directory structure.

        The item directory (parent of the file) should contain:
        - The original data file
        - STAC item metadata JSON
        - Possibly converted files

        NOT a nested subdirectory named after the data file.
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

            # Mimic real-world structure: collection/tile_id/TILE_ID.tif
            Path("aerial").mkdir()
            Path("aerial/26453e204934n").mkdir()  # lowercase dir name

            # Uppercase filename (like the real bug scenario)
            tif_file = Path("aerial/26453e204934n/26453E204934N.tif")
            shutil.copy(cog_fixture, tif_file)

            result = runner.invoke(cli, ["add", str(tif_file), "-v"])

            assert result.exit_code == 0, f"Add failed: {result.output}"

            item_dir = Path("aerial/26453e204934n")
            contents = list(item_dir.iterdir())
            content_names = [p.name for p in contents]
            subdirs = [p.name for p in contents if p.is_dir()]

            # The ONLY acceptable subdirectory would be something like .portolan
            # NOT a directory named after the file stem
            assert "26453E204934N" not in subdirs, (
                f"BUG: Duplicate nested directory '26453E204934N/' found!\n"
                f"Directory contents: {content_names}\n"
                f"Subdirectories: {subdirs}"
            )

    @pytest.mark.integration
    def test_find_shows_correct_structure_after_add(
        self, runner: CliRunner, cog_fixture: Path
    ) -> None:
        """Verify the file structure after add matches expectations.

        This is a diagnostic test that shows exactly what files/dirs are created.
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

            Path("collection").mkdir()
            Path("collection/item_dir").mkdir()

            tif_file = Path("collection/item_dir/MYFILE.tif")
            shutil.copy(cog_fixture, tif_file)

            result = runner.invoke(cli, ["add", str(tif_file), "-v"])

            assert result.exit_code == 0, f"Add failed: {result.output}"

            # Collect all files and dirs under collection/
            all_paths = []
            for path in Path("collection").rglob("*"):
                rel_path = path.relative_to(Path("collection"))
                path_type = "dir" if path.is_dir() else "file"
                all_paths.append(f"{path_type}: {rel_path}")

            # Check no MYFILE directory was created
            myfile_dir = Path("collection/item_dir/MYFILE")
            assert not myfile_dir.exists(), (
                "BUG: Duplicate nested directory created!\n"
                "Structure after add:\n" + "\n".join(sorted(all_paths))
            )
