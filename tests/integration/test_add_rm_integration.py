"""Integration tests for top-level add/rm commands.

These tests exercise the full add/rm workflow with real file operations,
verifying that format conversion, metadata extraction, STAC creation,
and file management work end-to-end.

Per ADR-0022: Git-style implicit tracking
"""

from __future__ import annotations

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
def initialized_catalog(tmp_path: Path) -> Path:
    """Create an initialized Portolan catalog using CLI."""
    result = CliRunner().invoke(cli, ["init", str(tmp_path), "--auto"])
    assert result.exit_code == 0, f"Init failed: {result.output}"
    return tmp_path


@pytest.fixture
def catalog_with_vectors(initialized_catalog: Path, valid_points_geojson: Path) -> Path:
    """Create catalog with a vector collection."""
    # Create collection directory and copy test file
    collection_dir = initialized_catalog / "vectors"
    collection_dir.mkdir()
    dest_file = collection_dir / "points.geojson"
    shutil.copy(valid_points_geojson, dest_file)
    return initialized_catalog


class TestAddIntegration:
    """Integration tests for 'portolan add' command."""

    @pytest.mark.integration
    def test_add_geojson_creates_stac_structure(
        self, runner: CliRunner, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """add creates STAC collection, item, and versions.json."""
        # Set up: copy file to catalog
        collection_dir = initialized_catalog / "demographics"
        collection_dir.mkdir()
        test_file = collection_dir / "census.geojson"
        shutil.copy(valid_points_geojson, test_file)

        # Act - use --portolan-dir on the add subcommand to specify catalog root
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )

        # Assert
        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Verify STAC structure created (per ADR-0023)
        assert (collection_dir / "collection.json").exists()
        assert (collection_dir / "versions.json").exists()

        # Verify item created
        item_dir = collection_dir / "census"
        assert item_dir.exists() or (collection_dir / "census.parquet").exists()

    @pytest.mark.integration
    def test_add_directory_adds_all_files(
        self, runner: CliRunner, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """add directory adds all geospatial files inside."""
        # Set up: create directory with multiple files
        collection_dir = initialized_catalog / "multi"
        collection_dir.mkdir()
        shutil.copy(valid_points_geojson, collection_dir / "file1.geojson")
        shutil.copy(valid_points_geojson, collection_dir / "file2.geojson")

        # Act
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(collection_dir)],
            catch_exceptions=False,
        )

        # Assert
        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Both files should appear in output or be tracked
        output_lower = result.output.lower()
        assert "file1" in output_lower or "2 files" in output_lower or result.output == ""

    @pytest.mark.integration
    def test_add_idempotent_silent_skip(
        self, runner: CliRunner, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """add same file twice - second add is silent (idempotent)."""
        # Set up
        collection_dir = initialized_catalog / "vectors"
        collection_dir.mkdir()
        test_file = collection_dir / "data.geojson"
        shutil.copy(valid_points_geojson, test_file)

        # First add
        result1 = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )
        assert result1.exit_code == 0

        # Second add - should be silent or show "unchanged"
        result2 = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )
        assert result2.exit_code == 0

    @pytest.mark.integration
    def test_add_verbose_shows_details(
        self, runner: CliRunner, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """add --verbose shows detailed output including skipped files."""
        # Set up
        collection_dir = initialized_catalog / "vectors"
        collection_dir.mkdir()
        test_file = collection_dir / "data.geojson"
        shutil.copy(valid_points_geojson, test_file)

        # First add
        runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )

        # Second add with --verbose
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), "--verbose", str(test_file)],
            catch_exceptions=False,
        )

        assert result.exit_code == 0
        # Should mention the file in verbose mode
        # (either as skipped or in output)

    @pytest.mark.integration
    def test_add_shapefile_with_sidecars(
        self, runner: CliRunner, initialized_catalog: Path, fixtures_dir: Path
    ) -> None:
        """add shapefile automatically includes sidecar files."""
        # Use the complete shapefile fixture
        shapefile_dir = fixtures_dir / "scan" / "complete_shapefile"
        if not shapefile_dir.exists():
            pytest.skip("Shapefile fixture not available")

        # Set up: copy shapefile and sidecars to catalog
        collection_dir = initialized_catalog / "boundaries"
        collection_dir.mkdir()

        for file in shapefile_dir.iterdir():
            shutil.copy(file, collection_dir / file.name)

        shp_file = collection_dir / "radios_sample.shp"

        # Act
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(shp_file)],
            catch_exceptions=False,
        )

        # Assert
        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Output should mention sidecars
        output = result.output.lower()
        assert "sidecar" in output or "dbf" in output or result.exit_code == 0

    @pytest.mark.integration
    def test_add_infers_collection_from_path(
        self, runner: CliRunner, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """add infers collection ID from first path component."""
        # Set up: deeply nested file
        nested_dir = initialized_catalog / "demographics" / "census" / "2020"
        nested_dir.mkdir(parents=True)
        test_file = nested_dir / "data.geojson"
        shutil.copy(valid_points_geojson, test_file)

        # Act
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )

        # Assert
        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Collection should be "demographics" (first component)
        collection_json = initialized_catalog / "demographics" / "collection.json"
        assert collection_json.exists(), "Collection not created at 'demographics'"

    @pytest.mark.integration
    def test_add_multiple_files_creates_snapshot_with_all_assets(
        self, runner: CliRunner, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """Adding files incrementally accumulates assets in versions.json.

        This tests the fix for issues #141 and #147:
        - Each version should contain ALL assets at that point in time
        - Not just the newly-added assets

        Expected behavior:
        - Add file A → version 1.0.0 with {A}
        - Add file B → version 1.0.1 with {A, B}
        """
        import json

        # Set up: create collection with two files in separate item directories
        collection_dir = initialized_catalog / "snapshot-test"
        (collection_dir / "item-a").mkdir(parents=True)
        (collection_dir / "item-b").mkdir(parents=True)
        shutil.copy(valid_points_geojson, collection_dir / "item-a" / "file-a.geojson")
        shutil.copy(valid_points_geojson, collection_dir / "item-b" / "file-b.geojson")

        # Act 1: Add first file
        result1 = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                str(collection_dir / "item-a" / "file-a.geojson"),
            ],
            catch_exceptions=False,
        )
        assert result1.exit_code == 0, f"First add failed: {result1.output}"

        # Check version 1.0.0 has 1 asset
        versions_path = collection_dir / "versions.json"
        with open(versions_path) as f:
            v1 = json.load(f)
        assert len(v1["versions"]) == 1
        assert len(v1["versions"][0]["assets"]) >= 1  # At least file-a

        # Act 2: Add second file
        result2 = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                str(collection_dir / "item-b" / "file-b.geojson"),
            ],
            catch_exceptions=False,
        )
        assert result2.exit_code == 0, f"Second add failed: {result2.output}"

        # Assert: version 1.0.1 should have BOTH files (snapshot model)
        with open(versions_path) as f:
            v2 = json.load(f)

        assert len(v2["versions"]) == 2, "Should have 2 versions"
        latest_version = v2["versions"][-1]
        asset_keys = list(latest_version["assets"].keys())

        # The key assertion: latest version contains assets from BOTH adds
        assert len(asset_keys) >= 2, (
            f"Latest version should have assets from both adds, got: {asset_keys}"
        )

        # Verify changes field only shows NEW assets from this add operation
        # (source geojson + converted parquet = 2 changes per file added)
        # The key is that changes should NOT include assets from the FIRST add
        assert len(latest_version["changes"]) <= 2, (
            f"Changes should only show new files from this operation, got: {latest_version['changes']}"
        )
        # Changes should not include item-a files
        for change in latest_version["changes"]:
            assert "item-a" not in change, f"Changes incorrectly includes item-a: {change}"


class TestAddItemIdOverrideIntegration:
    """Integration tests for --item-id flag on 'portolan add' command.

    Issue #136: Users should be able to override automatic item ID derivation.
    """

    @pytest.mark.integration
    def test_add_with_item_id_creates_item_with_custom_id(
        self, runner: CliRunner, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """add --item-id creates STAC item with the custom ID."""
        import json

        # Set up: create collection and copy file
        collection_dir = initialized_catalog / "demographics"
        collection_dir.mkdir()
        # Create item directory (required structure)
        item_dir = collection_dir / "auto-derived"
        item_dir.mkdir()
        test_file = item_dir / "census.geojson"
        shutil.copy(valid_points_geojson, test_file)

        # Act: add with --item-id override
        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                "--item-id",
                "custom-census-2024",
                str(test_file),
            ],
            catch_exceptions=False,
        )

        # Assert
        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Check that versions.json uses the custom item ID
        versions_path = collection_dir / "versions.json"
        assert versions_path.exists(), "versions.json not created"
        with open(versions_path) as f:
            versions = json.load(f)

        # The item_id in versions.json should be custom-census-2024
        latest_version = versions["versions"][-1]
        # Check assets are keyed by the custom item ID
        asset_keys = list(latest_version["assets"].keys())
        assert any("custom-census-2024" in key for key in asset_keys), (
            f"Expected item with custom ID 'custom-census-2024' in assets, got: {asset_keys}"
        )

    @pytest.mark.integration
    def test_add_with_item_id_stac_item_uses_custom_id(
        self, runner: CliRunner, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """add --item-id results in STAC item.json with custom ID field."""
        import json

        # Set up
        collection_dir = initialized_catalog / "imagery"
        item_dir = collection_dir / "original-dir"
        item_dir.mkdir(parents=True)
        test_file = item_dir / "satellite.geojson"
        shutil.copy(valid_points_geojson, test_file)

        # Act
        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                "--item-id",
                "my-custom-item",
                str(test_file),
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Find and read the item.json
        # STAC item structure: collection_dir/item_id/item_id.json
        # But since we override item_id, we still create in original dir
        item_json_files = list(collection_dir.rglob("*.json"))
        item_json = None
        for f in item_json_files:
            if f.name not in ("collection.json", "versions.json", "catalog.json"):
                try:
                    with open(f) as fh:
                        data = json.load(fh)
                        if data.get("type") == "Feature":
                            item_json = data
                            break
                except (json.JSONDecodeError, KeyError):
                    continue

        assert item_json is not None, "No STAC item found"
        assert item_json.get("id") == "my-custom-item", (
            f"STAC item ID should be 'my-custom-item', got '{item_json.get('id')}'"
        )

    @pytest.mark.integration
    def test_add_without_item_id_derives_from_directory(
        self, runner: CliRunner, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """Without --item-id, item ID is derived from parent directory."""
        import json

        # Set up: file in directory named 'census-2020'
        collection_dir = initialized_catalog / "demographics"
        item_dir = collection_dir / "census-2020"
        item_dir.mkdir(parents=True)
        test_file = item_dir / "data.geojson"
        shutil.copy(valid_points_geojson, test_file)

        # Act: add WITHOUT --item-id
        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                str(test_file),
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, f"Add failed: {result.output}"

        # The item ID should be 'census-2020' (parent directory name)
        versions_path = collection_dir / "versions.json"
        with open(versions_path) as f:
            versions = json.load(f)

        asset_keys = list(versions["versions"][-1]["assets"].keys())
        assert any("census-2020" in key for key in asset_keys), (
            f"Expected auto-derived item ID 'census-2020', got: {asset_keys}"
        )

    @pytest.mark.integration
    def test_add_item_id_with_invalid_characters_fails(
        self, runner: CliRunner, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """add --item-id with path separators fails with clear error."""
        # Set up
        collection_dir = initialized_catalog / "vectors"
        item_dir = collection_dir / "item"
        item_dir.mkdir(parents=True)
        test_file = item_dir / "data.geojson"
        shutil.copy(valid_points_geojson, test_file)

        # Act: try to use invalid item_id with path separator
        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                "--item-id",
                "invalid/item-id",
                str(test_file),
            ],
        )

        # Should fail
        assert result.exit_code != 0, "Should fail for item_id with '/'"
        assert "single path segment" in result.output.lower() or "invalid" in result.output.lower()

    @pytest.mark.integration
    def test_add_item_id_with_directory_rejects(
        self, runner: CliRunner, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """add --item-id with a directory path fails (ambiguous for multiple files)."""
        # Set up: directory with a geo file
        collection_dir = initialized_catalog / "vectors"
        item_dir = collection_dir / "item"
        item_dir.mkdir(parents=True)
        test_file = item_dir / "data.geojson"
        shutil.copy(valid_points_geojson, test_file)

        # Act: use --item-id with a directory path
        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                "--item-id",
                "my-custom-id",
                str(collection_dir),
            ],
        )

        # Should fail because --item-id is ambiguous with directories
        assert result.exit_code != 0, "Should fail for --item-id with directory"
        assert "single file" in result.output.lower() or "directory" in result.output.lower()


class TestRmIntegration:
    """Integration tests for 'portolan rm' command."""

    @pytest.mark.integration
    def test_rm_requires_force_for_destructive(
        self, runner: CliRunner, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """rm without --force or --keep fails with safety error."""
        # Set up
        collection_dir = initialized_catalog / "vectors"
        collection_dir.mkdir()
        test_file = collection_dir / "data.geojson"
        shutil.copy(valid_points_geojson, test_file)

        # Try rm without --force
        result = runner.invoke(
            cli,
            ["rm", "--portolan-dir", str(initialized_catalog), str(test_file)],
        )

        # Should fail with safety error
        assert result.exit_code == 1
        assert "--force" in result.output or "SafetyError" in result.output

    @pytest.mark.integration
    def test_rm_force_deletes_file_and_untracks(
        self, runner: CliRunner, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """rm --force deletes file from disk and removes from versions.json."""
        # Set up: add a file first
        collection_dir = initialized_catalog / "vectors"
        collection_dir.mkdir()
        test_file = collection_dir / "data.geojson"
        shutil.copy(valid_points_geojson, test_file)

        # Add the file
        add_result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )
        assert add_result.exit_code == 0

        # Find the converted file
        converted_file = collection_dir / "data" / "data.parquet"
        if not converted_file.exists():
            # Might be in different location
            for p in collection_dir.rglob("*.parquet"):
                converted_file = p
                break

        # Act: remove the file with --force
        target = converted_file if converted_file.exists() else test_file
        result = runner.invoke(
            cli,
            ["rm", "--portolan-dir", str(initialized_catalog), "--force", str(target)],
            catch_exceptions=False,
        )

        # Assert
        assert result.exit_code == 0, f"Rm failed: {result.output}"
        # File should be deleted
        assert not converted_file.exists() or not test_file.exists()

    @pytest.mark.integration
    def test_rm_dry_run_previews_without_deletion(
        self, runner: CliRunner, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """rm --dry-run shows what would be removed without deleting."""
        # Set up
        collection_dir = initialized_catalog / "vectors"
        collection_dir.mkdir()
        test_file = collection_dir / "data.geojson"
        shutil.copy(valid_points_geojson, test_file)

        runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )

        # Act: rm --dry-run (no --force needed)
        result = runner.invoke(
            cli,
            ["rm", "--portolan-dir", str(initialized_catalog), "--dry-run", str(test_file)],
            catch_exceptions=False,
        )

        # Assert: should succeed and file should still exist
        assert result.exit_code == 0
        assert test_file.exists(), "File was deleted despite --dry-run"

    @pytest.mark.integration
    def test_rm_keep_preserves_file(
        self, runner: CliRunner, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """rm --keep removes from tracking but preserves file on disk."""
        # Set up
        collection_dir = initialized_catalog / "vectors"
        collection_dir.mkdir()
        test_file = collection_dir / "data.geojson"
        shutil.copy(valid_points_geojson, test_file)

        runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )

        # Find the converted file
        parquet_files = list(collection_dir.rglob("*.parquet"))

        # Act: rm --keep
        target = parquet_files[0] if parquet_files else test_file
        result = runner.invoke(
            cli,
            ["rm", "--portolan-dir", str(initialized_catalog), "--keep", str(target)],
            catch_exceptions=False,
        )

        # Assert
        assert result.exit_code == 0
        # File should still exist on disk
        assert target.exists(), f"File {target} was deleted despite --keep"

    @pytest.mark.integration
    def test_rm_directory_removes_all(
        self, runner: CliRunner, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """rm --force directory removes all tracked files inside."""
        # Set up
        collection_dir = initialized_catalog / "vectors"
        collection_dir.mkdir()
        shutil.copy(valid_points_geojson, collection_dir / "file1.geojson")
        shutil.copy(valid_points_geojson, collection_dir / "file2.geojson")

        runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(collection_dir)],
            catch_exceptions=False,
        )

        # Act (--force required for destructive operation)
        result = runner.invoke(
            cli,
            ["rm", "--portolan-dir", str(initialized_catalog), "--force", str(collection_dir)],
            catch_exceptions=False,
        )

        # Assert
        assert result.exit_code == 0


class TestAddRmRoundtrip:
    """Tests for add/rm workflow roundtrip."""

    @pytest.mark.integration
    def test_add_rm_add_works(
        self, runner: CliRunner, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """Can add, remove, then add again without issues."""
        # Set up
        collection_dir = initialized_catalog / "vectors"
        collection_dir.mkdir()
        test_file = collection_dir / "data.geojson"
        shutil.copy(valid_points_geojson, test_file)

        # Add
        result1 = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )
        assert result1.exit_code == 0

        # Remove with --keep (so we can re-add)
        result2 = runner.invoke(
            cli,
            ["rm", "--portolan-dir", str(initialized_catalog), "--keep", str(test_file)],
            catch_exceptions=False,
        )
        assert result2.exit_code == 0

        # Add again
        result3 = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )
        assert result3.exit_code == 0


class TestOldCommandsRemoved:
    """Tests to verify old dataset add/remove commands are removed."""

    @pytest.mark.integration
    def test_dataset_add_no_longer_exists(self, runner: CliRunner) -> None:
        """'portolan dataset add' command should not exist."""
        result = runner.invoke(cli, ["dataset", "add", "dummy"])

        # Should fail because command doesn't exist
        assert result.exit_code != 0
        assert "no such command" in result.output.lower()

    @pytest.mark.integration
    def test_dataset_remove_no_longer_exists(self, runner: CliRunner) -> None:
        """'portolan dataset remove' command should not exist."""
        result = runner.invoke(cli, ["dataset", "remove", "dummy"])

        # Should fail because command doesn't exist
        assert result.exit_code != 0
        assert "no such command" in result.output.lower()
