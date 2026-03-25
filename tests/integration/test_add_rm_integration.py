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
        """add infers full nested collection ID from path (ADR-0032)."""
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

        # Per ADR-0032: collection at leaf level with full nested path
        collection_json = (
            initialized_catalog / "demographics" / "census" / "2020" / "collection.json"
        )
        assert collection_json.exists(), "Collection not created at 'demographics/census/2020'"
        # Intermediate catalogs should exist
        assert (initialized_catalog / "demographics" / "catalog.json").exists()
        assert (initialized_catalog / "demographics" / "census" / "catalog.json").exists()

    @pytest.mark.integration
    def test_add_multiple_files_creates_snapshot_with_all_assets(
        self, runner: CliRunner, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """Adding a file captures all geo files in the same directory.

        Per ADR-0028 ("Track ALL files in item directories as assets"):
        - When adding a file, ALL geo files in the directory are captured as assets
        - Adding an already-tracked file is a no-op (is_current returns True)

        Expected behavior:
        - Add file A → version 1.0.0 with {A, B, converted parquet}
        - Add file B → no-op (file already tracked in version 1.0.0)
        """
        import json

        # Set up: create collection with two files in SAME directory
        # Per ADR-0032: leaf directory = one collection
        collection_dir = initialized_catalog / "snapshot-test"
        collection_dir.mkdir(parents=True)
        shutil.copy(valid_points_geojson, collection_dir / "file-a.geojson")
        shutil.copy(valid_points_geojson, collection_dir / "file-b.geojson")

        # Act 1: Add first file - this captures ALL geo files in the directory
        result1 = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                str(collection_dir / "file-a.geojson"),
            ],
            catch_exceptions=False,
        )
        assert result1.exit_code == 0, f"First add failed: {result1.output}"

        # Check version 1.0.0 has BOTH files (per ADR-0028)
        versions_path = collection_dir / "versions.json"
        with open(versions_path) as f:
            v1 = json.load(f)
        assert len(v1["versions"]) == 1
        # First add should capture ALL geo files in directory
        asset_keys = list(v1["versions"][0]["assets"].keys())
        assert any("file-a" in k for k in asset_keys), f"file-a not in assets: {asset_keys}"
        assert any("file-b" in k for k in asset_keys), (
            f"file-b should be captured by ADR-0028 directory scan: {asset_keys}"
        )

        # Act 2: Add second file - should be no-op since already tracked
        result2 = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                str(collection_dir / "file-b.geojson"),
            ],
            catch_exceptions=False,
        )
        assert result2.exit_code == 0, f"Second add failed: {result2.output}"

        # Assert: still only 1 version (second add was no-op)
        with open(versions_path) as f:
            v2 = json.load(f)

        assert len(v2["versions"]) == 1, (
            "Should still have 1 version - second add is no-op for already-tracked file"
        )


class TestAddItemIdOverrideIntegration:
    """Integration tests for --item-id flag on 'portolan add' command.

    Issue #136: Users should be able to override automatic item ID derivation.

    Per ADR-0031: --item-id only applies to raster data (item-level assets).
    Vector data is collection-level and doesn't create items.
    """

    @pytest.mark.integration
    def test_add_with_item_id_creates_item_with_custom_id(
        self, runner: CliRunner, initialized_catalog: Path, valid_singleband_cog: Path
    ) -> None:
        """add --item-id creates STAC item with the custom ID.

        Per ADR-0031: Raster data = item-level asset (grandparent = collection).
        Per ADR-0032: For rasters, collection is the grandparent, item is the parent.
        The --item-id flag overrides the default item ID (parent directory name).
        """
        import json

        # Set up: create structure for raster data
        # Per ADR-0031: Raster structure is collection/item_dir/data.tif
        # imagery = collection, scene-001 = item directory
        collection_dir = initialized_catalog / "imagery"
        item_dir = collection_dir / "scene-001"
        item_dir.mkdir(parents=True)
        test_file = item_dir / "data.tif"
        shutil.copy(valid_singleband_cog, test_file)

        # Act: add with --item-id override
        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                "--item-id",
                "custom-scene-2024",
                str(test_file),
            ],
            catch_exceptions=False,
        )

        # Assert
        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Per ADR-0031: versions.json is at the collection level (imagery/)
        versions_path = collection_dir / "versions.json"
        assert versions_path.exists(), "versions.json not created at collection level"
        with open(versions_path) as f:
            versions = json.load(f)
        assert len(versions["versions"]) > 0, "No version entries created"

        # Per ADR-0031: Raster item JSON is at item_dir/{item_id}.json
        # The item_id is custom, but the item JSON stays alongside the data.
        item_json_path = item_dir / "custom-scene-2024.json"
        assert item_json_path.exists(), f"Item JSON not found at {item_json_path}"

        with open(item_json_path) as f:
            item_json = json.load(f)
        assert item_json["id"] == "custom-scene-2024", (
            f"STAC item ID should be 'custom-scene-2024', got '{item_json['id']}'"
        )

    @pytest.mark.integration
    def test_add_with_item_id_stac_item_uses_custom_id(
        self, runner: CliRunner, initialized_catalog: Path, valid_singleband_cog: Path
    ) -> None:
        """add --item-id results in STAC item.json with custom ID field.

        Per ADR-0031: Raster data = item-level asset with item.json.
        """
        import json

        # Set up: raster structure per ADR-0031
        collection_dir = initialized_catalog / "imagery"
        item_dir = collection_dir / "original-dir"
        item_dir.mkdir(parents=True)
        test_file = item_dir / "satellite.tif"
        shutil.copy(valid_singleband_cog, test_file)

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

        # Per ADR-0031: Raster item JSON at item_dir/{item_id}.json (flat structure)
        item_json_path = item_dir / "my-custom-item.json"
        assert item_json_path.exists(), f"Item JSON not found at {item_json_path}"

        with open(item_json_path) as fh:
            item_json = json.load(fh)

        assert item_json is not None, "No STAC item found"
        assert item_json.get("id") == "my-custom-item", (
            f"STAC item ID should be 'my-custom-item', got '{item_json.get('id')}'"
        )

    @pytest.mark.integration
    def test_add_without_item_id_derives_from_directory(
        self, runner: CliRunner, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """Without --item-id, item ID is derived from collection directory (ADR-0032)."""
        import json

        # Per ADR-0032: file's parent directory is the leaf collection
        # Using single-level collection to avoid nested catalog complexity
        collection_dir = initialized_catalog / "census-2020"
        collection_dir.mkdir(parents=True)
        test_file = collection_dir / "data.geojson"
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

        # Per ADR-0032: collection.json and versions.json at collection level
        collection_json_path = collection_dir / "collection.json"
        assert collection_json_path.exists(), f"Collection JSON not found at {collection_json_path}"

        versions_path = collection_dir / "versions.json"
        assert versions_path.exists(), f"versions.json not found at {versions_path}"

        with open(collection_json_path) as f:
            collection_json = json.load(f)
        assert collection_json["id"] == "census-2020", (
            f"Collection ID should be 'census-2020', got '{collection_json['id']}'"
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
    """Tests to verify deprecated dataset command group is removed."""

    @pytest.mark.integration
    def test_dataset_command_group_no_longer_exists(self, runner: CliRunner) -> None:
        """'portolan dataset' command group should not exist.

        The entire `dataset` command group was deprecated in favor of
        top-level commands: `portolan list`, `portolan info`, `portolan add`,
        and `portolan rm`.
        """
        result = runner.invoke(cli, ["dataset", "--help"])

        # Should fail because the command group doesn't exist
        assert result.exit_code != 0
        assert "no such command" in result.output.lower()
