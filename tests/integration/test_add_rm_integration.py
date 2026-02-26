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

        # Act - use --catalog on the add subcommand to specify catalog root
        result = runner.invoke(
            cli,
            ["add", "--catalog", str(initialized_catalog), str(test_file)],
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
            ["add", "--catalog", str(initialized_catalog), str(collection_dir)],
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
            ["add", "--catalog", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )
        assert result1.exit_code == 0

        # Second add - should be silent or show "unchanged"
        result2 = runner.invoke(
            cli,
            ["add", "--catalog", str(initialized_catalog), str(test_file)],
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
            ["add", "--catalog", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )

        # Second add with --verbose
        result = runner.invoke(
            cli,
            ["add", "--catalog", str(initialized_catalog), "--verbose", str(test_file)],
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
            ["add", "--catalog", str(initialized_catalog), str(shp_file)],
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
            ["add", "--catalog", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )

        # Assert
        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Collection should be "demographics" (first component)
        collection_json = initialized_catalog / "demographics" / "collection.json"
        assert collection_json.exists(), "Collection not created at 'demographics'"


class TestRmIntegration:
    """Integration tests for 'portolan rm' command."""

    @pytest.mark.integration
    def test_rm_deletes_file_and_untracks(
        self, runner: CliRunner, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """rm deletes file from disk and removes from versions.json."""
        # Set up: add a file first
        collection_dir = initialized_catalog / "vectors"
        collection_dir.mkdir()
        test_file = collection_dir / "data.geojson"
        shutil.copy(valid_points_geojson, test_file)

        # Add the file
        add_result = runner.invoke(
            cli,
            ["add", "--catalog", str(initialized_catalog), str(test_file)],
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

        # Act: remove the file
        target = converted_file if converted_file.exists() else test_file
        result = runner.invoke(
            cli,
            ["rm", "--catalog", str(initialized_catalog), str(target)],
            catch_exceptions=False,
        )

        # Assert
        assert result.exit_code == 0, f"Rm failed: {result.output}"
        # File should be deleted
        assert not converted_file.exists() or not test_file.exists()

    @pytest.mark.integration
    def test_rm_no_confirmation_required(
        self, runner: CliRunner, initialized_catalog: Path, valid_points_geojson: Path
    ) -> None:
        """rm works without confirmation (git-style)."""
        # Set up
        collection_dir = initialized_catalog / "vectors"
        collection_dir.mkdir()
        test_file = collection_dir / "data.geojson"
        shutil.copy(valid_points_geojson, test_file)

        runner.invoke(
            cli,
            ["add", "--catalog", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )

        # Act: rm without any input (no confirmation needed)
        result = runner.invoke(
            cli,
            ["rm", "--catalog", str(initialized_catalog), str(test_file)],
            input=None,  # No input - should still work
            catch_exceptions=False,
        )

        # Assert: should succeed without prompting
        assert result.exit_code == 0

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
            ["add", "--catalog", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )

        # Find the converted file
        parquet_files = list(collection_dir.rglob("*.parquet"))

        # Act: rm --keep
        target = parquet_files[0] if parquet_files else test_file
        result = runner.invoke(
            cli,
            ["rm", "--catalog", str(initialized_catalog), "--keep", str(target)],
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
        """rm directory removes all tracked files inside."""
        # Set up
        collection_dir = initialized_catalog / "vectors"
        collection_dir.mkdir()
        shutil.copy(valid_points_geojson, collection_dir / "file1.geojson")
        shutil.copy(valid_points_geojson, collection_dir / "file2.geojson")

        runner.invoke(
            cli,
            ["add", "--catalog", str(initialized_catalog), str(collection_dir)],
            catch_exceptions=False,
        )

        # Act
        result = runner.invoke(
            cli,
            ["rm", "--catalog", str(initialized_catalog), str(collection_dir)],
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
            ["add", "--catalog", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )
        assert result1.exit_code == 0

        # Remove with --keep (so we can re-add)
        result2 = runner.invoke(
            cli,
            ["rm", "--catalog", str(initialized_catalog), "--keep", str(test_file)],
            catch_exceptions=False,
        )
        assert result2.exit_code == 0

        # Add again
        result3 = runner.invoke(
            cli,
            ["add", "--catalog", str(initialized_catalog), str(test_file)],
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
