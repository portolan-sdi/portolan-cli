"""Integration tests for `portolan clean` command.

Tests for:
- Full workflow: init -> add -> clean -> verify metadata gone, data preserved
- Clean from subdirectory (should find catalog root)
- Clean on empty catalog (just .portolan/)
- Clean with --dry-run shows correct files
- Clean removes empty directories
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.catalog import find_catalog_root
from portolan_cli.cli import cli


class TestCleanFullWorkflow:
    """Integration tests for full clean workflow."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_init_add_clean_preserves_data(
        self, runner: CliRunner, tmp_path: Path, valid_points_geojson: Path
    ) -> None:
        """Full workflow: init -> add files -> clean -> verify data preserved."""
        import shutil

        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Step 1: Initialize catalog
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0

            # Step 2: Create a collection structure with data
            collection_dir = Path("census")
            collection_dir.mkdir()
            # Copy fixture data
            shutil.copy(valid_points_geojson, collection_dir / "points.geojson")

            # Create STAC metadata manually (simulating what 'add' would do)
            (collection_dir / "collection.json").write_text(
                json.dumps(
                    {
                        "type": "Collection",
                        "id": "census",
                        "stac_version": "1.0.0",
                        "description": "Census data",
                        "links": [],
                        "extent": {
                            "spatial": {"bbox": [[-180, -90, 180, 90]]},
                            "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
                        },
                        "license": "proprietary",
                    }
                )
            )
            (collection_dir / "versions.json").write_text(
                json.dumps({"schema_version": "1.0.0", "versions": []})
            )

            item_dir = collection_dir / "2020"
            item_dir.mkdir()
            shutil.copy(valid_points_geojson, item_dir / "data.geojson")
            (item_dir / "item.json").write_text(
                json.dumps(
                    {
                        "type": "Feature",
                        "id": "2020",
                        "stac_version": "1.0.0",
                        "geometry": None,
                        "bbox": [-180, -90, 180, 90],
                        "properties": {"datetime": "2020-01-01T00:00:00Z"},
                        "links": [],
                        "assets": {},
                    }
                )
            )

            # Verify metadata exists
            assert Path("catalog.json").exists()
            assert Path(".portolan").exists()
            assert (collection_dir / "collection.json").exists()
            assert (item_dir / "item.json").exists()

            # Step 3: Clean
            result = runner.invoke(cli, ["clean"])
            assert result.exit_code == 0

            # Step 4: Verify metadata is gone
            assert not Path("catalog.json").exists()
            assert not Path(".portolan").exists()
            assert not (collection_dir / "collection.json").exists()
            assert not (collection_dir / "versions.json").exists()
            # Item directory might be gone if only had metadata
            if item_dir.exists():
                assert not (item_dir / "item.json").exists()

            # Step 5: Verify data is preserved
            assert (collection_dir / "points.geojson").exists()
            # Item data should also be preserved if the directory still exists
            # (depends on whether it had data)

    @pytest.mark.integration
    def test_clean_after_init_only(self, runner: CliRunner, tmp_path: Path) -> None:
        """Clean on freshly initialized catalog (just .portolan/ and catalog.json)."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Initialize
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0

            # Clean
            result = runner.invoke(cli, ["clean"])
            assert result.exit_code == 0

            # Everything should be gone
            assert not Path("catalog.json").exists()
            assert not Path("versions.json").exists()
            assert not Path(".portolan").exists()

    @pytest.mark.integration
    def test_clean_preserves_all_data_formats(self, runner: CliRunner, tmp_path: Path) -> None:
        """Clean should preserve all common data file formats."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Initialize
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0

            # Create data files of various formats
            data_files = [
                ("data.parquet", b"PAR1"),
                ("imagery.tif", b"II*\x00"),
                ("points.gpkg", b"SQLite format 3"),
                ("boundary.geojson", b'{"type":"FeatureCollection"}'),
                ("legacy.shp", b"\x00\x00\x27\x0a"),
                ("style.json", b'{"version": 8}'),  # Non-STAC JSON
            ]

            for filename, content in data_files:
                Path(filename).write_bytes(content)

            # Clean
            result = runner.invoke(cli, ["clean"])
            assert result.exit_code == 0

            # All data files should be preserved
            for filename, _content in data_files:
                assert Path(filename).exists(), f"{filename} should be preserved"


class TestCleanFromSubdirectory:
    """Integration tests for running clean from subdirectories."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_clean_from_collection_subdirectory(self, runner: CliRunner, tmp_path: Path) -> None:
        """Clean from collection subdirectory should find catalog root and clean."""
        with runner.isolated_filesystem(temp_dir=tmp_path) as td:
            catalog_root = Path(td)

            # Initialize
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0

            # Create collection structure
            collection_dir = catalog_root / "census"
            collection_dir.mkdir()
            (collection_dir / "collection.json").write_text(
                json.dumps(
                    {
                        "type": "Collection",
                        "id": "census",
                        "stac_version": "1.0.0",
                        "description": "test",
                        "links": [],
                        "extent": {
                            "spatial": {"bbox": [[]]},
                            "temporal": {"interval": [[]]},
                        },
                        "license": "proprietary",
                    }
                )
            )

            # Run clean from subdirectory using absolute path for os.chdir
            # to avoid Windows path resolution issues with relative paths.
            import os

            original_cwd = os.getcwd()
            try:
                os.chdir(str(collection_dir))
                result = runner.invoke(cli, ["clean"])
            finally:
                os.chdir(original_cwd)

            assert result.exit_code == 0, (
                f"Expected exit code 0, got {result.exit_code}. Output: {result.output}"
            )
            # Catalog should be cleaned
            assert not (catalog_root / "catalog.json").exists()
            assert not (catalog_root / ".portolan").exists()

    @pytest.mark.integration
    def test_clean_from_deep_subdirectory(self, runner: CliRunner, tmp_path: Path) -> None:
        """Clean from deeply nested subdirectory should find catalog root."""
        with runner.isolated_filesystem(temp_dir=tmp_path) as td:
            catalog_root = Path(td)

            # Initialize
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0

            # Create deep directory structure using absolute path
            deep_dir = catalog_root / "census" / "2020" / "tracts" / "state01"
            deep_dir.mkdir(parents=True)

            # Run clean from deep subdirectory using absolute path for os.chdir
            import os

            original_cwd = os.getcwd()
            try:
                os.chdir(str(deep_dir))
                result = runner.invoke(cli, ["clean"])
            finally:
                os.chdir(original_cwd)

            assert result.exit_code == 0, (
                f"Expected exit code 0, got {result.exit_code}. Output: {result.output}"
            )
            assert not (catalog_root / "catalog.json").exists()


class TestCleanDryRun:
    """Integration tests for --dry-run mode."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_dry_run_lists_all_files(self, runner: CliRunner, tmp_path: Path) -> None:
        """--dry-run should list all files that would be removed."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Initialize
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0

            # Create collection with metadata
            collection_dir = Path("census")
            collection_dir.mkdir()
            (collection_dir / "collection.json").write_text(
                json.dumps(
                    {
                        "type": "Collection",
                        "id": "census",
                        "stac_version": "1.0.0",
                        "description": "test",
                        "links": [],
                        "extent": {
                            "spatial": {"bbox": [[]]},
                            "temporal": {"interval": [[]]},
                        },
                        "license": "proprietary",
                    }
                )
            )
            (collection_dir / "versions.json").write_text(json.dumps({"schema_version": "1.0.0"}))

            result = runner.invoke(cli, ["clean", "--dry-run"])

            assert result.exit_code == 0
            # Should list all metadata files
            assert "catalog.json" in result.output
            assert ".portolan" in result.output
            assert "collection.json" in result.output
            assert "versions.json" in result.output

    @pytest.mark.integration
    def test_dry_run_does_not_modify(self, runner: CliRunner, tmp_path: Path) -> None:
        """--dry-run should not modify any files."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Initialize
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0

            # Run dry-run
            result = runner.invoke(cli, ["clean", "--dry-run"])
            assert result.exit_code == 0

            # Nothing should be modified
            assert Path("catalog.json").exists()
            assert Path("versions.json").exists()
            assert Path(".portolan").exists()
            assert Path(".portolan/config.yaml").exists()
            # state.json removed per issue #290


class TestCleanEmptyDirectories:
    """Integration tests for empty directory cleanup."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_removes_empty_directories_after_cleanup(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Clean should remove directories that become empty."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Initialize
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0

            # Create item directory with only metadata (no data)
            item_dir = Path("census") / "2020"
            item_dir.mkdir(parents=True)
            (Path("census") / "collection.json").write_text(
                json.dumps(
                    {
                        "type": "Collection",
                        "id": "census",
                        "stac_version": "1.0.0",
                        "description": "test",
                        "links": [],
                        "extent": {
                            "spatial": {"bbox": [[]]},
                            "temporal": {"interval": [[]]},
                        },
                        "license": "proprietary",
                    }
                )
            )
            (item_dir / "item.json").write_text(
                json.dumps(
                    {
                        "type": "Feature",
                        "id": "2020",
                        "stac_version": "1.0.0",
                        "geometry": None,
                        "bbox": None,
                        "properties": {},
                        "links": [],
                        "assets": {},
                    }
                )
            )

            # Clean
            result = runner.invoke(cli, ["clean"])
            assert result.exit_code == 0

            # Empty directories should be removed
            assert not item_dir.exists()
            # Census dir should also be removed (it only contained metadata)
            assert not Path("census").exists()

    @pytest.mark.integration
    def test_preserves_directories_with_data(self, runner: CliRunner, tmp_path: Path) -> None:
        """Clean should preserve directories that contain data files."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Initialize
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0

            # Create collection with both metadata and data
            collection_dir = Path("census")
            collection_dir.mkdir()
            (collection_dir / "collection.json").write_text(
                json.dumps(
                    {
                        "type": "Collection",
                        "id": "census",
                        "stac_version": "1.0.0",
                        "description": "test",
                        "links": [],
                        "extent": {
                            "spatial": {"bbox": [[]]},
                            "temporal": {"interval": [[]]},
                        },
                        "license": "proprietary",
                    }
                )
            )
            (collection_dir / "data.parquet").write_bytes(b"PAR1")

            # Clean
            result = runner.invoke(cli, ["clean"])
            assert result.exit_code == 0

            # Directory should be preserved (has data)
            assert collection_dir.exists()
            assert (collection_dir / "data.parquet").exists()
            # Metadata should be gone
            assert not (collection_dir / "collection.json").exists()


class TestCleanFindCatalogRoot:
    """Integration tests for catalog root detection after clean."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_find_catalog_root_returns_none_after_clean(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """After clean, find_catalog_root() should return None."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Initialize
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0

            # Verify catalog exists
            root = find_catalog_root(Path.cwd())
            assert root is not None

            # Clean
            result = runner.invoke(cli, ["clean"])
            assert result.exit_code == 0

            # Verify catalog no longer detected
            root = find_catalog_root(Path.cwd())
            assert root is None


class TestCleanJsonOutput:
    """Integration tests for JSON output mode."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_clean_json_output_success(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan --format json clean should output JSON envelope."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Initialize
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0

            # Clean with JSON output
            result = runner.invoke(cli, ["--format", "json", "clean"])

            assert result.exit_code == 0
            data = json.loads(result.output)
            assert data["success"] is True
            assert data["command"] == "clean"
            assert "files_removed" in data["data"]
            assert "directories_removed" in data["data"]

    @pytest.mark.integration
    def test_clean_json_output_error(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan --format json clean should output JSON error envelope."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Don't initialize - should error
            result = runner.invoke(cli, ["--format", "json", "clean"])

            assert result.exit_code == 1
            data = json.loads(result.output)
            assert data["success"] is False
            assert len(data["errors"]) > 0

    @pytest.mark.integration
    def test_clean_dry_run_json_output(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan --format json clean --dry-run should show preview in JSON."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Initialize
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0

            # Clean with dry-run and JSON output
            result = runner.invoke(cli, ["--format", "json", "clean", "--dry-run"])

            assert result.exit_code == 0
            data = json.loads(result.output)
            assert data["success"] is True
            assert data["command"] == "clean"
            assert "would_remove_files" in data["data"]
            assert "would_remove_directories" in data["data"]


class TestCleanEdgeCases:
    """Integration tests for edge cases."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_clean_partial_catalog(self, runner: CliRunner, tmp_path: Path) -> None:
        """Clean should handle partial catalog (missing some metadata)."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Create partial catalog (only .portolan, no catalog.json yet)
            portolan_dir = Path(".portolan")
            portolan_dir.mkdir()
            (portolan_dir / "config.yaml").write_text("# config")
            # Create catalog.json (required for MANAGED state)
            Path("catalog.json").write_text(json.dumps({"type": "Catalog", "id": "test"}))

            result = runner.invoke(cli, ["clean"])

            assert result.exit_code == 0
            assert not portolan_dir.exists()
            assert not Path("catalog.json").exists()

    @pytest.mark.integration
    def test_clean_orphan_item(self, runner: CliRunner, tmp_path: Path) -> None:
        """Clean should handle orphan item.json without collection.json."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Initialize
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0

            # Create orphan item (no parent collection.json)
            item_dir = Path("orphan-collection") / "item1"
            item_dir.mkdir(parents=True)
            (item_dir / "item.json").write_text(
                json.dumps(
                    {
                        "type": "Feature",
                        "id": "item1",
                        "stac_version": "1.0.0",
                        "geometry": None,
                        "bbox": None,
                        "properties": {},
                        "links": [],
                        "assets": {},
                    }
                )
            )

            result = runner.invoke(cli, ["clean"])

            assert result.exit_code == 0
            # Orphan item should be cleaned
            assert not (item_dir / "item.json").exists()

    @pytest.mark.integration
    def test_clean_nested_stac_json_in_data(self, runner: CliRunner, tmp_path: Path) -> None:
        """Clean should handle STAC-like JSON that's actually user data."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Initialize
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0

            # Create a JSON file that looks like STAC but is user data
            # (e.g., a downloaded STAC item from another source)
            user_data = Path("downloads")
            user_data.mkdir()
            # This is tricky - if it has type=Feature, we'd consider it STAC
            # But if it's in a 'downloads' or similar directory, user might want to keep it
            # For MVP, we'll still remove it if it matches STAC pattern
            # Users should use exclude patterns in future versions
            (user_data / "external-item.json").write_text(
                json.dumps(
                    {
                        "type": "Feature",
                        "id": "external",
                        "stac_version": "1.0.0",
                        "geometry": None,
                        "bbox": None,
                        "properties": {},
                        "links": [],
                        "assets": {},
                    }
                )
            )

            result = runner.invoke(cli, ["clean"])

            assert result.exit_code == 0
            # For MVP, STAC-type JSON files are removed regardless of location.
            # This is documented behavior - users can back up before cleaning.
            assert not (user_data / "external-item.json").exists()
