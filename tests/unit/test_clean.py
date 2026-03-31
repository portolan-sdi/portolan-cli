"""Unit tests for `portolan clean` command.

Tests for:
- is_stac_metadata() correctly identifies STAC files
- Non-STAC JSON files are skipped
- .portolan/ is always removed
- versions.json is always removed
- dry-run doesn't actually delete
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


class TestIsStacMetadata:
    """Tests for STAC metadata detection logic."""

    @pytest.mark.unit
    def test_identifies_catalog_json(self, tmp_path: Path) -> None:
        """is_stac_metadata should return True for catalog.json with type=Catalog."""
        from portolan_cli.clean import is_stac_metadata

        catalog_file = tmp_path / "catalog.json"
        catalog_file.write_text(
            json.dumps({"type": "Catalog", "id": "test", "stac_version": "1.0.0"})
        )

        assert is_stac_metadata(catalog_file) is True

    @pytest.mark.unit
    def test_identifies_collection_json(self, tmp_path: Path) -> None:
        """is_stac_metadata should return True for collection.json with type=Collection."""
        from portolan_cli.clean import is_stac_metadata

        collection_file = tmp_path / "collection.json"
        collection_file.write_text(
            json.dumps({"type": "Collection", "id": "test", "stac_version": "1.0.0"})
        )

        assert is_stac_metadata(collection_file) is True

    @pytest.mark.unit
    def test_identifies_item_json(self, tmp_path: Path) -> None:
        """is_stac_metadata should return True for item.json with type=Feature."""
        from portolan_cli.clean import is_stac_metadata

        item_file = tmp_path / "item.json"
        item_file.write_text(json.dumps({"type": "Feature", "id": "test", "stac_version": "1.0.0"}))

        assert is_stac_metadata(item_file) is True

    @pytest.mark.unit
    def test_rejects_non_stac_json(self, tmp_path: Path) -> None:
        """is_stac_metadata should return False for JSON without STAC type."""
        from portolan_cli.clean import is_stac_metadata

        style_file = tmp_path / "style.json"
        style_file.write_text(json.dumps({"version": 8, "layers": []}))

        assert is_stac_metadata(style_file) is False

    @pytest.mark.unit
    def test_rejects_non_json_files(self, tmp_path: Path) -> None:
        """is_stac_metadata should return False for non-JSON files."""
        from portolan_cli.clean import is_stac_metadata

        data_file = tmp_path / "data.parquet"
        data_file.write_bytes(b"PAR1")

        assert is_stac_metadata(data_file) is False

    @pytest.mark.unit
    def test_handles_malformed_json(self, tmp_path: Path) -> None:
        """is_stac_metadata should return False for malformed JSON."""
        from portolan_cli.clean import is_stac_metadata

        bad_file = tmp_path / "bad.json"
        bad_file.write_text("{not valid json")

        assert is_stac_metadata(bad_file) is False

    @pytest.mark.unit
    def test_handles_empty_json(self, tmp_path: Path) -> None:
        """is_stac_metadata should return False for empty JSON object."""
        from portolan_cli.clean import is_stac_metadata

        empty_file = tmp_path / "empty.json"
        empty_file.write_text("{}")

        assert is_stac_metadata(empty_file) is False

    @pytest.mark.unit
    def test_rejects_json_with_non_stac_type(self, tmp_path: Path) -> None:
        """is_stac_metadata should return False if type is not Catalog/Collection/Feature."""
        from portolan_cli.clean import is_stac_metadata

        other_file = tmp_path / "other.json"
        other_file.write_text(json.dumps({"type": "SomethingElse"}))

        assert is_stac_metadata(other_file) is False

    @pytest.mark.unit
    def test_handles_json_array(self, tmp_path: Path) -> None:
        """is_stac_metadata should return False for JSON arrays (non-dict root)."""
        from portolan_cli.clean import is_stac_metadata

        array_file = tmp_path / "array.json"
        array_file.write_text(json.dumps([{"type": "Feature"}]))

        assert is_stac_metadata(array_file) is False


class TestIsVersionsJson:
    """Tests for versions.json detection logic."""

    @pytest.mark.unit
    def test_identifies_versions_json(self, tmp_path: Path) -> None:
        """is_versions_json should return True for versions.json."""
        from portolan_cli.clean import is_versions_json

        versions_file = tmp_path / "versions.json"
        versions_file.write_text(
            json.dumps(
                {
                    "schema_version": "1.0.0",
                    "catalog_id": "test",
                    "collections": {},
                }
            )
        )

        assert is_versions_json(versions_file) is True

    @pytest.mark.unit
    def test_rejects_other_json_files(self, tmp_path: Path) -> None:
        """is_versions_json should return False for other JSON files."""
        from portolan_cli.clean import is_versions_json

        other_file = tmp_path / "other.json"
        other_file.write_text(json.dumps({"key": "value"}))

        assert is_versions_json(other_file) is False

    @pytest.mark.unit
    def test_rejects_versions_in_other_path(self, tmp_path: Path) -> None:
        """is_versions_json should only match files named versions.json."""
        from portolan_cli.clean import is_versions_json

        # A file that contains "versions" but isn't named versions.json
        other_file = tmp_path / "my_versions.json"
        other_file.write_text(json.dumps({"versions": []}))

        assert is_versions_json(other_file) is False


class TestCleanCollectFiles:
    """Tests for collecting files to remove."""

    @pytest.mark.unit
    def test_collects_portolan_directory(self, tmp_path: Path) -> None:
        """clean should always collect .portolan directory."""
        from portolan_cli.clean import collect_files_to_remove

        # Create managed catalog
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("# config")
        (tmp_path / "catalog.json").write_text(json.dumps({"type": "Catalog", "id": "test"}))

        files, dirs = collect_files_to_remove(tmp_path)

        assert portolan_dir in dirs

    @pytest.mark.unit
    def test_collects_catalog_json(self, tmp_path: Path) -> None:
        """clean should collect catalog.json at root."""
        from portolan_cli.clean import collect_files_to_remove

        # Create managed catalog
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("# config")

        catalog_file = tmp_path / "catalog.json"
        catalog_file.write_text(json.dumps({"type": "Catalog", "id": "test"}))

        files, _dirs = collect_files_to_remove(tmp_path)

        assert catalog_file in files

    @pytest.mark.unit
    def test_collects_collection_json(self, tmp_path: Path) -> None:
        """clean should collect collection.json files."""
        from portolan_cli.clean import collect_files_to_remove

        # Create managed catalog with collection
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("# config")
        (tmp_path / "catalog.json").write_text(json.dumps({"type": "Catalog", "id": "test"}))

        collection_dir = tmp_path / "census"
        collection_dir.mkdir()
        collection_file = collection_dir / "collection.json"
        collection_file.write_text(json.dumps({"type": "Collection", "id": "census"}))

        files, _dirs = collect_files_to_remove(tmp_path)

        assert collection_file in files

    @pytest.mark.unit
    def test_collects_versions_json(self, tmp_path: Path) -> None:
        """clean should collect versions.json files."""
        from portolan_cli.clean import collect_files_to_remove

        # Create managed catalog with versions
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("# config")
        (tmp_path / "catalog.json").write_text(json.dumps({"type": "Catalog", "id": "test"}))

        # Root versions.json
        versions_file = tmp_path / "versions.json"
        versions_file.write_text(json.dumps({"schema_version": "1.0.0"}))

        # Collection versions.json
        collection_dir = tmp_path / "census"
        collection_dir.mkdir()
        collection_versions = collection_dir / "versions.json"
        collection_versions.write_text(json.dumps({"schema_version": "1.0.0"}))

        files, _dirs = collect_files_to_remove(tmp_path)

        assert versions_file in files
        assert collection_versions in files

    @pytest.mark.unit
    def test_collects_item_json(self, tmp_path: Path) -> None:
        """clean should collect item.json files (STAC items)."""
        from portolan_cli.clean import collect_files_to_remove

        # Create managed catalog with item
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("# config")
        (tmp_path / "catalog.json").write_text(json.dumps({"type": "Catalog", "id": "test"}))

        collection_dir = tmp_path / "census"
        collection_dir.mkdir()
        item_dir = collection_dir / "2020"
        item_dir.mkdir()
        item_file = item_dir / "item.json"
        item_file.write_text(json.dumps({"type": "Feature", "id": "2020"}))

        files, _dirs = collect_files_to_remove(tmp_path)

        assert item_file in files

    @pytest.mark.unit
    def test_preserves_non_stac_json(self, tmp_path: Path) -> None:
        """clean should NOT collect non-STAC JSON files."""
        from portolan_cli.clean import collect_files_to_remove

        # Create managed catalog
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("# config")
        (tmp_path / "catalog.json").write_text(json.dumps({"type": "Catalog", "id": "test"}))

        # Non-STAC JSON file (e.g., Mapbox style)
        style_file = tmp_path / "style.json"
        style_file.write_text(json.dumps({"version": 8, "layers": []}))

        files, _dirs = collect_files_to_remove(tmp_path)

        assert style_file not in files

    @pytest.mark.unit
    def test_preserves_data_files(self, tmp_path: Path) -> None:
        """clean should NOT collect data files."""
        from portolan_cli.clean import collect_files_to_remove

        # Create managed catalog with data
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("# config")
        (tmp_path / "catalog.json").write_text(json.dumps({"type": "Catalog", "id": "test"}))

        collection_dir = tmp_path / "census"
        collection_dir.mkdir()
        parquet_file = collection_dir / "data.parquet"
        parquet_file.write_bytes(b"PAR1")
        tif_file = collection_dir / "imagery.tif"
        tif_file.write_bytes(b"TIFF")

        files, _dirs = collect_files_to_remove(tmp_path)

        assert parquet_file not in files
        assert tif_file not in files


class TestCleanCommand:
    """Tests for the `portolan clean` CLI command."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.unit
    def test_clean_not_in_catalog_fails(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan clean should fail if not inside a catalog."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["clean"])

            assert result.exit_code == 1
            assert "not inside" in result.output.lower() or "portolan" in result.output.lower()

    @pytest.mark.unit
    def test_clean_dry_run_shows_preview(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan clean --dry-run should show what would be removed."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Create managed catalog
            portolan_dir = Path(".portolan")
            portolan_dir.mkdir()
            (portolan_dir / "config.yaml").write_text("# config")
            Path("catalog.json").write_text(json.dumps({"type": "Catalog", "id": "test"}))
            Path("versions.json").write_text(json.dumps({"schema_version": "1.0.0"}))

            result = runner.invoke(cli, ["clean", "--dry-run"])

            assert result.exit_code == 0
            assert "would" in result.output.lower() or "dry run" in result.output.lower()
            assert "catalog.json" in result.output
            assert ".portolan" in result.output

    @pytest.mark.unit
    def test_clean_dry_run_does_not_delete(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan clean --dry-run should NOT actually delete files."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Create managed catalog
            portolan_dir = Path(".portolan")
            portolan_dir.mkdir()
            (portolan_dir / "config.yaml").write_text("# config")
            Path("catalog.json").write_text(json.dumps({"type": "Catalog", "id": "test"}))

            result = runner.invoke(cli, ["clean", "--dry-run"])

            assert result.exit_code == 0
            # Files should still exist
            assert Path("catalog.json").exists()
            assert portolan_dir.exists()
            assert (portolan_dir / "config.yaml").exists()

    @pytest.mark.unit
    def test_clean_removes_metadata(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan clean should remove all metadata files."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Create managed catalog
            portolan_dir = Path(".portolan")
            portolan_dir.mkdir()
            (portolan_dir / "config.yaml").write_text("# config")
            Path("catalog.json").write_text(json.dumps({"type": "Catalog", "id": "test"}))
            Path("versions.json").write_text(json.dumps({"schema_version": "1.0.0"}))

            result = runner.invoke(cli, ["clean"])

            assert result.exit_code == 0
            # Metadata should be gone
            assert not Path("catalog.json").exists()
            assert not Path("versions.json").exists()
            assert not portolan_dir.exists()

    @pytest.mark.unit
    def test_clean_preserves_data(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan clean should preserve all data files."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Create managed catalog with data
            portolan_dir = Path(".portolan")
            portolan_dir.mkdir()
            (portolan_dir / "config.yaml").write_text("# config")
            Path("catalog.json").write_text(json.dumps({"type": "Catalog", "id": "test"}))

            # Create collection with data
            collection_dir = Path("census")
            collection_dir.mkdir()
            (collection_dir / "collection.json").write_text(
                json.dumps({"type": "Collection", "id": "census"})
            )
            data_file = collection_dir / "data.parquet"
            data_file.write_bytes(b"PAR1")

            result = runner.invoke(cli, ["clean"])

            assert result.exit_code == 0
            # Data should still exist
            assert data_file.exists()
            assert collection_dir.exists()
            # Metadata should be gone
            assert not (collection_dir / "collection.json").exists()

    @pytest.mark.unit
    def test_clean_preserves_non_stac_json(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan clean should preserve non-STAC JSON files."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Create managed catalog
            portolan_dir = Path(".portolan")
            portolan_dir.mkdir()
            (portolan_dir / "config.yaml").write_text("# config")
            Path("catalog.json").write_text(json.dumps({"type": "Catalog", "id": "test"}))

            # Create non-STAC JSON file
            style_file = Path("style.json")
            style_file.write_text(json.dumps({"version": 8, "layers": []}))

            result = runner.invoke(cli, ["clean"])

            assert result.exit_code == 0
            # Non-STAC JSON should still exist
            assert style_file.exists()

    @pytest.mark.unit
    def test_clean_outputs_summary(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan clean should output a summary of removed files."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Create managed catalog
            portolan_dir = Path(".portolan")
            portolan_dir.mkdir()
            (portolan_dir / "config.yaml").write_text("# config")
            Path("catalog.json").write_text(json.dumps({"type": "Catalog", "id": "test"}))

            result = runner.invoke(cli, ["clean"])

            assert result.exit_code == 0
            # Should mention removal
            output = result.output.lower()
            assert "removed" in output or "\u2713" in result.output


class TestCleanFromSubdirectory:
    """Tests for running clean from a subdirectory."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.unit
    def test_clean_from_subdirectory(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan clean should work from any subdirectory in the catalog."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Create managed catalog
            portolan_dir = Path(".portolan")
            portolan_dir.mkdir()
            (portolan_dir / "config.yaml").write_text("# config")
            Path("catalog.json").write_text(json.dumps({"type": "Catalog", "id": "test"}))

            # Create subdirectory
            subdir = Path("census") / "2020"
            subdir.mkdir(parents=True)

            # Change to subdirectory and run clean
            import os

            original_cwd = os.getcwd()
            try:
                os.chdir(subdir)
                result = runner.invoke(cli, ["clean"])
            finally:
                os.chdir(original_cwd)

            assert result.exit_code == 0
            # Catalog should be cleaned
            assert not Path("catalog.json").exists()
            assert not portolan_dir.exists()


class TestCleanEmptyDirectoryCleanup:
    """Tests for cleaning up empty directories after removing metadata."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.unit
    def test_removes_empty_item_directories(self, runner: CliRunner, tmp_path: Path) -> None:
        """clean should remove directories that become empty after metadata removal."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Create managed catalog
            portolan_dir = Path(".portolan")
            portolan_dir.mkdir()
            (portolan_dir / "config.yaml").write_text("# config")
            Path("catalog.json").write_text(json.dumps({"type": "Catalog", "id": "test"}))

            # Create item directory with only metadata
            item_dir = Path("census") / "2020"
            item_dir.mkdir(parents=True)
            (item_dir / "item.json").write_text(json.dumps({"type": "Feature", "id": "2020"}))

            result = runner.invoke(cli, ["clean"])

            assert result.exit_code == 0
            # Empty item directory should be removed
            assert not item_dir.exists()

    @pytest.mark.unit
    def test_preserves_directories_with_data(self, runner: CliRunner, tmp_path: Path) -> None:
        """clean should NOT remove directories that contain data files."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Create managed catalog
            portolan_dir = Path(".portolan")
            portolan_dir.mkdir()
            (portolan_dir / "config.yaml").write_text("# config")
            Path("catalog.json").write_text(json.dumps({"type": "Catalog", "id": "test"}))

            # Create item directory with data
            item_dir = Path("census") / "2020"
            item_dir.mkdir(parents=True)
            (item_dir / "item.json").write_text(json.dumps({"type": "Feature", "id": "2020"}))
            (item_dir / "data.parquet").write_bytes(b"PAR1")

            result = runner.invoke(cli, ["clean"])

            assert result.exit_code == 0
            # Directory should be preserved (has data)
            assert item_dir.exists()
            assert (item_dir / "data.parquet").exists()
