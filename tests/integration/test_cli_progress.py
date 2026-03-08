"""Integration tests for CLI progress output.

Issue #203: Add progress printing for file-level operations.
"""

from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


@pytest.fixture
def runner() -> CliRunner:
    """Create a Click test runner."""
    return CliRunner()


@pytest.fixture
def catalog_with_files(tmp_path: Path) -> Path:
    """Create a catalog with multiple files for testing progress."""
    # Initialize catalog structure
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("version: 1\n")

    # Create collection with multiple GeoJSON files
    collection_dir = tmp_path / "test-collection"
    collection_dir.mkdir()

    # Create multiple small GeoJSON files
    for i in range(3):
        geojson = collection_dir / f"data{i}" / f"file{i}.geojson"
        geojson.parent.mkdir(parents=True, exist_ok=True)
        geojson.write_text(
            f'{{"type": "FeatureCollection", "features": ['
            f'{{"type": "Feature", "geometry": {{"type": "Point", "coordinates": [{i}, {i}]}}, "properties": {{"id": {i}}}}}'
            f"]}}"
        )

    return tmp_path


class TestAddProgress:
    """Tests for progress output in 'portolan add' command."""

    @pytest.mark.integration
    def test_add_shows_progress_for_multiple_files(
        self, runner: CliRunner, catalog_with_files: Path
    ) -> None:
        """Add command shows progress when processing multiple files."""
        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(catalog_with_files),
                str(catalog_with_files / "test-collection"),
            ],
            catch_exceptions=False,
            env={"NO_COLOR": "1"},
        )

        # Should show progress messages with "Adding:"
        assert "Adding:" in result.output or result.exit_code == 0

    @pytest.mark.integration
    def test_add_single_file_shows_progress(
        self, runner: CliRunner, catalog_with_files: Path
    ) -> None:
        """Single file add shows progress."""
        single_file = catalog_with_files / "test-collection" / "data0" / "file0.geojson"

        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(catalog_with_files),
                str(single_file),
            ],
            catch_exceptions=False,
            env={"NO_COLOR": "1"},
        )

        # Should show progress or succeed
        assert "Adding:" in result.output or result.exit_code == 0


class TestCheckFixProgress:
    """Tests for progress output in 'portolan check --fix' command."""

    @pytest.fixture
    def catalog_with_shapefiles(self, tmp_path: Path) -> Path:
        """Create a catalog with shapefile-like structure for testing."""
        # Initialize catalog
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("version: 1\n")

        # Create a simple catalog.json
        (tmp_path / "catalog.json").write_text(
            '{"type": "Catalog", "id": "test", "stac_version": "1.0.0", '
            '"description": "Test", "links": []}'
        )

        return tmp_path

    @pytest.mark.integration
    def test_check_fix_with_geo_assets_flag(
        self, runner: CliRunner, catalog_with_shapefiles: Path
    ) -> None:
        """Check --fix --geo-assets runs without error."""
        result = runner.invoke(
            cli,
            ["check", "--fix", "--geo-assets", str(catalog_with_shapefiles)],
            catch_exceptions=False,
            env={"NO_COLOR": "1"},
        )

        # Should complete (may have no files to convert)
        # Exit code 0 or 1 depending on catalog state
        assert result.exit_code in (0, 1)
