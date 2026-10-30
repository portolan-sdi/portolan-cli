"""Integration tests for CLI progress output.

Issue #203: Add progress printing for file-level operations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli

if TYPE_CHECKING:
    from pathlib import Path


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
    (portolan_dir / "metadata.yaml").write_text('license: "CC-BY-4.0"\n')

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

        # A non-TTY run prints no progress bar, only the summary
        assert result.exit_code == 0, result.output
        assert "Added 3 files" in result.output

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

        # A non-TTY run prints no progress bar, only the summary
        assert result.exit_code == 0, result.output
        assert "Added 1 file to" in result.output


class TestCheckFixProgress:
    """Tests for progress output in 'portolan check --fix' command."""

    @pytest.fixture
    def catalog_with_shapefiles(self, tmp_path: Path) -> Path:
        """Create a catalog with shapefile-like structure for testing."""
        # Initialize catalog
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("version: 1\n")
        (portolan_dir / "metadata.yaml").write_text('license: "CC-BY-4.0"\n')

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

        # The catalog holds no shapefiles, so there is nothing to convert
        assert result.exit_code == 0, result.output
        assert "No files to convert" in result.output
