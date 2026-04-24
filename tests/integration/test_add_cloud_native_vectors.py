"""Integration tests for adding cloud-native vector formats (PMTiles, FlatGeobuf).

Per Issue #368: Adding PMTiles/FlatGeobuf should work without conversion.
Per ADR-0031: Vector files are collection-level assets.

These tests verify:
1. PMTiles/FlatGeobuf are NOT converted to GeoParquet
2. Files are copied to collection directory
3. Metadata is correctly extracted
4. Assets are registered in collection.json
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
def initialized_catalog(tmp_path: Path) -> Path:
    """Create an initialized Portolan catalog using CLI."""
    result = CliRunner().invoke(cli, ["init", str(tmp_path), "--auto"])
    assert result.exit_code == 0, f"Init failed: {result.output}"
    return tmp_path


@pytest.fixture
def sample_pmtiles() -> Path:
    """Path to sample PMTiles fixture."""
    fixture_path = Path(__file__).parent.parent / "fixtures" / "cloud_native" / "sample.pmtiles"
    if not fixture_path.exists():
        pytest.skip("PMTiles fixture not found")
    return fixture_path


@pytest.fixture
def sample_fgb() -> Path:
    """Path to sample FlatGeobuf fixture."""
    fixture_path = Path(__file__).parent.parent / "fixtures" / "cloud_native" / "sample.fgb"
    if not fixture_path.exists():
        pytest.skip("FlatGeobuf fixture not found")
    return fixture_path


class TestAddPMTiles:
    """Tests for adding PMTiles files."""

    @pytest.mark.integration
    def test_add_pmtiles_no_conversion_error(
        self, runner: CliRunner, initialized_catalog: Path, sample_pmtiles: Path
    ) -> None:
        """Adding PMTiles should NOT fail with CRS extraction error (issue #368)."""
        # Set up: copy PMTiles to catalog
        collection_dir = initialized_catalog / "tiles"
        collection_dir.mkdir()
        test_file = collection_dir / "map.pmtiles"
        shutil.copy(sample_pmtiles, test_file)

        # Act
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )

        # Assert - should NOT fail
        assert result.exit_code == 0, f"Add failed: {result.output}"
        assert "No CRS found" not in result.output

    @pytest.mark.integration
    def test_add_pmtiles_preserves_format(
        self, runner: CliRunner, initialized_catalog: Path, sample_pmtiles: Path
    ) -> None:
        """PMTiles should be copied, not converted to GeoParquet."""
        collection_dir = initialized_catalog / "tiles"
        collection_dir.mkdir()
        test_file = collection_dir / "map.pmtiles"
        shutil.copy(sample_pmtiles, test_file)

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        # PMTiles should still exist (not converted)
        assert test_file.exists() or (collection_dir / "map.pmtiles").exists()
        # Should NOT have created a .parquet file from conversion
        parquet_files = list(collection_dir.rglob("*.parquet"))
        # Filter out items.parquet which is a STAC index file
        data_parquets = [p for p in parquet_files if p.name != "items.parquet"]
        assert len(data_parquets) == 0, f"Unexpected parquet conversion: {data_parquets}"

    @pytest.mark.integration
    def test_add_pmtiles_creates_collection(
        self, runner: CliRunner, initialized_catalog: Path, sample_pmtiles: Path
    ) -> None:
        """Adding PMTiles creates collection.json with correct metadata."""
        collection_dir = initialized_catalog / "tiles"
        collection_dir.mkdir()
        test_file = collection_dir / "map.pmtiles"
        shutil.copy(sample_pmtiles, test_file)

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        # Collection should be created
        collection_json = collection_dir / "collection.json"
        assert collection_json.exists()

        # Load and verify
        data = json.loads(collection_json.read_text())
        assert data["type"] == "Collection"
        assert "extent" in data
        assert "spatial" in data["extent"]

    @pytest.mark.integration
    def test_add_pmtiles_idempotent(
        self, runner: CliRunner, initialized_catalog: Path, sample_pmtiles: Path
    ) -> None:
        """Adding same PMTiles file twice is idempotent (no error, no duplicate)."""
        collection_dir = initialized_catalog / "tiles"
        collection_dir.mkdir()
        test_file = collection_dir / "map.pmtiles"
        shutil.copy(sample_pmtiles, test_file)

        # First add
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        # Second add of same file should succeed (idempotent)
        result2 = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )
        assert result2.exit_code == 0


class TestAddFlatGeobuf:
    """Tests for adding FlatGeobuf files."""

    @pytest.mark.integration
    def test_add_flatgeobuf_no_conversion_error(
        self, runner: CliRunner, initialized_catalog: Path, sample_fgb: Path
    ) -> None:
        """Adding FlatGeobuf should NOT fail with CRS extraction error."""
        collection_dir = initialized_catalog / "boundaries"
        collection_dir.mkdir()
        test_file = collection_dir / "borders.fgb"
        shutil.copy(sample_fgb, test_file)

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, f"Add failed: {result.output}"

    @pytest.mark.integration
    def test_add_flatgeobuf_preserves_format(
        self, runner: CliRunner, initialized_catalog: Path, sample_fgb: Path
    ) -> None:
        """FlatGeobuf should be copied, not converted to GeoParquet."""
        collection_dir = initialized_catalog / "boundaries"
        collection_dir.mkdir()
        test_file = collection_dir / "borders.fgb"
        shutil.copy(sample_fgb, test_file)

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        # FlatGeobuf should still exist (not converted)
        assert test_file.exists() or (collection_dir / "borders.fgb").exists()
        # Should NOT have created a .parquet file from conversion
        parquet_files = list(collection_dir.rglob("*.parquet"))
        data_parquets = [p for p in parquet_files if p.name != "items.parquet"]
        assert len(data_parquets) == 0, f"Unexpected parquet conversion: {data_parquets}"

    @pytest.mark.integration
    def test_add_flatgeobuf_extracts_crs(
        self, runner: CliRunner, initialized_catalog: Path, sample_fgb: Path
    ) -> None:
        """FlatGeobuf CRS should be extracted and recorded in collection."""
        collection_dir = initialized_catalog / "boundaries"
        collection_dir.mkdir()
        test_file = collection_dir / "borders.fgb"
        shutil.copy(sample_fgb, test_file)

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        # Check collection has proj:epsg
        collection_json = collection_dir / "collection.json"
        assert collection_json.exists()

        data = json.loads(collection_json.read_text())
        assert data.get("proj:epsg") == 4326
        assert data.get("flatgeobuf:geometry_type") == "Point"
        assert data.get("flatgeobuf:feature_count") == 3


class TestCollectionLevelAssetBehavior:
    """Tests for ADR-0031: Collection-level asset registration."""

    @pytest.mark.integration
    def test_pmtiles_is_collection_level_asset(
        self, runner: CliRunner, initialized_catalog: Path, sample_pmtiles: Path
    ) -> None:
        """PMTiles should be registered as collection-level asset (no item.json)."""
        collection_dir = initialized_catalog / "tiles"
        collection_dir.mkdir()
        test_file = collection_dir / "map.pmtiles"
        shutil.copy(sample_pmtiles, test_file)

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        # No item.json should exist
        item_jsons = list(collection_dir.rglob("item.json"))
        assert len(item_jsons) == 0, f"Unexpected item.json: {item_jsons}"

        # Asset should be in collection.json
        data = json.loads((collection_dir / "collection.json").read_text())
        assert "map" in data.get("assets", {})
        assert data["assets"]["map"]["type"] == "application/vnd.pmtiles"

    @pytest.mark.integration
    def test_pmtiles_stac_properties_in_collection(
        self, runner: CliRunner, initialized_catalog: Path, sample_pmtiles: Path
    ) -> None:
        """PMTiles properties should be in collection.json (not item)."""
        collection_dir = initialized_catalog / "tiles"
        collection_dir.mkdir()
        test_file = collection_dir / "map.pmtiles"
        shutil.copy(sample_pmtiles, test_file)

        runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )

        data = json.loads((collection_dir / "collection.json").read_text())

        # PMTiles are always Web Mercator (3857)
        assert data.get("proj:epsg") == 3857

        # PMTiles-specific properties
        assert data.get("pmtiles:min_zoom") == 4
        assert data.get("pmtiles:max_zoom") == 8
        assert data.get("pmtiles:tile_type") == "mvt"

        # Projection extension should be declared
        assert "https://stac-extensions.github.io/projection/v1.1.0/schema.json" in data.get(
            "stac_extensions", []
        )

    @pytest.mark.integration
    def test_flatgeobuf_is_collection_level_asset(
        self, runner: CliRunner, initialized_catalog: Path, sample_fgb: Path
    ) -> None:
        """FlatGeobuf should be registered as collection-level asset (no item.json)."""
        collection_dir = initialized_catalog / "boundaries"
        collection_dir.mkdir()
        test_file = collection_dir / "borders.fgb"
        shutil.copy(sample_fgb, test_file)

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(test_file)],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        # No item.json should exist
        item_jsons = list(collection_dir.rglob("item.json"))
        assert len(item_jsons) == 0, f"Unexpected item.json: {item_jsons}"

        # Asset should be in collection.json
        data = json.loads((collection_dir / "collection.json").read_text())
        assert "borders" in data.get("assets", {})
        assert data["assets"]["borders"]["type"] == "application/vnd.flatgeobuf"
