"""Integration tests for adding multi-layer files (GeoPackage, FileGDB).

Tests that `portolan add` correctly handles multi-layer formats by:
1. Detecting all layers in the file
2. Converting each layer to a separate GeoParquet file
3. Creating proper STAC structure for all layers

Per GitHub issue #265.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "multilayer"


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI test runner."""
    return CliRunner()


@pytest.fixture
def initialized_catalog(tmp_path: Path) -> Path:
    """Create an initialized Portolan catalog using CLI."""
    result = CliRunner().invoke(cli, ["init", str(tmp_path), "--auto", "--license", "CC-BY-4.0"])
    assert result.exit_code == 0, f"Init failed: {result.output}"
    return tmp_path


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason=(
        "geoparquet-io aborts on multilayer conversion on macOS, in "
        "read_spatial_to_arrow (upstream bug)"
    ),
)
class TestAddMultilayerIntegration:
    """Integration tests for adding multi-layer files."""

    @pytest.mark.integration
    def test_a_layer_whose_asset_key_collides_is_a_skip(
        self, runner: CliRunner, initialized_catalog: Path
    ) -> None:
        """A layer that two files claim one key for failed with a generic error.

        A single-file add reports the same collision as a skip.
        """
        collection_dir = initialized_catalog / "geodata"
        collection_dir.mkdir()
        gpkg_file = collection_dir / "multilayer.gpkg"
        shutil.copy(FIXTURES_DIR / "multilayer.gpkg", gpkg_file)
        (collection_dir / "multilayer_points.pmtiles").write_bytes(b"tiles")
        (collection_dir / "multilayer_points-tiles").write_bytes(b"notes")

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(gpkg_file)],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output
        assert (
            "Skipping multilayer_points.parquet: Collection 'geodata': asset key "
            "'multilayer_points-tiles' already holds 'multilayer_points.pmtiles'. Adding "
            "'multilayer_points-tiles' under the same key would remove that asset. Remove "
            "one of the two files from the collection, or rename it."
        ) in " ".join(result.output.split())
        assert not (collection_dir / "multilayer_points.parquet").exists()

    @pytest.mark.integration
    def test_add_geopackage_converts_all_layers(
        self, runner: CliRunner, initialized_catalog: Path
    ) -> None:
        """Adding a multi-layer GeoPackage converts each layer to separate parquet."""
        # Set up: copy multilayer fixture to catalog
        collection_dir = initialized_catalog / "geodata"
        collection_dir.mkdir()
        gpkg_file = collection_dir / "multilayer.gpkg"
        shutil.copy(FIXTURES_DIR / "multilayer.gpkg", gpkg_file)

        # Act
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(gpkg_file)],
            catch_exceptions=False,
        )

        # Assert - command succeeded
        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Verify all 3 layers were converted
        expected_parquets = {
            "multilayer_points.parquet",
            "multilayer_lines.parquet",
            "multilayer_polygons.parquet",
        }
        actual_parquets = {f.name for f in collection_dir.glob("*.parquet")}

        assert expected_parquets.issubset(actual_parquets), (
            f"Expected {expected_parquets}, got {actual_parquets}"
        )

    @pytest.mark.integration
    def test_add_filegdb_converts_all_layers(
        self, runner: CliRunner, initialized_catalog: Path
    ) -> None:
        """Adding a multi-layer FileGDB converts each layer to separate parquet."""
        # Set up: copy FileGDB fixture to catalog
        # Using multilayer.gdb generated from multilayer.gpkg (3 points, 2 lines, 2 polygons)
        collection_dir = initialized_catalog / "geodata"
        collection_dir.mkdir()
        gdb_dir = collection_dir / "multilayer.gdb"
        shutil.copytree(FIXTURES_DIR / "multilayer.gdb", gdb_dir)

        # Act
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(gdb_dir)],
            catch_exceptions=False,
        )

        # Assert - command succeeded
        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Verify all 3 layers were converted
        expected_parquets = {
            "multilayer_points.parquet",
            "multilayer_lines.parquet",
            "multilayer_polygons.parquet",
        }
        actual_parquets = {f.name for f in collection_dir.glob("*.parquet")}

        assert expected_parquets.issubset(actual_parquets), (
            f"Expected {expected_parquets}, got {actual_parquets}"
        )

    @pytest.mark.integration
    def test_add_multilayer_creates_stac_structure(
        self, runner: CliRunner, initialized_catalog: Path
    ) -> None:
        """Adding a multi-layer file creates proper STAC collection structure."""
        # Set up
        collection_dir = initialized_catalog / "layers"
        collection_dir.mkdir()
        gpkg_file = collection_dir / "multilayer.gpkg"
        shutil.copy(FIXTURES_DIR / "multilayer.gpkg", gpkg_file)

        # Act
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(gpkg_file)],
            catch_exceptions=False,
        )

        # Assert
        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Verify STAC structure
        assert (collection_dir / "collection.json").exists(), "collection.json not created"
        assert (collection_dir / "versions.json").exists(), "versions.json not created"


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason=(
        "geoparquet-io aborts on multilayer conversion on macOS, in "
        "read_spatial_to_arrow (upstream bug)"
    ),
)
class TestTheProvenanceHashGoesThroughTheRun:
    """Each layer output records the hash of the GeoPackage. The run hashes it once."""

    @pytest.mark.integration
    def test_a_geopackage_of_three_layers_is_hashed_once(self, initialized_catalog: Path) -> None:
        from portolan_cli import trace
        from portolan_cli.convert import convert_multilayer_file
        from portolan_cli.run_state import RunState

        collection_dir = initialized_catalog / "geodata"
        collection_dir.mkdir()
        gpkg_file = collection_dir / "multilayer.gpkg"
        shutil.copy(FIXTURES_DIR / "multilayer.gpkg", gpkg_file)
        state = RunState.open(initialized_catalog)

        with trace.tracing():
            results = convert_multilayer_file(gpkg_file, collection_dir, state=state)
            # The asset scan of the add hashes the GeoPackage through the same memo.
            state.checksum(gpkg_file)
            hashed = trace.counters()["checksum.files"]

        assert [r.success for r in results] == [True, True, True]
        assert hashed == 1
