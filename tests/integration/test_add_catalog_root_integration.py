"""Integration tests for 'portolan add .' at catalog root (Issue #137).

These tests exercise the full add workflow when the user runs `add .` from the
catalog root directory. They verify:
- Collection is correctly inferred from subdirectory structure
- All geo-assets are discovered recursively
- No "Cannot determine collection from path" error is raised
- Output correctly attributes files to their collections
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


class TestAddCatalogRootIntegration:
    """Integration tests for add . at catalog root."""

    @pytest.mark.integration
    def test_add_catalog_root_does_not_error(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        valid_points_geojson: Path,
    ) -> None:
        """add . at catalog root completes without 'Cannot determine collection' error."""
        # Set up: create geo-files in collection subdirectories
        (initialized_catalog / "demographics").mkdir()
        shutil.copy(
            valid_points_geojson,
            initialized_catalog / "demographics" / "census.geojson",
        )

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(initialized_catalog)],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, (
            f"Expected exit code 0, got {result.exit_code}.\nOutput: {result.output}"
        )
        assert "cannot determine collection" not in result.output.lower(), (
            f"Unexpected error in output: {result.output}"
        )

    @pytest.mark.integration
    def test_add_catalog_root_creates_collection_structures(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        valid_points_geojson: Path,
    ) -> None:
        """add . creates STAC collection structure for each subdirectory."""
        # Set up: multiple collection directories
        (initialized_catalog / "demographics").mkdir()
        shutil.copy(
            valid_points_geojson,
            initialized_catalog / "demographics" / "census.geojson",
        )

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(initialized_catalog)],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, f"Add failed: {result.output}"
        # STAC collection structure created per ADR-0023
        assert (initialized_catalog / "demographics" / "collection.json").exists(), (
            "collection.json was not created for 'demographics'"
        )
        assert (initialized_catalog / "demographics" / "versions.json").exists(), (
            "versions.json was not created for 'demographics'"
        )

    @pytest.mark.integration
    def test_add_catalog_root_handles_multiple_collections(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        valid_points_geojson: Path,
    ) -> None:
        """add . processes all collection subdirectories."""
        # Set up: two collections
        (initialized_catalog / "col1").mkdir()
        (initialized_catalog / "col2").mkdir()
        shutil.copy(valid_points_geojson, initialized_catalog / "col1" / "a.geojson")
        shutil.copy(valid_points_geojson, initialized_catalog / "col2" / "b.geojson")

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(initialized_catalog)],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, f"Add failed: {result.output}"
        # Both collections should be created
        assert (initialized_catalog / "col1" / "collection.json").exists(), (
            "collection.json not created for col1"
        )
        assert (initialized_catalog / "col2" / "collection.json").exists(), (
            "collection.json not created for col2"
        )

    @pytest.mark.integration
    def test_add_catalog_root_infers_collection_from_first_path_component(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        valid_points_geojson: Path,
    ) -> None:
        """add . infers collection from first directory component (ADR-0022)."""
        # Set up: deeply nested file
        nested_dir = initialized_catalog / "rivers" / "2020" / "q1"
        nested_dir.mkdir(parents=True)
        shutil.copy(valid_points_geojson, nested_dir / "data.geojson")

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(initialized_catalog)],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, f"Add failed: {result.output}"
        # Collection should be "rivers" (first component), not "rivers/2020/q1"
        assert (initialized_catalog / "rivers" / "collection.json").exists(), (
            "Collection 'rivers' was not created"
        )

    @pytest.mark.integration
    def test_add_catalog_root_skips_portolan_dir(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        valid_points_geojson: Path,
    ) -> None:
        """add . does not try to add files from .portolan directory."""
        # .portolan dir is created by init
        (initialized_catalog / "real_data").mkdir()
        shutil.copy(valid_points_geojson, initialized_catalog / "real_data" / "data.geojson")

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(initialized_catalog)],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, f"Add failed: {result.output}"

    @pytest.mark.integration
    def test_add_catalog_root_idempotent(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        valid_points_geojson: Path,
    ) -> None:
        """add . can be run twice - second run silently skips unchanged files."""
        (initialized_catalog / "data").mkdir()
        shutil.copy(valid_points_geojson, initialized_catalog / "data" / "points.geojson")

        # First add
        result1 = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(initialized_catalog)],
            catch_exceptions=False,
        )
        assert result1.exit_code == 0, f"First add failed: {result1.output}"

        # Second add - should succeed without errors
        result2 = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(initialized_catalog)],
            catch_exceptions=False,
        )
        assert result2.exit_code == 0, f"Second add failed: {result2.output}"

    @pytest.mark.integration
    def test_add_catalog_root_empty_catalog_succeeds(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
    ) -> None:
        """add . on empty catalog (no geo-files) exits cleanly."""
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(initialized_catalog)],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, f"Expected exit 0, got: {result.output}"


class TestAddCatalogRootVsSubdirBehavior:
    """Tests comparing add . vs add <subdir> behavior to ensure backward compat."""

    @pytest.mark.integration
    def test_add_subdir_still_works(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        valid_points_geojson: Path,
    ) -> None:
        """Existing add <collection_dir> behavior is not broken by fix."""
        (initialized_catalog / "demographics").mkdir()
        shutil.copy(
            valid_points_geojson,
            initialized_catalog / "demographics" / "census.geojson",
        )

        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                str(initialized_catalog / "demographics"),
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, f"Subdirectory add failed: {result.output}"
        assert (initialized_catalog / "demographics" / "collection.json").exists()

    @pytest.mark.integration
    def test_add_single_file_still_works(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        valid_points_geojson: Path,
    ) -> None:
        """Existing add <file> behavior is not broken by fix."""
        (initialized_catalog / "vectors").mkdir()
        dest_file = initialized_catalog / "vectors" / "data.geojson"
        shutil.copy(valid_points_geojson, dest_file)

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(dest_file)],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, f"Single file add failed: {result.output}"

    @pytest.mark.integration
    def test_add_file_at_root_level_still_fails(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        valid_points_geojson: Path,
    ) -> None:
        """A file placed directly at catalog root (no collection dir) still fails."""
        # File at catalog root level without a collection dir
        stray_file = initialized_catalog / "stray.geojson"
        shutil.copy(valid_points_geojson, stray_file)

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(stray_file)],
        )

        # Should fail: file must be inside a collection subdirectory
        assert result.exit_code == 1
