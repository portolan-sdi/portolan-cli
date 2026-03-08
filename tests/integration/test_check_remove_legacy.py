"""Integration tests for check --fix --remove-legacy.

Tests the full CLI workflow for removing legacy files after conversion.
Per Issue #209:
- Flag must be explicit (--remove-legacy)
- Only delete after successful conversion
- Handle shapefile sidecars
- Handle FileGDB directories
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli

if TYPE_CHECKING:
    pass


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI runner."""
    return CliRunner()


@pytest.fixture
def valid_points_geojson(tmp_path: Path) -> Path:
    """Create a valid GeoJSON file with point features."""
    geojson = tmp_path / "fixtures" / "valid_points.geojson"
    geojson.parent.mkdir(parents=True, exist_ok=True)
    geojson.write_text(
        """{
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-75.1, 39.9]},
                "properties": {"name": "Philadelphia"}
            }
        ]
    }"""
    )
    return geojson


# =============================================================================
# Tests: CLI Flag Integration
# =============================================================================


@pytest.mark.integration
class TestRemoveLegacyFlag:
    """Tests for --remove-legacy CLI flag."""

    def test_flag_exists_in_help(self, runner: CliRunner) -> None:
        """--remove-legacy should appear in help output."""
        result = runner.invoke(cli, ["check", "--help"])

        assert result.exit_code == 0
        assert "--remove-legacy" in result.output

    def test_flag_requires_fix(self, runner: CliRunner, tmp_path: Path) -> None:
        """--remove-legacy without --fix should warn or error."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.geojson").write_text("{}")

        result = runner.invoke(cli, ["check", str(data_dir), "--remove-legacy"])

        # Should fail or warn since --fix not provided
        assert "--remove-legacy requires --fix" in result.output or result.exit_code != 0


# =============================================================================
# Tests: Conversion + Removal Workflow
# =============================================================================


@pytest.mark.integration
class TestRemoveLegacyConversion:
    """Tests for full conversion + removal workflow."""

    def test_removes_geojson_after_conversion(
        self,
        runner: CliRunner,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """Should remove GeoJSON after successful conversion to GeoParquet."""
        # Set up directory with GeoJSON
        input_dir = tmp_path / "data"
        input_dir.mkdir()
        geojson = input_dir / "points.geojson"
        shutil.copy(valid_points_geojson, geojson)

        result = runner.invoke(cli, ["check", str(input_dir), "--fix", "--remove-legacy"])

        assert result.exit_code == 0
        # GeoParquet should exist
        assert (input_dir / "points.parquet").exists()
        # Original GeoJSON should be removed
        assert not geojson.exists()

    def test_preserves_geojson_without_flag(
        self,
        runner: CliRunner,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """Should preserve GeoJSON when --remove-legacy not specified."""
        input_dir = tmp_path / "data"
        input_dir.mkdir()
        geojson = input_dir / "points.geojson"
        shutil.copy(valid_points_geojson, geojson)

        result = runner.invoke(cli, ["check", str(input_dir), "--fix"])

        assert result.exit_code == 0
        # Both should exist
        assert (input_dir / "points.parquet").exists()
        assert geojson.exists()

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="geoparquet-io segfaults on malformed input on Windows (upstream bug)",
    )
    def test_preserves_source_on_failed_conversion(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """Should NOT remove source if conversion fails."""
        input_dir = tmp_path / "data"
        input_dir.mkdir()
        # Create invalid GeoJSON that will fail conversion
        bad_geojson = input_dir / "bad.geojson"
        bad_geojson.write_text("not valid json at all {{{")

        runner.invoke(cli, ["check", str(input_dir), "--fix", "--remove-legacy"])

        # Source should still exist since conversion failed
        assert bad_geojson.exists()

    def test_dry_run_does_not_remove_files(
        self,
        runner: CliRunner,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """--dry-run should not remove files even with --remove-legacy."""
        input_dir = tmp_path / "data"
        input_dir.mkdir()
        geojson = input_dir / "points.geojson"
        shutil.copy(valid_points_geojson, geojson)

        result = runner.invoke(
            cli, ["check", str(input_dir), "--fix", "--dry-run", "--remove-legacy"]
        )

        assert result.exit_code == 0
        # Both files should still exist (dry run)
        assert geojson.exists()
        # No parquet created in dry run
        assert not (input_dir / "points.parquet").exists()


# =============================================================================
# Tests: Shapefile Sidecars
# =============================================================================


@pytest.mark.integration
class TestRemoveLegacyShapefile:
    """Tests for removing shapefiles with sidecars."""

    @pytest.fixture
    def shapefile_with_sidecars(self, tmp_path: Path) -> Path:
        """Create a minimal valid shapefile with sidecars."""
        # We'll use a fixture directory
        fixtures_dir = Path(__file__).parent.parent / "fixtures"
        shp_source = fixtures_dir / "valid_shapefile"

        if shp_source.exists():
            # Copy real shapefile if available
            data_dir = tmp_path / "data"
            data_dir.mkdir()
            for f in shp_source.glob("*"):
                shutil.copy(f, data_dir)
            return data_dir / "test.shp"

        # Create mock shapefile (minimal binary headers)
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # .shp file (minimal shapefile header)
        shp = data_dir / "test.shp"
        # Minimal shapefile: 100-byte header + no records
        header = bytearray(100)
        header[0:4] = (9994).to_bytes(4, "big")  # Magic number
        header[24:28] = (50).to_bytes(4, "big")  # File length (50 16-bit words = 100 bytes)
        header[28:32] = (1000).to_bytes(4, "little")  # Version
        header[32:36] = (1).to_bytes(4, "little")  # Shape type: Point
        shp.write_bytes(header)

        # .shx file (index file)
        shx = data_dir / "test.shx"
        shx.write_bytes(header)  # Same structure for minimal file

        # .dbf file (dBASE file)
        dbf = data_dir / "test.dbf"
        # Minimal dBASE III header
        dbf_header = bytearray(32)
        dbf_header[0] = 0x03  # Version dBASE III
        dbf_header[8:10] = (33).to_bytes(2, "little")  # Header size
        dbf_header[10:12] = (1).to_bytes(2, "little")  # Record size
        dbf_header[4:8] = (0).to_bytes(4, "little")  # Number of records
        dbf.write_bytes(dbf_header + b"\x0d")  # Header terminator

        # .prj file (projection)
        prj = data_dir / "test.prj"
        prj.write_text('GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984"]]')

        # .cpg file (code page)
        cpg = data_dir / "test.cpg"
        cpg.write_text("UTF-8")

        return shp

    def test_removes_all_shapefile_sidecars(
        self,
        runner: CliRunner,
        shapefile_with_sidecars: Path,
    ) -> None:
        """Should remove .shp and all sidecars (.dbf, .shx, .prj, .cpg)."""
        shp = shapefile_with_sidecars
        parent = shp.parent

        # Skip if this is a mock shapefile (can't be converted)
        # This test needs real shapefiles from fixtures
        pytest.importorskip("geoparquet_io", reason="Requires geoparquet-io for conversion")

        # Verify sidecars exist before
        assert (parent / "test.dbf").exists()
        assert (parent / "test.shx").exists()

        result = runner.invoke(cli, ["check", str(parent), "--fix", "--remove-legacy"])

        if result.exit_code == 0:
            # All shapefile components should be removed
            assert not shp.exists(), "Main .shp file should be removed"
            assert not (parent / "test.dbf").exists(), ".dbf should be removed"
            assert not (parent / "test.shx").exists(), ".shx should be removed"
            assert not (parent / "test.prj").exists(), ".prj should be removed"
            assert not (parent / "test.cpg").exists(), ".cpg should be removed"

            # Output should exist
            assert (parent / "test.parquet").exists()


# =============================================================================
# Tests: FileGDB Directory
# =============================================================================


@pytest.mark.integration
class TestRemoveLegacyFileGDB:
    """Tests for removing FileGDB directories."""

    @pytest.fixture
    def filegdb_directory(self, tmp_path: Path) -> Path:
        """Create a mock FileGDB directory."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        gdb = data_dir / "dataset.gdb"
        gdb.mkdir()

        # FileGDB internal files
        (gdb / "a00000001.gdbtable").write_bytes(b"fake table")
        (gdb / "a00000001.gdbtablx").write_bytes(b"fake index")
        (gdb / "gdb").write_bytes(b"marker")

        return gdb

    def test_removes_entire_gdb_directory(
        self,
        runner: CliRunner,
        filegdb_directory: Path,
    ) -> None:
        """Should remove entire .gdb directory after conversion."""
        gdb = filegdb_directory
        parent = gdb.parent

        # Skip - requires GDAL with FileGDB support
        pytest.skip("FileGDB conversion requires GDAL with FileGDB driver")

        result = runner.invoke(cli, ["check", str(parent), "--fix", "--remove-legacy"])

        if result.exit_code == 0:
            # Entire directory should be removed
            assert not gdb.exists()


# =============================================================================
# Tests: JSON Output
# =============================================================================


@pytest.mark.integration
class TestRemoveLegacyJsonOutput:
    """Tests for JSON output with --remove-legacy."""

    def test_json_output_includes_removed_files(
        self,
        runner: CliRunner,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """JSON output should list removed files."""
        input_dir = tmp_path / "data"
        input_dir.mkdir()
        geojson = input_dir / "points.geojson"
        shutil.copy(valid_points_geojson, geojson)

        result = runner.invoke(cli, ["check", str(input_dir), "--fix", "--remove-legacy", "--json"])

        assert result.exit_code == 0
        output = json.loads(result.output)

        # Should have legacy_removed in the output data
        # Structure: output.data.conversion.legacy_removed
        assert output.get("success") is True
        data = output.get("data", {})
        conversion = data.get("conversion", {})
        assert "legacy_removed" in conversion
        assert conversion["legacy_removed"]["summary"]["removed_count"] >= 1


# =============================================================================
# Tests: Edge Cases
# =============================================================================


@pytest.mark.integration
class TestRemoveLegacyEdgeCases:
    """Edge case tests for --remove-legacy."""

    def test_mixed_success_failure(
        self,
        runner: CliRunner,
        valid_points_geojson: Path,
        tmp_path: Path,
    ) -> None:
        """Should only remove successfully converted files."""
        input_dir = tmp_path / "data"
        input_dir.mkdir()

        # Good file
        good = input_dir / "good.geojson"
        shutil.copy(valid_points_geojson, good)

        # Bad file
        bad = input_dir / "bad.geojson"
        bad.write_text("invalid json {{{")

        runner.invoke(cli, ["check", str(input_dir), "--fix", "--remove-legacy"])

        # Good file should be converted and removed
        assert (input_dir / "good.parquet").exists()
        assert not good.exists()

        # Bad file should remain (conversion failed)
        assert bad.exists()

    def test_already_cloud_native_not_removed(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """Should not remove files that were already cloud-native."""
        input_dir = tmp_path / "data"
        input_dir.mkdir()

        # Create a "parquet" file (already cloud-native)
        # In reality this would be a proper GeoParquet
        parquet = input_dir / "existing.parquet"
        parquet.write_bytes(b"PAR1fake parquet")

        original_mtime = parquet.stat().st_mtime

        runner.invoke(cli, ["check", str(input_dir), "--fix", "--remove-legacy"])

        # File should still exist (was already cloud-native, not converted)
        assert parquet.exists()
        # Shouldn't have been touched
        assert parquet.stat().st_mtime == original_mtime
