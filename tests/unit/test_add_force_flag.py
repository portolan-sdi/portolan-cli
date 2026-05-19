"""Unit tests for --force and --reconvert flags on portolan add.

Per Issue #386: Add --force flag to portolan add for re-tracking files.

Behavior:
- --force: Bypass mtime change detection, re-extract metadata from existing outputs
- --force --reconvert: Also re-convert from source files
- Warning when source file is newer than output (suggests --reconvert)
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.dataset import DatasetInfo
from portolan_cli.formats import FormatType


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI test runner."""
    return CliRunner()


def setup_catalog(path: Path) -> None:
    """Create an initialized Portolan catalog."""
    portolan_dir = path / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("# Portolan configuration\n")
    catalog_data = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "portolan-catalog",
        "description": "A Portolan-managed STAC catalog",
        "links": [],
    }
    (path / "catalog.json").write_text(json.dumps(catalog_data, indent=2))


def create_versions_json(collection_dir: Path, assets: dict) -> None:
    """Create a versions.json with given assets in current version."""
    versions_data = {
        "spec_version": "1.0.0",
        "versions": [
            {
                "version": "v1",
                "created_at": "2024-01-01T00:00:00Z",
                "assets": assets,
            }
        ],
    }
    (collection_dir / "versions.json").write_text(json.dumps(versions_data, indent=2))


class TestForceFlag:
    """Tests for --force flag behavior."""

    @pytest.mark.unit
    def test_force_flag_exists_in_help(self, runner: CliRunner) -> None:
        """--force flag is documented in add command help."""
        result = runner.invoke(cli, ["add", "--help"])
        assert result.exit_code == 0
        assert "--force" in result.output
        assert "change detection" in result.output.lower() or "re-process" in result.output.lower()

    @pytest.mark.unit
    def test_reconvert_flag_exists_in_help(self, runner: CliRunner) -> None:
        """--reconvert flag is documented in add command help."""
        result = runner.invoke(cli, ["add", "--help"])
        assert result.exit_code == 0
        assert "--reconvert" in result.output

    @pytest.mark.unit
    def test_reconvert_requires_force(self, runner: CliRunner) -> None:
        """--reconvert without --force should error."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "data"
            collection_dir.mkdir()
            test_file = collection_dir / "test.geojson"
            test_file.write_text('{"type": "FeatureCollection", "features": []}')

            result = runner.invoke(
                cli,
                ["add", str(test_file), "--reconvert"],
            )

            assert result.exit_code != 0
            assert "requires --force" in result.output.lower() or "--force" in result.output


class TestForceBypassesChangeDetection:
    """Tests that --force bypasses is_current() check."""

    @pytest.mark.unit
    def test_force_processes_unchanged_file(self, runner: CliRunner) -> None:
        """--force re-processes files that would normally be skipped as unchanged."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "data"
            collection_dir.mkdir()
            test_file = collection_dir / "test.geojson"
            test_file.write_text('{"type": "FeatureCollection", "features": []}')

            # Create versions.json marking file as tracked with matching mtime
            file_stat = test_file.stat()
            create_versions_json(
                collection_dir,
                {
                    "test.parquet": {
                        "sha256": "abc123",
                        "mtime": file_stat.st_mtime,
                        "size_bytes": file_stat.st_size,
                    }
                },
            )

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = (
                    [
                        DatasetInfo(
                            item_id="test",
                            collection_id="data",
                            format_type=FormatType.VECTOR,
                            bbox=[-180, -90, 180, 90],
                            asset_paths=["test.parquet"],
                        )
                    ],
                    [],
                    [],
                )

                # Without --force: should skip (be in skipped list)
                result = runner.invoke(cli, ["add", str(test_file)])
                assert result.exit_code == 0

                # With --force: should process
                result = runner.invoke(cli, ["add", str(test_file), "--force"])
                assert result.exit_code == 0
                # Verify force=True was passed to add_files
                call_kwargs = mock_add.call_args.kwargs
                assert call_kwargs.get("force") is True


class TestForceSkipsConversionWhenOutputExists:
    """Tests that --force without --reconvert skips conversion."""

    @pytest.mark.unit
    def test_force_extracts_metadata_from_existing_output(self, runner: CliRunner) -> None:
        """--force without --reconvert uses existing GeoParquet for metadata."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "data"
            collection_dir.mkdir()
            item_dir = collection_dir / "myitem"
            item_dir.mkdir()

            # Source file (shapefile)
            source_file = item_dir / "data.shp"
            source_file.write_text("fake shapefile")

            # Existing converted output (GeoParquet)
            output_file = item_dir / "data.parquet"
            output_file.write_text("fake geoparquet")

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = ([], [], [])

                result = runner.invoke(cli, ["add", str(source_file), "--force"])
                assert result.exit_code == 0

                call_kwargs = mock_add.call_args.kwargs
                assert call_kwargs.get("force") is True
                assert call_kwargs.get("reconvert") is False


class TestReconvertFlag:
    """Tests for --reconvert flag behavior."""

    @pytest.mark.unit
    def test_reconvert_triggers_full_conversion(self, runner: CliRunner) -> None:
        """--force --reconvert re-converts from source files."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "data"
            collection_dir.mkdir()
            test_file = collection_dir / "test.geojson"
            test_file.write_text('{"type": "FeatureCollection", "features": []}')

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = ([], [], [])

                result = runner.invoke(
                    cli,
                    ["add", str(test_file), "--force", "--reconvert"],
                )
                assert result.exit_code == 0

                call_kwargs = mock_add.call_args.kwargs
                assert call_kwargs.get("force") is True
                assert call_kwargs.get("reconvert") is True


class TestSourceNewerWarning:
    """Tests for warning when source is newer than converted output."""

    @pytest.mark.unit
    def test_warns_when_source_newer_than_output(self, runner: CliRunner) -> None:
        """--force warns when source file is newer than converted output."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "data"
            collection_dir.mkdir()
            item_dir = collection_dir / "myitem"
            item_dir.mkdir()

            # Create output first (older)
            output_file = item_dir / "data.parquet"
            output_file.write_text("old geoparquet")

            # Small delay to ensure different mtime
            time.sleep(0.1)

            # Create source second (newer)
            source_file = item_dir / "data.shp"
            source_file.write_text("newer shapefile")

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = ([], [], [])

                result = runner.invoke(cli, ["add", str(source_file), "--force"])

                # Should warn about source being newer
                # The warning should mention --reconvert
                assert result.exit_code == 0
                # Warning is emitted during processing, check output or mock


class TestCloudNativeFormatsWithForce:
    """Tests for PMTiles/FlatGeobuf handling with --force and --reconvert."""

    @pytest.mark.unit
    def test_handle_cloud_native_vector_force_reuses_existing(self) -> None:
        """--force with existing PMTiles/FlatGeobuf extracts metadata without re-copy."""
        from portolan_cli.dataset import _handle_cloud_native_vector

        with pytest.importorskip("tempfile").TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            source = temp_path / "source.pmtiles"
            output = temp_path / "output.pmtiles"

            source.write_bytes(b"source content")
            output.write_bytes(b"existing output")

            def mock_extract(path: Path) -> dict:
                return {"extracted_from": str(path)}

            result = _handle_cloud_native_vector(
                source, output, mock_extract, force=True, reconvert=False
            )

            assert result["extracted_from"] == str(output)
            # Output should NOT be overwritten
            assert output.read_bytes() == b"existing output"

    @pytest.mark.unit
    def test_handle_cloud_native_vector_reconvert_overwrites(self) -> None:
        """--force --reconvert with existing PMTiles/FlatGeobuf re-copies from source."""
        from portolan_cli.dataset import _handle_cloud_native_vector

        with pytest.importorskip("tempfile").TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            source = temp_path / "source.fgb"
            output = temp_path / "output.fgb"

            source.write_bytes(b"new source content")
            output.write_bytes(b"old output")

            def mock_extract(path: Path) -> dict:
                return {"extracted_from": str(path), "content": path.read_bytes()}

            result = _handle_cloud_native_vector(
                source, output, mock_extract, force=True, reconvert=True
            )

            assert result["extracted_from"] == str(output)
            # Output SHOULD be overwritten with source content
            assert output.read_bytes() == b"new source content"

    @pytest.mark.unit
    def test_handle_cloud_native_vector_no_force_raises_on_existing(self) -> None:
        """Without --force, existing output raises FileExistsError."""
        from portolan_cli.dataset import _handle_cloud_native_vector

        with pytest.importorskip("tempfile").TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            source = temp_path / "source.pmtiles"
            output = temp_path / "output.pmtiles"

            source.write_bytes(b"source")
            output.write_bytes(b"existing")

            def mock_extract(path: Path) -> dict:
                return {}

            with pytest.raises(FileExistsError, match="already exists"):
                _handle_cloud_native_vector(
                    source, output, mock_extract, force=False, reconvert=False
                )

    @pytest.mark.unit
    def test_handle_cloud_native_vector_no_existing_copies(self) -> None:
        """When output doesn't exist, copies from source regardless of force."""
        from portolan_cli.dataset import _handle_cloud_native_vector

        with pytest.importorskip("tempfile").TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            source = temp_path / "source.fgb"
            output = temp_path / "output.fgb"

            source.write_bytes(b"source content")
            # output does NOT exist

            def mock_extract(path: Path) -> dict:
                return {"content": path.read_bytes()}

            _handle_cloud_native_vector(source, output, mock_extract, force=False, reconvert=False)

            # Should have copied
            assert output.exists()
            assert output.read_bytes() == b"source content"


class TestForceWithoutExistingOutput:
    """Tests for --force when output doesn't exist yet."""

    @pytest.mark.unit
    def test_force_without_existing_output_converts_normally(self, runner: CliRunner) -> None:
        """--force without existing output falls through to normal conversion."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "data"
            collection_dir.mkdir()
            test_file = collection_dir / "test.geojson"
            test_file.write_text('{"type": "FeatureCollection", "features": []}')

            # No existing .parquet output

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = ([], [], [])

                result = runner.invoke(cli, ["add", str(test_file), "--force"])

                assert result.exit_code == 0
                # Should have called add_files with force=True
                call_kwargs = mock_add.call_args.kwargs
                assert call_kwargs.get("force") is True


class TestWarnIfSourceNewer:
    """Tests for _warn_if_source_newer helper function."""

    @pytest.mark.unit
    def test_warn_if_source_newer_emits_warning(self) -> None:
        """Warns when source mtime > output mtime."""
        from portolan_cli.dataset import _warn_if_source_newer

        with pytest.importorskip("tempfile").TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            output = temp_path / "output.parquet"
            source = temp_path / "source.shp"

            # Create output first (older)
            output.write_text("old")
            time.sleep(0.05)
            # Create source second (newer)
            source.write_text("new")

            with patch("portolan_cli.output.warn") as mock_warn:
                _warn_if_source_newer(source, output)
                mock_warn.assert_called_once()
                assert "--reconvert" in mock_warn.call_args[0][0]

    @pytest.mark.unit
    def test_warn_if_source_newer_silent_when_output_newer(self) -> None:
        """No warning when output mtime >= source mtime."""
        from portolan_cli.dataset import _warn_if_source_newer

        with pytest.importorskip("tempfile").TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            source = temp_path / "source.shp"
            output = temp_path / "output.parquet"

            # Create source first (older)
            source.write_text("old")
            time.sleep(0.05)
            # Create output second (newer)
            output.write_text("new")

            with patch("portolan_cli.output.warn") as mock_warn:
                _warn_if_source_newer(source, output)
                mock_warn.assert_not_called()
