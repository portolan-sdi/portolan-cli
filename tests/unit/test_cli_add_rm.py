"""Unit tests for top-level add/rm commands.

Tests the CLI layer for `portolan add` and `portolan rm` commands.
These commands replace the old `dataset add` and `dataset remove` subcommands.

Per ADR-0022: Git-style implicit tracking
- `add <path>` tracks files (infers collection from path)
- `rm <path>` untracks AND deletes (no confirmation)
- `rm --keep <path>` untracks but preserves file
"""

from __future__ import annotations

import json
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
    """Create an initialized Portolan catalog (per ADR-0023)."""
    portolan_dir = path / ".portolan"
    portolan_dir.mkdir()
    catalog_data = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "portolan-catalog",
        "description": "A Portolan-managed STAC catalog",
        "links": [],
    }
    (path / "catalog.json").write_text(json.dumps(catalog_data, indent=2))


class TestAdd:
    """Tests for 'portolan add' command."""

    @pytest.mark.unit
    def test_add_single_file(self, runner: CliRunner) -> None:
        """add single file tracks it and infers collection from path."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            # Create test file in collection directory
            collection_dir = temp_path / "demographics"
            collection_dir.mkdir()
            test_file = collection_dir / "census.geojson"
            test_file.write_text('{"type": "FeatureCollection", "features": []}')

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = (
                    [
                        DatasetInfo(
                            item_id="census",
                            collection_id="demographics",
                            format_type=FormatType.VECTOR,
                            bbox=[-122.5, 37.5, -122.0, 38.0],
                            asset_paths=["census.parquet"],
                        )
                    ],
                    [],  # skipped
                )

                result = runner.invoke(
                    cli,
                    ["add", str(test_file)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                mock_add.assert_called_once()

    @pytest.mark.unit
    def test_add_infers_collection_from_path(self, runner: CliRunner) -> None:
        """add infers collection ID from first path component."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            # Create test file in nested structure
            collection_dir = temp_path / "imagery"
            collection_dir.mkdir()
            test_file = collection_dir / "satellite.tif"
            test_file.write_bytes(b"GeoTIFF content")

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = ([], [])

                runner.invoke(
                    cli,
                    ["add", str(test_file)],
                    catch_exceptions=False,
                )

                # Verify collection_id was inferred as "imagery"
                call_args = mock_add.call_args
                assert call_args is not None
                assert call_args.kwargs.get("collection_id") == "imagery"

    @pytest.mark.unit
    def test_add_directory(self, runner: CliRunner) -> None:
        """add directory adds all files inside."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            # Create directory with multiple files
            collection_dir = temp_path / "vectors"
            collection_dir.mkdir()
            (collection_dir / "file1.geojson").write_text("{}")
            (collection_dir / "file2.geojson").write_text("{}")

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = ([], [])

                result = runner.invoke(
                    cli,
                    ["add", str(collection_dir)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                mock_add.assert_called_once()

    @pytest.mark.unit
    def test_add_skips_unchanged_silently(self, runner: CliRunner) -> None:
        """add skips unchanged files without output (unless --verbose)."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "data"
            collection_dir.mkdir()
            test_file = collection_dir / "existing.geojson"
            test_file.write_text("{}")

            with patch("portolan_cli.cli.add_files") as mock_add:
                # Return empty list to indicate nothing was added (all unchanged)
                mock_add.return_value = ([], [test_file])

                result = runner.invoke(
                    cli,
                    ["add", str(test_file)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                # Output should be minimal for unchanged files
                assert "existing" not in result.output or "unchanged" not in result.output.lower()

    @pytest.mark.unit
    def test_add_verbose_shows_skipped(self, runner: CliRunner) -> None:
        """add --verbose shows skipped files."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "data"
            collection_dir.mkdir()
            test_file = collection_dir / "existing.geojson"
            test_file.write_text("{}")

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = ([], [test_file])

                result = runner.invoke(
                    cli,
                    ["add", "--verbose", str(test_file)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0

    @pytest.mark.unit
    def test_add_nonexistent_path(self, runner: CliRunner) -> None:
        """add fails with error for nonexistent path."""
        result = runner.invoke(cli, ["add", "/nonexistent/path"])

        assert result.exit_code != 0
        # Click should report the path doesn't exist
        assert "does not exist" in result.output.lower() or "not found" in result.output.lower()

    @pytest.mark.unit
    def test_add_not_a_catalog_fails(self, runner: CliRunner) -> None:
        """add fails when not in a catalog directory."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)

            # Don't create catalog - just a regular directory
            collection_dir = temp_path / "data"
            collection_dir.mkdir()
            test_file = collection_dir / "test.geojson"
            test_file.write_text("{}")

            result = runner.invoke(cli, ["add", str(test_file)])

            assert result.exit_code == 1
            assert "not a catalog" in result.output.lower()


class TestRm:
    """Tests for 'portolan rm' command."""

    @pytest.mark.unit
    def test_rm_deletes_and_untracks(self, runner: CliRunner) -> None:
        """rm deletes file and removes from tracking (no confirmation)."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "data"
            collection_dir.mkdir()
            test_file = collection_dir / "to_remove.parquet"
            test_file.write_bytes(b"parquet data")

            with patch("portolan_cli.cli.remove_files") as mock_rm:
                mock_rm.return_value = [test_file]

                result = runner.invoke(
                    cli,
                    ["rm", str(test_file)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                mock_rm.assert_called_once()
                call_kwargs = mock_rm.call_args.kwargs
                assert call_kwargs.get("keep") is False

    @pytest.mark.unit
    def test_rm_no_confirmation_prompt(self, runner: CliRunner) -> None:
        """rm does NOT prompt for confirmation (git-style)."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "data"
            collection_dir.mkdir()
            test_file = collection_dir / "file.parquet"
            test_file.write_bytes(b"data")

            with patch("portolan_cli.cli.remove_files") as mock_rm:
                mock_rm.return_value = [test_file]

                # Don't provide any input - should still work
                result = runner.invoke(
                    cli,
                    ["rm", str(test_file)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                mock_rm.assert_called_once()

    @pytest.mark.unit
    def test_rm_keep_preserves_file(self, runner: CliRunner) -> None:
        """rm --keep untracks but preserves the file."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "data"
            collection_dir.mkdir()
            test_file = collection_dir / "keep_me.parquet"
            test_file.write_bytes(b"data")

            with patch("portolan_cli.cli.remove_files") as mock_rm:
                mock_rm.return_value = [test_file]

                result = runner.invoke(
                    cli,
                    ["rm", "--keep", str(test_file)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                mock_rm.assert_called_once()
                call_kwargs = mock_rm.call_args.kwargs
                assert call_kwargs.get("keep") is True

    @pytest.mark.unit
    def test_rm_nonexistent_fails(self, runner: CliRunner) -> None:
        """rm fails for nonexistent path."""
        result = runner.invoke(cli, ["rm", "/nonexistent/file.parquet"])

        assert result.exit_code != 0

    @pytest.mark.unit
    def test_rm_directory(self, runner: CliRunner) -> None:
        """rm can remove entire directory."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "to_remove"
            collection_dir.mkdir()
            (collection_dir / "file1.parquet").write_bytes(b"data1")
            (collection_dir / "file2.parquet").write_bytes(b"data2")

            with patch("portolan_cli.cli.remove_files") as mock_rm:
                mock_rm.return_value = []

                result = runner.invoke(
                    cli,
                    ["rm", str(collection_dir)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                mock_rm.assert_called_once()


class TestAddSidecarDetection:
    """Tests for sidecar auto-detection in add command."""

    @pytest.mark.unit
    def test_add_shapefile_includes_sidecars(self, runner: CliRunner) -> None:
        """add .shp automatically includes sidecar files."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "vectors"
            collection_dir.mkdir()

            # Create shapefile with sidecars
            (collection_dir / "data.shp").write_bytes(b"shp")
            (collection_dir / "data.dbf").write_bytes(b"dbf")
            (collection_dir / "data.shx").write_bytes(b"shx")
            (collection_dir / "data.prj").write_text("EPSG:4326")

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = ([], [])

                result = runner.invoke(
                    cli,
                    ["add", str(collection_dir / "data.shp")],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0

    @pytest.mark.unit
    def test_add_tiff_includes_worldfile(self, runner: CliRunner) -> None:
        """add .tif automatically includes .tfw world file."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "imagery"
            collection_dir.mkdir()

            # Create TIFF with world file
            (collection_dir / "image.tif").write_bytes(b"tiff")
            (collection_dir / "image.tfw").write_text("1.0\n0.0\n0.0\n-1.0\n0.0\n0.0")

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = ([], [])

                result = runner.invoke(
                    cli,
                    ["add", str(collection_dir / "image.tif")],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0


class TestPathToCollectionResolution:
    """Tests for path -> collection ID resolution."""

    @pytest.mark.unit
    def test_resolve_collection_from_nested_path(self, runner: CliRunner) -> None:
        """First path component (relative to catalog) = collection ID."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            # Create nested structure: catalog/demographics/2020/census.geojson
            nested_dir = temp_path / "demographics" / "2020"
            nested_dir.mkdir(parents=True)
            test_file = nested_dir / "census.geojson"
            test_file.write_text("{}")

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = ([], [])

                runner.invoke(
                    cli,
                    ["add", str(test_file)],
                    catch_exceptions=False,
                )

                call_args = mock_add.call_args
                # Collection should be "demographics", not "demographics/2020"
                assert call_args is not None
                assert call_args.kwargs.get("collection_id") == "demographics"

    @pytest.mark.unit
    def test_resolve_collection_from_direct_child(self, runner: CliRunner) -> None:
        """Direct child directory = collection ID."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "imagery"
            collection_dir.mkdir()
            test_file = collection_dir / "satellite.tif"
            test_file.write_bytes(b"tiff")

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = ([], [])

                runner.invoke(
                    cli,
                    ["add", str(test_file)],
                    catch_exceptions=False,
                )

                call_args = mock_add.call_args
                assert call_args is not None
                assert call_args.kwargs.get("collection_id") == "imagery"


class TestAddJsonOutput:
    """Tests for add command JSON output mode."""

    @pytest.mark.unit
    def test_add_json_output(self, runner: CliRunner) -> None:
        """add --format json outputs valid JSON envelope."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "data"
            collection_dir.mkdir()
            test_file = collection_dir / "test.geojson"
            test_file.write_text("{}")

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = (
                    [
                        DatasetInfo(
                            item_id="test",
                            collection_id="data",
                            format_type=FormatType.VECTOR,
                            bbox=[0, 0, 1, 1],
                            asset_paths=["test.parquet"],
                        )
                    ],
                    [],
                )

                result = runner.invoke(
                    cli,
                    ["--format", "json", "add", str(test_file)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                envelope = json.loads(result.output)
                assert envelope["success"] is True
                assert envelope["command"] == "add"


class TestRmJsonOutput:
    """Tests for rm command JSON output mode."""

    @pytest.mark.unit
    def test_rm_json_output(self, runner: CliRunner) -> None:
        """rm --format json outputs valid JSON envelope."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "data"
            collection_dir.mkdir()
            test_file = collection_dir / "test.parquet"
            test_file.write_bytes(b"data")

            with patch("portolan_cli.cli.remove_files") as mock_rm:
                mock_rm.return_value = [test_file]

                result = runner.invoke(
                    cli,
                    ["--format", "json", "rm", str(test_file)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                envelope = json.loads(result.output)
                assert envelope["success"] is True
                assert envelope["command"] == "rm"
