"""Unit tests for 'portolan add .' at catalog root (Issue #137).

Tests the fix for: add . fails at catalog root with
"Cannot determine collection from path: /path/to/my-catalog"

The root cause: resolve_collection_id raises ValueError when path == catalog_root
because relative_to produces an empty parts tuple.

Fix: detect when target_path == catalog_root and call add_files with
collection_id=None, letting it infer each file's collection from its path.
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
    """Create an initialized Portolan catalog (per ADR-0023, ADR-0029).

    Creates full managed catalog structure with:
    - .portolan/config.yaml (sentinel per ADR-0029)
    - .portolan/state.json (operational file)
    - catalog.json at root (STAC standard)
    """
    portolan_dir = path / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("# Portolan configuration\n")
    (portolan_dir / "state.json").write_text("{}")
    catalog_data = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "portolan-catalog",
        "description": "A Portolan-managed STAC catalog",
        "links": [],
    }
    (path / "catalog.json").write_text(json.dumps(catalog_data, indent=2))


class TestAddAtCatalogRoot:
    """Tests for 'portolan add .' executed at catalog root."""

    @pytest.mark.unit
    def test_add_dot_at_catalog_root_succeeds(self, runner: CliRunner) -> None:
        """add . at catalog root does not raise 'Cannot determine collection' error."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            # Create geo-files in subdirectories (collections)
            (temp_path / "demographics").mkdir()
            (temp_path / "demographics" / "census.geojson").write_text("{}")
            (temp_path / "imagery").mkdir()
            (temp_path / "imagery" / "satellite.tif").write_bytes(b"GeoTIFF")

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = ([], [], [])

                result = runner.invoke(
                    cli,
                    ["add", "--portolan-dir", str(temp_path), str(temp_path)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0, f"Expected success, got: {result.output}"

    @pytest.mark.unit
    def test_add_dot_at_catalog_root_calls_add_files_with_no_collection_id(
        self, runner: CliRunner
    ) -> None:
        """add . passes collection_id=None so add_files infers per-file."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            # Create structure
            (temp_path / "vectors").mkdir()
            (temp_path / "vectors" / "data.geojson").write_text("{}")

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = ([], [], [])

                runner.invoke(
                    cli,
                    ["add", "--portolan-dir", str(temp_path), str(temp_path)],
                    catch_exceptions=False,
                )

                call_args = mock_add.call_args
                assert call_args is not None, "add_files was not called"
                # collection_id must be None so each file gets its own collection inferred
                assert call_args.kwargs.get("collection_id") is None, (
                    "Expected collection_id=None for catalog root add, "
                    f"got: {call_args.kwargs.get('collection_id')}"
                )

    @pytest.mark.unit
    def test_add_dot_at_catalog_root_passes_catalog_root_path(self, runner: CliRunner) -> None:
        """add . passes the catalog root path itself to add_files."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            (temp_path / "col").mkdir()
            (temp_path / "col" / "data.geojson").write_text("{}")

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = ([], [], [])

                runner.invoke(
                    cli,
                    ["add", "--portolan-dir", str(temp_path), str(temp_path)],
                    catch_exceptions=False,
                )

                call_args = mock_add.call_args
                assert call_args is not None
                paths_arg = call_args.kwargs.get("paths", [])
                # The catalog root should be in the paths list
                assert any(p == temp_path.resolve() for p in paths_arg), (
                    f"Expected catalog root in paths, got: {paths_arg}"
                )

    @pytest.mark.unit
    def test_add_dot_returns_results_from_multiple_collections(self, runner: CliRunner) -> None:
        """add . aggregates results from multiple collections in output."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            (temp_path / "col1").mkdir()
            (temp_path / "col1" / "a.geojson").write_text("{}")
            (temp_path / "col2").mkdir()
            (temp_path / "col2" / "b.geojson").write_text("{}")

            added_datasets = [
                DatasetInfo(
                    item_id="a",
                    collection_id="col1",
                    format_type=FormatType.VECTOR,
                    bbox=[-1.0, -1.0, 1.0, 1.0],
                    asset_paths=["a.parquet"],
                ),
                DatasetInfo(
                    item_id="b",
                    collection_id="col2",
                    format_type=FormatType.VECTOR,
                    bbox=[-1.0, -1.0, 1.0, 1.0],
                    asset_paths=["b.parquet"],
                ),
            ]

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = (added_datasets, [], [])

                result = runner.invoke(
                    cli,
                    ["add", "--portolan-dir", str(temp_path), str(temp_path)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0, f"Expected success, got: {result.output}"
                # Verify multi-collection output format shows BOTH collections
                assert "col1" in result.output, f"Expected col1 in output: {result.output}"
                assert "col2" in result.output, f"Expected col2 in output: {result.output}"
                assert "2 collections" in result.output, (
                    f"Expected '2 collections' in output: {result.output}"
                )

    @pytest.mark.unit
    def test_add_dot_at_catalog_root_with_json_output(self, runner: CliRunner) -> None:
        """add . --format json at catalog root returns valid JSON envelope."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            (temp_path / "col").mkdir()
            (temp_path / "col" / "data.geojson").write_text("{}")

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = ([], [], [])

                result = runner.invoke(
                    cli,
                    [
                        "--format",
                        "json",
                        "add",
                        "--portolan-dir",
                        str(temp_path),
                        str(temp_path),
                    ],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                envelope = json.loads(result.output)
                assert envelope["success"] is True
                assert envelope["command"] == "add"

    @pytest.mark.unit
    def test_add_dot_empty_catalog_succeeds(self, runner: CliRunner) -> None:
        """add . on a catalog with no geo-files exits gracefully with informative message."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)
            # No geo files

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = ([], [], [])

                result = runner.invoke(
                    cli,
                    ["add", "--portolan-dir", str(temp_path), str(temp_path)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                # Should show informative message, not "Adding 0 files to catalog"
                assert "no geospatial files" in result.output.lower(), (
                    f"Expected 'no geospatial files' message, got: {result.output}"
                )

    @pytest.mark.unit
    def test_add_dot_not_a_catalog_fails(self, runner: CliRunner) -> None:
        """add . on non-catalog directory still fails with appropriate error."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            # No .portolan/config.yaml - not a managed catalog

            result = runner.invoke(
                cli,
                ["add", str(temp_path)],
            )

            assert result.exit_code == 1
            # Per ADR-0029, error message references .portolan/config.yaml sentinel
            assert "no .portolan/config.yaml found" in result.output.lower()

    @pytest.mark.unit
    def test_add_subdirectory_still_infers_collection(self, runner: CliRunner) -> None:
        """add <collection_subdir> still infers single collection_id (existing behavior)."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            collection_dir = temp_path / "demographics"
            collection_dir.mkdir()
            test_file = collection_dir / "census.geojson"
            test_file.write_text("{}")

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = ([], [], [])

                runner.invoke(
                    cli,
                    ["add", "--portolan-dir", str(temp_path), str(collection_dir)],
                    catch_exceptions=False,
                )

                call_args = mock_add.call_args
                assert call_args is not None
                # For a non-root subdirectory, collection_id should still be inferred
                assert call_args.kwargs.get("collection_id") == "demographics"

    @pytest.mark.unit
    def test_add_file_directly_at_root_fails_with_clear_error(self, runner: CliRunner) -> None:
        """add <file> placed directly at catalog root (no collection) still errors."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            # File directly in catalog root (no collection directory)
            test_file = temp_path / "stray.geojson"
            test_file.write_text("{}")

            result = runner.invoke(
                cli,
                ["add", "--portolan-dir", str(temp_path), str(test_file)],
            )

            # Should fail - file must be inside a collection subdirectory
            assert result.exit_code == 1


class TestAddCatalogRootCollectionInference:
    """Tests that collection inference works correctly for catalog root add."""

    @pytest.mark.unit
    def test_add_root_with_portolan_dir_flag(self, runner: CliRunner) -> None:
        """add . with explicit --portolan-dir works correctly."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            (temp_path / "rivers").mkdir()
            (temp_path / "rivers" / "amazon.geojson").write_text("{}")

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = ([], [], [])

                result = runner.invoke(
                    cli,
                    [
                        "add",
                        "--portolan-dir",
                        str(temp_path),
                        str(temp_path),
                    ],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                call_args = mock_add.call_args
                assert call_args is not None
                assert call_args.kwargs.get("collection_id") is None

    @pytest.mark.unit
    def test_catalog_root_detection_uses_resolved_path(self, runner: CliRunner) -> None:
        """Catalog root detection resolves symlinks before comparison."""
        with runner.isolated_filesystem() as temp_dir:
            temp_path = Path(temp_dir)
            setup_catalog(temp_path)

            (temp_path / "col").mkdir()
            (temp_path / "col" / "f.geojson").write_text("{}")

            with patch("portolan_cli.cli.add_files") as mock_add:
                mock_add.return_value = ([], [], [])

                # Use the absolute path (same as catalog root)
                result = runner.invoke(
                    cli,
                    ["add", "--portolan-dir", str(temp_path), str(temp_path)],
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
