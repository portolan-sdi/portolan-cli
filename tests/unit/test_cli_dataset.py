"""Unit tests for dataset CLI commands.

Tests the CLI layer for dataset list/info commands.

Note: dataset add/remove were moved to top-level `portolan add` and `portolan rm`
commands (see test_cli_add_rm.py).

Note: `portolan list` is the promoted top-level command (ADR-0022).
`portolan dataset list` remains as a deprecated alias.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner
from hypothesis import given, settings
from hypothesis import strategies as st

from portolan_cli.cli import cli
from portolan_cli.dataset import DatasetInfo
from portolan_cli.formats import FormatType


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI test runner."""
    return CliRunner()


@pytest.fixture
def initialized_catalog(tmp_path: Path) -> Path:
    """Create an initialized Portolan catalog (per ADR-0023)."""
    # Create .portolan for internal state
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()
    # catalog.json at root level (per ADR-0023)
    catalog_data = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "portolan-catalog",
        "description": "A Portolan-managed STAC catalog",
        "links": [],
    }
    (tmp_path / "catalog.json").write_text(json.dumps(catalog_data, indent=2))
    return tmp_path


class TestDatasetList:
    """Tests for 'portolan dataset list' command."""

    @pytest.mark.unit
    def test_list_empty_catalog(self, runner: CliRunner, initialized_catalog: Path) -> None:
        """dataset list shows empty message for catalog with no datasets."""
        with runner.isolated_filesystem():
            # Create catalog in isolated filesystem
            portolan_dir = Path(".portolan")
            portolan_dir.mkdir()
            (portolan_dir / "catalog.json").write_text(
                json.dumps(
                    {
                        "type": "Catalog",
                        "stac_version": "1.0.0",
                        "id": "test",
                        "description": "Test",
                        "links": [],
                    }
                )
            )

            result = runner.invoke(cli, ["dataset", "list"])

            assert result.exit_code == 0
            assert "no datasets" in result.output.lower() or result.output.strip() == ""

    @pytest.mark.unit
    def test_list_with_datasets(self, runner: CliRunner) -> None:
        """dataset list shows datasets in tree view format.

        Note: dataset list now uses the tree view format and shows asset
        filenames, collections, and format types (not item IDs).
        """
        with patch("portolan_cli.cli.list_datasets") as mock_list:
            mock_list.return_value = [
                DatasetInfo(
                    item_id="item1",
                    collection_id="col1",
                    format_type=FormatType.VECTOR,
                    bbox=[0, 0, 1, 1],
                    asset_paths=["data.parquet"],
                ),
                DatasetInfo(
                    item_id="item2",
                    collection_id="col2",
                    format_type=FormatType.RASTER,
                    bbox=[1, 1, 2, 2],
                    asset_paths=["data.tif"],
                ),
            ]

            with runner.isolated_filesystem():
                Path(".portolan").mkdir()
                (Path(".portolan") / "catalog.json").write_text("{}")

                result = runner.invoke(cli, ["dataset", "list"])

                assert result.exit_code == 0
                # Output is now in tree view format with collections and filenames
                assert "col1/" in result.output
                assert "col2/" in result.output
                assert "data.parquet" in result.output
                assert "data.tif" in result.output

    @pytest.mark.unit
    def test_list_filter_by_collection(self, runner: CliRunner) -> None:
        """dataset list --collection filters by collection."""
        with patch("portolan_cli.cli.list_datasets") as mock_list:
            mock_list.return_value = [
                DatasetInfo(
                    item_id="item1",
                    collection_id="target",
                    format_type=FormatType.VECTOR,
                    bbox=[0, 0, 1, 1],
                    asset_paths=[],
                ),
            ]

            with runner.isolated_filesystem():
                Path(".portolan").mkdir()
                (Path(".portolan") / "catalog.json").write_text("{}")

                result = runner.invoke(cli, ["dataset", "list", "--collection", "target"])

                assert result.exit_code == 0
                mock_list.assert_called_once()
                call_kwargs = mock_list.call_args.kwargs
                assert call_kwargs.get("collection_id") == "target"


class TestDatasetInfo:
    """Tests for 'portolan dataset info' command."""

    @pytest.mark.unit
    def test_info_existing_dataset(self, runner: CliRunner) -> None:
        """dataset info shows details for existing dataset."""
        with patch("portolan_cli.cli.get_dataset_info") as mock_info:
            mock_info.return_value = DatasetInfo(
                item_id="my-item",
                collection_id="my-collection",
                format_type=FormatType.VECTOR,
                bbox=[-122.5, 37.5, -122.0, 38.0],
                asset_paths=["data.parquet"],
                title="My Dataset",
                description="A test dataset",
            )

            with runner.isolated_filesystem():
                Path(".portolan").mkdir()
                (Path(".portolan") / "catalog.json").write_text("{}")

                result = runner.invoke(cli, ["dataset", "info", "my-collection/my-item"])

                assert result.exit_code == 0
                assert "my-item" in result.output
                assert "my-collection" in result.output

    @pytest.mark.unit
    def test_info_not_found(self, runner: CliRunner) -> None:
        """dataset info exits with error for nonexistent dataset."""
        with patch("portolan_cli.cli.get_dataset_info") as mock_info:
            mock_info.side_effect = KeyError("Dataset not found: nonexistent/item")

            with runner.isolated_filesystem():
                Path(".portolan").mkdir()
                (Path(".portolan") / "catalog.json").write_text("{}")

                result = runner.invoke(cli, ["dataset", "info", "nonexistent/item"])

                assert result.exit_code == 1


class TestDatasetListWithTitle:
    """Tests for dataset list with title display.

    Note: The tree view format (ADR-0022) shows filenames and formats,
    not titles. Titles are still available via JSON output or dataset info.
    """

    @pytest.mark.unit
    def test_list_shows_format_and_filename(self, runner: CliRunner) -> None:
        """dataset list displays format type and filename in tree view.

        The tree view (ADR-0022) shows: collection/ -> filename (Format, Size)
        Titles are available via --json output.
        """
        with patch("portolan_cli.cli.list_datasets") as mock_list:
            mock_list.return_value = [
                DatasetInfo(
                    item_id="item1",
                    collection_id="col1",
                    format_type=FormatType.VECTOR,
                    bbox=[0, 0, 1, 1],
                    asset_paths=["census.parquet"],
                    title="My Dataset Title",
                ),
            ]

            with runner.isolated_filesystem():
                Path(".portolan").mkdir()
                (Path(".portolan") / "catalog.json").write_text("{}")

                result = runner.invoke(cli, ["dataset", "list"])

                assert result.exit_code == 0
                # Tree view shows collection, filename, and format
                assert "col1/" in result.output
                assert "census.parquet" in result.output
                assert "GeoParquet" in result.output


class TestDatasetInfoJson:
    """Tests for dataset info --json output."""

    @pytest.mark.unit
    def test_info_json_output(self, runner: CliRunner) -> None:
        """dataset info --json outputs valid JSON."""
        with patch("portolan_cli.cli.get_dataset_info") as mock_info:
            mock_info.return_value = DatasetInfo(
                item_id="my-item",
                collection_id="my-collection",
                format_type=FormatType.VECTOR,
                bbox=[-122.5, 37.5, -122.0, 38.0],
                asset_paths=["data.parquet"],
                title="Test Title",
                description="Test Description",
            )

            with runner.isolated_filesystem():
                Path(".portolan").mkdir()
                (Path(".portolan") / "catalog.json").write_text("{}")

                result = runner.invoke(cli, ["dataset", "info", "my-collection/my-item", "--json"])

                assert result.exit_code == 0
                # Parse JSON to verify it's valid and has envelope structure
                envelope = json.loads(result.output)
                assert envelope["success"] is True
                assert envelope["command"] == "dataset_info"
                data = envelope["data"]
                assert data["item_id"] == "my-item"
                assert data["collection_id"] == "my-collection"
                assert data["title"] == "Test Title"
                assert data["description"] == "Test Description"
                assert data["bbox"] == [-122.5, 37.5, -122.0, 38.0]

    @pytest.mark.unit
    def test_info_displays_description(self, runner: CliRunner) -> None:
        """dataset info displays description when present."""
        with patch("portolan_cli.cli.get_dataset_info") as mock_info:
            mock_info.return_value = DatasetInfo(
                item_id="item",
                collection_id="col",
                format_type=FormatType.VECTOR,
                bbox=[0, 0, 1, 1],
                asset_paths=["data.parquet"],
                description="A detailed description",
            )

            with runner.isolated_filesystem():
                Path(".portolan").mkdir()
                (Path(".portolan") / "catalog.json").write_text("{}")

                result = runner.invoke(cli, ["dataset", "info", "col/item"])

                assert result.exit_code == 0
                assert "detailed description" in result.output

    @pytest.mark.unit
    def test_info_displays_assets(self, runner: CliRunner) -> None:
        """dataset info displays asset paths."""
        with patch("portolan_cli.cli.get_dataset_info") as mock_info:
            mock_info.return_value = DatasetInfo(
                item_id="item",
                collection_id="col",
                format_type=FormatType.VECTOR,
                bbox=[0, 0, 1, 1],
                asset_paths=["data.parquet", "thumbnail.png"],
            )

            with runner.isolated_filesystem():
                Path(".portolan").mkdir()
                (Path(".portolan") / "catalog.json").write_text("{}")

                result = runner.invoke(cli, ["dataset", "info", "col/item"])

                assert result.exit_code == 0
                assert "data.parquet" in result.output


# =============================================================================
# Top-level 'portolan list' command tests (ADR-0022)
# =============================================================================


class TestTopLevelList:
    """Tests for 'portolan list' top-level command (ADR-0022)."""

    @pytest.mark.unit
    def test_list_command_exists(self, runner: CliRunner) -> None:
        """portolan list command is available at top level."""
        result = runner.invoke(cli, ["list", "--help"])

        assert result.exit_code == 0
        assert "list" in result.output.lower()

    @pytest.mark.unit
    def test_list_empty_catalog(self, runner: CliRunner) -> None:
        """portolan list shows empty message for catalog with no items."""
        with runner.isolated_filesystem():
            # Create catalog structure per ADR-0023
            Path("catalog.json").write_text(
                json.dumps(
                    {
                        "type": "Catalog",
                        "stac_version": "1.0.0",
                        "id": "test",
                        "description": "Test",
                        "links": [],
                    }
                )
            )
            Path(".portolan").mkdir()

            result = runner.invoke(cli, ["list"])

            assert result.exit_code == 0
            assert "no tracked items" in result.output.lower()

    @pytest.mark.unit
    def test_list_empty_shows_guidance_scan(self, runner: CliRunner) -> None:
        """portolan list shows guidance about scan command when empty."""
        with runner.isolated_filesystem():
            # Create catalog structure per ADR-0023
            Path("catalog.json").write_text(
                json.dumps(
                    {
                        "type": "Catalog",
                        "stac_version": "1.0.0",
                        "id": "test",
                        "description": "Test",
                        "links": [],
                    }
                )
            )
            Path(".portolan").mkdir()

            result = runner.invoke(cli, ["list"])

            assert result.exit_code == 0
            # Check for scan guidance
            assert "scan" in result.output.lower()

    @pytest.mark.unit
    def test_list_empty_shows_guidance_add(self, runner: CliRunner) -> None:
        """portolan list shows guidance about add command when empty."""
        with runner.isolated_filesystem():
            # Create catalog structure per ADR-0023
            Path("catalog.json").write_text(
                json.dumps(
                    {
                        "type": "Catalog",
                        "stac_version": "1.0.0",
                        "id": "test",
                        "description": "Test",
                        "links": [],
                    }
                )
            )
            Path(".portolan").mkdir()

            result = runner.invoke(cli, ["list"])

            assert result.exit_code == 0
            # Check for add guidance
            assert "add" in result.output.lower()

    @pytest.mark.unit
    def test_list_empty_guidance_not_shown_in_json_mode(self, runner: CliRunner) -> None:
        """portolan list --json returns valid empty envelope without guidance text."""
        with runner.isolated_filesystem():
            # Create catalog structure per ADR-0023
            Path("catalog.json").write_text(
                json.dumps(
                    {
                        "type": "Catalog",
                        "stac_version": "1.0.0",
                        "id": "test",
                        "description": "Test",
                        "links": [],
                    }
                )
            )
            Path(".portolan").mkdir()

            result = runner.invoke(cli, ["list", "--json"])

            assert result.exit_code == 0
            # Parse JSON and verify it's a valid empty response
            envelope = json.loads(result.output)
            assert envelope["success"] is True
            assert envelope["command"] == "list"
            # Items should be empty list, not contain guidance text
            assert envelope["data"]["count"] == 0
            assert envelope["data"]["items"] == []

    @pytest.mark.unit
    def test_list_empty_guidance_mentions_portolan_commands(self, runner: CliRunner) -> None:
        """portolan list empty guidance mentions full portolan commands."""
        with runner.isolated_filesystem():
            # Create catalog structure per ADR-0023
            Path("catalog.json").write_text(
                json.dumps(
                    {
                        "type": "Catalog",
                        "stac_version": "1.0.0",
                        "id": "test",
                        "description": "Test",
                        "links": [],
                    }
                )
            )
            Path(".portolan").mkdir()

            result = runner.invoke(cli, ["list"])

            assert result.exit_code == 0
            # Check for command references
            assert "portolan scan" in result.output or "scan ." in result.output
            assert "portolan add" in result.output or "add <path>" in result.output

    @pytest.mark.unit
    def test_list_shows_tree_view_format(self, runner: CliRunner) -> None:
        """portolan list shows items in tree view format per ADR-0022.

        Expected format:
        demographics/
          census.parquet (GeoParquet, 4.2MB)
          boundaries.parquet (GeoParquet, 1.1MB)
        """
        with patch("portolan_cli.cli.list_datasets") as mock_list:
            mock_list.return_value = [
                DatasetInfo(
                    item_id="census",
                    collection_id="demographics",
                    format_type=FormatType.VECTOR,
                    bbox=[0, 0, 1, 1],
                    asset_paths=["./census.parquet"],
                ),
                DatasetInfo(
                    item_id="boundaries",
                    collection_id="demographics",
                    format_type=FormatType.VECTOR,
                    bbox=[0, 0, 1, 1],
                    asset_paths=["./boundaries.parquet"],
                ),
            ]

            with runner.isolated_filesystem():
                Path("catalog.json").write_text("{}")
                Path(".portolan").mkdir()
                # Create mock files for size calculation
                Path("demographics").mkdir()
                Path("demographics/census").mkdir()
                census_file = Path("demographics/census/census.parquet")
                census_file.write_bytes(b"x" * 4_400_000)  # ~4.2MB
                Path("demographics/boundaries").mkdir()
                boundaries_file = Path("demographics/boundaries/boundaries.parquet")
                boundaries_file.write_bytes(b"x" * 1_100_000)  # ~1.1MB

                result = runner.invoke(cli, ["list"])

                assert result.exit_code == 0
                # Check tree structure - collection header
                assert "demographics/" in result.output
                # Check items show with filenames (per ADR-0022)
                assert "census.parquet" in result.output
                assert "boundaries.parquet" in result.output

    @pytest.mark.unit
    def test_list_shows_file_sizes(self, runner: CliRunner) -> None:
        """portolan list displays human-readable file sizes."""
        with patch("portolan_cli.cli.list_datasets") as mock_list:
            mock_list.return_value = [
                DatasetInfo(
                    item_id="large-raster",
                    collection_id="imagery",
                    format_type=FormatType.RASTER,
                    bbox=[0, 0, 1, 1],
                    asset_paths=["./satellite.tif"],
                ),
            ]

            with runner.isolated_filesystem():
                Path("catalog.json").write_text("{}")
                Path(".portolan").mkdir()
                # Create mock file
                Path("imagery").mkdir()
                Path("imagery/large-raster").mkdir()
                raster_file = Path("imagery/large-raster/satellite.tif")
                raster_file.write_bytes(b"x" * 120_000_000)  # ~120MB

                result = runner.invoke(cli, ["list"])

                assert result.exit_code == 0
                # Should show file size in human-readable format
                assert "MB" in result.output or "mb" in result.output.lower()

    @pytest.mark.unit
    def test_list_shows_format_type(self, runner: CliRunner) -> None:
        """portolan list displays format type (GeoParquet, COG)."""
        with patch("portolan_cli.cli.list_datasets") as mock_list:
            mock_list.return_value = [
                DatasetInfo(
                    item_id="vector-item",
                    collection_id="data",
                    format_type=FormatType.VECTOR,
                    bbox=[0, 0, 1, 1],
                    asset_paths=["./data.parquet"],
                ),
                DatasetInfo(
                    item_id="raster-item",
                    collection_id="imagery",
                    format_type=FormatType.RASTER,
                    bbox=[0, 0, 1, 1],
                    asset_paths=["./data.tif"],
                ),
            ]

            with runner.isolated_filesystem():
                Path("catalog.json").write_text("{}")
                Path(".portolan").mkdir()
                # Create mock files
                Path("data").mkdir()
                Path("data/vector-item").mkdir()
                Path("data/vector-item/data.parquet").write_bytes(b"x" * 1000)
                Path("imagery").mkdir()
                Path("imagery/raster-item").mkdir()
                Path("imagery/raster-item/data.tif").write_bytes(b"x" * 1000)

                result = runner.invoke(cli, ["list"])

                assert result.exit_code == 0
                # Should show format types
                assert "GeoParquet" in result.output or "vector" in result.output.lower()
                assert "COG" in result.output or "raster" in result.output.lower()

    @pytest.mark.unit
    def test_list_filter_by_collection(self, runner: CliRunner) -> None:
        """portolan list --collection filters by collection."""
        with patch("portolan_cli.cli.list_datasets") as mock_list:
            mock_list.return_value = [
                DatasetInfo(
                    item_id="item1",
                    collection_id="target",
                    format_type=FormatType.VECTOR,
                    bbox=[0, 0, 1, 1],
                    asset_paths=["./item1/data.parquet"],
                ),
            ]

            with runner.isolated_filesystem():
                Path("catalog.json").write_text("{}")
                Path(".portolan").mkdir()
                Path("target").mkdir()
                Path("target/item1").mkdir()
                Path("target/item1/data.parquet").write_bytes(b"x" * 1000)

                result = runner.invoke(cli, ["list", "--collection", "target"])

                assert result.exit_code == 0
                mock_list.assert_called_once()
                call_kwargs = mock_list.call_args.kwargs
                assert call_kwargs.get("collection_id") == "target"

    @pytest.mark.unit
    def test_list_json_output(self, runner: CliRunner) -> None:
        """portolan list --json outputs valid JSON envelope."""
        with patch("portolan_cli.cli.list_datasets") as mock_list:
            mock_list.return_value = [
                DatasetInfo(
                    item_id="item1",
                    collection_id="col1",
                    format_type=FormatType.VECTOR,
                    bbox=[0, 0, 1, 1],
                    asset_paths=["./item1/data.parquet"],
                    title="Test Item",
                ),
            ]

            with runner.isolated_filesystem():
                Path("catalog.json").write_text("{}")
                Path(".portolan").mkdir()
                Path("col1").mkdir()
                Path("col1/item1").mkdir()
                Path("col1/item1/data.parquet").write_bytes(b"x" * 1000)

                result = runner.invoke(cli, ["list", "--json"])

                assert result.exit_code == 0
                envelope = json.loads(result.output)
                assert envelope["success"] is True
                assert envelope["command"] == "list"
                assert "items" in envelope["data"] or "datasets" in envelope["data"]


class TestDatasetListDeprecation:
    """Tests for deprecated 'portolan dataset list' command."""

    @pytest.mark.unit
    def test_dataset_list_shows_deprecation_warning(self, runner: CliRunner) -> None:
        """dataset list shows deprecation warning."""
        with patch("portolan_cli.cli.list_datasets") as mock_list:
            mock_list.return_value = []

            with runner.isolated_filesystem():
                Path("catalog.json").write_text("{}")
                Path(".portolan").mkdir()

                result = runner.invoke(cli, ["dataset", "list"])

                assert result.exit_code == 0
                # Should show deprecation warning
                assert "deprecated" in result.output.lower()
                assert "portolan list" in result.output.lower()

    @pytest.mark.unit
    def test_dataset_list_still_works(self, runner: CliRunner) -> None:
        """dataset list still functions as an alias."""
        with patch("portolan_cli.cli.list_datasets") as mock_list:
            mock_list.return_value = [
                DatasetInfo(
                    item_id="item1",
                    collection_id="col1",
                    format_type=FormatType.VECTOR,
                    bbox=[0, 0, 1, 1],
                    asset_paths=["./item1/data.parquet"],
                ),
            ]

            with runner.isolated_filesystem():
                Path("catalog.json").write_text("{}")
                Path(".portolan").mkdir()
                Path("col1").mkdir()
                Path("col1/item1").mkdir()
                Path("col1/item1/data.parquet").write_bytes(b"x" * 1000)

                result = runner.invoke(cli, ["dataset", "list"])

                assert result.exit_code == 0
                # Should still show items (after deprecation warning)
                assert "col1" in result.output


class TestDatasetInfoDeprecation:
    """Tests for deprecated 'portolan dataset info' command."""

    @pytest.mark.unit
    def test_dataset_info_shows_deprecation_warning(self, runner: CliRunner) -> None:
        """dataset info shows deprecation warning."""
        with patch("portolan_cli.cli.get_dataset_info") as mock_info:
            mock_info.return_value = DatasetInfo(
                item_id="my-item",
                collection_id="my-collection",
                format_type=FormatType.VECTOR,
                bbox=[-122.5, 37.5, -122.0, 38.0],
                asset_paths=["data.parquet"],
            )

            with runner.isolated_filesystem():
                Path(".portolan").mkdir()
                (Path(".portolan") / "catalog.json").write_text("{}")

                result = runner.invoke(cli, ["dataset", "info", "my-collection/my-item"])

                assert result.exit_code == 0
                assert "deprecated" in result.output.lower()
                assert "portolan info" in result.output.lower()

    @pytest.mark.unit
    def test_dataset_info_still_works_after_deprecation_warning(self, runner: CliRunner) -> None:
        """dataset info still returns correct information despite deprecation warning."""
        with patch("portolan_cli.cli.get_dataset_info") as mock_info:
            mock_info.return_value = DatasetInfo(
                item_id="census",
                collection_id="demographics",
                format_type=FormatType.VECTOR,
                bbox=[-122.5, 37.5, -122.0, 38.0],
                asset_paths=["data.parquet"],
                title="Census 2020",
            )

            with runner.isolated_filesystem():
                Path(".portolan").mkdir()
                (Path(".portolan") / "catalog.json").write_text("{}")

                result = runner.invoke(cli, ["dataset", "info", "demographics/census"])

                assert result.exit_code == 0
                # Warning present
                assert "deprecated" in result.output.lower()
                # But data also present
                assert "census" in result.output
                assert "demographics" in result.output

    @pytest.mark.unit
    def test_dataset_info_deprecation_warning_not_in_json_mode(self, runner: CliRunner) -> None:
        """dataset info does not show deprecation warning in JSON mode."""
        with patch("portolan_cli.cli.get_dataset_info") as mock_info:
            mock_info.return_value = DatasetInfo(
                item_id="my-item",
                collection_id="my-collection",
                format_type=FormatType.VECTOR,
                bbox=[-122.5, 37.5, -122.0, 38.0],
                asset_paths=["data.parquet"],
            )

            with runner.isolated_filesystem():
                Path(".portolan").mkdir()
                (Path(".portolan") / "catalog.json").write_text("{}")

                result = runner.invoke(cli, ["dataset", "info", "my-collection/my-item", "--json"])

                assert result.exit_code == 0
                # JSON output should be parseable without the warning contaminating it
                data = json.loads(result.output)
                assert data["success"] is True
                assert "deprecated" not in result.output.lower()

    @pytest.mark.unit
    def test_dataset_info_deprecation_warning_points_to_info_command(
        self, runner: CliRunner
    ) -> None:
        """dataset info deprecation warning points users to 'portolan info' specifically."""
        with patch("portolan_cli.cli.get_dataset_info") as mock_info:
            mock_info.return_value = DatasetInfo(
                item_id="my-item",
                collection_id="my-collection",
                format_type=FormatType.VECTOR,
                bbox=[-122.5, 37.5, -122.0, 38.0],
                asset_paths=["data.parquet"],
            )

            with runner.isolated_filesystem():
                Path(".portolan").mkdir()
                (Path(".portolan") / "catalog.json").write_text("{}")

                result = runner.invoke(cli, ["dataset", "info", "my-collection/my-item"])

                assert result.exit_code == 0
                # Must specifically mention 'portolan info' (not just any info)
                assert "portolan info" in result.output

    @pytest.mark.unit
    @given(
        item_id=st.from_regex(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,39}", fullmatch=True),
        collection_id=st.from_regex(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,39}", fullmatch=True),
    )
    @settings(max_examples=20)
    def test_dataset_info_deprecation_warning_always_present_for_any_dataset_id(
        self, item_id: str, collection_id: str
    ) -> None:
        """dataset info deprecation warning is always shown regardless of dataset_id value."""
        runner = CliRunner()
        with patch("portolan_cli.cli.get_dataset_info") as mock_info:
            mock_info.return_value = DatasetInfo(
                item_id=item_id,
                collection_id=collection_id,
                format_type=FormatType.VECTOR,
                bbox=[-180.0, -90.0, 180.0, 90.0],
                asset_paths=["data.parquet"],
            )

            with runner.isolated_filesystem():
                Path(".portolan").mkdir()
                (Path(".portolan") / "catalog.json").write_text("{}")

                result = runner.invoke(cli, ["dataset", "info", f"{collection_id}/{item_id}"])

                assert result.exit_code == 0
                assert "deprecated" in result.output.lower()
                assert "portolan info" in result.output


class TestListFormatSize:
    """Tests for format_size helper function."""

    @pytest.mark.unit
    def test_format_size_bytes(self) -> None:
        """format_size handles bytes correctly."""
        from portolan_cli.cli import format_size

        assert format_size(0) == "0B"
        assert format_size(100) == "100B"
        assert format_size(999) == "999B"

    @pytest.mark.unit
    def test_format_size_kilobytes(self) -> None:
        """format_size handles kilobytes correctly."""
        from portolan_cli.cli import format_size

        assert format_size(1024) == "1.0KB"
        assert format_size(1536) == "1.5KB"
        assert format_size(10240) == "10.0KB"

    @pytest.mark.unit
    def test_format_size_megabytes(self) -> None:
        """format_size handles megabytes correctly."""
        from portolan_cli.cli import format_size

        assert format_size(1024 * 1024) == "1.0MB"
        assert format_size(4_400_000) == "4.2MB"

    @pytest.mark.unit
    def test_format_size_gigabytes(self) -> None:
        """format_size handles gigabytes correctly."""
        from portolan_cli.cli import format_size

        assert format_size(1024 * 1024 * 1024) == "1.0GB"
        assert format_size(2 * 1024 * 1024 * 1024) == "2.0GB"
