"""Unit tests for dataset CLI commands.

Tests the CLI layer for dataset list/info commands.

Note: dataset add/remove were moved to top-level `portolan add` and `portolan rm`
commands (see test_cli_add_rm.py).
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
        """dataset list shows datasets."""
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
                assert "item1" in result.output
                assert "item2" in result.output

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
    """Tests for dataset list with title display."""

    @pytest.mark.unit
    def test_list_shows_titles(self, runner: CliRunner) -> None:
        """dataset list displays titles when present."""
        with patch("portolan_cli.cli.list_datasets") as mock_list:
            mock_list.return_value = [
                DatasetInfo(
                    item_id="item1",
                    collection_id="col1",
                    format_type=FormatType.VECTOR,
                    bbox=[0, 0, 1, 1],
                    asset_paths=["data.parquet"],
                    title="My Dataset Title",
                ),
            ]

            with runner.isolated_filesystem():
                Path(".portolan").mkdir()
                (Path(".portolan") / "catalog.json").write_text("{}")

                result = runner.invoke(cli, ["dataset", "list"])

                assert result.exit_code == 0
                assert "My Dataset Title" in result.output


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
