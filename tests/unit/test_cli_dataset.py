"""Unit tests for dataset CLI commands.

Tests the CLI layer for dataset add/list/info/remove commands.
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
    """Create an initialized Portolan catalog."""
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()
    catalog_data = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "portolan-catalog",
        "description": "A Portolan-managed STAC catalog",
        "links": [],
    }
    (portolan_dir / "catalog.json").write_text(json.dumps(catalog_data, indent=2))
    return tmp_path


class TestDatasetAdd:
    """Tests for 'portolan dataset add' command."""

    @pytest.mark.unit
    def test_add_dataset_success(
        self, runner: CliRunner, initialized_catalog: Path, tmp_path: Path
    ) -> None:
        """dataset add succeeds with valid input."""
        # Create a test file
        test_file = tmp_path / "test.geojson"
        test_file.write_text('{"type": "FeatureCollection", "features": []}')

        with patch("portolan_cli.cli.add_dataset") as mock_add:
            mock_add.return_value = DatasetInfo(
                item_id="test",
                collection_id="my-collection",
                format_type=FormatType.VECTOR,
                bbox=[-122.5, 37.5, -122.0, 38.0],
                asset_paths=["test.parquet"],
            )

            result = runner.invoke(
                cli,
                ["dataset", "add", str(test_file), "--collection", "my-collection"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0
            assert "test" in result.output or "my-collection" in result.output

    @pytest.mark.unit
    def test_add_dataset_missing_collection(self, runner: CliRunner, tmp_path: Path) -> None:
        """dataset add fails without --collection."""
        test_file = tmp_path / "test.geojson"
        test_file.write_text('{"type": "FeatureCollection", "features": []}')

        result = runner.invoke(
            cli,
            ["dataset", "add", str(test_file)],
        )

        assert result.exit_code != 0
        assert "collection" in result.output.lower() or "required" in result.output.lower()

    @pytest.mark.unit
    def test_add_dataset_with_title(
        self, runner: CliRunner, initialized_catalog: Path, tmp_path: Path
    ) -> None:
        """dataset add accepts --title option."""
        test_file = tmp_path / "test.geojson"
        test_file.write_text('{"type": "FeatureCollection", "features": []}')

        with patch("portolan_cli.cli.add_dataset") as mock_add:
            mock_add.return_value = DatasetInfo(
                item_id="test",
                collection_id="col",
                format_type=FormatType.VECTOR,
                bbox=[0, 0, 1, 1],
                asset_paths=["test.parquet"],
                title="My Title",
            )

            result = runner.invoke(
                cli,
                [
                    "dataset",
                    "add",
                    str(test_file),
                    "--collection",
                    "col",
                    "--title",
                    "My Title",
                ],
                catch_exceptions=False,
            )

            assert result.exit_code == 0
            mock_add.assert_called_once()
            call_kwargs = mock_add.call_args.kwargs
            assert call_kwargs.get("title") == "My Title"


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


class TestDatasetRemove:
    """Tests for 'portolan dataset remove' command."""

    @pytest.mark.unit
    def test_remove_existing_dataset(self, runner: CliRunner) -> None:
        """dataset remove succeeds for existing dataset."""
        with patch("portolan_cli.cli.remove_dataset") as mock_remove:
            with runner.isolated_filesystem():
                Path(".portolan").mkdir()
                (Path(".portolan") / "catalog.json").write_text("{}")

                result = runner.invoke(cli, ["dataset", "remove", "col/item", "--yes"])

                assert result.exit_code == 0
                mock_remove.assert_called_once()

    @pytest.mark.unit
    def test_remove_prompts_for_confirmation(self, runner: CliRunner) -> None:
        """dataset remove prompts for confirmation without --yes."""
        with patch("portolan_cli.cli.remove_dataset") as mock_remove:
            with runner.isolated_filesystem():
                Path(".portolan").mkdir()
                (Path(".portolan") / "catalog.json").write_text("{}")

                # Simulate 'n' response
                runner.invoke(cli, ["dataset", "remove", "col/item"], input="n\n")

                # Should not have called remove
                mock_remove.assert_not_called()

    @pytest.mark.unit
    def test_remove_collection(self, runner: CliRunner) -> None:
        """dataset remove --collection removes entire collection."""
        with patch("portolan_cli.cli.remove_dataset") as mock_remove:
            with runner.isolated_filesystem():
                Path(".portolan").mkdir()
                (Path(".portolan") / "catalog.json").write_text("{}")

                result = runner.invoke(
                    cli, ["dataset", "remove", "my-collection", "--collection", "--yes"]
                )

                assert result.exit_code == 0
                mock_remove.assert_called_once()
                call_kwargs = mock_remove.call_args.kwargs
                assert call_kwargs.get("remove_collection") is True
