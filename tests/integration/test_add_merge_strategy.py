"""Integration tests for --merge-strategy flag in portolan add.

Issue #446: portolan add strips human-authored asset and column metadata.

These tests verify the CLI flag works end-to-end:
1. smart (default): Preserves human-enrichable fields, updates machine-derivable
2. keep: Preserves all existing fields
3. overwrite: Replaces everything with auto-detected values
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
def collection_with_metadata(initialized_catalog: Path, valid_points_parquet: Path) -> Path:
    """Create a collection with hand-authored metadata that should be preserved."""
    collection_dir = initialized_catalog / "census"
    collection_dir.mkdir()

    # Copy parquet file
    data_file = collection_dir / "data.parquet"
    shutil.copy(valid_points_parquet, data_file)

    # Create collection.json with human-authored metadata
    collection_json = {
        "type": "Collection",
        "stac_version": "1.1.0",
        "id": "census",
        "description": "Test collection",
        "license": "proprietary",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
        },
        "links": [],
        "assets": {
            "data": {
                "href": "./data.parquet",
                "type": "application/vnd.apache.parquet",
                "roles": ["data"],
                "title": "Census Demographics 2020",
                "description": "Detailed census data with population demographics by tract.",
            }
        },
        "table:columns": [
            {
                "name": "boundary_id",
                "type": "int64",
                "description": "Unique building boundary identifier.",
            },
            {
                "name": "geometry",
                "type": "binary",
                "description": "Building footprint polygon in WGS84.",
            },
        ],
    }
    (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

    return collection_dir


class TestMergeStrategySmartIntegration:
    """Integration tests for --merge-strategy=smart (default)."""

    @pytest.mark.integration
    def test_smart_preserves_asset_title(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        collection_with_metadata: Path,
    ) -> None:
        """Smart strategy preserves existing asset title through CLI."""
        data_file = collection_with_metadata / "data.parquet"

        # Re-add the file (simulating update)
        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                "--force",
                str(data_file),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Verify title preserved
        collection_json = json.loads((collection_with_metadata / "collection.json").read_text())
        assert collection_json["assets"]["data"]["title"] == "Census Demographics 2020"

    @pytest.mark.integration
    def test_smart_preserves_asset_description(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        collection_with_metadata: Path,
    ) -> None:
        """Smart strategy preserves existing asset description through CLI."""
        data_file = collection_with_metadata / "data.parquet"

        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                "--force",
                str(data_file),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        collection_json = json.loads((collection_with_metadata / "collection.json").read_text())
        assert "census data with population" in collection_json["assets"]["data"]["description"]

    @pytest.mark.integration
    def test_smart_preserves_column_descriptions(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        collection_with_metadata: Path,
    ) -> None:
        """Smart strategy preserves table:columns descriptions through CLI."""
        data_file = collection_with_metadata / "data.parquet"

        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                "--force",
                str(data_file),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        collection_json = json.loads((collection_with_metadata / "collection.json").read_text())

        # Find the boundary_id column and check description preserved
        columns = collection_json.get("table:columns", [])
        boundary_col = next((c for c in columns if c["name"] == "boundary_id"), None)
        assert boundary_col is not None, "boundary_id column should exist in table:columns"
        assert boundary_col.get("description") == "Unique building boundary identifier."


class TestMergeStrategyKeepIntegration:
    """Integration tests for --merge-strategy=keep."""

    @pytest.mark.integration
    def test_keep_preserves_all_metadata(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        collection_with_metadata: Path,
    ) -> None:
        """Keep strategy preserves all existing metadata through CLI."""
        data_file = collection_with_metadata / "data.parquet"

        # Get original metadata
        original = json.loads((collection_with_metadata / "collection.json").read_text())
        original_title = original["assets"]["data"]["title"]
        original_description = original["assets"]["data"]["description"]

        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                "--force",
                "--merge-strategy=keep",
                str(data_file),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Verify all metadata preserved
        collection_json = json.loads((collection_with_metadata / "collection.json").read_text())
        assert collection_json["assets"]["data"]["title"] == original_title
        assert collection_json["assets"]["data"]["description"] == original_description


class TestMergeStrategyOverwriteIntegration:
    """Integration tests for --merge-strategy=overwrite."""

    @pytest.mark.integration
    def test_overwrite_clears_human_metadata(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
        collection_with_metadata: Path,
    ) -> None:
        """Overwrite strategy replaces metadata through CLI."""
        data_file = collection_with_metadata / "data.parquet"

        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                "--force",
                "--merge-strategy=overwrite",
                str(data_file),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Verify metadata overwritten (title/description should be None or missing)
        collection_json = json.loads((collection_with_metadata / "collection.json").read_text())
        asset = collection_json["assets"]["data"]
        # After overwrite, human-authored fields should be cleared
        assert asset.get("title") is None or "title" not in asset
        assert asset.get("description") is None or "description" not in asset


class TestMergeStrategyCLIValidation:
    """Tests for CLI flag validation."""

    @pytest.mark.integration
    def test_invalid_merge_strategy_rejected(
        self, runner: CliRunner, initialized_catalog: Path
    ) -> None:
        """Invalid merge strategy values are rejected by CLI."""
        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                "--merge-strategy=invalid",
                "somefile.parquet",
            ],
        )
        assert result.exit_code != 0
        assert "invalid" in result.output.lower() or "choice" in result.output.lower()
