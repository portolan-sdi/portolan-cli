"""Tests for the CLI info command.

TDD-first tests for the top-level `portolan info` command.
Tests file-level, collection-level, and catalog-level info display.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from click.testing import CliRunner

if TYPE_CHECKING:
    pass


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def cli_runner() -> CliRunner:
    """Create a Click CLI test runner."""
    return CliRunner()


@pytest.fixture
def catalog_with_tracked_file(tmp_path: Path, valid_points_parquet: Path) -> Path:
    """Create a catalog with a tracked GeoParquet file."""
    import shutil

    # Create catalog structure
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()

    # Create .portolan directory (for MANAGED state per ADR-0027)
    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("# Portolan config\n")
    (portolan_dir / "state.json").write_text("{}\n")

    # Create catalog.json
    catalog_json = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "test-catalog",
        "description": "Test catalog for info command",
        "links": [
            {"rel": "root", "href": "./catalog.json"},
            {"rel": "child", "href": "./demographics/collection.json"},
        ],
    }
    (catalog_root / "catalog.json").write_text(json.dumps(catalog_json, indent=2))

    # Create collection directory
    collection_dir = catalog_root / "demographics"
    collection_dir.mkdir()

    # Create collection.json
    collection_json = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": "demographics",
        "description": "Demographics data collection",
        "title": "Demographics",
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[-122.5, 37.7, -122.3, 37.9]]},
            "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
        },
        "links": [
            {"rel": "root", "href": "../catalog.json"},
            {"rel": "self", "href": "./collection.json"},
            {"rel": "item", "href": "./census/census.json"},
        ],
    }
    (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

    # Create item directory
    item_dir = collection_dir / "census"
    item_dir.mkdir()

    # Copy test parquet file
    dest_parquet = item_dir / "census.parquet"
    shutil.copy2(valid_points_parquet, dest_parquet)

    # Create item.json
    item_json = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "census",
        "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]},
        "bbox": [-122.5, 37.7, -122.3, 37.9],
        "properties": {"datetime": "2024-01-01T00:00:00Z", "title": "Census Data"},
        "assets": {
            "data": {
                "href": "./census.parquet",
                "type": "application/x-parquet",
                "roles": ["data"],
            }
        },
        "links": [],
    }
    (item_dir / "census.json").write_text(json.dumps(item_json, indent=2))

    # Create versions.json
    versions_json = {
        "spec_version": "1.0.0",
        "current_version": "1.2.0",
        "versions": [
            {
                "version": "1.2.0",
                "created": "2024-01-15T00:00:00Z",
                "breaking": False,
                "assets": {
                    "census.parquet": {
                        "sha256": "abc123",
                        "size_bytes": 1500,
                        "href": "demographics/census/census.parquet",
                    }
                },
                "changes": ["census.parquet"],
            }
        ],
    }
    (collection_dir / "versions.json").write_text(json.dumps(versions_json, indent=2))

    return catalog_root


# =============================================================================
# Test: File Info Command
# =============================================================================


class TestInfoCommandFile:
    """Tests for `portolan info <file>` command."""

    @pytest.mark.unit
    def test_info_file_shows_metadata(
        self, cli_runner: CliRunner, catalog_with_tracked_file: Path
    ) -> None:
        """Test that info command displays file metadata."""
        from portolan_cli.cli import cli

        file_path = catalog_with_tracked_file / "demographics" / "census" / "census.parquet"

        result = cli_runner.invoke(
            cli,
            ["info", str(file_path), "--catalog", str(catalog_with_tracked_file)],
        )

        assert result.exit_code == 0
        assert "Format:" in result.output or "GeoParquet" in result.output
        assert "CRS:" in result.output
        assert "Bbox:" in result.output

    @pytest.mark.unit
    def test_info_file_shows_version_when_tracked(
        self, cli_runner: CliRunner, catalog_with_tracked_file: Path
    ) -> None:
        """Test that info command shows version for tracked files."""
        from portolan_cli.cli import cli

        file_path = catalog_with_tracked_file / "demographics" / "census" / "census.parquet"

        result = cli_runner.invoke(
            cli,
            ["info", str(file_path), "--catalog", str(catalog_with_tracked_file)],
        )

        assert result.exit_code == 0
        assert "Version:" in result.output
        assert "v1.2.0" in result.output

    @pytest.mark.unit
    def test_info_file_json_output(
        self, cli_runner: CliRunner, catalog_with_tracked_file: Path
    ) -> None:
        """Test that info command produces valid JSON with --json flag."""
        from portolan_cli.cli import cli

        file_path = catalog_with_tracked_file / "demographics" / "census" / "census.parquet"

        result = cli_runner.invoke(
            cli,
            ["info", str(file_path), "--catalog", str(catalog_with_tracked_file), "--json"],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["success"] is True
        assert data["command"] == "info"
        assert "format" in data["data"]

    @pytest.mark.unit
    def test_info_nonexistent_file_fails(
        self, cli_runner: CliRunner, catalog_with_tracked_file: Path
    ) -> None:
        """Test that info command fails for non-existent file."""
        from portolan_cli.cli import cli

        result = cli_runner.invoke(
            cli,
            [
                "info",
                str(catalog_with_tracked_file / "nonexistent.parquet"),
                "--catalog",
                str(catalog_with_tracked_file),
            ],
        )

        assert result.exit_code != 0


# =============================================================================
# Test: Collection Info Command
# =============================================================================


class TestInfoCommandCollection:
    """Tests for `portolan info <collection/>` command."""

    @pytest.mark.unit
    def test_info_collection_shows_metadata(
        self, cli_runner: CliRunner, catalog_with_tracked_file: Path
    ) -> None:
        """Test that info command displays collection metadata."""
        from portolan_cli.cli import cli

        collection_path = catalog_with_tracked_file / "demographics"

        result = cli_runner.invoke(
            cli,
            ["info", str(collection_path), "--catalog", str(catalog_with_tracked_file)],
        )

        assert result.exit_code == 0
        assert "Collection:" in result.output or "demographics" in result.output
        assert "Description:" in result.output or "Items:" in result.output

    @pytest.mark.unit
    def test_info_collection_json_output(
        self, cli_runner: CliRunner, catalog_with_tracked_file: Path
    ) -> None:
        """Test that info command produces valid JSON for collections."""
        from portolan_cli.cli import cli

        collection_path = catalog_with_tracked_file / "demographics"

        result = cli_runner.invoke(
            cli,
            ["info", str(collection_path), "--catalog", str(catalog_with_tracked_file), "--json"],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["success"] is True
        assert "collection_id" in data["data"]


# =============================================================================
# Test: Catalog Info Command (no argument)
# =============================================================================


class TestInfoCommandCatalog:
    """Tests for `portolan info` (catalog-level) command."""

    @pytest.mark.unit
    def test_info_catalog_shows_metadata(
        self, cli_runner: CliRunner, catalog_with_tracked_file: Path
    ) -> None:
        """Test that info command displays catalog metadata when no arg given."""
        from portolan_cli.cli import cli

        result = cli_runner.invoke(
            cli,
            ["info", "--catalog", str(catalog_with_tracked_file)],
        )

        assert result.exit_code == 0
        assert "Catalog:" in result.output or "test-catalog" in result.output

    @pytest.mark.unit
    def test_info_catalog_json_output(
        self, cli_runner: CliRunner, catalog_with_tracked_file: Path
    ) -> None:
        """Test that info command produces valid JSON for catalog."""
        from portolan_cli.cli import cli

        result = cli_runner.invoke(
            cli,
            ["info", "--catalog", str(catalog_with_tracked_file), "--json"],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["success"] is True
        assert "catalog_id" in data["data"]
