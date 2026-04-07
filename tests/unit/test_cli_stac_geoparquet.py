"""Tests for 'portolan stac-geoparquet' CLI command.

TDD-first tests for the stac-geoparquet command per issue #319.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI test runner."""
    return CliRunner()


@pytest.fixture
def catalog_with_collection(tmp_path: Path) -> Path:
    """Create a catalog with a collection containing items."""
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()

    # Create .portolan directory (marks as initialized catalog)
    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("")

    # Create catalog.json
    catalog_json = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "test-catalog",
        "description": "Test catalog",
        "links": [
            {"rel": "root", "href": "./catalog.json"},
            {"rel": "child", "href": "./imagery/collection.json"},
        ],
    }
    (catalog_root / "catalog.json").write_text(json.dumps(catalog_json, indent=2))

    # Create collection directory
    collection_dir = catalog_root / "imagery"
    collection_dir.mkdir()

    # Create 3 items
    item_links = []
    for i in range(3):
        item_id = f"scene-{i:03d}"
        item_dir = collection_dir / item_id
        item_dir.mkdir()

        item_json = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": item_id,
            "geometry": {
                "type": "Point",
                "coordinates": [-122.0 + i * 0.1, 37.5],
            },
            "bbox": [-122.0 + i * 0.1, 37.5, -122.0 + i * 0.1, 37.5],
            "properties": {"datetime": "2024-01-01T00:00:00Z"},
            "assets": {
                "data": {
                    "href": f"./{item_id}.tif",
                    "type": "image/tiff; application=geotiff",
                    "roles": ["data"],
                }
            },
            "links": [],
            "collection": "imagery",
        }
        (item_dir / f"{item_id}.json").write_text(json.dumps(item_json, indent=2))
        item_links.append({"rel": "item", "href": f"./{item_id}/{item_id}.json"})

    # Create collection.json
    collection_json = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": "imagery",
        "description": "Test imagery",
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[-122.2, 37.5, -121.8, 37.5]]},
            "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
        },
        "links": [
            {"rel": "root", "href": "../catalog.json"},
            {"rel": "self", "href": "./collection.json"},
            *item_links,
        ],
    }
    (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

    return catalog_root


# =============================================================================
# Test: Basic Command Execution
# =============================================================================


class TestStacGeoparquetCommand:
    """Tests for 'portolan stac-geoparquet' command."""

    @pytest.mark.unit
    def test_command_exists(self, runner: CliRunner) -> None:
        """Test that stac-geoparquet command is registered."""
        result = runner.invoke(cli, ["stac-geoparquet", "--help"])
        assert result.exit_code == 0
        assert "Generate items.parquet" in result.output or "stac-geoparquet" in result.output

    @pytest.mark.unit
    def test_generate_creates_parquet(
        self, runner: CliRunner, catalog_with_collection: Path
    ) -> None:
        """Test that generate creates items.parquet file."""
        collection_path = catalog_with_collection / "imagery"

        result = runner.invoke(
            cli,
            [
                "stac-geoparquet",
                "--collection",
                "imagery",
                "--catalog",
                str(catalog_with_collection),
            ],
        )

        assert result.exit_code == 0
        assert (collection_path / "items.parquet").exists()

    @pytest.mark.unit
    def test_generate_adds_link_to_collection(
        self, runner: CliRunner, catalog_with_collection: Path
    ) -> None:
        """Test that generate adds parquet link to collection.json."""
        result = runner.invoke(
            cli,
            [
                "stac-geoparquet",
                "--collection",
                "imagery",
                "--catalog",
                str(catalog_with_collection),
            ],
        )

        assert result.exit_code == 0

        # Check collection.json has the link
        collection_json = json.loads(
            (catalog_with_collection / "imagery" / "collection.json").read_text()
        )
        parquet_links = [
            link for link in collection_json["links"] if link.get("type") == "application/x-parquet"
        ]
        assert len(parquet_links) == 1
        assert parquet_links[0]["rel"] == "items"

    @pytest.mark.unit
    def test_generate_outputs_success_message(
        self, runner: CliRunner, catalog_with_collection: Path
    ) -> None:
        """Test that successful generation shows confirmation message."""
        result = runner.invoke(
            cli,
            [
                "stac-geoparquet",
                "--collection",
                "imagery",
                "--catalog",
                str(catalog_with_collection),
            ],
        )

        assert result.exit_code == 0
        assert "items.parquet" in result.output or "Generated" in result.output

    @pytest.mark.unit
    def test_generate_json_output(self, runner: CliRunner, catalog_with_collection: Path) -> None:
        """Test JSON output mode."""
        result = runner.invoke(
            cli,
            [
                "stac-geoparquet",
                "--collection",
                "imagery",
                "--catalog",
                str(catalog_with_collection),
                "--json",
            ],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["success"] is True
        assert "parquet_path" in data["data"]


# =============================================================================
# Test: Error Handling
# =============================================================================


class TestStacGeoparquetErrors:
    """Tests for error handling in stac-geoparquet command."""

    @pytest.mark.unit
    def test_missing_collection_shows_error(
        self, runner: CliRunner, catalog_with_collection: Path
    ) -> None:
        """Test error when collection doesn't exist."""
        result = runner.invoke(
            cli,
            [
                "stac-geoparquet",
                "--collection",
                "nonexistent",
                "--catalog",
                str(catalog_with_collection),
            ],
        )

        assert result.exit_code != 0
        assert "not found" in result.output.lower() or "error" in result.output.lower()

    @pytest.mark.unit
    def test_empty_collection_shows_error(self, runner: CliRunner, tmp_path: Path) -> None:
        """Test error when collection has no items."""
        # Create catalog with empty collection
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()
        (catalog_root / ".portolan" / "config.yaml").write_text("")

        catalog_json = {
            "type": "Catalog",
            "stac_version": "1.0.0",
            "id": "test",
            "description": "Test",
            "links": [
                {"rel": "root", "href": "./catalog.json"},
                {"rel": "child", "href": "./empty/collection.json"},
            ],
        }
        (catalog_root / "catalog.json").write_text(json.dumps(catalog_json, indent=2))

        collection_dir = catalog_root / "empty"
        collection_dir.mkdir()
        collection_json = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "empty",
            "description": "Empty",
            "license": "CC-BY-4.0",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [{"rel": "self", "href": "./collection.json"}],
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

        result = runner.invoke(
            cli,
            [
                "stac-geoparquet",
                "--collection",
                "empty",
                "--catalog",
                str(catalog_root),
            ],
        )

        assert result.exit_code != 0
        assert "no items" in result.output.lower() or "empty" in result.output.lower()


# =============================================================================
# Test: Dry Run Mode
# =============================================================================


class TestStacGeoparquetDryRun:
    """Tests for dry-run mode."""

    @pytest.mark.unit
    def test_dry_run_does_not_create_file(
        self, runner: CliRunner, catalog_with_collection: Path
    ) -> None:
        """Test that --dry-run doesn't create parquet file."""
        result = runner.invoke(
            cli,
            [
                "stac-geoparquet",
                "--collection",
                "imagery",
                "--catalog",
                str(catalog_with_collection),
                "--dry-run",
            ],
        )

        assert result.exit_code == 0
        assert not (catalog_with_collection / "imagery" / "items.parquet").exists()

    @pytest.mark.unit
    def test_dry_run_shows_what_would_happen(
        self, runner: CliRunner, catalog_with_collection: Path
    ) -> None:
        """Test that --dry-run shows preview message."""
        result = runner.invoke(
            cli,
            [
                "stac-geoparquet",
                "--collection",
                "imagery",
                "--catalog",
                str(catalog_with_collection),
                "--dry-run",
            ],
        )

        assert result.exit_code == 0
        assert "would" in result.output.lower() or "dry" in result.output.lower()
