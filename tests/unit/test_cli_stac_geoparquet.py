"""Tests for 'portolan stac-geoparquet' CLI command.

TDD-first tests for the stac-geoparquet command per issue #319.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

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


@pytest.fixture
def catalog_with_small_collection(tmp_path: Path) -> Path:
    """Create a catalog with a single-item collection and a versions.json at 1.0.0.

    Used to reproduce issue #653: a below-threshold collection must not have
    items.parquet generated (nor its version bumped) during a BULK sweep.
    """
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()

    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("")

    catalog_json = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "test-catalog",
        "description": "Test catalog",
        "links": [
            {"rel": "root", "href": "./catalog.json"},
            {"rel": "child", "href": "./tiny/collection.json"},
        ],
    }
    (catalog_root / "catalog.json").write_text(json.dumps(catalog_json, indent=2))

    collection_dir = catalog_root / "tiny"
    collection_dir.mkdir()

    item_id = "scene-000"
    item_dir = collection_dir / item_id
    item_dir.mkdir()
    item_json = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": item_id,
        "geometry": {"type": "Point", "coordinates": [-122.0, 37.5]},
        "bbox": [-122.0, 37.5, -122.0, 37.5],
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
        "assets": {
            "data": {
                "href": f"./{item_id}.tif",
                "type": "image/tiff; application=geotiff",
                "roles": ["data"],
            }
        },
        "links": [],
        "collection": "tiny",
    }
    (item_dir / f"{item_id}.json").write_text(json.dumps(item_json, indent=2))

    collection_json = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": "tiny",
        "description": "Tiny collection",
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[-122.0, 37.5, -122.0, 37.5]]},
            "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
        },
        "links": [
            {"rel": "root", "href": "../catalog.json"},
            {"rel": "self", "href": "./collection.json"},
            {"rel": "item", "href": f"./{item_id}/{item_id}.json"},
        ],
    }
    (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

    # Pre-existing versions.json pinned at 1.0.0 (no items.parquet asset yet).
    versions_json = {
        "spec_version": "1.0.0",
        "current_version": "1.0.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-01T00:00:00Z",
                "breaking": False,
                "assets": {
                    "scene-000/scene-000.tif": {
                        "sha256": "0" * 64,
                        "size_bytes": 1,
                        "href": "tiny/scene-000/scene-000.tif",
                    }
                },
                "changes": [],
                "message": "initial",
            }
        ],
    }
    (collection_dir / "versions.json").write_text(json.dumps(versions_json, indent=2))

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
            link
            for link in collection_json["links"]
            if link.get("type") == "application/vnd.apache.parquet"
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
        # When processing single collection, results is an array with one entry
        assert data["data"]["collections_processed"] == 1
        assert len(data["data"]["results"]) == 1
        assert "parquet_path" in data["data"]["results"][0]


# =============================================================================
# Test: Catalog-Level Operation
# =============================================================================


class TestStacGeoparquetCatalogLevel:
    """Tests for catalog-level stac-geoparquet generation."""

    @pytest.mark.unit
    def test_generate_all_collections(
        self, runner: CliRunner, catalog_with_collection: Path
    ) -> None:
        """Test that omitting --collection generates for above-threshold collections.

        The fixture has 3 items (below the default 100 threshold), so lower the
        threshold via config to keep exercising the bulk-iteration path.
        """
        (catalog_with_collection / ".portolan" / "config.yaml").write_text(
            "parquet:\n  threshold: 0\n"
        )
        result = runner.invoke(
            cli,
            [
                "stac-geoparquet",
                "--catalog",
                str(catalog_with_collection),
            ],
        )

        assert result.exit_code == 0
        # Should have generated parquet for the imagery collection
        assert (catalog_with_collection / "imagery" / "items.parquet").exists()

    @pytest.mark.unit
    def test_generate_all_collections_json(
        self, runner: CliRunner, catalog_with_collection: Path
    ) -> None:
        """Test JSON output for catalog-level generation."""
        (catalog_with_collection / ".portolan" / "config.yaml").write_text(
            "parquet:\n  threshold: 0\n"
        )
        result = runner.invoke(
            cli,
            [
                "stac-geoparquet",
                "--catalog",
                str(catalog_with_collection),
                "--json",
            ],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["success"] is True
        assert data["data"]["collections_processed"] >= 1


# =============================================================================
# Test: Bulk Threshold Gate (issue #653)
# =============================================================================


class TestStacGeoparquetBulkThreshold:
    """Bulk generation must respect the item-count threshold (issue #653)."""

    @pytest.mark.unit
    def test_bulk_skips_below_threshold_collection(
        self, runner: CliRunner, catalog_with_small_collection: Path
    ) -> None:
        """BULK: a 1-item collection (<= threshold 100) is skipped, not generated.

        Regression for #653: no items.parquet on disk, versions.json stays at
        1.0.0 with no items.parquet asset, and the collection is reported skipped.
        """
        collection_dir = catalog_with_small_collection / "tiny"

        result = runner.invoke(
            cli,
            [
                "stac-geoparquet",
                "--catalog",
                str(catalog_with_small_collection),
                "--json",
            ],
        )

        assert result.exit_code == 0

        # No derived parquet file was created.
        assert not (collection_dir / "items.parquet").exists()

        # versions.json was not bumped and has no items.parquet asset.
        versions = json.loads((collection_dir / "versions.json").read_text())
        assert versions["current_version"] == "1.0.0"
        assert len(versions["versions"]) == 1
        for version in versions["versions"]:
            assert "items.parquet" not in version["assets"]

        # The skip is surfaced in the JSON envelope (no silent drop).
        data = json.loads(result.output)
        assert data["success"] is True
        skipped_ids = [entry["collection"] for entry in data["data"].get("skipped", [])]
        assert "tiny" in skipped_ids

    @pytest.mark.unit
    def test_bulk_generates_above_threshold_collection(
        self, runner: CliRunner, catalog_with_small_collection: Path
    ) -> None:
        """BULK: a collection with > 100 items still generates (no over-correction).

        Item generation is mocked so we do not have to create 100+ real items.
        """
        collection_dir = catalog_with_small_collection / "tiny"
        collection_json = json.loads((collection_dir / "collection.json").read_text())
        # Inflate to 101 item links so count_items() > default threshold (100).
        base_links = [link for link in collection_json["links"] if link.get("rel") != "item"]
        item_links = [
            {"rel": "item", "href": f"./scene-{i:03d}/scene-{i:03d}.json"} for i in range(101)
        ]
        collection_json["links"] = base_links + item_links
        (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

        with (
            patch("portolan_cli.stac_parquet.generate_items_parquet") as gen,
            patch("portolan_cli.stac_parquet.add_parquet_link_to_collection"),
            patch("portolan_cli.stac_parquet.track_parquet_in_versions"),
        ):
            gen.return_value = collection_dir / "items.parquet"
            result = runner.invoke(
                cli,
                [
                    "stac-geoparquet",
                    "--catalog",
                    str(catalog_with_small_collection),
                    "--json",
                ],
            )

        assert result.exit_code == 0
        assert gen.called
        assert gen.call_args.args[0] == collection_dir

    @pytest.mark.unit
    def test_explicit_small_collection_still_generates(
        self, runner: CliRunner, catalog_with_small_collection: Path
    ) -> None:
        """EXPLICIT --collection: a 1-item collection still generates (explicit intent)."""
        collection_dir = catalog_with_small_collection / "tiny"

        result = runner.invoke(
            cli,
            [
                "stac-geoparquet",
                "--collection",
                "tiny",
                "--catalog",
                str(catalog_with_small_collection),
            ],
        )

        assert result.exit_code == 0
        assert (collection_dir / "items.parquet").exists()

    @pytest.mark.unit
    def test_bulk_respects_threshold_override(
        self, runner: CliRunner, catalog_with_small_collection: Path
    ) -> None:
        """BULK: a configured threshold of 0 makes a 1-item collection generate.

        Guards against a hardcoded 100, the threshold must come from config.
        """
        (catalog_with_small_collection / ".portolan" / "config.yaml").write_text(
            "parquet:\n  threshold: 0\n"
        )
        collection_dir = catalog_with_small_collection / "tiny"

        result = runner.invoke(
            cli,
            [
                "stac-geoparquet",
                "--catalog",
                str(catalog_with_small_collection),
            ],
        )

        assert result.exit_code == 0
        assert (collection_dir / "items.parquet").exists()


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
