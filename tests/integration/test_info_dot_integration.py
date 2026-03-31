"""Integration tests for `portolan info .` at catalog and subcatalog roots.

Tests the fix for GitHub issues #291 and #292 where `portolan info .` failed
with "Collection not found" instead of showing catalog-level information.

These tests verify:
1. Real directory structure creation and navigation
2. Full CLI invocation with path resolution
3. Both human-readable and JSON output modes
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner


@pytest.fixture
def nested_catalog_structure(tmp_path: Path) -> Path:
    """Create a realistic nested catalog structure for integration testing.

    Structure:
        catalog/
        ├── .portolan/
        │   └── config.yaml
        ├── catalog.json          (root catalog)
        ├── climate/
        │   ├── .portolan/
        │   │   └── config.yaml
        │   ├── catalog.json      (subcatalog)
        │   └── temperature/
        │       └── collection.json
        └── demographics/
            └── collection.json
    """
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()

    # Root .portolan directory
    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("# Root config\n")

    # Root catalog.json
    root_catalog = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "root-catalog",
        "description": "Root catalog for integration testing",
        "links": [
            {"rel": "root", "href": "./catalog.json"},
            {"rel": "child", "href": "./climate/catalog.json"},
            {"rel": "child", "href": "./demographics/collection.json"},
        ],
    }
    (catalog_root / "catalog.json").write_text(json.dumps(root_catalog, indent=2))

    # Climate subcatalog
    climate_dir = catalog_root / "climate"
    climate_dir.mkdir()

    climate_portolan = climate_dir / ".portolan"
    climate_portolan.mkdir()
    (climate_portolan / "config.yaml").write_text("# Climate config\n")

    climate_catalog = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "climate-subcatalog",
        "description": "Climate data subcatalog",
        "links": [
            {"rel": "root", "href": "../catalog.json"},
            {"rel": "parent", "href": "../catalog.json"},
            {"rel": "self", "href": "./catalog.json"},
            {"rel": "child", "href": "./temperature/collection.json"},
        ],
    }
    (climate_dir / "catalog.json").write_text(json.dumps(climate_catalog, indent=2))

    # Temperature collection inside climate subcatalog
    temp_dir = climate_dir / "temperature"
    temp_dir.mkdir()

    temp_collection = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": "temperature",
        "description": "Temperature measurements",
        "title": "Temperature Data",
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[4.0, 52.0, 5.0, 53.0]]},
            "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
        },
        "links": [
            {"rel": "root", "href": "../../catalog.json"},
            {"rel": "parent", "href": "../catalog.json"},
            {"rel": "self", "href": "./collection.json"},
        ],
    }
    (temp_dir / "collection.json").write_text(json.dumps(temp_collection, indent=2))

    # Demographics collection at root level
    demo_dir = catalog_root / "demographics"
    demo_dir.mkdir()

    demo_collection = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": "demographics",
        "description": "Demographics data",
        "title": "Demographics",
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[-122.5, 37.7, -122.3, 37.9]]},
            "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
        },
        "links": [
            {"rel": "root", "href": "../catalog.json"},
            {"rel": "parent", "href": "../catalog.json"},
            {"rel": "self", "href": "./collection.json"},
        ],
    }
    (demo_dir / "collection.json").write_text(json.dumps(demo_collection, indent=2))

    return catalog_root


class TestInfoDotIntegration:
    """Integration tests for `portolan info .` behavior."""

    @pytest.mark.integration
    def test_info_dot_from_catalog_root(self, nested_catalog_structure: Path) -> None:
        """Test `portolan info .` when cwd is the catalog root."""
        from portolan_cli.cli import cli

        runner = CliRunner()

        # Change to catalog root and run `info .`
        with runner.isolated_filesystem():
            # Can't actually chdir in isolated_filesystem, so use absolute path
            result = runner.invoke(
                cli,
                [
                    "info",
                    str(nested_catalog_structure),
                    "--catalog",
                    str(nested_catalog_structure),
                ],
            )

            assert result.exit_code == 0, f"Failed: {result.output}"
            assert "root-catalog" in result.output
            assert "Collection not found" not in result.output

    @pytest.mark.integration
    def test_info_subcatalog_path(self, nested_catalog_structure: Path) -> None:
        """Test `portolan info ./climate` from catalog root."""
        from portolan_cli.cli import cli

        runner = CliRunner()
        subcatalog_path = nested_catalog_structure / "climate"

        result = runner.invoke(
            cli,
            [
                "info",
                str(subcatalog_path),
                "--catalog",
                str(nested_catalog_structure),
            ],
        )

        assert result.exit_code == 0, f"Failed: {result.output}"
        assert "climate-subcatalog" in result.output
        assert "Collection not found" not in result.output

    @pytest.mark.integration
    def test_info_collection_still_works(self, nested_catalog_structure: Path) -> None:
        """Ensure collections are still recognized correctly."""
        from portolan_cli.cli import cli

        runner = CliRunner()
        collection_path = nested_catalog_structure / "demographics"

        result = runner.invoke(
            cli,
            [
                "info",
                str(collection_path),
                "--catalog",
                str(nested_catalog_structure),
            ],
        )

        assert result.exit_code == 0, f"Failed: {result.output}"
        # Collection output should show collection-specific info
        assert "demographics" in result.output.lower()

    @pytest.mark.integration
    def test_info_nested_collection(self, nested_catalog_structure: Path) -> None:
        """Test info on a collection nested inside a subcatalog."""
        from portolan_cli.cli import cli

        runner = CliRunner()
        collection_path = nested_catalog_structure / "climate" / "temperature"

        result = runner.invoke(
            cli,
            [
                "info",
                str(collection_path),
                "--catalog",
                str(nested_catalog_structure),
            ],
        )

        assert result.exit_code == 0, f"Failed: {result.output}"
        assert "temperature" in result.output.lower()

    @pytest.mark.integration
    def test_info_json_output_catalog_vs_collection(self, nested_catalog_structure: Path) -> None:
        """Verify JSON output correctly identifies catalog vs collection."""
        from portolan_cli.cli import cli

        runner = CliRunner()

        # Catalog JSON output
        catalog_result = runner.invoke(
            cli,
            [
                "info",
                str(nested_catalog_structure),
                "--catalog",
                str(nested_catalog_structure),
                "--json",
            ],
        )
        assert catalog_result.exit_code == 0
        catalog_data = json.loads(catalog_result.output)
        assert catalog_data["success"] is True
        assert "catalog_id" in catalog_data["data"]

        # Collection JSON output
        collection_result = runner.invoke(
            cli,
            [
                "info",
                str(nested_catalog_structure / "demographics"),
                "--catalog",
                str(nested_catalog_structure),
                "--json",
            ],
        )
        assert collection_result.exit_code == 0
        collection_data = json.loads(collection_result.output)
        assert collection_data["success"] is True
        assert "collection_id" in collection_data["data"]

    @pytest.mark.integration
    def test_info_invalid_directory_error(self, nested_catalog_structure: Path) -> None:
        """Test that directories without catalog.json or collection.json fail clearly."""
        from portolan_cli.cli import cli

        runner = CliRunner()

        # Create an empty directory
        empty_dir = nested_catalog_structure / "empty"
        empty_dir.mkdir()

        result = runner.invoke(
            cli,
            [
                "info",
                str(empty_dir),
                "--catalog",
                str(nested_catalog_structure),
            ],
        )

        assert result.exit_code != 0
        assert "not a catalog or collection" in result.output.lower()
