"""Regression tests for pystac normalize_hrefs dot-in-path bug (issue #401).

pystac's normalize_hrefs() uses a heuristic: paths with dots in the final
component are treated as files, not directories. This causes catalog.json
and collection.json to be written to the PARENT directory when the path
contains a dot (e.g., /tmp/tmp.XXXXXX from mktemp -d).

These tests verify the fix works across all affected workflows.
"""

import json
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from portolan_cli.cli import cli


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI runner."""
    return CliRunner()


@pytest.fixture
def simple_geojson(tmp_path: Path) -> Path:
    """Create a minimal GeoJSON file for testing."""
    geojson_path = tmp_path / "test.geojson"
    geojson_path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [0, 0]},
                        "properties": {"name": "test"},
                    }
                ],
            }
        )
    )
    return geojson_path


class TestDottedPathRegression:
    """Regression tests for dotted path handling in pystac normalize_hrefs."""

    @pytest.mark.integration
    def test_add_in_dotted_catalog_path(
        self, runner: CliRunner, tmp_path: Path, simple_geojson: Path
    ) -> None:
        """Adding files to a catalog with dotted path should not leak to parent.

        Regression test for issue #401: collection.json must be written to the
        collection directory, not to the catalog root or its parent.
        """
        # Create catalog with dotted path (like mktemp -d produces)
        catalog_root = tmp_path / "my.catalog"
        catalog_root.mkdir()

        # Initialize catalog
        (catalog_root / "catalog.json").write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "stac_version": "1.1.0",
                    "id": "test-catalog",
                    "description": "Test catalog",
                    "links": [],
                }
            )
        )
        (catalog_root / "versions.json").write_text(json.dumps({"version": 1}))

        portolan_dir = catalog_root / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text(
            yaml.dump({"version": 1, "statistics": {"enabled": False}})
        )

        # Create collection directory with file directly in it
        # (portolan infers collection from directory structure)
        # Note: collection name must NOT have dots (portolan rejects them)
        # The BUG is about catalog_root having dots, not collection names
        collection_dir = catalog_root / "test-collection"
        collection_dir.mkdir()

        # Copy geojson directly into collection directory
        collection_geojson = collection_dir / "data.geojson"
        collection_geojson.write_text(simple_geojson.read_text())

        # Add the file using --portolan-dir
        result = runner.invoke(
            cli,
            [
                "add",
                str(collection_geojson),
                "--portolan-dir",
                str(catalog_root),
            ],
        )

        assert result.exit_code == 0, f"Failed: {result.output}"

        # Positive assertions: files in correct locations
        assert collection_dir.exists(), "Collection directory should exist"
        assert (collection_dir / "collection.json").exists(), (
            "collection.json should be in collection directory"
        )

        # Negative assertions: files must NOT leak
        assert not (catalog_root / "collection.json").exists(), (
            "collection.json must NOT leak to catalog root"
        )
        assert not (tmp_path / "collection.json").exists(), (
            "collection.json must NOT leak to parent of catalog"
        )
        assert not (tmp_path / "catalog.json").exists(), (
            "catalog.json must NOT leak to parent directory"
        )

    @pytest.mark.integration
    def test_update_catalog_links_with_dotted_path(
        self, runner: CliRunner, tmp_path: Path, simple_geojson: Path
    ) -> None:
        """Adding second collection should not corrupt catalog.json location.

        Tests _update_catalog_links which has its own normalize_hrefs call.
        """
        catalog_root = tmp_path / "another.catalog"
        catalog_root.mkdir()

        # Initialize with one collection already present
        (catalog_root / "catalog.json").write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "stac_version": "1.1.0",
                    "id": "test-catalog",
                    "description": "Test catalog",
                    "links": [{"rel": "child", "href": "./existing/collection.json"}],
                }
            )
        )
        (catalog_root / "versions.json").write_text(json.dumps({"version": 1}))

        portolan_dir = catalog_root / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text(
            yaml.dump({"version": 1, "statistics": {"enabled": False}})
        )

        # Create existing collection stub with item
        existing_coll = catalog_root / "existing"
        existing_coll.mkdir()
        (existing_coll / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "stac_version": "1.1.0",
                    "id": "existing",
                    "description": "Existing",
                    "license": "proprietary",
                    "extent": {
                        "spatial": {"bbox": [[-180, -90, 180, 90]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                    "links": [],
                }
            )
        )

        # Create NEW collection directory with file
        # Note: collection name must NOT have dots (portolan rejects them)
        # The BUG is about catalog_root having dots, not collection names
        new_collection_dir = catalog_root / "new-collection"
        new_collection_dir.mkdir()
        new_collection_geojson = new_collection_dir / "data.geojson"
        new_collection_geojson.write_text(simple_geojson.read_text())

        # Add to NEW collection (triggers _update_catalog_links)
        result = runner.invoke(
            cli,
            [
                "add",
                str(new_collection_geojson),
                "--portolan-dir",
                str(catalog_root),
            ],
        )

        assert result.exit_code == 0, f"Failed: {result.output}"

        # catalog.json must still be in catalog_root
        assert (catalog_root / "catalog.json").exists(), (
            "catalog.json should remain in catalog root"
        )
        assert not (tmp_path / "catalog.json").exists(), "catalog.json must NOT leak to parent"

        # Verify catalog has new collection linked
        catalog_data = json.loads((catalog_root / "catalog.json").read_text())
        child_hrefs = [link["href"] for link in catalog_data["links"] if link["rel"] == "child"]
        assert "./new-collection/collection.json" in child_hrefs
