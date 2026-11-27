"""Regression tests for pystac normalize_hrefs dot-in-path bug."""

from __future__ import annotations

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
        """Adding files to a catalog with dotted path should not leak to parent."""
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
        (portolan_dir / "metadata.yaml").write_text('license: "CC-BY-4.0"\n')

        # The collection name has no dots (portolan rejects them).
        # The defect is a dotted catalog_root, not a dotted collection name.
        collection_dir = catalog_root / "test-collection"
        collection_dir.mkdir()

        # Copy geojson directly into collection directory
        collection_geojson = collection_dir / "data.geojson"
        collection_geojson.write_text(simple_geojson.read_text(encoding="utf-8"))

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
        (portolan_dir / "metadata.yaml").write_text('license: "CC-BY-4.0"\n')

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

        # The collection name has no dots (portolan rejects them).
        # The defect is a dotted catalog_root, not a dotted collection name.
        new_collection_dir = catalog_root / "new-collection"
        new_collection_dir.mkdir()
        new_collection_geojson = new_collection_dir / "data.geojson"
        new_collection_geojson.write_text(simple_geojson.read_text(encoding="utf-8"))

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


class TestDefaultPathRegression:
    """Regression tests for `init` with no path argument."""

    @pytest.mark.integration
    def test_init_no_path_argument_in_dotted_working_directory(
        self, runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`portolan init` with no path writes catalog.json to the working directory."""
        working_dir = tmp_path / "tmp.ckdIXs70TT"
        working_dir.mkdir()
        monkeypatch.chdir(working_dir)

        result = runner.invoke(
            cli, ["init", "--auto", "--title", "Base test", "--license", "CC-BY-4.0"]
        )

        assert result.exit_code == 0, f"Failed: {result.output}"
        assert result.exception is None, f"Unhandled exception: {result.exception!r}"

        # Positive assertion: catalog.json lands in the working directory.
        assert (working_dir / "catalog.json").exists(), (
            "catalog.json should be in the working directory"
        )
        # Negative assertion: it must not leak to the parent.
        assert not (tmp_path / "catalog.json").exists(), (
            "catalog.json must NOT leak to the parent directory"
        )

        # The catalog the CLI reports must be the one on disk.
        catalog_data = json.loads((working_dir / "catalog.json").read_text())
        assert catalog_data["id"] == "tmp-ckdIXs70TT"
        assert catalog_data["title"] == "Base test"

    @pytest.mark.integration
    def test_init_no_path_reports_catalog_id_as_json(
        self, runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The JSON envelope holds for `init` with no path."""
        working_dir = tmp_path / "tmp.9Qz.Wm"
        working_dir.mkdir()
        monkeypatch.chdir(working_dir)

        result = runner.invoke(cli, ["init", "--auto", "--json", "--license", "CC-BY-4.0"])

        assert result.exit_code == 0, f"Failed: {result.output}"
        payload = json.loads(result.output)
        assert payload["success"] is True
        assert payload["command"] == "init"
        assert payload["data"]["catalog_id"] == "tmp-9Qz-Wm"
        assert payload["data"]["path"] == str(working_dir.resolve())


class TestInitCatalogReturnValue:
    """`init_catalog` must return a path its caller can read."""

    @pytest.mark.integration
    def test_returned_catalog_file_is_absolute(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The returned catalog_file resolves without depending on the caller's cwd."""
        from portolan_cli.catalog import init_catalog

        working_dir = tmp_path / "tmp.relative"
        working_dir.mkdir()
        monkeypatch.chdir(working_dir)

        catalog_file, _warnings = init_catalog(Path(), license_id="CC-BY-4.0")

        assert catalog_file.is_absolute(), f"catalog_file must be absolute, got {catalog_file}"
        assert catalog_file == working_dir.resolve() / "catalog.json"
        assert catalog_file.read_text(encoding="utf-8"), "returned path must be readable"
