"""Integration tests for the status command with real filesystem structures.

These tests exercise the full status pipeline — filesystem traversal, versions.json
parsing, and result aggregation — using realistic catalog layouts.

Per issue #137: the key scenario is detecting untracked files in uninitialized
    collections (directories with geo-assets but without collection.json).

Per ADR-0007: all logic is in the library layer; CLI is a thin wrapper.
Per ADR-0023: STAC structure at root, Portolan internals in .portolan/.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.status import get_catalog_status

# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────


def _write_catalog(root: Path, collection_ids: list[str] | None = None) -> None:
    """Write a minimal catalog.json."""
    links = [{"rel": "child", "href": f"./{c}/collection.json"} for c in (collection_ids or [])]
    (root / "catalog.json").write_text(
        json.dumps(
            {
                "type": "Catalog",
                "id": "integration-catalog",
                "stac_version": "1.0.0",
                "description": "Integration test catalog",
                "links": links,
            }
        )
    )


def _write_collection(col_dir: Path) -> None:
    """Write a minimal collection.json."""
    col_dir.mkdir(parents=True, exist_ok=True)
    (col_dir / "collection.json").write_text(
        json.dumps(
            {
                "type": "Collection",
                "id": col_dir.name,
                "stac_version": "1.0.0",
                "description": f"Collection {col_dir.name}",
                "license": "proprietary",
                "extent": {
                    "spatial": {"bbox": [[-180, -90, 180, 90]]},
                    "temporal": {"interval": [[None, None]]},
                },
                "links": [],
            }
        )
    )


def _write_versions_json(col_dir: Path, assets: dict[str, dict[str, object]]) -> None:
    """Write a versions.json at collection root."""
    (col_dir / "versions.json").write_text(
        json.dumps(
            {
                "spec_version": "1.0.0",
                "current_version": "1.0.0",
                "versions": [
                    {
                        "version": "1.0.0",
                        "created": "2024-01-15T10:30:00Z",
                        "breaking": False,
                        "assets": assets,
                        "changes": list(assets.keys()),
                    }
                ],
            }
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
# Integration tests: Bug #137 — uninitialized collection detection
# ─────────────────────────────────────────────────────────────────────────────


class TestUninitializedCollectionIntegration:
    """Integration tests for the #137 fix.

    These tests simulate realistic catalog layouts where users have placed
    geo-asset files in subdirectories but have not yet run `portolan add`.
    The status command must detect these as untracked rather than silently
    ignoring them.
    """

    @pytest.mark.integration
    def test_realistic_parquet_workflow_before_add(self, tmp_path: Path) -> None:
        """Simulates a user who dropped a GeoParquet file before running portolan add.

        Before #137 fix: status would show "clean" even though demographics/
        contained a parquet file — the chicken-and-egg problem.

        After fix: the file surfaces as untracked so the user knows to run
        portolan add.
        """
        _write_catalog(tmp_path)

        # User dropped files into the expected catalog location but hasn't run
        # portolan add yet — so no collection.json exists.
        col_dir = tmp_path / "demographics"
        col_dir.mkdir()
        item_dir = col_dir / "census-2020"
        item_dir.mkdir()
        (item_dir / "census_data.parquet").write_bytes(b"fake parquet data" * 100)

        result = get_catalog_status(tmp_path)

        assert not result.is_clean(), "Status should not be clean when geo-assets exist"
        assert len(result.untracked) == 1
        assert result.untracked[0].collection_id == "demographics"
        assert result.untracked[0].item_id == "census-2020"
        assert result.untracked[0].filename == "census_data.parquet"

    @pytest.mark.integration
    def test_realistic_geojson_workflow_before_add(self, tmp_path: Path) -> None:
        """User dropped a GeoJSON file before running portolan add."""
        _write_catalog(tmp_path)
        col_dir = tmp_path / "boundaries"
        col_dir.mkdir()
        item_dir = col_dir / "country-borders-2024"
        item_dir.mkdir()
        (item_dir / "borders.geojson").write_text(
            json.dumps({"type": "FeatureCollection", "features": []})
        )

        result = get_catalog_status(tmp_path)

        assert len(result.untracked) == 1
        assert result.untracked[0].collection_id == "boundaries"
        assert result.untracked[0].filename == "borders.geojson"

    @pytest.mark.integration
    def test_realistic_tiff_raster_before_add(self, tmp_path: Path) -> None:
        """User dropped a TIFF raster file before running portolan add."""
        _write_catalog(tmp_path)
        col_dir = tmp_path / "imagery"
        col_dir.mkdir()
        item_dir = col_dir / "landsat-scene-001"
        item_dir.mkdir()
        (item_dir / "scene.tif").write_bytes(b"\x00\x49\x49" + b"\x00" * 256)

        result = get_catalog_status(tmp_path)

        assert len(result.untracked) == 1
        assert result.untracked[0].collection_id == "imagery"
        assert result.untracked[0].filename == "scene.tif"

    @pytest.mark.integration
    def test_multiple_uninitialized_collections_all_detected(self, tmp_path: Path) -> None:
        """Multiple uninitialized collection directories are all detected."""
        _write_catalog(tmp_path)

        collections = {
            "vector-data": ("roads", "roads.geojson"),
            "raster-data": ("elevation", "dem.tif"),
            "parquet-data": ("population", "pop.parquet"),
        }

        for col_name, (item_name, filename) in collections.items():
            col_dir = tmp_path / col_name
            col_dir.mkdir()
            item_dir = col_dir / item_name
            item_dir.mkdir()
            if filename.endswith(".geojson"):
                (item_dir / filename).write_text('{"type":"FeatureCollection","features":[]}')
            else:
                (item_dir / filename).write_bytes(b"binary content")

        result = get_catalog_status(tmp_path)

        assert len(result.untracked) == 3
        collection_ids = {f.collection_id for f in result.untracked}
        assert collection_ids == {"vector-data", "raster-data", "parquet-data"}

    @pytest.mark.integration
    def test_mix_initialized_and_uninitialized_both_reported(self, tmp_path: Path) -> None:
        """Initialized and uninitialized collections both report untracked files."""
        _write_catalog(tmp_path, ["roads"])

        # Initialized collection (has collection.json)
        roads_dir = tmp_path / "roads"
        _write_collection(roads_dir)
        roads_item = roads_dir / "highway-2024"
        roads_item.mkdir()
        (roads_item / "highways.fgb").write_bytes(b"flatgeobuf content")

        # Uninitialized collection (no collection.json)
        elevation_dir = tmp_path / "elevation"
        elevation_dir.mkdir()
        elevation_item = elevation_dir / "dem-001"
        elevation_item.mkdir()
        (elevation_item / "dem.tif").write_bytes(b"\x00" * 128)

        result = get_catalog_status(tmp_path)

        collection_ids = {f.collection_id for f in result.untracked}
        assert "roads" in collection_ids, "Initialized collection should be reported"
        assert "elevation" in collection_ids, "Uninitialized collection should be reported"
        assert len(result.untracked) == 2

    @pytest.mark.integration
    def test_non_geo_directory_still_ignored(self, tmp_path: Path) -> None:
        """Directories containing only non-geo files are still ignored."""
        _write_catalog(tmp_path)

        # Directory with only text/doc files — not geo-assets
        docs_dir = tmp_path / "docs"
        docs_dir.mkdir()
        item_dir = docs_dir / "readme"
        item_dir.mkdir()
        (item_dir / "README.txt").write_text("documentation")
        (item_dir / "schema.sql").write_text("CREATE TABLE foo (id INTEGER);")

        result = get_catalog_status(tmp_path)
        assert result.is_clean(), "Non-geo directories should not appear in status"

    @pytest.mark.integration
    def test_empty_directory_still_ignored(self, tmp_path: Path) -> None:
        """Completely empty directories are ignored."""
        _write_catalog(tmp_path)
        (tmp_path / "empty-dir").mkdir()

        result = get_catalog_status(tmp_path)
        assert result.is_clean()

    @pytest.mark.integration
    def test_portolan_hidden_dirs_not_treated_as_collections(self, tmp_path: Path) -> None:
        """Hidden directories like .portolan are never treated as collections."""
        _write_catalog(tmp_path)

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("remote: s3://my-bucket")
        # Even if .portolan contains a .parquet file, it's hidden → ignored
        (portolan_dir / "state.parquet").write_bytes(b"internal state")

        result = get_catalog_status(tmp_path)
        assert result.is_clean()

    @pytest.mark.integration
    def test_flatgeobuf_and_pmtiles_detected_as_geo_assets(self, tmp_path: Path) -> None:
        """FlatGeobuf (.fgb) and PMTiles (.pmtiles) are recognized as geo-assets."""
        _write_catalog(tmp_path)

        col_dir = tmp_path / "cloud-native"
        col_dir.mkdir()
        item_dir = col_dir / "roads-item"
        item_dir.mkdir()
        (item_dir / "roads.fgb").write_bytes(b"flatgeobuf content")
        (item_dir / "roads.pmtiles").write_bytes(b"pmtiles content")

        result = get_catalog_status(tmp_path)

        filenames = {f.filename for f in result.untracked}
        assert "roads.fgb" in filenames
        assert "roads.pmtiles" in filenames

    @pytest.mark.integration
    def test_shapefile_detected_as_geo_asset_in_uninitialized_dir(self, tmp_path: Path) -> None:
        """Shapefiles (.shp) in uninitialized directories are detected as geo-assets."""
        _write_catalog(tmp_path)

        col_dir = tmp_path / "vector-legacy"
        col_dir.mkdir()
        item_dir = col_dir / "roads-shapefile"
        item_dir.mkdir()
        (item_dir / "roads.shp").write_bytes(b"shapefile content")
        (item_dir / "roads.dbf").write_bytes(b"dbf content")
        (item_dir / "roads.shx").write_bytes(b"shx content")

        result = get_catalog_status(tmp_path)

        filenames = {f.filename for f in result.untracked}
        assert "roads.shp" in filenames

    @pytest.mark.integration
    def test_gpkg_detected_as_geo_asset_in_uninitialized_dir(self, tmp_path: Path) -> None:
        """GeoPackage (.gpkg) in uninitialized directories is detected as a geo-asset."""
        _write_catalog(tmp_path)

        col_dir = tmp_path / "vector-gpkg"
        col_dir.mkdir()
        item_dir = col_dir / "roads-gpkg"
        item_dir.mkdir()
        (item_dir / "roads.gpkg").write_bytes(b"geopackage sqlite content")

        result = get_catalog_status(tmp_path)

        assert len(result.untracked) == 1
        assert result.untracked[0].filename == "roads.gpkg"


# ─────────────────────────────────────────────────────────────────────────────
# Integration tests: CLI output verification
# ─────────────────────────────────────────────────────────────────────────────


class TestStatusCLIIntegration:
    """Integration tests verifying CLI output for uninitialized collection scenarios."""

    @pytest.mark.integration
    def test_cli_reports_untracked_in_uninitialized_collection(self, tmp_path: Path) -> None:
        """CLI status command shows untracked files in uninitialized directories."""
        _write_catalog(tmp_path)
        col_dir = tmp_path / "demographics"
        col_dir.mkdir()
        item_dir = col_dir / "census-2020"
        item_dir.mkdir()
        (item_dir / "data.parquet").write_bytes(b"fake parquet data")

        runner = CliRunner()
        old_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            result = runner.invoke(cli, ["status"], catch_exceptions=False)
        finally:
            os.chdir(old_cwd)

        assert result.exit_code == 0
        assert "Untracked:" in result.output
        assert "demographics/census-2020/data.parquet" in result.output

    @pytest.mark.integration
    def test_cli_does_not_say_clean_when_geo_files_present(self, tmp_path: Path) -> None:
        """CLI must not say 'clean' when uninitialized collection has geo-assets.

        This is the core regression test for bug #137: before the fix,
        status would report 'Nothing to commit, working tree clean' even when
        geospatial files existed in uninitialized directories.
        """
        _write_catalog(tmp_path)
        col_dir = tmp_path / "imagery"
        col_dir.mkdir()
        item_dir = col_dir / "scene-001"
        item_dir.mkdir()
        (item_dir / "scene.tif").write_bytes(b"\x49\x49\x2a\x00" + b"\x00" * 128)

        runner = CliRunner()
        old_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            result = runner.invoke(cli, ["status"], catch_exceptions=False)
        finally:
            os.chdir(old_cwd)

        assert result.exit_code == 0
        # Must NOT say clean
        assert "Nothing to commit" not in result.output
        assert "working tree clean" not in result.output
        # Must show the untracked file
        assert "imagery/scene-001/scene.tif" in result.output

    @pytest.mark.integration
    def test_cli_clean_when_only_non_geo_files_in_subdirs(self, tmp_path: Path) -> None:
        """CLI reports clean when subdirectories contain only non-geo files."""
        _write_catalog(tmp_path)
        col_dir = tmp_path / "docs"
        col_dir.mkdir()
        item_dir = col_dir / "readme"
        item_dir.mkdir()
        (item_dir / "README.txt").write_text("not geospatial")

        runner = CliRunner()
        old_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            result = runner.invoke(cli, ["status"], catch_exceptions=False)
        finally:
            os.chdir(old_cwd)

        assert result.exit_code == 0
        assert "Nothing to commit" in result.output

    @pytest.mark.integration
    def test_sorted_output_across_collections(self, tmp_path: Path) -> None:
        """Status output is sorted consistently across collections."""
        _write_catalog(tmp_path)

        # Two uninitialized collections
        for col_name, item_name, filename in [
            ("zzz-last", "item-a", "data.parquet"),
            ("aaa-first", "item-b", "data.tif"),
        ]:
            col_dir = tmp_path / col_name
            col_dir.mkdir()
            item_dir = col_dir / item_name
            item_dir.mkdir()
            (item_dir / filename).write_bytes(b"content")

        result = get_catalog_status(tmp_path)

        # Results should be sorted by path
        paths = [f.path for f in result.untracked]
        assert paths == sorted(paths), f"Expected sorted paths but got: {paths}"
