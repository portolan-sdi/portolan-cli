"""Reproduction test for issue #388: CRS change not detected after reprojection.

This test verifies that when a GeoParquet file is reprojected externally,
`portolan add` detects the change and updates the collection metadata.

See: https://github.com/portolan-sdi/portolan-cli/issues/388
"""

from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pytest
from click.testing import CliRunner
from shapely.geometry import Point

from portolan_cli.cli import cli
from portolan_cli.dataset import add_files, is_current


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI test runner."""
    return CliRunner()


@pytest.fixture
def catalog_with_3857_geoparquet(tmp_path: Path) -> tuple[Path, Path, Path]:
    """Create a catalog with a GeoParquet file in EPSG:3857.

    Uses collection-level asset (file directly in collection dir) per ADR-0031.

    Returns:
        Tuple of (catalog_root, collection_dir, geoparquet_path).
    """
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()

    # Initialize catalog via CLI
    result = CliRunner().invoke(cli, ["init", str(catalog_root), "--auto"])
    assert result.exit_code == 0, f"Init failed: {result.output}"

    # Create collection directory (file goes directly here, no item subdir)
    collection_dir = catalog_root / "test-collection"
    collection_dir.mkdir(parents=True)

    # Create a simple GeoDataFrame in EPSG:3857 (Web Mercator)
    gdf = gpd.GeoDataFrame(
        {"name": ["point1", "point2"]},
        geometry=[
            Point(-8238310, 4970072),  # ~NYC in EPSG:3857
            Point(-8237000, 4971000),
        ],
        crs="EPSG:3857",
    )

    # Save as GeoParquet directly in collection (collection-level asset)
    parquet_path = collection_dir / "data.parquet"
    gdf.to_parquet(parquet_path)

    return catalog_root, collection_dir, parquet_path


class TestCRSChangeDetection:
    """Tests for issue #388: CRS change detection after reprojection."""

    @pytest.mark.integration
    def test_is_current_detects_reprojected_file(
        self, catalog_with_3857_geoparquet: tuple[Path, Path, Path]
    ) -> None:
        """Verify is_current() returns False for a reprojected file.

        This tests the detection layer: after reprojection, the file
        should be detected as changed (not current).
        """
        catalog_root, collection_dir, parquet_path = catalog_with_3857_geoparquet

        # Add the file initially
        added, skipped, failures = add_files(
            paths=[parquet_path],
            catalog_root=catalog_root,
        )
        assert len(added) == 1, f"Expected 1 added, got {len(added)}"
        assert len(failures) == 0, f"Unexpected failures: {failures}"

        # Verify file is now "current" (tracked)
        versions_path = collection_dir / "versions.json"
        assert versions_path.exists(), f"versions.json should exist at {versions_path}"
        assert is_current(parquet_path, versions_path), "File should be current after add"

        # Simulate external reprojection: read, reproject, overwrite
        gdf = gpd.read_parquet(parquet_path)
        assert gdf.crs.to_epsg() == 3857, f"Expected EPSG:3857, got {gdf.crs}"

        # Reproject to EPSG:4326
        gdf_4326 = gdf.to_crs("EPSG:4326")
        assert gdf_4326.crs.to_epsg() == 4326

        # Overwrite the original file (simulating gpio convert + mv)
        gdf_4326.to_parquet(parquet_path)

        # Verify CRS actually changed in file
        gdf_check = gpd.read_parquet(parquet_path)
        assert gdf_check.crs.to_epsg() == 4326, "File should now be EPSG:4326"

        # THE KEY TEST: is_current() should return False (file changed)
        assert not is_current(parquet_path, versions_path), (
            "is_current() should detect reprojected file as changed"
        )

    @pytest.mark.integration
    def test_readd_reprocesses_reprojected_file(
        self, catalog_with_3857_geoparquet: tuple[Path, Path, Path]
    ) -> None:
        """Verify re-adding a reprojected file reprocesses it (not skipped).

        This is the core fix for issue #388: after external reprojection,
        `portolan add` must detect the change and reprocess the file.

        Note: CRS propagation to collection.json is a separate concern.
        GeoParquetMetadata.to_stac_properties() currently doesn't include CRS,
        so proj:epsg/proj:code won't appear in collection.json for collection-level
        GeoParquet assets. That's a potential enhancement for a future issue.
        """
        catalog_root, collection_dir, parquet_path = catalog_with_3857_geoparquet

        # Add the collection-level file
        added, skipped, failures = add_files(
            paths=[parquet_path],
            catalog_root=catalog_root,
        )
        assert len(added) == 1

        # Reproject the file
        gdf = gpd.read_parquet(parquet_path)
        gdf_4326 = gdf.to_crs("EPSG:4326")
        gdf_4326.to_parquet(parquet_path)

        # Re-add
        added2, skipped2, failures2 = add_files(
            paths=[parquet_path],
            catalog_root=catalog_root,
        )

        # THE KEY TEST: file should NOT be skipped (issue #388 fix)
        assert len(skipped2) == 0, (
            f"File should NOT be skipped after reprojection, but was: {skipped2}"
        )
        assert len(added2) == 1, f"Expected 1 added, got {len(added2)}"
        assert len(failures2) == 0, f"Unexpected failures: {failures2}"

    @pytest.mark.integration
    def test_versions_json_tracks_reprojection(
        self, catalog_with_3857_geoparquet: tuple[Path, Path, Path]
    ) -> None:
        """Verify versions.json sha256 changes after reprojection.

        This confirms the detection mechanism: sha256 should differ for
        the reprojected file, triggering reprocessing on re-add.
        """
        catalog_root, collection_dir, parquet_path = catalog_with_3857_geoparquet

        # Add the file
        add_files(paths=[parquet_path], catalog_root=catalog_root)

        # Get initial sha256 from versions.json
        versions_path = collection_dir / "versions.json"
        versions_data = json.loads(versions_path.read_text())
        initial_assets = versions_data["versions"][-1]["assets"]
        initial_sha256 = initial_assets.get("data.parquet", {}).get("sha256")

        print(f"Initial sha256: {initial_sha256}")
        assert initial_sha256 is not None, "Asset should have sha256"

        # Reproject the file
        gdf = gpd.read_parquet(parquet_path)
        gdf_4326 = gdf.to_crs("EPSG:4326")
        gdf_4326.to_parquet(parquet_path)

        # Re-add
        add_files(paths=[parquet_path], catalog_root=catalog_root)

        # Get final sha256
        versions_data_after = json.loads(versions_path.read_text())
        final_assets = versions_data_after["versions"][-1]["assets"]
        final_sha256 = final_assets.get("data.parquet", {}).get("sha256")

        print(f"Final sha256: {final_sha256}")

        # sha256 MUST differ for reprojected file
        assert final_sha256 != initial_sha256, (
            "sha256 should change after reprojection — file content is different"
        )
