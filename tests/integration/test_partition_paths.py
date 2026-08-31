"""Integration tests for spatial partitioning path handling.

Per Issue #349/#399: Partitioned GeoParquet files should:
1. Use Hive-style directory names (kdtree_cell=XXXX/)
2. Have versions.json paths match actual filesystem structure
3. Have glob patterns that work with DuckDB/PyArrow

These tests verify the fix for the path mismatch bug where:
- versions.json recorded "data_XXXX/XXXX.parquet"
- But actual files were "kdtree_cell=XXXX/XXXX.parquet"
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


def _set_partitioning_config(catalog_root: Path, threshold_gb: float = 0.00001) -> None:
    """Enable partitioning with low threshold via direct config file manipulation."""
    config_path = catalog_root / ".portolan" / "config.yaml"
    config_content = f"""# Portolan configuration
partitioning.enabled: true
partitioning.threshold_gb: {threshold_gb}
"""
    config_path.write_text(config_content)


@pytest.fixture(scope="module")
def partitioned_catalog(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    """Initialize a catalog, write 100k points, and run one partitioning `add`.

    The build partitions the data into 512+ Hive directories and costs tens of
    seconds. Every test in this module reads the result and does not change it,
    so the fixture is module-scoped and the build runs one time. The CI runner
    uses `--dist loadscope`, which keeps the module on one xdist worker, so the
    fixture does not rebuild across workers.

    Returns (catalog_root, collection_dir).
    """
    import geopandas as gpd
    import numpy as np

    catalog_root = tmp_path_factory.mktemp("partition-catalog")
    result = CliRunner().invoke(
        cli, ["init", str(catalog_root), "--auto", "--license", "CC-BY-4.0"]
    )
    assert result.exit_code == 0, f"Init failed: {result.output}"

    collection_dir = catalog_root / "points"
    collection_dir.mkdir()

    # 100k points exceeds the partitioning minimum
    # (512 partitions * 100 rows = 51,200). Vectorized construction with
    # points_from_xy: a Python loop of shapely Point objects took tens of
    # seconds per build.
    n = 100_000
    rng = np.random.default_rng(42)
    gdf = gpd.GeoDataFrame(
        {"id": range(n), "val": rng.random(n)},
        geometry=gpd.points_from_xy(rng.uniform(-180, 180, n), rng.uniform(-90, 90, n)),
        crs="EPSG:4326",
    )
    gdf.to_parquet(collection_dir / "data.parquet")

    _set_partitioning_config(catalog_root)

    result = CliRunner().invoke(
        cli,
        ["add", "--force", "--portolan-dir", str(catalog_root), str(collection_dir)],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, f"Add failed: {result.output}"
    assert "Added" in result.output
    return catalog_root, collection_dir


@pytest.mark.integration
class TestPartitionPathConsistency:
    """Tests for partition path consistency between filesystem and versions.json."""

    def test_versions_json_paths_match_hive_structure(
        self, partitioned_catalog: tuple[Path, Path]
    ) -> None:
        """versions.json paths should match actual Hive-style directory structure."""
        _catalog_root, collection_dir = partitioned_catalog

        versions_path = collection_dir / "versions.json"
        assert versions_path.exists(), "versions.json not created"

        versions_data = json.loads(versions_path.read_text())
        assets = versions_data["versions"][0]["assets"]

        # Get actual partition directories
        partition_dirs = list(collection_dir.glob("kdtree_cell=*"))
        assert len(partition_dirs) > 0, "No partition directories created"

        # Verify each partition directory has a corresponding entry in versions.json
        for partition_dir in partition_dirs:
            dir_name = partition_dir.name  # e.g., "kdtree_cell=0000000000"
            parquet_files = list(partition_dir.glob("*.parquet"))
            assert len(parquet_files) == 1, f"Expected 1 parquet in {dir_name}"

            filename = parquet_files[0].name
            expected_key = f"{dir_name}/{filename}"

            assert expected_key in assets, (
                f"versions.json missing entry for {expected_key}. "
                f"Keys: {list(assets.keys())[:5]}..."
            )

    def test_glob_pattern_matches_actual_files(
        self, partitioned_catalog: tuple[Path, Path]
    ) -> None:
        """Glob pattern in collection.json should match actual partition files."""
        _catalog_root, collection_dir = partitioned_catalog

        # Check collection.json for glob asset
        collection_path = collection_dir / "collection.json"
        collection_data = json.loads(collection_path.read_text())

        # Find glob asset
        glob_asset = None
        for _key, asset in collection_data.get("assets", {}).items():
            if "*" in asset.get("href", ""):
                glob_asset = asset
                break

        assert glob_asset is not None, "No glob asset found in collection.json"

        # Extract glob pattern and verify structure (not exact match - allows strategy changes)
        href = glob_asset["href"]  # e.g., "./kdtree_cell=*/*.parquet"
        assert href.startswith("./"), f"Glob should be relative: {href}"
        assert "*" in href, f"Glob should contain wildcard: {href}"
        assert href.endswith("/*.parquet"), f"Glob should match parquet files: {href}"

        # Verify glob actually matches files
        pattern = href.lstrip("./")  # "kdtree_cell=*/*.parquet"
        matched_files = list(collection_dir.glob(pattern))

        assert len(matched_files) > 0, f"Glob pattern {pattern} matched no files"

    def test_glob_excludes_non_parquet_files(self, partitioned_catalog: tuple[Path, Path]) -> None:
        """Glob pattern should NOT match non-parquet files in partition directories."""
        _catalog_root, collection_dir = partitioned_catalog

        # Add a non-parquet file to a partition directory
        partition_dirs = list(collection_dir.glob("kdtree_cell=*"))
        assert len(partition_dirs) > 0
        decoy_file = partition_dirs[0] / "metadata.json"
        decoy_file.write_text('{"decoy": true}')

        try:
            # Get glob pattern and verify it doesn't match the decoy
            pattern = "kdtree_cell=*/*.parquet"
            matched_files = list(collection_dir.glob(pattern))

            # Verify decoy is NOT in matches
            matched_names = [f.name for f in matched_files]
            assert "metadata.json" not in matched_names, "Glob incorrectly matched non-parquet file"

            # All matches should be .parquet
            for f in matched_files:
                assert f.suffix == ".parquet", f"Non-parquet file matched: {f}"
        finally:
            # The catalog fixture is module-scoped and shared. Remove the decoy
            # so later tests see the exact `add` output.
            decoy_file.unlink()

    def test_duckdb_can_read_via_glob(self, partitioned_catalog: tuple[Path, Path]) -> None:
        """DuckDB should be able to read partitioned data via glob pattern."""
        pytest.importorskip("duckdb")
        import duckdb

        _catalog_root, collection_dir = partitioned_catalog

        # Read collection.json to get glob pattern
        collection_path = collection_dir / "collection.json"
        collection_data = json.loads(collection_path.read_text())

        # Find glob asset
        glob_href = None
        for asset in collection_data.get("assets", {}).values():
            if "*" in asset.get("href", ""):
                glob_href = asset["href"]
                break

        assert glob_href is not None, "No glob asset found"

        # Convert to absolute path for DuckDB
        glob_path = str(collection_dir / glob_href.lstrip("./"))

        # Query via DuckDB
        result = duckdb.sql(f"SELECT count(*) as cnt FROM '{glob_path}'").fetchone()
        assert result is not None
        row_count = result[0]

        # Should have all 100k rows
        assert row_count == 100_000, f"Expected 100000 rows, got {row_count}"


@pytest.mark.integration
class TestPushGlobTransformation:
    """Tests for partition:glob field transformation during push."""

    def test_push_dryrun_shows_correct_glob_url(
        self, partitioned_catalog: tuple[Path, Path]
    ) -> None:
        """Push dry-run should show correct glob URL transformation."""
        _catalog_root, collection_dir = partitioned_catalog

        # Verify transformation function works correctly
        from portolan_cli.sync.push import _transform_collection_glob_assets

        collection_path = collection_dir / "collection.json"
        content = collection_path.read_bytes()

        transformed = _transform_collection_glob_assets(content, "s3://bucket/catalog", "points")
        transformed_data = json.loads(transformed)

        # Find glob asset and verify partition:glob structure (not exact match)
        for asset in transformed_data.get("assets", {}).values():
            if "*" in asset.get("href", ""):
                assert "partition:glob" in asset, "partition:glob not added"
                assert "portolan:glob" not in asset, "the removed legacy field is back"
                glob_url = asset["partition:glob"]
                # Verify URL structure without hardcoding exact pattern
                assert glob_url.startswith("s3://bucket/catalog/points/"), (
                    f"Wrong base URL: {glob_url}"
                )
                assert "*" in glob_url, f"Missing wildcard in glob URL: {glob_url}"
                assert glob_url.endswith("/*.parquet"), f"Wrong suffix: {glob_url}"
                break
        else:
            pytest.fail("No glob asset found in transformed collection")
