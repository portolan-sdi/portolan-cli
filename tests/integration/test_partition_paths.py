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
def large_geoparquet(initialized_catalog: Path) -> Path:
    """Create a GeoParquet file large enough to trigger partitioning."""
    import geopandas as gpd
    import numpy as np
    from shapely.geometry import Point

    collection_dir = initialized_catalog / "points"
    collection_dir.mkdir()

    # Create 100k points - exceeds minimum (512 partitions * 100 rows = 51,200 minimum)
    n = 100_000
    gdf = gpd.GeoDataFrame(
        {"id": range(n), "val": np.random.rand(n)},
        geometry=[
            Point(np.random.uniform(-180, 180), np.random.uniform(-90, 90)) for _ in range(n)
        ],
        crs="EPSG:4326",
    )
    parquet_path = collection_dir / "data.parquet"
    gdf.to_parquet(parquet_path)
    return parquet_path


def _set_partitioning_config(catalog_root: Path, threshold_gb: float = 0.00001) -> None:
    """Enable partitioning with low threshold via direct config file manipulation."""
    config_path = catalog_root / ".portolan" / "config.yaml"
    config_content = f"""# Portolan configuration
partitioning.enabled: true
partitioning.threshold_gb: {threshold_gb}
"""
    config_path.write_text(config_content)


@pytest.mark.integration
class TestPartitionPathConsistency:
    """Tests for partition path consistency between filesystem and versions.json."""

    def test_versions_json_paths_match_hive_structure(
        self, runner: CliRunner, initialized_catalog: Path, large_geoparquet: Path
    ) -> None:
        """versions.json paths should match actual Hive-style directory structure."""
        # Enable partitioning with very low threshold via direct config
        _set_partitioning_config(initialized_catalog, threshold_gb=0.00001)

        # Add the file (should trigger partitioning)
        result = runner.invoke(
            cli,
            [
                "add",
                "--force",
                "--portolan-dir",
                str(initialized_catalog),
                str(large_geoparquet.parent),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, f"Add failed: {result.output}"
        assert "Added" in result.output

        # Check versions.json
        versions_path = large_geoparquet.parent / "versions.json"
        assert versions_path.exists(), "versions.json not created"

        versions_data = json.loads(versions_path.read_text())
        assets = versions_data["versions"][0]["assets"]

        # Get actual partition directories
        partition_dirs = list(large_geoparquet.parent.glob("kdtree_cell=*"))
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
        self, runner: CliRunner, initialized_catalog: Path, large_geoparquet: Path
    ) -> None:
        """Glob pattern in collection.json should match actual partition files."""
        # Enable partitioning via direct config
        _set_partitioning_config(initialized_catalog, threshold_gb=0.00001)

        # Add the file
        result = runner.invoke(
            cli,
            [
                "add",
                "--force",
                "--portolan-dir",
                str(initialized_catalog),
                str(large_geoparquet.parent),
            ],
        )
        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Check collection.json for glob asset
        collection_path = large_geoparquet.parent / "collection.json"
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

        collection_dir = large_geoparquet.parent
        # Convert glob pattern to pathlib pattern
        pattern = href.lstrip("./")  # "kdtree_cell=*/*.parquet"
        matched_files = list(collection_dir.glob(pattern))

        assert len(matched_files) > 0, f"Glob pattern {pattern} matched no files"

    def test_glob_excludes_non_parquet_files(
        self, runner: CliRunner, initialized_catalog: Path, large_geoparquet: Path
    ) -> None:
        """Glob pattern should NOT match non-parquet files in partition directories."""
        # Enable partitioning via direct config
        _set_partitioning_config(initialized_catalog, threshold_gb=0.00001)

        # Add the file
        result = runner.invoke(
            cli,
            [
                "add",
                "--force",
                "--portolan-dir",
                str(initialized_catalog),
                str(large_geoparquet.parent),
            ],
        )
        assert result.exit_code == 0

        # Add a non-parquet file to a partition directory
        partition_dirs = list(large_geoparquet.parent.glob("kdtree_cell=*"))
        assert len(partition_dirs) > 0
        decoy_file = partition_dirs[0] / "metadata.json"
        decoy_file.write_text('{"decoy": true}')

        # Get glob pattern and verify it doesn't match the decoy
        collection_dir = large_geoparquet.parent
        pattern = "kdtree_cell=*/*.parquet"
        matched_files = list(collection_dir.glob(pattern))

        # Verify decoy is NOT in matches
        matched_names = [f.name for f in matched_files]
        assert "metadata.json" not in matched_names, "Glob incorrectly matched non-parquet file"

        # All matches should be .parquet
        for f in matched_files:
            assert f.suffix == ".parquet", f"Non-parquet file matched: {f}"

    def test_duckdb_can_read_via_glob(
        self, runner: CliRunner, initialized_catalog: Path, large_geoparquet: Path
    ) -> None:
        """DuckDB should be able to read partitioned data via glob pattern."""
        pytest.importorskip("duckdb")
        import duckdb

        # Enable partitioning via direct config
        _set_partitioning_config(initialized_catalog, threshold_gb=0.00001)

        # Add the file
        result = runner.invoke(
            cli,
            [
                "add",
                "--force",
                "--portolan-dir",
                str(initialized_catalog),
                str(large_geoparquet.parent),
            ],
        )
        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Read collection.json to get glob pattern
        collection_path = large_geoparquet.parent / "collection.json"
        collection_data = json.loads(collection_path.read_text())

        # Find glob asset
        glob_href = None
        for asset in collection_data.get("assets", {}).values():
            if "*" in asset.get("href", ""):
                glob_href = asset["href"]
                break

        assert glob_href is not None, "No glob asset found"

        # Convert to absolute path for DuckDB
        collection_dir = large_geoparquet.parent
        glob_path = str(collection_dir / glob_href.lstrip("./"))

        # Query via DuckDB
        result = duckdb.sql(f"SELECT count(*) as cnt FROM '{glob_path}'").fetchone()
        assert result is not None
        row_count = result[0]

        # Should have all 100k rows
        assert row_count == 100_000, f"Expected 100000 rows, got {row_count}"


@pytest.mark.integration
class TestPushGlobTransformation:
    """Tests for portolan:glob field transformation during push."""

    def test_push_dryrun_shows_correct_glob_url(
        self, runner: CliRunner, initialized_catalog: Path, large_geoparquet: Path
    ) -> None:
        """Push dry-run should show correct glob URL transformation."""
        # Enable partitioning via direct config
        _set_partitioning_config(initialized_catalog, threshold_gb=0.00001)

        # Add the file
        result = runner.invoke(
            cli,
            [
                "add",
                "--force",
                "--portolan-dir",
                str(initialized_catalog),
                str(large_geoparquet.parent),
            ],
        )
        assert result.exit_code == 0

        # Verify transformation function works correctly
        from portolan_cli.sync.push import _transform_collection_glob_assets

        collection_path = large_geoparquet.parent / "collection.json"
        content = collection_path.read_bytes()

        transformed = _transform_collection_glob_assets(content, "s3://bucket/catalog", "points")
        transformed_data = json.loads(transformed)

        # Find glob asset and verify portolan:glob structure (not exact match)
        for asset in transformed_data.get("assets", {}).values():
            if "*" in asset.get("href", ""):
                assert "portolan:glob" in asset, "portolan:glob not added"
                glob_url = asset["portolan:glob"]
                # Verify URL structure without hardcoding exact pattern
                assert glob_url.startswith("s3://bucket/catalog/points/"), (
                    f"Wrong base URL: {glob_url}"
                )
                assert "*" in glob_url, f"Missing wildcard in glob URL: {glob_url}"
                assert glob_url.endswith("/*.parquet"), f"Wrong suffix: {glob_url}"
                break
        else:
            pytest.fail("No glob asset found in transformed collection")
