"""Integration tests for arbitrary Hive partition support in portolan add (Issue #443).

Tests the full workflow:
1. Adding pre-existing Hive-partitioned data with arbitrary column names
2. Merge behavior preserving hand-authored partition metadata
3. Glob pattern generation for arbitrary partitions
4. Schema consistency validation across partitions
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


def create_partitioned_parquet(
    base_dir: Path, partition_col: str, partition_values: list[str]
) -> None:
    """Create a Hive-partitioned GeoParquet collection with consistent schema.

    Creates valid GeoParquet files with point geometries so they're recognized
    as vector data by Portolan.

    Args:
        base_dir: Base directory for the partitioned collection.
        partition_col: Name of the partition column.
        partition_values: List of partition values to create.
    """
    import geopandas as gpd
    from shapely.geometry import Point

    for i, val in enumerate(partition_values):
        partition_dir = base_dir / f"{partition_col}={val}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        # Create GeoDataFrame with point geometries
        gdf = gpd.GeoDataFrame(
            {
                "id": [i * 10 + j for j in range(5)],
                "name": [f"item_{i}_{j}" for j in range(5)],
                "value": [float(i * 10 + j) for j in range(5)],
            },
            geometry=[Point(j, i) for j in range(5)],
            crs="EPSG:4326",
        )
        gdf.to_parquet(partition_dir / "data.parquet")


def create_multilevel_partitioned_parquet(
    base_dir: Path,
    level1_col: str,
    level1_values: list[str],
    level2_col: str,
    level2_values: list[str],
) -> None:
    """Create a multi-level Hive-partitioned GeoParquet collection.

    Args:
        base_dir: Base directory for the partitioned collection.
        level1_col: Name of first partition level column.
        level1_values: Values for first partition level.
        level2_col: Name of second partition level column.
        level2_values: Values for second partition level.
    """
    import geopandas as gpd
    from shapely.geometry import Point

    for i, l1_val in enumerate(level1_values):
        for j, l2_val in enumerate(level2_values):
            partition_dir = base_dir / f"{level1_col}={l1_val}" / f"{level2_col}={l2_val}"
            partition_dir.mkdir(parents=True, exist_ok=True)

            # Create GeoDataFrame with point geometries
            gdf = gpd.GeoDataFrame(
                {"id": [1, 2], "measurement": [1.5, 2.5]},
                geometry=[Point(i, j), Point(i + 0.1, j + 0.1)],
                crs="EPSG:4326",
            )
            gdf.to_parquet(partition_dir / "data.parquet")


class TestAddArbitraryHivePartitions:
    """Tests for adding pre-existing Hive-partitioned data with arbitrary column names."""

    @pytest.mark.integration
    def test_add_detects_arbitrary_partition_column(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
    ) -> None:
        """portolan add detects arbitrary partition column names like gms_feature_id."""
        # Create partitions directly under the collection directory
        collection_dir = initialized_catalog / "sites"
        collection_dir.mkdir()

        # Create Hive-partitioned data with domain-meaningful column
        create_partitioned_parquet(
            collection_dir,
            partition_col="gms_feature_id",
            partition_values=["abc-123", "def-456", "ghi-789"],
        )

        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                "--force",
                str(collection_dir),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Verify partition metadata was detected
        collection_json = json.loads((collection_dir / "collection.json").read_text())
        assert collection_json.get("partition:scheme") == "hive"
        partition_keys = collection_json.get("partition:keys", [])
        key_names = [k["name"] for k in partition_keys]
        assert "gms_feature_id" in key_names

    @pytest.mark.integration
    def test_add_generates_glob_for_arbitrary_column(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
    ) -> None:
        """portolan add generates correct glob pattern for arbitrary partition columns."""
        collection_dir = initialized_catalog / "observations"
        collection_dir.mkdir()

        create_partitioned_parquet(
            collection_dir,
            partition_col="station_id",
            partition_values=["st001", "st002"],
        )

        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                "--force",
                str(collection_dir),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Check that glob asset has correct pattern
        collection_json = json.loads((collection_dir / "collection.json").read_text())
        assets = collection_json.get("assets", {})

        # Find the glob asset (should have a glob pattern in href)
        glob_assets = [a for a in assets.values() if "*" in a.get("href", "")]
        assert len(glob_assets) >= 1, "Should have at least one glob asset"

        glob_asset = glob_assets[0]
        assert "station_id=*" in glob_asset["href"]


class TestAddMultilevelHivePartitions:
    """Tests for multi-level Hive partition support."""

    @pytest.mark.integration
    def test_add_detects_multilevel_partitions(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
    ) -> None:
        """portolan add detects multi-level Hive partitions like year=*/month=*."""
        collection_dir = initialized_catalog / "timeseries"
        collection_dir.mkdir()

        create_multilevel_partitioned_parquet(
            collection_dir,
            level1_col="year",
            level1_values=["2023", "2024"],
            level2_col="month",
            level2_values=["01", "02", "03"],
        )

        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                "--force",
                str(collection_dir),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Verify both partition levels detected
        collection_json = json.loads((collection_dir / "collection.json").read_text())
        assert collection_json.get("partition:scheme") == "hive"

        partition_keys = collection_json.get("partition:keys", [])
        key_names = [k["name"] for k in partition_keys]
        assert "year" in key_names
        assert "month" in key_names

    @pytest.mark.integration
    def test_add_detects_multilevel_partition_metadata(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
    ) -> None:
        """portolan add detects multi-level partitions and adds metadata."""
        collection_dir = initialized_catalog / "sensors"
        collection_dir.mkdir()

        create_multilevel_partitioned_parquet(
            collection_dir,
            level1_col="region",
            level1_values=["north", "south"],
            level2_col="sensor_type",
            level2_values=["temp", "humidity"],
        )

        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                "--force",
                str(collection_dir),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Verify partition metadata was added
        collection_json = json.loads((collection_dir / "collection.json").read_text())
        assert collection_json.get("partition:scheme") == "hive"
        partition_keys = collection_json.get("partition:keys", [])
        key_names = [k["name"] for k in partition_keys]
        assert "region" in key_names
        assert "sensor_type" in key_names

    @pytest.mark.integration
    def test_add_generates_multilevel_glob(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
    ) -> None:
        """portolan add generates correct glob for multi-level partitions."""
        collection_dir = initialized_catalog / "sensors"
        collection_dir.mkdir()

        create_multilevel_partitioned_parquet(
            collection_dir,
            level1_col="region",
            level1_values=["north", "south"],
            level2_col="sensor_type",
            level2_values=["temp", "humidity"],
        )

        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                "--force",
                str(collection_dir),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, f"Add failed: {result.output}"

        collection_json = json.loads((collection_dir / "collection.json").read_text())
        assets = collection_json.get("assets", {})

        glob_assets = [a for a in assets.values() if "*" in a.get("href", "")]
        assert len(glob_assets) >= 1

        glob_href = glob_assets[0]["href"]
        assert "region=*" in glob_href
        assert "sensor_type=*" in glob_href


class TestMergePreservesPartitionMetadata:
    """Tests that merge strategy preserves hand-authored partition metadata."""

    @pytest.mark.integration
    def test_smart_merge_preserves_partition_description(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
    ) -> None:
        """Smart merge preserves hand-authored partition key descriptions."""
        collection_dir = initialized_catalog / "sites"
        collection_dir.mkdir()

        # Create partitioned data directly under collection
        create_partitioned_parquet(
            collection_dir,
            partition_col="site_id",
            partition_values=["a", "b"],
        )

        # First add
        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                "--force",
                str(collection_dir),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        # Hand-author partition description
        collection_json = json.loads((collection_dir / "collection.json").read_text())
        partition_keys = collection_json.get("partition:keys", [])
        for key in partition_keys:
            if key["name"] == "site_id":
                key["description"] = "Site UUID — matches site_feature_id in sites.parquet"
        (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

        # Add another partition
        create_partitioned_parquet(
            collection_dir,
            partition_col="site_id",
            partition_values=["a", "b", "c"],  # Added "c"
        )

        # Re-add with smart merge (default)
        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                "--force",
                str(collection_dir),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        # Verify description preserved
        collection_json = json.loads((collection_dir / "collection.json").read_text())
        partition_keys = collection_json.get("partition:keys", [])
        site_key = next((k for k in partition_keys if k["name"] == "site_id"), None)
        assert site_key is not None
        assert "Site UUID" in site_key.get("description", "")

    @pytest.mark.integration
    def test_smart_merge_preserves_glob_asset_metadata(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
    ) -> None:
        """Smart merge preserves hand-authored glob asset title and description."""
        collection_dir = initialized_catalog / "contours"
        collection_dir.mkdir()

        create_partitioned_parquet(
            collection_dir,
            partition_col="feature_id",
            partition_values=["001", "002"],
        )

        # First add
        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                "--force",
                str(collection_dir),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        # Hand-author asset metadata
        collection_json = json.loads((collection_dir / "collection.json").read_text())
        for _asset_key, asset in collection_json.get("assets", {}).items():
            if "*" in asset.get("href", ""):
                asset["title"] = "Contour Lines by Feature"
                asset["description"] = "Partitioned contour data for each feature polygon."
        (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

        # Re-add
        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                "--force",
                str(collection_dir),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        # Verify metadata preserved
        collection_json = json.loads((collection_dir / "collection.json").read_text())
        glob_assets = [
            a for a in collection_json.get("assets", {}).values() if "*" in a.get("href", "")
        ]
        assert len(glob_assets) >= 1
        assert glob_assets[0].get("title") == "Contour Lines by Feature"
        assert "Partitioned contour data" in glob_assets[0].get("description", "")


class TestSchemaConsistencyValidation:
    """Tests for schema consistency validation during add."""

    @pytest.mark.integration
    def test_add_warns_on_inconsistent_schemas(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
    ) -> None:
        """portolan add warns when partition schemas are inconsistent."""
        import geopandas as gpd
        from shapely.geometry import Point

        collection_dir = initialized_catalog / "mixed"
        collection_dir.mkdir()

        # Create partitions with inconsistent schemas (different columns)
        partition1 = collection_dir / "part=001"
        partition1.mkdir()
        gdf1 = gpd.GeoDataFrame(
            {"id": [1], "name": ["a"]},
            geometry=[Point(0, 0)],
            crs="EPSG:4326",
        )
        gdf1.to_parquet(partition1 / "data.parquet")

        partition2 = collection_dir / "part=002"
        partition2.mkdir()
        gdf2 = gpd.GeoDataFrame(
            {"id": [2], "extra_col": [1.5]},  # Different column!
            geometry=[Point(1, 1)],
            crs="EPSG:4326",
        )
        gdf2.to_parquet(partition2 / "data.parquet")

        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                "--force",
                str(collection_dir),
            ],
            catch_exceptions=False,
        )

        # Should succeed but with warning about schema inconsistency
        assert result.exit_code == 0
        # Check for warning in output (schema mismatch warning)
        # The warning should mention schema inconsistency
        assert "schema" in result.output.lower() or "inconsistent" in result.output.lower(), (
            f"Expected schema inconsistency warning in output, got: {result.output}"
        )

    @pytest.mark.integration
    def test_add_succeeds_with_consistent_schemas(
        self,
        runner: CliRunner,
        initialized_catalog: Path,
    ) -> None:
        """portolan add succeeds cleanly when all partition schemas match."""
        collection_dir = initialized_catalog / "consistent"
        collection_dir.mkdir()

        create_partitioned_parquet(
            collection_dir,
            partition_col="region",
            partition_values=["north", "south", "east", "west"],
        )

        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                "--force",
                str(collection_dir),
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0
        # Should not have schema warnings
        assert "inconsistent" not in result.output.lower()
