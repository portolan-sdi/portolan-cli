"""Tests for arbitrary Hive partition column support (Issue #443).

Tests the ability to:
- Detect and handle arbitrary partition column names (not just kdtree/h3/s2/quadkey/a5)
- Generate glob patterns for arbitrary column names
- Support multi-level Hive partitions
- Validate schema consistency across partitions
- Configure partition columns via config.yaml
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


class TestArbitraryPartitionColumnDetection:
    """Tests for detecting arbitrary Hive partition column names."""

    @pytest.mark.unit
    def test_detect_partitioning_arbitrary_column_name(self, tmp_path: Path) -> None:
        """detect_partitioning works with arbitrary column names like gms_feature_id."""
        from portolan_cli.partitioning import detect_partitioning

        # Create Hive structure with domain-meaningful partition key
        (tmp_path / "gms_feature_id=00b3a7de-1234-5678-9abc-def012345678").mkdir()
        (
            tmp_path / "gms_feature_id=00b3a7de-1234-5678-9abc-def012345678" / "contours.parquet"
        ).write_bytes(b"x")
        (tmp_path / "gms_feature_id=020e386a-9876-5432-fedc-ba0987654321").mkdir()
        (
            tmp_path / "gms_feature_id=020e386a-9876-5432-fedc-ba0987654321" / "contours.parquet"
        ).write_bytes(b"x")

        result = detect_partitioning(tmp_path)

        assert result is not None
        assert result["partition:scheme"] == "hive"
        # Strategy should be omitted for unknown column names
        assert "partition:strategy" not in result
        assert result["partition:file_count"] == 2
        assert len(result["partition:keys"]) == 1
        assert result["partition:keys"][0]["name"] == "gms_feature_id"

    @pytest.mark.unit
    def test_detect_partitioning_preserves_column_order(self, tmp_path: Path) -> None:
        """detect_partitioning preserves detected column name exactly."""
        from portolan_cli.partitioning import detect_partitioning

        # Use a column name with underscores and numbers
        (tmp_path / "site_id_v2=abc123").mkdir()
        (tmp_path / "site_id_v2=abc123" / "data.parquet").write_bytes(b"x")

        result = detect_partitioning(tmp_path)

        assert result is not None
        assert result["partition:keys"][0]["name"] == "site_id_v2"


class TestMultiLevelHivePartitions:
    """Tests for multi-level Hive partition support."""

    @pytest.mark.unit
    def test_detect_multilevel_partitions(self, tmp_path: Path) -> None:
        """detect_partitioning detects multi-level Hive partitions like year=*/month=*."""
        from portolan_cli.partitioning import detect_partitioning

        # Create multi-level Hive structure
        (tmp_path / "year=2024" / "month=01").mkdir(parents=True)
        (tmp_path / "year=2024" / "month=01" / "data.parquet").write_bytes(b"x")
        (tmp_path / "year=2024" / "month=02").mkdir(parents=True)
        (tmp_path / "year=2024" / "month=02" / "data.parquet").write_bytes(b"x")
        (tmp_path / "year=2025" / "month=01").mkdir(parents=True)
        (tmp_path / "year=2025" / "month=01" / "data.parquet").write_bytes(b"x")

        result = detect_partitioning(tmp_path)

        assert result is not None
        assert result["partition:scheme"] == "hive"
        assert result["partition:file_count"] == 3
        # Should detect both partition levels
        key_names = [k["name"] for k in result["partition:keys"]]
        assert "year" in key_names
        assert "month" in key_names

    @pytest.mark.unit
    def test_build_glob_pattern_multilevel(self) -> None:
        """build_glob_pattern generates correct pattern for multi-level partitions."""
        from portolan_cli.partitioning import build_glob_pattern

        result = build_glob_pattern(partition_columns=["year", "month"])

        assert result == "./year=*/month=*/*.parquet"

    @pytest.mark.unit
    def test_build_glob_pattern_single_arbitrary_column(self) -> None:
        """build_glob_pattern works with single arbitrary column name."""
        from portolan_cli.partitioning import build_glob_pattern

        result = build_glob_pattern(partition_columns=["gms_feature_id"])

        assert result == "./gms_feature_id=*/*.parquet"

    @pytest.mark.unit
    def test_build_remote_glob_multilevel(self) -> None:
        """build_remote_glob works with multi-level partition columns."""
        from portolan_cli.partitioning import build_remote_glob

        result = build_remote_glob(
            remote_base="s3://bucket/catalog",
            collection_id="timeseries",
            partition_columns=["year", "month"],
        )

        assert result == "s3://bucket/catalog/timeseries/year=*/month=*/*.parquet"

    @pytest.mark.unit
    def test_build_remote_glob_arbitrary_column(self) -> None:
        """build_remote_glob works with arbitrary partition column name."""
        from portolan_cli.partitioning import build_remote_glob

        result = build_remote_glob(
            remote_base="s3://bucket/catalog",
            collection_id="sites",
            partition_columns=["gms_feature_id"],
        )

        assert result == "s3://bucket/catalog/sites/gms_feature_id=*/*.parquet"


class TestBackwardsCompatibility:
    """Tests ensuring backwards compatibility with strategy-based API."""

    @pytest.mark.unit
    def test_build_glob_pattern_strategy_still_works(self) -> None:
        """build_glob_pattern still works with strategy parameter for backwards compat."""
        from portolan_cli.partitioning import build_glob_pattern

        # Old API with strategy should still work
        result = build_glob_pattern(strategy="kdtree")

        assert result == "./kdtree_cell=*/*.parquet"

    @pytest.mark.unit
    def test_build_remote_glob_strategy_still_works(self) -> None:
        """build_remote_glob still works with strategy parameter for backwards compat."""
        from portolan_cli.partitioning import build_remote_glob

        result = build_remote_glob("s3://bucket/catalog", "buildings", strategy="kdtree")

        assert result == "s3://bucket/catalog/buildings/kdtree_cell=*/*.parquet"

    @pytest.mark.unit
    def test_partition_columns_takes_precedence_over_strategy(self) -> None:
        """When both partition_columns and strategy are provided, partition_columns wins."""
        from portolan_cli.partitioning import build_glob_pattern

        # partition_columns should take precedence
        result = build_glob_pattern(
            strategy="kdtree",
            partition_columns=["custom_col"],
        )

        assert result == "./custom_col=*/*.parquet"


class TestSchemaConsistencyValidation:
    """Tests for schema consistency validation across partitions."""

    @pytest.mark.unit
    def test_validate_partition_schemas_consistent(self, tmp_path: Path) -> None:
        """validate_partition_schemas passes when all partitions have same schema."""
        from portolan_cli.partitioning import validate_partition_schemas

        # Create partitions with identical schemas
        schema = pa.schema(
            [
                ("id", pa.int64()),
                ("name", pa.string()),
                ("value", pa.float64()),
            ]
        )

        for i, partition_id in enumerate(["001", "002", "003"]):
            partition_dir = tmp_path / f"partition_id={partition_id}"
            partition_dir.mkdir()
            table = pa.table({"id": [i], "name": [f"name{i}"], "value": [float(i)]}, schema=schema)
            pq.write_table(table, partition_dir / "data.parquet")

        # Should not raise
        result = validate_partition_schemas(tmp_path)
        assert result.is_consistent is True
        assert result.schema is not None
        assert len(result.schema) == 3

    @pytest.mark.unit
    def test_validate_partition_schemas_inconsistent_columns(self, tmp_path: Path) -> None:
        """validate_partition_schemas detects when partitions have different columns."""
        from portolan_cli.partitioning import validate_partition_schemas

        # Create partitions with different schemas
        partition1 = tmp_path / "id=001"
        partition1.mkdir()
        table1 = pa.table({"id": [1], "name": ["a"]})
        pq.write_table(table1, partition1 / "data.parquet")

        partition2 = tmp_path / "id=002"
        partition2.mkdir()
        table2 = pa.table({"id": [2], "extra_col": ["b"]})  # Different column!
        pq.write_table(table2, partition2 / "data.parquet")

        result = validate_partition_schemas(tmp_path)
        assert result.is_consistent is False
        assert "extra_col" in result.error_message or "name" in result.error_message

    @pytest.mark.unit
    def test_validate_partition_schemas_inconsistent_types(self, tmp_path: Path) -> None:
        """validate_partition_schemas detects when partitions have different column types."""
        from portolan_cli.partitioning import validate_partition_schemas

        partition1 = tmp_path / "id=001"
        partition1.mkdir()
        table1 = pa.table({"value": pa.array([1, 2, 3], type=pa.int64())})
        pq.write_table(table1, partition1 / "data.parquet")

        partition2 = tmp_path / "id=002"
        partition2.mkdir()
        table2 = pa.table({"value": pa.array(["a", "b", "c"], type=pa.string())})  # Different type!
        pq.write_table(table2, partition2 / "data.parquet")

        result = validate_partition_schemas(tmp_path)
        assert result.is_consistent is False
        assert "value" in result.error_message

    @pytest.mark.unit
    def test_validate_partition_schemas_returns_unified_schema(self, tmp_path: Path) -> None:
        """validate_partition_schemas returns the unified schema when consistent."""
        from portolan_cli.partitioning import validate_partition_schemas

        schema = pa.schema(
            [
                ("geometry", pa.binary()),
                ("name", pa.string()),
            ]
        )

        for partition_id in ["a", "b"]:
            partition_dir = tmp_path / f"site={partition_id}"
            partition_dir.mkdir()
            table = pa.table({"geometry": [b"wkb"], "name": [partition_id]}, schema=schema)
            pq.write_table(table, partition_dir / "data.parquet")

        result = validate_partition_schemas(tmp_path)
        assert result.is_consistent is True
        assert result.schema is not None
        field_names = [f.name for f in result.schema]
        assert "geometry" in field_names
        assert "name" in field_names


class TestPartitionConfigSupport:
    """Tests for config-based partition column specification."""

    @pytest.mark.unit
    def test_config_partition_columns_setting(self, tmp_path: Path) -> None:
        """Config supports partitioning.columns setting for custom column names."""
        from portolan_cli.config import DEFAULT_SETTINGS, KNOWN_SETTINGS

        # Verify the setting is registered
        assert "partitioning.columns" in KNOWN_SETTINGS

        # Default should be None (auto-detect)
        assert DEFAULT_SETTINGS.get("partitioning.columns") is None

    @pytest.mark.unit
    def test_config_partition_description_setting(self, tmp_path: Path) -> None:
        """Config supports partitioning.description for semantic documentation."""
        from portolan_cli.config import KNOWN_SETTINGS

        assert "partitioning.description" in KNOWN_SETTINGS

    @pytest.mark.unit
    def test_get_partition_metadata_uses_config_columns(self, tmp_path: Path) -> None:
        """get_partition_metadata uses config-specified columns when provided."""
        from portolan_cli.partitioning import get_partition_metadata

        # Create Hive structure
        (tmp_path / "gms_feature_id=abc").mkdir()
        (tmp_path / "gms_feature_id=abc" / "data.parquet").write_bytes(b"x")

        # Pass explicit partition columns (simulating config-provided value)
        result = get_partition_metadata(
            tmp_path,
            partition_columns=["gms_feature_id"],
            description="Site UUID — matches gms_site_feature_id in aois.parquet",
        )

        assert result["partition:keys"][0]["name"] == "gms_feature_id"
        assert "Site UUID" in result["partition:keys"][0]["description"]

    @pytest.mark.unit
    def test_get_partition_metadata_auto_detects_without_config(self, tmp_path: Path) -> None:
        """get_partition_metadata auto-detects columns when not specified in config."""
        from portolan_cli.partitioning import get_partition_metadata

        (tmp_path / "custom_key=value1").mkdir()
        (tmp_path / "custom_key=value1" / "data.parquet").write_bytes(b"x")

        # No partition_columns specified — should auto-detect
        result = get_partition_metadata(tmp_path)

        assert result["partition:keys"][0]["name"] == "custom_key"


class TestPartitionMetadataDescription:
    """Tests for free-text partition column descriptions."""

    @pytest.mark.unit
    def test_partition_key_includes_description(self, tmp_path: Path) -> None:
        """Partition key metadata includes description field."""
        from portolan_cli.partitioning import get_partition_metadata

        (tmp_path / "site_id=abc").mkdir()
        (tmp_path / "site_id=abc" / "data.parquet").write_bytes(b"x")

        result = get_partition_metadata(
            tmp_path,
            partition_columns=["site_id"],
            description="Unique site identifier from the sites registry",
        )

        key_def = result["partition:keys"][0]
        assert key_def["name"] == "site_id"
        assert key_def["description"] == "Unique site identifier from the sites registry"

    @pytest.mark.unit
    def test_multilevel_partition_descriptions(self, tmp_path: Path) -> None:
        """Multi-level partitions can have per-column descriptions."""
        from portolan_cli.partitioning import get_partition_metadata

        (tmp_path / "year=2024" / "region=west").mkdir(parents=True)
        (tmp_path / "year=2024" / "region=west" / "data.parquet").write_bytes(b"x")

        result = get_partition_metadata(
            tmp_path,
            partition_columns=["year", "region"],
            descriptions={
                "year": "Calendar year of observation",
                "region": "Geographic region code",
            },
        )

        keys_by_name = {k["name"]: k for k in result["partition:keys"]}
        assert keys_by_name["year"]["description"] == "Calendar year of observation"
        assert keys_by_name["region"]["description"] == "Geographic region code"


class TestGlobTransformArbitraryColumns:
    """Tests for glob transformation with arbitrary column names."""

    @pytest.mark.unit
    def test_transform_glob_assets_arbitrary_column(self) -> None:
        """_transform_collection_glob_assets handles arbitrary column names in href."""
        import json

        from portolan_cli.push import _transform_collection_glob_assets

        collection_json = {
            "type": "Collection",
            "id": "sites",
            "assets": {
                "contours": {
                    "href": "./gms_feature_id=*/contours.parquet",
                    "type": "application/vnd.apache.parquet",
                },
            },
        }

        content = json.dumps(collection_json).encode("utf-8")
        result = _transform_collection_glob_assets(content, "s3://bucket/catalog", "sites")
        result_json = json.loads(result)

        asset = result_json["assets"]["contours"]
        expected_glob = "s3://bucket/catalog/sites/gms_feature_id=*/contours.parquet"

        assert asset["partition:glob"] == expected_glob
        assert asset["portolan:glob"] == expected_glob

    @pytest.mark.unit
    def test_transform_glob_assets_multilevel(self) -> None:
        """_transform_collection_glob_assets handles multi-level partition paths."""
        import json

        from portolan_cli.push import _transform_collection_glob_assets

        collection_json = {
            "type": "Collection",
            "id": "timeseries",
            "assets": {
                "data": {
                    "href": "./year=*/month=*/data.parquet",
                    "type": "application/vnd.apache.parquet",
                },
            },
        }

        content = json.dumps(collection_json).encode("utf-8")
        result = _transform_collection_glob_assets(content, "gs://bucket/catalog", "timeseries")
        result_json = json.loads(result)

        asset = result_json["assets"]["data"]
        expected_glob = "gs://bucket/catalog/timeseries/year=*/month=*/data.parquet"

        assert asset["partition:glob"] == expected_glob
