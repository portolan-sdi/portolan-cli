"""Tests for generic Hive partition detection in collection ID inference."""

from __future__ import annotations

from typing import TYPE_CHECKING

from portolan_cli.scan.core import _infer_collection_id_from_relative_path
from portolan_cli.scan.detect import is_hive_partition_dir

if TYPE_CHECKING:
    from pathlib import Path


class TestIsHivePartitionDir:
    """Tests for Hive partition directory detection."""

    def test_standard_partition_key(self) -> None:
        """Standard partition keys like year, month are detected."""
        assert is_hive_partition_dir("year=2024") == ("year", "2024")
        assert is_hive_partition_dir("month=01") == ("month", "01")

    def test_custom_partition_key(self) -> None:
        """Custom partition keys like gms_feature_id are detected."""
        result = is_hive_partition_dir("gms_feature_id=abc123")
        assert result == ("gms_feature_id", "abc123")

    def test_uuid_values(self) -> None:
        """UUID values in partitions are handled correctly."""
        result = is_hive_partition_dir("gms_feature_id=550e8400-e29b-41d4-a716-446655440000")
        assert result == ("gms_feature_id", "550e8400-e29b-41d4-a716-446655440000")

    def test_spatial_partition_keys(self) -> None:
        """Spatial partition keys (kdtree, h3, etc.) are detected."""
        assert is_hive_partition_dir("kdtree_cell=42") == ("kdtree_cell", "42")
        assert is_hive_partition_dir("h3_cell=8928308280fffff") == (
            "h3_cell",
            "8928308280fffff",
        )
        assert is_hive_partition_dir("s2_cell=89c25a31") == ("s2_cell", "89c25a31")

    def test_non_partition_directories(self) -> None:
        """Regular directories are not detected as partitions."""
        assert is_hive_partition_dir("sites") is None
        assert is_hive_partition_dir("contours") is None
        assert is_hive_partition_dir("data-2024") is None

    def test_invalid_key_format(self) -> None:
        """Keys must start with letter or underscore."""
        # Keys starting with numbers are invalid identifiers
        assert is_hive_partition_dir("2024=data") is None
        # Empty key or value
        assert is_hive_partition_dir("=value") is None
        assert is_hive_partition_dir("key=") is None


class TestInferCollectionIdFromRelativePath:
    """Tests for collection ID inference with Hive partition stripping."""

    def test_simple_collection(self) -> None:
        """Simple collection without partitions."""
        assert _infer_collection_id_from_relative_path("collection/data.parquet") == "collection"

    def test_nested_collection(self) -> None:
        """Nested collection path without partitions."""
        assert (
            _infer_collection_id_from_relative_path("climate/hittekaart/data.parquet")
            == "climate/hittekaart"
        )

    def test_root_level_file(self) -> None:
        """File at root level has empty collection ID."""
        assert _infer_collection_id_from_relative_path("data.parquet") == ""

    def test_strips_single_hive_partition(self) -> None:
        result = _infer_collection_id_from_relative_path(
            "sites/contours/gms_feature_id=abc123/contours.parquet"
        )
        assert result == "sites/contours"

    def test_strips_uuid_partition(self) -> None:
        """UUID-valued partitions are stripped."""
        result = _infer_collection_id_from_relative_path(
            "sites/contours/gms_feature_id=550e8400-e29b-41d4-a716-446655440000/data.parquet"
        )
        assert result == "sites/contours"

    def test_strips_spatial_partition(self) -> None:
        """Spatial partitions (kdtree, h3) are stripped."""
        assert (
            _infer_collection_id_from_relative_path(
                "demographics/census/kdtree_cell=42/data.parquet"
            )
            == "demographics/census"
        )
        assert (
            _infer_collection_id_from_relative_path(
                "climate/temperature/h3_cell=8928308280fffff/data.parquet"
            )
            == "climate/temperature"
        )

    def test_strips_multiple_partition_levels(self) -> None:
        """Multiple nested partitions are all stripped.

        Example: year=2024/month=01/data.parquet under collection/
        """
        result = _infer_collection_id_from_relative_path(
            "timeseries/weather/year=2024/month=01/data.parquet"
        )
        assert result == "timeseries/weather"

    def test_partition_at_root_level(self) -> None:
        """Partition directory directly under root."""
        # If file is at root with only partition dirs, collection ID is empty
        result = _infer_collection_id_from_relative_path("kdtree_cell=42/data.parquet")
        assert result == ""

    def test_preserves_non_partition_dirs_after_partition(self) -> None:
        # collection/partition=val/subdir/file.parquet
        # → "collection/subdir" (partition stripped, subdir preserved)
        result = _infer_collection_id_from_relative_path("collection/year=2024/subdir/data.parquet")
        # After stripping year=2024, we get "collection/subdir"
        assert result == "collection/subdir"

    def test_deep_nesting_with_partition(self) -> None:
        """Deep nesting with partition at various levels."""
        # env/air/quality/region=north/pm25.parquet → "env/air/quality"
        result = _infer_collection_id_from_relative_path(
            "env/air/quality/region=north/pm25.parquet"
        )
        assert result == "env/air/quality"

    def test_empty_segments_from_double_slash(self) -> None:
        # foo//bar/data.parquet → split gives ["foo", "", "bar"]
        # Empty segments are filtered out to produce clean collection IDs
        result = _infer_collection_id_from_relative_path("foo//bar/data.parquet")
        assert result == "foo/bar"  # Empty segment filtered out

    def test_false_positive_legitimate_equals_directory(self) -> None:
        # This IS stripped because it matches key=value pattern
        result = _infer_collection_id_from_relative_path("project/version=2.0/data.parquet")
        assert result == "project"  # version=2.0 stripped!

        # More examples of "false positive" stripping
        assert (
            _infer_collection_id_from_relative_path("data/config=production/settings.json")
            == "data"
        )

        assert (
            _infer_collection_id_from_relative_path("buildings/type=residential/parcels.parquet")
            == "buildings"
        )


class TestCollectionIdValidationIntegration:
    """Integration tests ensuring Hive partitions don't trigger validation errors."""

    def test_partition_key_not_in_collection_id(self) -> None:
        from portolan_cli.collection_id import validate_collection_id

        # Infer collection ID from a path with Hive partition
        collection_id = _infer_collection_id_from_relative_path(
            "sites/contours/gms_feature_id=abc123/data.parquet"
        )

        # The collection ID should be valid (no '=' character)
        is_valid, error = validate_collection_id(collection_id)
        assert is_valid, f"Collection ID '{collection_id}' should be valid: {error}"
        assert "=" not in collection_id


class TestDeriveItemIdHivePartitions:
    """Tests for _derive_item_id_and_asset_level with Hive partitions."""

    def test_single_level_hive_partition_uses_parent_name(self, tmp_path: Path) -> None:
        """Single-level Hive partitions use parent directory name as item_id.

        This is the common case for kdtree partitioning and should work as before.
        """
        from portolan_cli.add import _derive_item_id_and_asset_level

        collection_dir = tmp_path / "collection"
        collection_dir.mkdir()
        partition_dir = collection_dir / "kdtree_cell=0000000001"
        partition_dir.mkdir(parents=True)
        parquet_file = partition_dir / "0000000001.parquet"
        parquet_file.write_bytes(b"x")

        item_id, is_collection_level = _derive_item_id_and_asset_level(
            path=parquet_file,
            collection_dir=collection_dir,
            item_id=None,
        )

        # Single-level partition: use parent dir name, NOT collection-level
        assert is_collection_level is False
        assert item_id == "kdtree_cell=0000000001"

    def test_multi_level_hive_partition_has_unique_item_id(self, tmp_path: Path) -> None:
        from portolan_cli.add import _derive_item_id_and_asset_level

        collection_dir = tmp_path / "collection"
        collection_dir.mkdir()

        # Create two files in different partition branches with same leaf dir name
        partition1 = collection_dir / "year=2023" / "month=01"
        partition2 = collection_dir / "year=2024" / "month=01"
        partition1.mkdir(parents=True)
        partition2.mkdir(parents=True)

        file1 = partition1 / "data.parquet"
        file2 = partition2 / "data.parquet"
        file1.write_bytes(b"x")
        file2.write_bytes(b"x")

        item_id_1, is_coll_1 = _derive_item_id_and_asset_level(
            path=file1,
            collection_dir=collection_dir,
            item_id=None,
        )

        item_id_2, is_coll_2 = _derive_item_id_and_asset_level(
            path=file2,
            collection_dir=collection_dir,
            item_id=None,
        )

        # Both should be item-level (not collection-level)
        assert is_coll_1 is False
        assert is_coll_2 is False

        # Item IDs MUST be different (this was the bug)
        assert item_id_1 != item_id_2

        # Item IDs should be full relative paths joined with underscore
        assert item_id_1 == "year=2023_month=01"
        assert item_id_2 == "year=2024_month=01"

    def test_single_level_partition_unique_per_cell(self, tmp_path: Path) -> None:
        """Single-level partitions have unique item_ids per partition cell."""
        from portolan_cli.add import _derive_item_id_and_asset_level

        collection_dir = tmp_path / "collection"
        collection_dir.mkdir()

        partition1 = collection_dir / "kdtree_cell=001"
        partition2 = collection_dir / "kdtree_cell=002"
        partition1.mkdir(parents=True)
        partition2.mkdir(parents=True)

        file1 = partition1 / "data.parquet"
        file2 = partition2 / "data.parquet"
        file1.write_bytes(b"x")
        file2.write_bytes(b"x")

        item_id_1, _ = _derive_item_id_and_asset_level(
            path=file1, collection_dir=collection_dir, item_id=None
        )
        item_id_2, _ = _derive_item_id_and_asset_level(
            path=file2, collection_dir=collection_dir, item_id=None
        )

        # Each partition cell has unique item_id
        assert item_id_1 == "kdtree_cell=001"
        assert item_id_2 == "kdtree_cell=002"
        assert item_id_1 != item_id_2

    def test_no_format_type_single_level(self, tmp_path: Path) -> None:
        """Single-level Hive partition without format_type uses parent dir name."""
        from portolan_cli.add import _derive_item_id_and_asset_level

        collection_dir = tmp_path / "collection"
        partition_dir = collection_dir / "region=north"
        partition_dir.mkdir(parents=True)
        data_file = partition_dir / "measurements.csv"
        data_file.write_bytes(b"x")

        item_id, is_collection_level = _derive_item_id_and_asset_level(
            path=data_file,
            collection_dir=collection_dir,
            item_id=None,
        )

        # Single-level partition: uses parent dir name
        assert is_collection_level is False
        assert item_id == "region=north"  # parent dir name for single-level

    def test_explicit_item_id_overrides_hive_logic(self, tmp_path: Path) -> None:
        """Explicit item_id takes precedence over Hive partition detection."""
        from portolan_cli.add import _derive_item_id_and_asset_level

        collection_dir = tmp_path / "collection"
        partition_dir = collection_dir / "year=2023"
        partition_dir.mkdir(parents=True)
        data_file = partition_dir / "data.parquet"
        data_file.write_bytes(b"x")

        item_id, is_collection_level = _derive_item_id_and_asset_level(
            path=data_file,
            collection_dir=collection_dir,
            item_id="explicit-id",  # User-provided
        )

        # Explicit ID used regardless of Hive detection
        assert item_id == "explicit-id"
        assert is_collection_level is False  # Explicit = item-level

    def test_non_hive_path_unchanged(self, tmp_path: Path) -> None:
        """Paths without Hive partitions work as before."""
        from portolan_cli.add import _derive_item_id_and_asset_level

        collection_dir = tmp_path / "collection"
        item_dir = collection_dir / "my_item"
        item_dir.mkdir(parents=True)
        data_file = item_dir / "data.parquet"
        data_file.write_bytes(b"x")

        item_id, is_collection_level = _derive_item_id_and_asset_level(
            path=data_file,
            collection_dir=collection_dir,
            item_id=None,
        )

        # Regular directory structure unchanged
        assert item_id == "my_item"  # parent dir name
        assert is_collection_level is False
