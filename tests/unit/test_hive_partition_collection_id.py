"""Tests for generic Hive partition detection in collection ID inference.

Issue #448: Hive partition detection uses hard-coded column allowlist,
causing directories like `gms_feature_id=<uuid>` to be misrouted as
collection IDs instead of being recognized as partition directories.

The fix: strip any `key=value/` path segments when inferring collection ID.
"""

from __future__ import annotations

from pathlib import Path

from portolan_cli.scan import _infer_collection_id_from_relative_path
from portolan_cli.scan_detect import is_hive_partition_dir


class TestIsHivePartitionDir:
    """Tests for Hive partition directory detection."""

    def test_standard_partition_key(self) -> None:
        """Standard partition keys like year, month are detected."""
        assert is_hive_partition_dir("year=2024") == ("year", "2024")
        assert is_hive_partition_dir("month=01") == ("month", "01")

    def test_custom_partition_key(self) -> None:
        """Custom partition keys like gms_feature_id are detected."""
        # This is the core fix for issue #448
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
        """Single Hive partition directory is stripped from collection ID.

        This is the core test for issue #448:
        sites/contours/gms_feature_id=abc123/contours.parquet
        → collection ID should be "sites/contours", not "sites/contours/gms_feature_id=abc123"
        """
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
        """Non-partition directories are preserved even if they appear after a partition.

        This is an edge case - typically partitions are at the leaf level,
        but we should handle the weird case gracefully.
        """
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
        """Double slashes create empty segments that are filtered out.

        This is an edge case where malformed paths with // could produce
        empty segments. We filter them to normalize collection IDs.
        """
        # foo//bar/data.parquet → split gives ["foo", "", "bar"]
        # Empty segments are filtered out to produce clean collection IDs
        result = _infer_collection_id_from_relative_path("foo//bar/data.parquet")
        assert result == "foo/bar"  # Empty segment filtered out

    def test_false_positive_legitimate_equals_directory(self) -> None:
        """Directories with = that aren't Hive partitions ARE stripped.

        IMPORTANT: This documents intentional behavior that may surprise users.
        Any directory matching key=value format is treated as a Hive partition,
        even if it's just a naming convention unrelated to partitioning.

        Examples that WILL be stripped (potentially surprising):
        - version=2.0/  → stripped (looks like partition)
        - config=prod/  → stripped (looks like partition)
        - type=residential/ → stripped (looks like partition)

        Users who use = in directory names without intending Hive partitioning
        should rename their directories or use a different separator (-, _).
        """
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
        """Partition keys with = don't end up in collection ID.

        The collection_id module rejects '=' characters, so if partition
        directories leak into the collection ID, validation will fail.
        """
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
    """Tests for _derive_item_id_and_asset_level with Hive partitions.

    Issue #443: Files in Hive partition directories must produce unique item IDs.
    Single-level partitions (e.g., kdtree_cell=XXX/) use parent dir name (existing behavior).
    Multi-level partitions (e.g., year=2023/month=01/) use full relative path to avoid collisions.
    """

    def test_single_level_hive_partition_uses_parent_name(self, tmp_path: Path) -> None:
        """Single-level Hive partitions use parent directory name as item_id.

        This is the common case for kdtree partitioning and should work as before.
        """
        from portolan_cli.add import _derive_item_id_and_asset_level
        from portolan_cli.formats import FormatType

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
            format_type=FormatType.VECTOR,
        )

        # Single-level partition: use parent dir name, NOT collection-level
        assert is_collection_level is False
        assert item_id == "kdtree_cell=0000000001"

    def test_multi_level_hive_partition_has_unique_item_id(self, tmp_path: Path) -> None:
        """Multi-level Hive partitions get unique item IDs from full relative path.

        Files at year=2023/month=01/file.parquet and year=2024/month=01/file.parquet
        must NOT both get item_id="month=01" (that would be a duplicate).
        """
        from portolan_cli.add import _derive_item_id_and_asset_level
        from portolan_cli.formats import FormatType

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
            format_type=FormatType.VECTOR,
        )

        item_id_2, is_coll_2 = _derive_item_id_and_asset_level(
            path=file2,
            collection_dir=collection_dir,
            item_id=None,
            format_type=FormatType.VECTOR,
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
            format_type=None,  # Unknown format
        )

        # Single-level partition: uses parent dir name
        assert is_collection_level is False
        assert item_id == "region=north"  # parent dir name for single-level

    def test_explicit_item_id_overrides_hive_logic(self, tmp_path: Path) -> None:
        """Explicit item_id takes precedence over Hive partition detection."""
        from portolan_cli.add import _derive_item_id_and_asset_level
        from portolan_cli.formats import FormatType

        collection_dir = tmp_path / "collection"
        partition_dir = collection_dir / "year=2023"
        partition_dir.mkdir(parents=True)
        data_file = partition_dir / "data.parquet"
        data_file.write_bytes(b"x")

        item_id, is_collection_level = _derive_item_id_and_asset_level(
            path=data_file,
            collection_dir=collection_dir,
            item_id="explicit-id",  # User-provided
            format_type=FormatType.VECTOR,
        )

        # Explicit ID used regardless of Hive detection
        assert item_id == "explicit-id"
        assert is_collection_level is False  # Explicit = item-level

    def test_non_hive_path_unchanged(self, tmp_path: Path) -> None:
        """Paths without Hive partitions work as before."""
        from portolan_cli.add import _derive_item_id_and_asset_level
        from portolan_cli.formats import FormatType

        collection_dir = tmp_path / "collection"
        item_dir = collection_dir / "my_item"
        item_dir.mkdir(parents=True)
        data_file = item_dir / "data.parquet"
        data_file.write_bytes(b"x")

        item_id, is_collection_level = _derive_item_id_and_asset_level(
            path=data_file,
            collection_dir=collection_dir,
            item_id=None,
            format_type=FormatType.VECTOR,
        )

        # Regular directory structure unchanged
        assert item_id == "my_item"  # parent dir name
        assert is_collection_level is False
