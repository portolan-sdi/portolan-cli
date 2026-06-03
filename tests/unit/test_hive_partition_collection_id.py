"""Tests for generic Hive partition detection in collection ID inference.

Issue #448: Hive partition detection uses hard-coded column allowlist,
causing directories like `gms_feature_id=<uuid>` to be misrouted as
collection IDs instead of being recognized as partition directories.

The fix: strip any `key=value/` path segments when inferring collection ID.
"""

from __future__ import annotations

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
