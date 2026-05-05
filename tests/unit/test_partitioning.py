"""Tests for portolan_cli.partitioning module.

Tests the partitioning functionality including:
- Size threshold detection (should_partition)
- Partitioning via geoparquet-io wrapper
- Config integration
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from unittest import mock

import pytest

if TYPE_CHECKING:
    pass


class TestShouldPartition:
    """Tests for should_partition function."""

    @pytest.mark.unit
    def test_should_partition_returns_false_for_small_file(self, tmp_path: Path) -> None:
        """Files under threshold should not be partitioned."""
        from portolan_cli.partitioning import should_partition

        # Create a small file (1 MB)
        small_file = tmp_path / "small.parquet"
        small_file.write_bytes(b"x" * (1 * 1024 * 1024))

        result = should_partition(small_file, threshold_gb=2.0)

        assert result is False

    @pytest.mark.unit
    def test_should_partition_returns_true_for_large_file(self, tmp_path: Path) -> None:
        """Files over threshold should be partitioned."""
        from portolan_cli.partitioning import should_partition

        # Create a large file (3 GB) - we'll mock the file size check
        large_file = tmp_path / "large.parquet"
        large_file.write_bytes(b"x" * 100)  # Small actual file

        # Mock stat to return large file size
        with mock.patch.object(Path, "stat") as mock_stat:
            mock_stat.return_value.st_size = 3 * 1024 * 1024 * 1024  # 3 GB
            result = should_partition(large_file, threshold_gb=2.0)

        assert result is True

    @pytest.mark.unit
    def test_should_partition_uses_config_threshold(self, tmp_path: Path) -> None:
        """should_partition respects custom threshold."""
        from portolan_cli.partitioning import should_partition

        # Create a 500 MB file
        file_path = tmp_path / "medium.parquet"
        file_path.write_bytes(b"x" * 100)

        with mock.patch.object(Path, "stat") as mock_stat:
            mock_stat.return_value.st_size = 500 * 1024 * 1024  # 500 MB

            # With 2GB threshold: should NOT partition
            assert should_partition(file_path, threshold_gb=2.0) is False

            # With 0.4GB threshold: should partition
            assert should_partition(file_path, threshold_gb=0.4) is True

    @pytest.mark.unit
    def test_should_partition_returns_false_when_disabled(self, tmp_path: Path) -> None:
        """should_partition returns False when partitioning is disabled."""
        from portolan_cli.partitioning import should_partition

        large_file = tmp_path / "large.parquet"
        large_file.write_bytes(b"x" * 100)

        with mock.patch.object(Path, "stat") as mock_stat:
            mock_stat.return_value.st_size = 5 * 1024 * 1024 * 1024  # 5 GB
            result = should_partition(large_file, threshold_gb=2.0, enabled=False)

        assert result is False


class TestPartitionGeoparquet:
    """Tests for partition_geoparquet function."""

    @pytest.mark.unit
    def test_partition_geoparquet_calls_gpio_kdtree(self, tmp_path: Path) -> None:
        """partition_geoparquet should call geoparquet-io partition_by_kdtree."""
        from portolan_cli.partitioning import partition_geoparquet

        input_file = tmp_path / "input.parquet"
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        with mock.patch(
            "geoparquet_io.core.partition.by_kdtree.partition_by_kdtree"
        ) as mock_partition:
            partition_geoparquet(
                input_path=input_file,
                output_dir=output_dir,
                strategy="kdtree",
                target_rows=120_000,
            )

            mock_partition.assert_called_once()
            call_kwargs = mock_partition.call_args.kwargs
            assert call_kwargs["input_parquet"] == str(input_file)
            assert call_kwargs["output_folder"] == str(output_dir)
            assert call_kwargs["hive"] is True  # Per ADR-0031
            assert call_kwargs["auto_target_rows"] == ("rows", 120_000)

    @pytest.mark.unit
    def test_partition_geoparquet_returns_partition_files(self, tmp_path: Path) -> None:
        """partition_geoparquet should return list of created partition files."""
        from portolan_cli.partitioning import partition_geoparquet

        input_file = tmp_path / "input.parquet"
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # Create mock partition output structure (Hive-style)
        (output_dir / "kdtree_cell=001").mkdir()
        (output_dir / "kdtree_cell=001" / "data.parquet").write_bytes(b"x")
        (output_dir / "kdtree_cell=002").mkdir()
        (output_dir / "kdtree_cell=002" / "data.parquet").write_bytes(b"x")

        with mock.patch("geoparquet_io.core.partition.by_kdtree.partition_by_kdtree"):
            result = partition_geoparquet(
                input_path=input_file,
                output_dir=output_dir,
                strategy="kdtree",
            )

        assert len(result) == 2
        assert all(p.name == "data.parquet" for p in result)
        assert {p.parent.name for p in result} == {"kdtree_cell=001", "kdtree_cell=002"}


class TestGetPartitionInfo:
    """Tests for get_partition_info function."""

    @pytest.mark.unit
    def test_get_partition_info_extracts_cell_id(self, tmp_path: Path) -> None:
        """get_partition_info should extract partition cell ID from path."""
        from portolan_cli.partitioning import get_partition_info

        partition_path = tmp_path / "kdtree_cell=042" / "data.parquet"
        partition_path.parent.mkdir(parents=True)
        partition_path.write_bytes(b"x")

        result = get_partition_info(partition_path)

        assert result["cell_id"] == "042"
        assert result["partition_column"] == "kdtree_cell"

    @pytest.mark.unit
    def test_get_partition_info_handles_different_strategies(self, tmp_path: Path) -> None:
        """get_partition_info should work with different partition column names."""
        from portolan_cli.partitioning import get_partition_info

        # H3 partition
        h3_path = tmp_path / "h3_cell=8928308280fffff" / "data.parquet"
        h3_path.parent.mkdir(parents=True)
        h3_path.write_bytes(b"x")

        result = get_partition_info(h3_path)

        assert result["cell_id"] == "8928308280fffff"
        assert result["partition_column"] == "h3_cell"
