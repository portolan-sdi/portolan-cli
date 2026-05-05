"""Partitioning support for large GeoParquet files.

This module provides automatic spatial partitioning of large GeoParquet files
using geoparquet-io's KD-tree partitioning. Per ADR-0031, partitioned datasets
use Hive-style directories where each partition becomes a STAC Item.

Usage:
    from portolan_cli.partitioning import should_partition, partition_geoparquet

    if should_partition(file_path, threshold_gb=2.0):
        partitions = partition_geoparquet(file_path, output_dir)
        # Each partition path can be used to create a STAC Item
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

# Default partitioning settings (per plan and geoparquet-io defaults)
DEFAULT_THRESHOLD_GB = 2.0
DEFAULT_TARGET_ROWS = 120_000
DEFAULT_STRATEGY = "kdtree"

# Partition column names by strategy
PARTITION_COLUMNS = {
    "kdtree": "kdtree_cell",
    "h3": "h3_cell",
    "s2": "s2_cell",
    "quadkey": "quadkey",
    "a5": "a5_cell",
}


def should_partition(
    file_path: Path,
    threshold_gb: float = DEFAULT_THRESHOLD_GB,
    enabled: bool = True,
) -> bool:
    """Check if a file should be partitioned based on size threshold.

    Args:
        file_path: Path to the GeoParquet file.
        threshold_gb: Size threshold in GB (default: 2.0 per OGC best practices).
        enabled: Whether partitioning is enabled (default: True).

    Returns:
        True if file exceeds threshold and partitioning is enabled.
    """
    if not enabled:
        return False

    threshold_bytes = threshold_gb * 1024 * 1024 * 1024
    file_size = file_path.stat().st_size

    return file_size > threshold_bytes


def partition_geoparquet(
    input_path: Path,
    output_dir: Path,
    strategy: str = DEFAULT_STRATEGY,
    target_rows: int = DEFAULT_TARGET_ROWS,
    verbose: bool = False,
) -> list[Path]:
    """Partition a GeoParquet file using spatial indexing.

    Uses geoparquet-io's partition_by_kdtree (or other strategies) to split
    large files into manageable partitions. Per ADR-0031, uses Hive-style
    partitioning so each partition can become a STAC Item.

    Args:
        input_path: Path to the input GeoParquet file.
        output_dir: Directory for partitioned output.
        strategy: Partitioning strategy (kdtree, h3, s2, quadkey). Default: kdtree.
        target_rows: Target rows per partition. Default: 120,000.
        verbose: Enable verbose output.

    Returns:
        List of paths to the created partition files.

    Raises:
        ValueError: If strategy is not supported.
    """
    import shutil

    from geoparquet_io.core.partition.by_kdtree import (  # type: ignore[import-untyped]
        partition_by_kdtree,
    )

    if strategy != "kdtree":
        raise ValueError(
            f"Strategy '{strategy}' not yet supported. Currently only 'kdtree' is implemented."
        )

    partition_col = PARTITION_COLUMNS.get(strategy, f"{strategy}_cell")

    try:
        # Call geoparquet-io partition function
        # Hive=True per ADR-0031 (each partition becomes a STAC Item with item.json)
        partition_by_kdtree(
            input_parquet=str(input_path),
            output_folder=str(output_dir),
            hive=True,
            auto_target_rows=("rows", target_rows),
            keep_kdtree_column=True,  # Enable partition pruning
            verbose=verbose,
            compression="ZSTD",
            compression_level=15,
        )
    except Exception:
        # Rollback: remove any partial partition directories created
        for partition_dir in output_dir.glob(f"{partition_col}=*"):
            if partition_dir.is_dir():
                shutil.rmtree(partition_dir)
        raise

    # Collect created partition files
    return _collect_partition_files(output_dir, strategy)


def _collect_partition_files(output_dir: Path, strategy: str) -> list[Path]:
    """Collect partition files from Hive-style output directory.

    Args:
        output_dir: Directory containing partitioned output.
        strategy: Partitioning strategy used.

    Returns:
        List of paths to partition parquet files.
    """
    partition_col = PARTITION_COLUMNS.get(strategy, f"{strategy}_cell")
    pattern = f"{partition_col}=*"

    partition_files = []
    for partition_dir in output_dir.glob(pattern):
        if partition_dir.is_dir():
            # Find parquet file in partition directory
            parquet_files = list(partition_dir.glob("*.parquet"))
            partition_files.extend(parquet_files)

    return sorted(partition_files)


def get_partition_info(partition_path: Path) -> dict[str, str]:
    """Extract partition information from a partition file path.

    Args:
        partition_path: Path to a partition parquet file in Hive-style structure.

    Returns:
        Dict with partition metadata:
        - cell_id: The partition cell identifier
        - partition_column: The column name used for partitioning
    """
    # Parse Hive-style directory name: "column_name=value"
    parent_name = partition_path.parent.name
    match = re.match(r"^(.+)=(.+)$", parent_name)

    if match:
        return {
            "partition_column": match.group(1),
            "cell_id": match.group(2),
        }

    # Fallback for non-Hive structure
    return {
        "partition_column": "unknown",
        "cell_id": partition_path.stem,
    }


def build_glob_pattern(collection_id: str, strategy: str = DEFAULT_STRATEGY) -> str:
    """Build glob pattern for collection-level asset href.

    Per Issue #351, partitioned datasets expose a glob URL for bulk access.

    Args:
        collection_id: The collection identifier.
        strategy: Partitioning strategy used.

    Returns:
        Relative glob pattern like "./kdtree_cell=*/data.parquet" for Hive-style partitions.
    """
    # Hive-style: each partition is in its own directory named by partition column
    partition_col = PARTITION_COLUMNS.get(strategy, f"{strategy}_cell")
    return f"./{partition_col}=*/data.parquet"


def build_remote_glob(
    remote_base: str, collection_id: str, strategy: str = DEFAULT_STRATEGY
) -> str:
    """Build absolute remote glob URL for portolan:glob field.

    Args:
        remote_base: Remote storage base URL (e.g., "s3://bucket/").
        collection_id: The collection identifier.
        strategy: Partitioning strategy used.

    Returns:
        Absolute glob URL like "s3://bucket/collection/kdtree_cell=*/data.parquet".
    """
    # Normalize: ensure no trailing slash on base
    base = remote_base.rstrip("/")
    partition_col = PARTITION_COLUMNS.get(strategy, f"{strategy}_cell")
    return f"{base}/{collection_id}/{partition_col}=*/data.parquet"
