"""Partitioning support for large GeoParquet files.

This module provides automatic spatial partitioning of large GeoParquet files
using geoparquet-io's KD-tree partitioning. Per ADR-0031, partitioned datasets
use Hive-style directories where each partition becomes a STAC Item.

Issue #443: Supports arbitrary Hive partition column names (not just kdtree/h3/s2/quadkey/a5),
multi-level partitions (year=*/month=*), and schema consistency validation.

Usage:
    from portolan_cli.partitioning import should_partition, partition_geoparquet

    if should_partition(file_path, threshold_gb=2.0):
        partitions = partition_geoparquet(file_path, output_dir)
        # Each partition path can be used to create a STAC Item

    # For pre-existing partitioned data with arbitrary columns:
    from portolan_cli.partitioning import detect_partitioning, build_glob_pattern

    metadata = detect_partitioning(directory)  # Auto-detects any Hive columns
    glob = build_glob_pattern(partition_columns=["gms_feature_id"])
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa

if TYPE_CHECKING:
    pass


@dataclass
class SchemaValidationResult:
    """Result of validating schema consistency across partitions."""

    is_consistent: bool
    """True if all partitions have identical schemas."""

    schema: pa.Schema | None
    """The unified schema if consistent, None otherwise."""

    error_message: str
    """Description of schema inconsistency if not consistent."""

    partition_count: int = 0
    """Number of partitions validated."""


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


def build_glob_pattern(
    strategy: str = DEFAULT_STRATEGY,
    partition_columns: list[str] | None = None,
) -> str:
    """Build glob pattern for collection-level asset href.

    Per Issue #351, partitioned datasets expose a glob URL for bulk access.
    Per Issue #443, supports arbitrary partition column names and multi-level partitions.

    Args:
        strategy: Partitioning strategy used (for backwards compatibility).
        partition_columns: Explicit list of partition column names. Takes precedence
            over strategy. For multi-level partitions, provide columns in order
            (e.g., ["year", "month"]).

    Returns:
        Relative glob pattern like "./kdtree_cell=*/*.parquet" for Hive-style partitions,
        or "./year=*/month=*/*.parquet" for multi-level partitions.

    Note:
        The pattern matches ALL .parquet files in partition directories.
        geoparquet-io creates files named by cell ID (e.g., "0000000000.parquet").
        Do not place additional parquet files in partition directories.
    """
    if partition_columns:
        # Build multi-level glob pattern from explicit columns
        pattern_parts = [f"{col}=*" for col in partition_columns]
        return "./" + "/".join(pattern_parts) + "/*.parquet"

    # Backwards compatibility: use strategy to determine column name
    partition_col = PARTITION_COLUMNS.get(strategy, f"{strategy}_cell")
    return f"./{partition_col}=*/*.parquet"


def build_remote_glob(
    remote_base: str,
    collection_id: str,
    strategy: str = DEFAULT_STRATEGY,
    partition_columns: list[str] | None = None,
) -> str:
    """Build absolute remote glob URL for portolan:glob field.

    Per Issue #443, supports arbitrary partition column names and multi-level partitions.

    Args:
        remote_base: Remote storage base URL (e.g., "s3://bucket/").
        collection_id: The collection identifier.
        strategy: Partitioning strategy used (for backwards compatibility).
        partition_columns: Explicit list of partition column names. Takes precedence
            over strategy. For multi-level partitions, provide columns in order.

    Returns:
        Absolute glob URL like "s3://bucket/collection/kdtree_cell=*/*.parquet",
        or "s3://bucket/collection/year=*/month=*/*.parquet" for multi-level.
    """
    # Normalize: ensure no trailing slash on base
    base = remote_base.rstrip("/")

    if partition_columns:
        # Build multi-level glob pattern from explicit columns
        pattern_parts = [f"{col}=*" for col in partition_columns]
        return f"{base}/{collection_id}/" + "/".join(pattern_parts) + "/*.parquet"

    # Backwards compatibility: use strategy to determine column name
    partition_col = PARTITION_COLUMNS.get(strategy, f"{strategy}_cell")
    return f"{base}/{collection_id}/{partition_col}=*/*.parquet"


def get_partition_metadata(
    output_dir: Path,
    strategy: str | None = None,
    partition_columns: list[str] | None = None,
    description: str | None = None,
    descriptions: dict[str, str] | None = None,
) -> dict[str, object]:
    """Extract partition metadata from Hive-style output directory.

    Returns metadata conforming to the STAC Partition Extension schema:
    https://portolan-sdi.github.io/stac-partition-extension/v1.0.0/schema.json

    Per Issue #443, supports arbitrary partition column names and custom descriptions.

    Args:
        output_dir: Directory containing Hive-style partitioned output.
        strategy: Partitioning strategy used (kdtree, h3, etc.). Optional if
            partition_columns is provided.
        partition_columns: Explicit list of partition column names. If not provided,
            auto-detects from directory structure.
        description: Free-text description for single-column partitions.
        descriptions: Per-column descriptions for multi-level partitions.
            Keys are column names, values are descriptions.

    Returns:
        Dict with partition extension fields:
        - partition:scheme: "hive"
        - partition:strategy: The strategy used (if known)
        - partition:keys: List of partition key definitions
        - partition:file_count: Total number of partition files
    """
    # Auto-detect partition columns if not provided
    if partition_columns is None:
        detected = detect_partitioning(output_dir)
        if detected:
            partition_columns = [k["name"] for k in detected["partition:keys"]]
        else:
            # Fall back to strategy-based column
            strategy = strategy or DEFAULT_STRATEGY
            partition_columns = [PARTITION_COLUMNS.get(strategy, f"{strategy}_cell")]

    # Detect strategy from column names if not provided
    detected_strategy = strategy
    if detected_strategy is None:
        for strategy_name, col in PARTITION_COLUMNS.items():
            if col in partition_columns:
                detected_strategy = strategy_name
                break

    # Count files across all partition directories
    file_count = _count_partition_files(output_dir, partition_columns)

    # Build partition key definitions
    keys: list[dict[str, str]] = []
    for col in partition_columns:
        key_def: dict[str, str] = {"name": col, "type": "string"}

        # Add description if provided
        if descriptions and col in descriptions:
            key_def["description"] = descriptions[col]
        elif description and len(partition_columns) == 1:
            key_def["description"] = description
        elif detected_strategy and col == PARTITION_COLUMNS.get(detected_strategy):
            key_def["description"] = (
                f"{detected_strategy.upper()} spatial partition cell identifier"
            )

        keys.append(key_def)

    result: dict[str, Any] = {
        "partition:scheme": "hive",
        "partition:keys": keys,
        "partition:file_count": file_count,
    }

    # Only include strategy if detected (avoid null in JSON output)
    if detected_strategy is not None:
        result["partition:strategy"] = detected_strategy

    return result


def _count_partition_files(directory: Path, partition_columns: list[str]) -> int:
    """Count parquet files across all partition directories.

    Handles both single-level and multi-level partitions.

    Args:
        directory: Root directory containing partitions.
        partition_columns: List of partition column names.

    Returns:
        Total count of .parquet files in leaf partition directories.
    """
    if not partition_columns:
        return 0

    # Build glob pattern to find all parquet files
    pattern_parts = [f"{col}=*" for col in partition_columns]
    pattern = "/".join(pattern_parts) + "/*.parquet"

    return len(list(directory.glob(pattern)))


def detect_partitioning(directory: Path) -> dict[str, Any] | None:
    """Detect existing Hive-style partitioning in a directory.

    Scans for directories matching the pattern "column=value/" and extracts
    partition metadata if found. Per Issue #443, supports multi-level partitions
    like year=*/month=*/*.parquet.

    Args:
        directory: Directory to scan for partitions.

    Returns:
        Partition metadata dict if partitioning detected, None otherwise.
    """
    # Look for Hive-style directories: column=value
    hive_pattern = re.compile(r"^([a-zA-Z_][a-zA-Z0-9_]*)=.+$")

    # Detect partition levels by walking the tree
    partition_keys: list[str] = []
    all_partition_dirs: list[Path] = []

    def _scan_level(current_dir: Path, depth: int = 0) -> None:
        """Recursively scan for Hive partition directories."""
        for item in current_dir.iterdir():
            if item.is_dir():
                match = hive_pattern.match(item.name)
                if match:
                    key_name = match.group(1)
                    # Track keys in order of first encounter (preserves level order)
                    if key_name not in partition_keys:
                        partition_keys.append(key_name)
                    all_partition_dirs.append(item)
                    # Recurse to find nested partition levels
                    _scan_level(item, depth + 1)

    _scan_level(directory)

    if not partition_keys:
        return None

    # Count parquet files at leaf level
    # Build glob pattern for all partition levels
    file_count = _count_partition_files(directory, partition_keys)

    # Try to detect strategy from column name
    strategy = None
    for strategy_name, col in PARTITION_COLUMNS.items():
        if col in partition_keys:
            strategy = strategy_name
            break

    result: dict[str, Any] = {
        "partition:scheme": "hive",
        "partition:keys": [{"name": key, "type": "string"} for key in partition_keys],
        "partition:file_count": file_count,
    }
    # Only include strategy if detected (avoid null in JSON output)
    if strategy is not None:
        result["partition:strategy"] = strategy
    return result


def validate_partition_schemas(directory: Path) -> SchemaValidationResult:
    """Validate schema consistency across all partitions in a Hive-partitioned dataset.

    Per Issue #443, reads Parquet metadata (not data) from each partition file
    to verify all partitions have identical schemas. This is fast because Parquet
    stores schema in the file footer.

    Args:
        directory: Root directory containing Hive-style partitioned data.

    Returns:
        SchemaValidationResult with:
        - is_consistent: True if all schemas match
        - schema: The unified schema if consistent
        - error_message: Description of mismatch if inconsistent
        - partition_count: Number of partitions validated
    """
    import pyarrow.parquet as pq

    # First detect the partition structure
    partition_info = detect_partitioning(directory)
    if partition_info is None:
        return SchemaValidationResult(
            is_consistent=True,
            schema=None,
            error_message="",
            partition_count=0,
        )

    partition_columns = [k["name"] for k in partition_info["partition:keys"]]

    # Build glob pattern to find all parquet files
    pattern_parts = [f"{col}=*" for col in partition_columns]
    pattern = "/".join(pattern_parts) + "/*.parquet"

    parquet_files = list(directory.glob(pattern))
    if not parquet_files:
        return SchemaValidationResult(
            is_consistent=True,
            schema=None,
            error_message="",
            partition_count=0,
        )

    # Read schema from first file as reference
    reference_file = parquet_files[0]
    try:
        reference_schema = pq.read_schema(reference_file)
    except Exception as e:
        return SchemaValidationResult(
            is_consistent=False,
            schema=None,
            error_message=f"Failed to read schema from {reference_file}: {e}",
            partition_count=len(parquet_files),
        )

    # Compare all other files against reference
    for pq_file in parquet_files[1:]:
        try:
            file_schema = pq.read_schema(pq_file)
        except Exception as e:
            return SchemaValidationResult(
                is_consistent=False,
                schema=None,
                error_message=f"Failed to read schema from {pq_file}: {e}",
                partition_count=len(parquet_files),
            )

        # Compare schemas
        if not _schemas_equal(reference_schema, file_schema):
            diff = _describe_schema_diff(reference_schema, file_schema, reference_file, pq_file)
            return SchemaValidationResult(
                is_consistent=False,
                schema=None,
                error_message=diff,
                partition_count=len(parquet_files),
            )

    return SchemaValidationResult(
        is_consistent=True,
        schema=reference_schema,
        error_message="",
        partition_count=len(parquet_files),
    )


def _schemas_equal(schema1: pa.Schema, schema2: pa.Schema) -> bool:
    """Check if two PyArrow schemas are equal.

    Compares field names and types.
    """
    if len(schema1) != len(schema2):
        return False

    for i in range(len(schema1)):
        field1 = schema1.field(i)
        field2 = schema2.field(i)
        if field1.name != field2.name or field1.type != field2.type:
            return False

    return True


def _describe_schema_diff(
    schema1: pa.Schema,
    schema2: pa.Schema,
    file1: Path,
    file2: Path,
) -> str:
    """Generate a human-readable description of schema differences."""
    fields1 = {f.name: f.type for f in schema1}
    fields2 = {f.name: f.type for f in schema2}

    diffs: list[str] = []

    # Fields in first but not second
    only_in_first = set(fields1.keys()) - set(fields2.keys())
    if only_in_first:
        diffs.append(f"Columns only in {file1.name}: {sorted(only_in_first)}")

    # Fields in second but not first
    only_in_second = set(fields2.keys()) - set(fields1.keys())
    if only_in_second:
        diffs.append(f"Columns only in {file2.name}: {sorted(only_in_second)}")

    # Fields with different types
    common_fields = set(fields1.keys()) & set(fields2.keys())
    for field in sorted(common_fields):
        if fields1[field] != fields2[field]:
            diffs.append(f"Column '{field}' type mismatch: {fields1[field]} vs {fields2[field]}")

    return "; ".join(diffs) if diffs else "Unknown schema difference"
