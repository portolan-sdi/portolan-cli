"""Partitioning support for large GeoParquet files."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq

from portolan_core.trace import count

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

    from portolan_core.config import ConfigSource


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

# The directory walk stops at this depth. The cap prevents unbounded recursion
# through a symlink loop or a deeply nested tree.
MAX_PARTITION_DEPTH = 20

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
    """Check if a file should be partitioned based on size threshold."""
    if not enabled:
        return False

    threshold_bytes = threshold_gb * 1024 * 1024 * 1024
    file_size = file_path.stat().st_size

    return file_size > threshold_bytes


@dataclass
class PartitionPromptDecision:
    """Config-driven decision inputs for the interactive partition prompt."""

    enabled: bool
    """Value of ``partitioning.enabled`` (default True)."""

    prompt: bool
    """Value of ``partitioning.prompt`` (default True)."""

    threshold_gb: float
    """Size threshold in GB (``partitioning.threshold_gb``, default 2.0)."""

    large_files: list[tuple[Path, float]]
    """(path, size_gb) for files exceeding the threshold. Empty when gating is
    off, so the CLI can short-circuit without prompting."""


def find_large_partition_candidates(
    resolved_paths: list[Path],
    catalog_root: Path,
    *,
    state: ConfigSource | None = None,
) -> PartitionPromptDecision:
    """Read partition config and scan paths for files that would be partitioned."""
    from portolan_core.config import get_setting

    def _setting(key: str) -> Any:
        if state is not None:
            return state.setting(key)
        return get_setting(key, catalog_path=catalog_root)

    enabled = _setting("partitioning.enabled")
    if enabled is None:
        enabled = True  # Default: enabled (prompt before partitioning large files)
    prompt = _setting("partitioning.prompt")
    if prompt is None:
        prompt = True  # Default: prompt in interactive mode
    threshold_gb = _setting("partitioning.threshold_gb") or 2.0

    if not (enabled and prompt):
        return PartitionPromptDecision(
            enabled=bool(enabled),
            prompt=bool(prompt),
            threshold_gb=threshold_gb,
            large_files=[],
        )

    large_files = _scan_partition_candidates(resolved_paths, threshold_gb)
    return PartitionPromptDecision(
        enabled=True,
        prompt=True,
        threshold_gb=threshold_gb,
        large_files=large_files,
    )


def _large_parquet(path: Path, threshold_gb: float) -> tuple[Path, float] | None:
    """Return the path and its size in GB when it exceeds ``threshold_gb``, else None."""
    try:
        if should_partition(path, threshold_gb=threshold_gb, enabled=True):
            return (path, path.stat().st_size / (1024 * 1024 * 1024))
    except OSError:
        # Skip files with permission errors or broken symlinks
        return None
    return None


def _scan_partition_candidates(
    resolved_paths: list[Path], threshold_gb: float
) -> list[tuple[Path, float]]:
    """Collect ``.parquet`` files exceeding ``threshold_gb`` from paths/dirs."""
    large_files: list[tuple[Path, float]] = []
    for p in resolved_paths:
        try:
            candidates: Iterable[Path] = []
            if p.is_file() and p.suffix.lower() == ".parquet":
                candidates = [p]
            elif p.is_dir():
                candidates = p.rglob("*.parquet")
            for candidate in candidates:
                found = _large_parquet(candidate, threshold_gb)
                if found is not None:
                    large_files.append(found)
        except OSError:
            # Skip paths with permission errors or broken symlinks
            continue
    return large_files


def partition_geoparquet(
    input_path: Path,
    output_dir: Path,
    strategy: str = DEFAULT_STRATEGY,
    target_rows: int = DEFAULT_TARGET_ROWS,
    verbose: bool = False,
) -> list[Path]:
    """Partition a GeoParquet file using spatial indexing."""
    import shutil

    from geoparquet_io.core.partition.by_kdtree import (
        partition_by_kdtree,
    )

    if strategy != "kdtree":
        raise ValueError(
            f"Strategy '{strategy}' not yet supported. Currently only 'kdtree' is implemented."
        )

    partition_col = PARTITION_COLUMNS.get(strategy, f"{strategy}_cell")

    try:
        # Call geoparquet-io partition function
        # Hive=True (each partition becomes a STAC Item with item.json)
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
    """Collect partition files from Hive-style output directory."""
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
    """Extract partition information from a partition file path."""
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
    """Build glob pattern for collection-level asset href."""
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
    """Build absolute remote glob URL for portolan:glob field."""
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
    """Extract partition metadata from Hive-style output directory."""
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
    """Count parquet files across all partition directories."""
    if not partition_columns:
        return 0

    # Build glob pattern to find all parquet files
    pattern_parts = [f"{col}=*" for col in partition_columns]
    pattern = "/".join(pattern_parts) + "/*.parquet"

    return len(list(directory.glob(pattern)))


def detect_partitioning(directory: Path) -> dict[str, Any] | None:
    """Detect existing Hive-style partitioning in a directory."""
    # Guard: directory must exist
    if not directory.exists() or not directory.is_dir():
        return None

    # Look for Hive-style directories: column=value
    hive_pattern = re.compile(r"^([a-zA-Z_][a-zA-Z0-9_]*)=.+$")

    # Detect partition levels by walking the tree
    partition_keys: list[str] = []
    all_partition_dirs: list[Path] = []

    def _scan_level(current_dir: Path, depth: int = 0) -> None:
        """Recursively scan for Hive partition directories."""
        if depth >= MAX_PARTITION_DEPTH:
            return  # Prevent unbounded recursion

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
    """Validate schema consistency across all partitions in a Hive-partitioned collection."""
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
        count("parquet.opens")
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
            count("parquet.opens")
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
    """Check if two PyArrow schemas are equal."""
    if len(schema1) != len(schema2):
        return False

    # Build name->type maps for order-independent comparison
    fields1 = {f.name: f.type for f in schema1}
    fields2 = {f.name: f.type for f in schema2}

    return fields1 == fields2


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
    diffs.extend(
        f"Column '{field}' type mismatch: {fields1[field]} vs {fields2[field]}"
        for field in sorted(common_fields)
        if fields1[field] != fields2[field]
    )

    return "; ".join(diffs) if diffs else "Unknown schema difference"


def partition_strategy(
    path: Path, catalog_root: Path | None, *, state: ConfigSource | None = None
) -> str | None:
    """Decide whether and how to partition a converted GeoParquet file."""
    if catalog_root is None:
        return None

    def _setting(key: str) -> Any:
        return _directory_setting(key, path.parent, catalog_root, state)

    if _setting("partitioning.enabled") is False:
        return None
    threshold_gb = float(_setting("partitioning.threshold_gb") or 2.0)
    if not should_partition(path, threshold_gb=threshold_gb):
        return None
    return str(_setting("partitioning.strategy") or DEFAULT_STRATEGY)


def _directory_setting(
    key: str, directory: Path, catalog_root: Path, state: ConfigSource | None
) -> Any:
    """Resolve a setting for the config hierarchy of ``directory``."""
    from portolan_cli.conversion_config import partition_override
    from portolan_core.config import get_setting

    override = partition_override(directory)
    if override is not None and key in override.values:
        return override.values[key]
    if state is not None:
        return state.setting(key, collection_dir=directory)
    return get_setting(key, catalog_path=catalog_root, collection_path=directory)


def partition_vector_file(
    path: Path, catalog_root: Path | None, *, state: ConfigSource | None = None
) -> list[Path]:
    """Partition a converted GeoParquet file when ``partition_strategy`` says so.

    Args:
        path: The converted GeoParquet file.
        catalog_root: Catalog root, for the settings.
        state: The run state, which parses the config once per run.

    Returns:
        The partition files, or an empty list when the file stays whole.

    Raises:
        PartitionCollisionError: The directory already holds partitions.
    """
    strategy = partition_strategy(path, catalog_root, state=state)
    if strategy is None or catalog_root is None:
        return []

    from portolan_core.errors import PartitionCollisionError

    column = PARTITION_COLUMNS.get(strategy, f"{strategy}_cell")
    existing = sorted(p.name for p in path.parent.glob(f"{column}=*") if p.is_dir())
    if existing:
        raise PartitionCollisionError(str(path), str(path.parent), existing)

    target_rows = _directory_setting("partitioning.target_rows", path.parent, catalog_root, state)
    files = partition_geoparquet(
        input_path=path,
        output_dir=path.parent,
        strategy=strategy,
        target_rows=int(target_rows or 120_000),
    )
    if files:
        path.unlink(missing_ok=True)
    return files


def partition_columns_of(files: list[Path]) -> list[str]:
    """Return the Hive partition column names that ``files`` sit under."""
    return sorted({f.parent.name.split("=", 1)[0] for f in files if "=" in f.parent.name})


def strategy_of_column(column: str) -> str:
    """Return the strategy that writes partitions under ``column``."""
    for strategy, name in PARTITION_COLUMNS.items():
        if column in (name, f"{name}_prefix"):
            return strategy
    return column.removesuffix("_cell")


@dataclass(frozen=True)
class PartitionPreview:
    """The result of ``portolan partition --preview``. Nothing is written."""

    file: Path
    size_gb: float
    recommended: bool
    strategy: str
    target_rows: int

    def to_dict(self) -> dict[str, Any]:
        """Return the envelope data of ``portolan partition --preview``."""
        return {
            "file": str(self.file),
            "size_gb": round(self.size_gb, 2),
            "recommended_partition": self.recommended,
            "strategy": self.strategy,
            "target_rows": self.target_rows,
        }


@dataclass(frozen=True)
class PartitionOutcome:
    """The result of ``portolan partition``: the partition files it wrote."""

    input: Path
    output_dir: Path
    files: list[Path]

    def to_dict(self) -> dict[str, Any]:
        """Return the envelope data of ``portolan partition``."""
        return {
            "input": str(self.input),
            "output_dir": str(self.output_dir),
            "partitions": len(self.files),
            "files": [str(p) for p in self.files],
        }


def partition_file(
    input_file: Path,
    output_dir: Path | None,
    *,
    strategy: str = DEFAULT_STRATEGY,
    target_rows: int = DEFAULT_TARGET_ROWS,
    preview: bool = False,
    verbose: bool = False,
) -> PartitionPreview | PartitionOutcome:
    """Partition a GeoParquet file into ``output_dir``, or preview the partition."""
    if preview:
        from portolan_core.config import get_setting

        threshold_gb = get_setting("partitioning.threshold_gb") or 2.0
        enabled = get_setting("partitioning.enabled")
        return PartitionPreview(
            file=input_file,
            size_gb=input_file.stat().st_size / (1024 * 1024 * 1024),
            recommended=should_partition(
                input_file, threshold_gb=float(threshold_gb), enabled=enabled is not False
            ),
            strategy=strategy,
            target_rows=target_rows,
        )

    from portolan_core.errors import PartitionError

    if output_dir is None:
        raise PartitionError("output_dir is required unless preview is set")
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        files = partition_geoparquet(
            input_path=input_file,
            output_dir=output_dir,
            strategy=strategy,
            target_rows=target_rows,
            verbose=verbose,
        )
    # geoparquet-io raises many types. The error keeps its message.
    except Exception as e:
        raise PartitionError(str(e)) from e
    return PartitionOutcome(input_file, output_dir, files)
