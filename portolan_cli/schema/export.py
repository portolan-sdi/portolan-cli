"""Schema export functionality for round-trip editing.

Exports schema.json to JSON, CSV, or Parquet for editing in external tools.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from portolan_cli.models.schema import SchemaModel


def export_schema_json(schema: SchemaModel, path: Path) -> Path:
    """Export schema to JSON file.

    Args:
        schema: SchemaModel to export.
        path: Output file path (must end in .json).

    Returns:
        Path to written file.
    """
    with open(path, "w") as f:
        json.dump(schema.to_dict(), f, indent=2)
    return path


def _write_sidecar_meta(schema: SchemaModel, path: Path) -> Path:
    """Write schema metadata to a sidecar .meta.json file.

    This preserves schema_version, format, crs, and statistics that
    aren't stored in the columnar CSV/Parquet format.
    """
    meta_path = path.with_suffix(path.suffix + ".meta.json")
    meta = {
        "schema_version": schema.schema_version,
        "format": schema.format,
        "crs": schema.crs,
        "statistics": schema.statistics,
    }
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)
    return meta_path


def export_schema_csv(schema: SchemaModel, path: Path) -> Path:
    """Export schema columns to CSV file for editing.

    The CSV format is a flat table with one row per column.
    Only user-editable fields (description, unit, semantic_type) should be modified.

    A sidecar .meta.json file is also written to preserve schema metadata
    (schema_version, format, crs, statistics).

    Args:
        schema: SchemaModel to export.
        path: Output file path (must end in .csv).

    Returns:
        Path to written file.
    """
    # Determine fieldnames based on format
    if schema.format == "geoparquet":
        fieldnames = [
            "name",
            "type",
            "nullable",
            "geometry_type",
            "crs",
            "description",
            "unit",
            "semantic_type",
        ]
    else:  # COG
        fieldnames = ["name", "data_type", "nodata", "description", "unit"]

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for col in schema.columns:
            if hasattr(col, "to_dict"):
                row = col.to_dict()
            else:
                row = dict(col)
            writer.writerow(row)

    # Write sidecar metadata
    _write_sidecar_meta(schema, path)

    return path


def export_schema_parquet(schema: SchemaModel, path: Path) -> Path:
    """Export schema columns to Parquet file for editing.

    The Parquet format is a flat table with one row per column.
    Useful for DuckDB/Pandas users who prefer working with Parquet.

    Args:
        schema: SchemaModel to export.
        path: Output file path (must end in .parquet).

    Returns:
        Path to written file.

    Raises:
        ImportError: If pyarrow is not installed.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Convert columns to list of dicts
    rows = []
    for col in schema.columns:
        if hasattr(col, "to_dict"):
            rows.append(col.to_dict())
        else:
            rows.append(dict(col))

    # Create table from rows
    table = pa.Table.from_pylist(rows)

    # Write to parquet
    pq.write_table(table, path)

    # Write sidecar metadata
    _write_sidecar_meta(schema, path)

    return path
