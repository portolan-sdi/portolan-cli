"""Schema export functionality for round-trip editing.

Exports schema.json to JSON, CSV, or Parquet for editing in external tools.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import TYPE_CHECKING

from portolan_core.json_io import write_json_atomic

if TYPE_CHECKING:
    from portolan_core.models.schema import SchemaModel


def export_schema_json(schema: SchemaModel, path: Path) -> Path:
    """Export schema to JSON file."""
    write_json_atomic(path, schema.to_dict())
    return path


def _write_sidecar_meta(schema: SchemaModel, path: Path) -> Path:
    """Write schema metadata to a sidecar .meta.json file."""
    meta_path = path.with_suffix(path.suffix + ".meta.json")
    meta = {
        "schema_version": schema.schema_version,
        "format": schema.format,
        "crs": schema.crs,
        "statistics": schema.statistics,
    }
    write_json_atomic(meta_path, meta)
    return meta_path


def export_schema_csv(schema: SchemaModel, path: Path) -> Path:
    """Export schema columns to CSV file for editing."""
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

    with Path(path).open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for col in schema.columns:
            row = col.to_dict() if hasattr(col, "to_dict") else dict(col)
            writer.writerow(row)

    # Write sidecar metadata
    _write_sidecar_meta(schema, path)

    return path


def export_schema_parquet(schema: SchemaModel, path: Path) -> Path:
    """Export schema columns to Parquet file for editing."""
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
