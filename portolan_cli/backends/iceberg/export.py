"""Static GeoParquet export from Iceberg snapshots.

Provides a fallback for non-Iceberg STAC clients by exporting the
current snapshot as a single GeoParquet file.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from pyiceberg.table import Table

# Columns added by portolake spatial processing — exclude from export
_DERIVED_PREFIXES = ("geohash_", "bbox_")


def export_current_snapshot(table: Table, output_path: Path) -> None:
    """Export the current Iceberg snapshot as a single Parquet file.

    Excludes portolake-derived columns (geohash_*, bbox_*) from the output.
    If the table has no data, writes an empty Parquet file with the schema.
    """
    snap = table.current_snapshot()
    if snap is None:
        # No data — write empty file with schema
        arrow_schema = table.schema().as_arrow()
        filtered = _filter_schema(arrow_schema)
        pq.write_table(filtered.empty_table(), output_path)
        return

    arrow_table = table.scan().to_arrow()
    # Drop derived columns
    for col in arrow_table.column_names:
        if any(col.startswith(prefix) for prefix in _DERIVED_PREFIXES):
            arrow_table = arrow_table.drop(col)

    pq.write_table(arrow_table, output_path)


def _filter_schema(arrow_schema: pa.Schema) -> pa.Schema:
    """Remove derived columns from an Arrow schema."""
    fields = [f for f in arrow_schema if not any(f.name.startswith(p) for p in _DERIVED_PREFIXES)]
    return pa.schema(fields)
