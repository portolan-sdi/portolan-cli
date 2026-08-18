"""Plain-Parquet (tabular) metadata extraction.

A tabular asset is a Parquet file with no ``geo`` metadata key. The spec's
Tabular Data section asks such a collection to document its columns with the
STAC table extension and to populate ``extent.temporal`` when the data carries
a time dimension (rashid PTL-DAT-015, issue #749).

Both answers come from the Parquet footer, so nothing here reads row data:
the Arrow schema gives column names and types, and per-row-group column
statistics give the temporal bounds.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


@dataclass(frozen=True)
class TabularMetadata:
    """Metadata extracted from a plain (geometry-less) Parquet file.

    Attributes:
        schema: Column names mapped to their Arrow type names.
        row_count: Number of rows.
        temporal_interval: Earliest and latest value across every temporal
            column, or None when the file carries no temporal column or the
            writer emitted no statistics for one.
    """

    schema: dict[str, str]
    row_count: int
    temporal_interval: tuple[datetime, datetime] | None


def extract_tabular_metadata(path: Path) -> TabularMetadata | None:
    """Extract table metadata from a plain Parquet file.

    Args:
        path: Path to the candidate asset.

    Returns:
        TabularMetadata, or None when ``path`` is not a readable Parquet file
        or is GeoParquet. GeoParquet is excluded deliberately: the geospatial
        storage rules own it, and its schema already reaches the collection
        through ``add_table_extension``.
    """
    if path.suffix.lower() != ".parquet":
        return None
    try:
        parquet = pq.ParquetFile(path)
    except Exception:  # noqa: BLE001 - unreadable Parquet: the format checks own it
        return None
    if (parquet.schema_arrow.metadata or {}).get(b"geo") is not None:
        return None

    return TabularMetadata(
        schema={field.name: str(field.type) for field in parquet.schema_arrow},
        row_count=parquet.metadata.num_rows,
        temporal_interval=_temporal_interval(parquet),
    )


def _temporal_interval(parquet: pq.ParquetFile) -> tuple[datetime, datetime] | None:
    """Earliest and latest value across every temporal column of the file.

    Reads per-row-group column statistics rather than the data. A column whose
    writer emitted no statistics contributes nothing, so a file where no
    temporal column carries statistics yields None and leaves the collection's
    extent alone rather than guessing at it.
    """
    temporal_columns = {
        field.name for field in parquet.schema_arrow if pa.types.is_temporal(field.type)
    }
    if not temporal_columns:
        return None

    bounds: list[datetime] = []
    metadata = parquet.metadata
    for group in range(metadata.num_row_groups):
        row_group = metadata.row_group(group)
        for index in range(row_group.num_columns):
            column = row_group.column(index)
            if column.path_in_schema not in temporal_columns:
                continue
            statistics = column.statistics
            if statistics is None or not statistics.has_min_max:
                continue
            bounds.extend(
                normalized
                for raw in (statistics.min, statistics.max)
                if (normalized := _as_utc(raw)) is not None
            )

    if not bounds:
        return None
    return min(bounds), max(bounds)


def _as_utc(value: object) -> datetime | None:
    """Normalize a Parquet temporal statistic to a timezone-aware UTC datetime.

    Parquet exposes date columns as ``date`` and timestamp columns as
    ``datetime``, which may be naive. STAC requires RFC 3339, so a date becomes
    midnight and a naive datetime is read as UTC. Time-of-day and duration
    statistics carry no calendar date, so they cannot bound an extent and are
    dropped.
    """
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=timezone.utc)
    return None
