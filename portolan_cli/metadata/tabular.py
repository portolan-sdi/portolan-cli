"""Plain-Parquet (tabular) metadata extraction."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

from portolan_cli.formats import parquet_footer, read_geo_metadata

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(frozen=True)
class TabularMetadata:
    """Metadata extracted from a plain (geometry-less) Parquet file."""

    schema: dict[str, str]
    row_count: int
    temporal_interval: tuple[datetime, datetime] | None


def extract_tabular_metadata(path: Path) -> TabularMetadata | None:
    """Extract table metadata from a plain Parquet file."""
    if path.suffix.lower() != ".parquet":
        return None
    try:
        parquet = parquet_footer(path)
    except Exception:  # unreadable Parquet: the format checks own it
        return None
    if read_geo_metadata(parquet) is not None:
        return None

    return TabularMetadata(
        schema={field.name: str(field.type) for field in parquet.schema_arrow},
        row_count=parquet.metadata.num_rows,
        temporal_interval=_temporal_interval(parquet),
    )


def _temporal_interval(parquet: pq.ParquetFile) -> tuple[datetime, datetime] | None:
    """Earliest and latest value across every temporal column of the file."""
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
    """Normalize a Parquet temporal statistic to a timezone-aware UTC datetime."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=UTC)
    return None
