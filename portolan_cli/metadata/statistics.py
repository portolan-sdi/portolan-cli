"""Statistics extraction module for raster bands and parquet columns.

Extracts statistics from:
- COG/raster files via rasterio (min, max, mean, stddev)
- GeoParquet files via PyArrow metadata (min, max, null_count)

- Stats computed by default
- Raster uses approx mode (~100ms via GDAL overviews)
- Parquet uses PyArrow metadata only (instant, no DuckDB)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Literal

import pyarrow.parquet as pq
import rasterio
from rasterio import Statistics as RasterioStats

from portolan_cli.trace import count

if TYPE_CHECKING:
    from pathlib import Path


def _serialize_stat_value(value: Any) -> Any:
    """Convert a statistic value to JSON-serializable form.

    PyArrow statistics can return native Python datetime/date objects
    for timestamp columns. These need to be converted to ISO 8601 strings.

    Args:
        value: The statistic value from PyArrow (min/max).

    Returns:
        JSON-serializable value (datetime → ISO string, others unchanged).
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


@dataclass
class BandStatistics:
    """Statistics for a single raster band.

    Attributes:
        minimum: Minimum pixel value.
        maximum: Maximum pixel value.
        mean: Mean pixel value.
        stddev: Standard deviation of pixel values.
        valid_percent: Percentage of valid (non-nodata) pixels (optional).
    """

    minimum: float
    maximum: float
    mean: float
    stddev: float
    valid_percent: float | None = None

    def to_stac_dict(self) -> dict[str, float]:
        """Format for STAC bands[].statistics.

        Returns only the core STAC statistics fields (no valid_percent).
        """
        return {
            "minimum": self.minimum,
            "maximum": self.maximum,
            "mean": self.mean,
            "stddev": self.stddev,
        }


@dataclass
class ColumnStatistics:
    """Statistics for a Parquet column.

    Attributes:
        name: Column name.
        min_value: Minimum value (typed).
        max_value: Maximum value (typed).
        null_count: Number of null values.
    """

    name: str
    min_value: Any | None = None
    max_value: Any | None = None
    null_count: int = 0

    def to_stac_dict(self) -> dict[str, Any]:
        """Format for STAC table:columns[].statistics.

        Omits None values and zero null_count.
        Converts datetime values to ISO 8601 strings for JSON serialization.
        """
        result: dict[str, Any] = {}
        if self.min_value is not None:
            result["minimum"] = _serialize_stat_value(self.min_value)
        if self.max_value is not None:
            result["maximum"] = _serialize_stat_value(self.max_value)
        if self.null_count > 0:
            result["null_count"] = self.null_count
        return result


def extract_band_statistics(
    path: Path,
    *,
    mode: Literal["cached", "approx", "exact"] = "approx",
) -> list[BandStatistics]:
    """Extract band statistics from a COG/raster file.

    Args:
        path: Path to raster file.
        mode: Statistics computation mode:
            - 'cached': Read from embedded GDAL metadata only (instant)
            - 'approx': Use GDAL approx mode with overviews (fast, default)
            - 'exact': Compute exact statistics (slow, reads all pixels)

    Returns:
        List of BandStatistics, one per band.
    """
    count("raster.opens")
    # PAM off: GDAL would write the statistics it computes to a .aux.xml
    # sidecar, which is no part of the published COG (issue #748).
    with rasterio.Env(GDAL_PAM_ENABLED="NO"), rasterio.open(path) as src:
        results: list[BandStatistics] = []
        computed_stats: list[RasterioStats] | None = None  # Lazy-computed via stats()

        for band_idx in range(1, src.count + 1):
            # Try cached stats first (GDAL metadata tags) - but not if exact mode requested
            tags = src.tags(bidx=band_idx)
            if mode != "exact" and "STATISTICS_MINIMUM" in tags:
                results.append(
                    BandStatistics(
                        minimum=float(tags["STATISTICS_MINIMUM"]),
                        maximum=float(tags["STATISTICS_MAXIMUM"]),
                        mean=float(tags["STATISTICS_MEAN"]),
                        stddev=float(tags["STATISTICS_STDDEV"]),
                        valid_percent=float(tags.get("STATISTICS_VALID_PERCENT", 100)),
                    )
                )
                continue

            if mode == "cached":
                continue  # No cached stats and cached-only mode, skip this band

            # Compute using GDAL's stats() (rasterio 2.0+ compatible)
            # Lazy-compute once for all bands that need it
            if computed_stats is None:
                computed_stats = src.stats(approx=(mode == "approx"))

            stat = computed_stats[band_idx - 1]  # stats() returns 0-indexed list
            results.append(
                BandStatistics(
                    minimum=_as_gdal_stores(stat.min),
                    maximum=_as_gdal_stores(stat.max),
                    mean=_as_gdal_stores(stat.mean),
                    stddev=_as_gdal_stores(stat.std),
                )
            )

        return results


#: The band tags that PTL-DAT-009 and PTL-DAT-010 read from a COG.
EMBEDDED_STATISTICS_TAGS = frozenset(
    {
        "STATISTICS_MINIMUM",
        "STATISTICS_MAXIMUM",
        "STATISTICS_MEAN",
        "STATISTICS_STDDEV",
        "STATISTICS_VALID_PERCENT",
    }
)


def _has_statistics(tags: dict[str, str]) -> bool:
    """Report whether band tags hold every statistic the band can have."""
    return set(tags) >= EMBEDDED_STATISTICS_TAGS or tags.get("STATISTICS_VALID_PERCENT") == "0"


def embedded_statistics(path: Path) -> Literal["present", "missing", "lossy"]:
    """Report whether a COG carries its band statistics in the file itself.

    The read has PAM off, as rashid reads, so a ``.aux.xml`` does not count.

    Args:
        path: A COG.

    Returns:
        ``present`` when every band has the five statistics tags, or has no
        valid pixel (``STATISTICS_VALID_PERCENT=0``), so that no statistics
        exist to embed. ``lossy``
        when some are missing and the tiles use lossy compression, such as JPEG
        or lossy WEBP. A rewrite would re-encode those pixels. ``missing``
        otherwise.
    """
    count("raster.opens")
    with rasterio.Env(GDAL_PAM_ENABLED="NO"), rasterio.open(path) as src:
        if all(_has_statistics(src.tags(bidx=band)) for band in src.indexes):
            return "present"
        structure = src.tags(ns="IMAGE_STRUCTURE")
    lossy = structure.get("COMPRESSION_REVERSIBILITY", "").upper() == "LOSSY"
    return "lossy" if lossy or "JPEG" in structure.get("COMPRESSION", "").upper() else "missing"


def _as_gdal_stores(value: float) -> float:
    """Round a computed statistic to the 14 significant digits GDAL stores.

    GDAL writes the statistics it computes into the .aux.xml sidecar as
    ``%.14g`` text, and the next run reads them back from there. Without the
    rounding, ``add --force`` wrote other values than the first ``add``.
    """
    return float(f"{value:.14g}")


def _is_stats_compatible_type(field: Any) -> bool:
    """Check if a PyArrow field type supports meaningful statistics.

    Binary, struct, list, and geometry columns don't have meaningful min/max.

    Args:
        field: PyArrow field from schema.

    Returns:
        True if the field type supports meaningful statistics.
    """
    import pyarrow as pa

    # Types that don't support meaningful min/max statistics
    incompatible_types = (
        pa.types.is_binary,
        pa.types.is_large_binary,
        pa.types.is_struct,
        pa.types.is_list,
        pa.types.is_large_list,
        pa.types.is_map,
        pa.types.is_nested,
    )

    return not any(check(field.type) for check in incompatible_types)


def extract_parquet_statistics(path: Path) -> dict[str, ColumnStatistics]:
    """Extract column statistics from Parquet file metadata.

    Reads only the file footer (~8KB), not actual data.
    Works on 10GB+ files instantly.

    Limitations:
    - Only min/max/null_count (no mean/stddev without DuckDB)
    - Statistics must be present in file (write-time setting)
    - Geometry columns (binary) are skipped - no meaningful min/max

    Args:
        path: Path to Parquet file.

    Returns:
        Dict mapping column name to ColumnStatistics.
        Binary/geometry columns are excluded.
    """
    count("parquet.opens")
    pf = pq.ParquetFile(path)
    schema = pf.schema_arrow
    num_row_groups = pf.metadata.num_row_groups

    stats: dict[str, ColumnStatistics] = {}

    for col_idx, field in enumerate(schema):
        col_name = field.name

        # Skip binary/geometry columns - no meaningful statistics
        if not _is_stats_compatible_type(field):
            continue

        global_min = None
        global_max = None
        total_nulls = 0

        for rg_idx in range(num_row_groups):
            rg = pf.metadata.row_group(rg_idx)
            col_chunk = rg.column(col_idx)
            col_stats = col_chunk.statistics

            if col_stats is not None and col_stats.has_min_max:
                if global_min is None or col_stats.min < global_min:
                    global_min = col_stats.min
                if global_max is None or col_stats.max > global_max:
                    global_max = col_stats.max
                total_nulls += col_stats.null_count

        stats[col_name] = ColumnStatistics(
            name=col_name,
            min_value=global_min,
            max_value=global_max,
            null_count=total_nulls,
        )

    return stats
