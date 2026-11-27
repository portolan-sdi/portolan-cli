"""Spatial utilities for geohash partitioning and bbox computation."""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pygeohash as pgh
import shapely

_PARTITION_MIN_ROWS = 100_000
_COARSE_PARTITION_MAX_ROWS = 10_000_000


def detect_geohash_precision(row_count: int) -> int | None:
    """Determine geohash precision based on data size."""
    if row_count < _PARTITION_MIN_ROWS:
        return None
    if row_count < _COARSE_PARTITION_MAX_ROWS:
        return 3
    return 4


def _find_geometry_column(table: pa.Table) -> str | None:
    """Find the geometry column in a PyArrow table.

    Looks for columns named 'geometry' or 'geom' with binary type.
    """
    for name in ("geometry", "geom"):
        if name in table.column_names:
            col = table.column(name)
            if pa.types.is_binary(col.type) or pa.types.is_large_binary(col.type):
                return name
    return None


def compute_geohash_column(table: pa.Table, precision: int = 4) -> pa.Table:
    """Add a geohash_{precision} column computed from geometry centroids.

    Uses vectorized shapely operations for WKB parsing and centroid computation.
    """
    col_name = _find_geometry_column(table)
    if col_name is None:
        return table

    geoms, present = _geometries(table, col_name)
    # get_coordinates skips missing and empty geometries, so encode only the
    # rows that have one and write null for the rest.
    coords = shapely.get_coordinates(shapely.centroid(geoms[present]))
    # pygeohash.encode is per-element (C extension, still fast)
    encoded = iter([pgh.encode(y, x, precision=precision) for x, y in coords])
    geohashes = [next(encoded) if has_geometry else None for has_geometry in present]

    geohash_col_name = f"geohash_{precision}"
    return table.append_column(geohash_col_name, pa.array(geohashes, type=pa.string()))


def compute_bbox_columns(table: pa.Table) -> pa.Table:
    """Add per-row bbox columns (xmin, ymin, xmax, ymax) from geometry bounds.

    Uses vectorized shapely.bounds for the entire array at once.
    """
    col_name = _find_geometry_column(table)
    if col_name is None:
        return table

    geoms, present = _geometries(table, col_name)
    bounds = shapely.bounds(geoms)  # (N, 4) array: xmin, ymin, xmax, ymax
    # A row without geometry has NaN bounds. Write null, which a spatial
    # filter on the table skips.
    for index, name in enumerate(("bbox_xmin", "bbox_ymin", "bbox_xmax", "bbox_ymax")):
        column = pa.array(bounds[:, index], type=pa.float64(), mask=~present)
        table = table.append_column(name, column)
    return table


def _geometries(table: pa.Table, col_name: str) -> tuple[Any, Any]:
    """The parsed geometries and a mask of the rows that hold a non-empty one."""
    geoms = shapely.from_wkb(table.column(col_name).to_pylist())
    present = ~(shapely.is_missing(geoms) | shapely.is_empty(geoms))
    return geoms, present


def add_spatial_columns(table: pa.Table, precision: int | None = None) -> pa.Table:
    """Add geohash and bbox columns if the table has geometry."""
    if _find_geometry_column(table) is None:
        return table

    if precision is None:
        precision = detect_geohash_precision(len(table))

    # Always add bbox columns for manifest statistics
    table = compute_bbox_columns(table)

    # Add geohash column (use default precision 4 for small data,
    # detected precision for larger ones)
    geohash_precision = precision if precision is not None else 4
    return compute_geohash_column(table, precision=geohash_precision)
