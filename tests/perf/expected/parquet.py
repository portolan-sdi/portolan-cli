"""Scale-gate constants of parquet: Parquet, raster, and vector opens, DuckDB connections. See gates.py."""

from __future__ import annotations

# fmt: off
EXPECTED: dict[tuple[str, str], tuple[int, int]] = {
    ("add-large-vector", "duckdb.connects"): (0, 5),  # preparation:_run x3, convert:apply_vector_settings x2
    ("add-large-vector", "parquet.opens"): (0, 2),  # formats:_read_footer x2
    ("add-many-collections", "duckdb.connects"): (15, 0),  # preparation:_run x18, convert:apply_vector_settings x12
    ("add-many-collections", "parquet.opens"): (6, 0),  # formats:_read_footer x12
    ("add-many-collections-thumbs", "duckdb.connects"): (10, 0),  # preparation:_run x12, convert:apply_vector_settings x8
    ("add-many-collections-thumbs", "parquet.opens"): (6, 0),  # formats:_read_footer x8, viz.thumbnail:_read_geoparquet_for_thumbnail x4
    ("add-one-collection", "duckdb.connects"): (15, 0),  # preparation:_run x18, convert:apply_vector_settings x12
    ("add-one-collection", "parquet.opens"): (6, 0),  # formats:_read_footer x12
    ("add-rasters", "raster.opens"): (9, 0),  # preparation:convert_raster x6, convert:place_cog x6
    ("add-with-companions", "duckdb.connects"): (21, 0),  # preparation:_run x30, convert:apply_vector_settings x12
    ("add-with-companions", "parquet.opens"): (9, 0),  # formats:_read_footer x18
    ("check-fix-geo-assets", "duckdb.connects"): (15, 0),  # preparation:_run x18, convert:apply_vector_settings x12
    ("check-fix-geo-assets", "parquet.opens"): (3, 0),  # formats:_read_footer x6
}
# fmt: on
