"""Scale-gate constants of parquet: Parquet, raster, and vector opens, DuckDB connections. See gates.py."""

from __future__ import annotations

# fmt: off
EXPECTED: dict[tuple[str, str], tuple[int, int]] = {
    ("add-large-vector", "duckdb.connects"): (0, 5),  # preparation:_run x3, convert:apply_vector_settings x2
    ("add-large-vector", "parquet.opens"): (0, 8),  # preparation:_run x6, formats:_read_footer x2
    ("add-many-collections", "duckdb.connects"): (15, 0),  # preparation:_run x18, convert:apply_vector_settings x12
    ("add-many-collections", "parquet.opens"): (24, 0),  # preparation:_run x36, formats:_read_footer x12
    ("add-many-collections-thumbs", "duckdb.connects"): (10, 0),  # preparation:_run x12, convert:apply_vector_settings x8
    ("add-many-collections-thumbs", "parquet.opens"): (18, 0),  # preparation:_run x24, formats:_read_footer x8
    ("add-one-collection", "duckdb.connects"): (15, 0),  # preparation:_run x18, convert:apply_vector_settings x12
    ("add-one-collection", "parquet.opens"): (24, 0),  # preparation:_run x36, formats:_read_footer x12
    ("add-rasters", "duckdb.connects"): (0, 1),  # stac_parquet:_spatially_ordered x1
    ("add-rasters", "raster.opens"): (15, 0),  # preparation:convert_raster x6, convert:copy_to_cog x6
    ("add-with-companions", "duckdb.connects"): (21, 0),  # preparation:_run x30, convert:apply_vector_settings x12
    ("add-with-companions", "parquet.opens"): (27, 0),  # preparation:_run x36, formats:_read_footer x18
    ("check-fix-geo-assets", "duckdb.connects"): (15, 0),  # preparation:_run x18, convert:apply_vector_settings x12
    ("check-fix-geo-assets", "parquet.opens"): (3, 0),  # formats:_read_footer x6
    ("check-fix-missing-rasters", "raster.opens"): (3, 0),  # preparation:convert_raster x6
}
# fmt: on
