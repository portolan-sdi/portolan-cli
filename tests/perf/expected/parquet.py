"""Scale-gate constants of parquet: Parquet, raster, and vector opens, DuckDB connections. See gates.py."""

from __future__ import annotations

# fmt: off
EXPECTED: dict[tuple[str, str], tuple[int, int]] = {
    ("add-large-vector", "duckdb.connects"): (0, 5),  # preparation:_run x3, convert:apply_vector_settings x2
    ("add-large-vector", "parquet.opens"): (0, 8),  # formats:read_geo_metadata x5, metadata.geoparquet:open_footer x2
    ("add-many-collections", "duckdb.connects"): (15, 0),  # preparation:_run x18, convert:apply_vector_settings x12
    ("add-many-collections", "parquet.opens"): (24, 0),  # formats:read_geo_metadata x30, metadata.geoparquet:open_footer x12
    ("add-many-collections-thumbs", "duckdb.connects"): (10, 0),  # preparation:_run x12, convert:apply_vector_settings x8
    ("add-many-collections-thumbs", "parquet.opens"): (22, 0),  # formats:read_geo_metadata x28, metadata.geoparquet:open_footer x8
    ("add-one-collection", "duckdb.connects"): (15, 0),  # preparation:_run x18, convert:apply_vector_settings x12
    ("add-one-collection", "parquet.opens"): (24, 0),  # formats:read_geo_metadata x30, metadata.geoparquet:open_footer x12
    ("add-rasters", "raster.opens"): (18, 0),  # metadata.statistics:embedded_statistics x6, conversion_config:derive_cog_defaults x6
    ("add-with-companions", "duckdb.connects"): (21, 0),  # preparation:_run x30, convert:apply_vector_settings x12
    ("add-with-companions", "parquet.opens"): (30, 0),  # formats:read_geo_metadata x36, metadata.geoparquet:_read_geoparquet_metadata x12
    ("check-fix-geo-assets", "duckdb.connects"): (15, 0),  # preparation:_run x18, convert:apply_vector_settings x12
    ("check-fix-geo-assets", "parquet.opens"): (6, 0),  # formats:read_geo_metadata x12
}
# fmt: on
