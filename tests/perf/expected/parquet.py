"""Scale-gate constants of parquet: Parquet and raster opens, DuckDB connections. See gates.py."""

from __future__ import annotations

# fmt: off
EXPECTED: dict[tuple[str, str], tuple[int, int]] = {
    ("add-large-vector", "duckdb.connects"): (0, 5),  # preparation:_run x3, convert:apply_vector_settings x2
    ("add-large-vector", "parquet.opens"): (0, 6),  # metadata.geoparquet:_read_geoparquet_metadata x3, metadata.geoparquet:read_rewrite_fidelity x2
    ("add-many-collections", "duckdb.connects"): (15, 0),  # preparation:_run x18, convert:apply_vector_settings x12
    ("add-many-collections", "parquet.opens"): (18, 0),  # metadata.geoparquet:_read_geoparquet_metadata x18, metadata.geoparquet:read_rewrite_fidelity x12
    ("add-many-collections-thumbs", "duckdb.connects"): (10, 0),  # preparation:_run x12, convert:apply_vector_settings x8
    ("add-many-collections-thumbs", "parquet.opens"): (12, 0),  # metadata.geoparquet:_read_geoparquet_metadata x12, metadata.geoparquet:read_rewrite_fidelity x8
    ("add-one-collection", "duckdb.connects"): (15, 0),  # preparation:_run x18, convert:apply_vector_settings x12
    ("add-one-collection", "parquet.opens"): (18, 0),  # metadata.geoparquet:_read_geoparquet_metadata x18, metadata.geoparquet:read_rewrite_fidelity x12
    ("add-rasters", "raster.opens"): (9, 0),  # metadata.statistics:embedded_statistics x6, metadata.cog:extract_cog_metadata x6
    ("add-with-companions", "duckdb.connects"): (21, 0),  # preparation:_run x30, convert:apply_vector_settings x12
    ("add-with-companions", "parquet.opens"): (21, 0),  # metadata.geoparquet:_read_geoparquet_metadata x24, metadata.geoparquet:read_rewrite_fidelity x12
}
# fmt: on
