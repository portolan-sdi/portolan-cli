"""Scale-gate constants of parquet: Parquet and raster opens, DuckDB connections. See gates.py."""

from __future__ import annotations

# fmt: off
EXPECTED: dict[tuple[str, str], tuple[int, int]] = {
    ("add-large-vector", "duckdb.connects"): (0, 5),  # preparation:_run x3, convert:apply_vector_settings x2
    ("add-large-vector", "parquet.opens"): (0, 7),  # metadata.geoparquet:extract_geoparquet_metadata x3, metadata.geoparquet:read_rewrite_fidelity x2
    ("add-many-collections", "duckdb.connects"): (15, 0),  # preparation:_run x18, convert:apply_vector_settings x12
    ("add-many-collections", "parquet.opens"): (21, 0),  # metadata.geoparquet:extract_geoparquet_metadata x18, metadata.geoparquet:read_rewrite_fidelity x12
    ("add-many-collections-thumbs", "duckdb.connects"): (10, 0),  # preparation:_run x12, convert:apply_vector_settings x8
    ("add-many-collections-thumbs", "parquet.opens"): (14, 0),  # metadata.geoparquet:extract_geoparquet_metadata x12, metadata.geoparquet:read_rewrite_fidelity x8
    ("add-one-collection", "duckdb.connects"): (15, 0),  # preparation:_run x18, convert:apply_vector_settings x12
    ("add-one-collection", "parquet.opens"): (21, 0),  # metadata.geoparquet:extract_geoparquet_metadata x18, metadata.geoparquet:read_rewrite_fidelity x12
    ("add-rasters", "raster.opens"): (6, 0),  # metadata.cog:extract_cog_metadata x6, metadata.statistics:extract_band_statistics x6
    ("add-with-companions", "duckdb.connects"): (21, 0),  # preparation:_run x30, convert:apply_vector_settings x12
    ("add-with-companions", "parquet.opens"): (24, 0),  # metadata.geoparquet:extract_geoparquet_metadata x24, metadata.geoparquet:read_rewrite_fidelity x12
    ("check-metadata", "parquet.opens"): (0, 2),  # metadata.geoparquet:extract_geoparquet_metadata x1, metadata.detection:compute_schema_fingerprint x1
    ("check-metadata", "raster.opens"): (3, 0),  # metadata.cog:extract_cog_metadata x6
}
# fmt: on

#: The tracemalloc peak of ``portolan.add`` for one file of 1,000,000 polygons,
#: in MB: 9.45 measured plus 15%. tracemalloc sees the Python heap and numpy,
#: not Arrow or DuckDB buffers. The same run peaks at 1,532 MB RSS, which the
#: nightly scenario job gates.
LARGE_VECTOR_TRACEMALLOC_MB = 10.9
