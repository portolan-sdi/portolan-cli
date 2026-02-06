"""Metadata extraction for cloud-native geospatial formats.

This module provides functions to extract metadata from GeoParquet and COG files:
- extract_geoparquet_metadata(): Extract from GeoParquet
- extract_cog_metadata(): Extract from COG (future)

Metadata includes: bbox, CRS, schema, feature/pixel count.
"""

from portolan_cli.metadata.geoparquet import (
    GeoParquetMetadata,
    extract_geoparquet_metadata,
)

__all__ = [
    "GeoParquetMetadata",
    "extract_geoparquet_metadata",
]
