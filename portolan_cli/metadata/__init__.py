"""Metadata extraction for cloud-native geospatial formats.

This module provides functions to extract metadata from GeoParquet and COG files:
- extract_geoparquet_metadata(): Extract from GeoParquet
- extract_cog_metadata(): Extract from COG

Metadata includes: bbox, CRS, schema/bands, feature/pixel count.
"""

from portolan_cli.metadata.cog import (
    COGMetadata,
    extract_cog_metadata,
)
from portolan_cli.metadata.geoparquet import (
    GeoParquetMetadata,
    extract_geoparquet_metadata,
)

__all__ = [
    "COGMetadata",
    "GeoParquetMetadata",
    "extract_cog_metadata",
    "extract_geoparquet_metadata",
]
