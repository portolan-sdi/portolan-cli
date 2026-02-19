"""Metadata extraction and validation for cloud-native geospatial formats.

This module provides:

Extraction functions:
- extract_geoparquet_metadata(): Extract from GeoParquet
- extract_cog_metadata(): Extract from COG

Check data structures:
- MetadataStatus: Enum for file metadata states (MISSING, FRESH, STALE, BREAKING)
- FileMetadataState: Holds current vs stored metadata for comparison
- MetadataCheckResult: Per-file validation result with status and fix hints
- MetadataReport: Aggregate report with counts and issue lists

Update functions:
- update_item_metadata(): Re-extract and update existing STAC item
- create_missing_item(): Create new STAC item for file without metadata
- update_collection_extent(): Recalculate extent from child items
- update_versions_tracking(): Update source_mtime in versions.json

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
from portolan_cli.metadata.models import (
    FileMetadataState,
    MetadataCheckResult,
    MetadataReport,
    MetadataStatus,
)
from portolan_cli.metadata.update import (
    create_missing_item,
    update_collection_extent,
    update_item_metadata,
    update_versions_tracking,
)

__all__ = [
    # Extraction
    "COGMetadata",
    "GeoParquetMetadata",
    "extract_cog_metadata",
    "extract_geoparquet_metadata",
    # Check models
    "FileMetadataState",
    "MetadataCheckResult",
    "MetadataReport",
    "MetadataStatus",
    # Update functions
    "create_missing_item",
    "update_collection_extent",
    "update_item_metadata",
    "update_versions_tracking",
]
