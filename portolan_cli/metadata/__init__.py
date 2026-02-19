"""Metadata extraction and validation for cloud-native geospatial formats.

This module provides:

Extraction functions:
- extract_geoparquet_metadata(): Extract from GeoParquet
- extract_cog_metadata(): Extract from COG

Detection functions:
- get_stored_metadata(): Read existing STAC item + versions.json
- get_current_metadata(): Extract fresh metadata from file
- is_stale(): MTIME check + heuristic fallback
- detect_changes(): Return list of what changed
- check_file_metadata(): Return MetadataCheckResult for single file
- compute_schema_fingerprint(): Generate hash of file schema

Validation functions:
- check_directory_metadata(): Scan directory for metadata issues
- validate_catalog_links(): Verify catalog links point to existing files
- validate_collection_extent(): Verify collection bbox contains all items

Update functions:
- update_item_metadata(): Re-extract and update existing STAC item
- create_missing_item(): Create new STAC item for file without metadata
- update_collection_extent(): Recalculate extent from child items
- update_versions_tracking(): Update source_mtime in versions.json

Check data structures:
- MetadataStatus: Enum for file metadata states (MISSING, FRESH, STALE, BREAKING)
- FileMetadataState: Holds current vs stored metadata for comparison
- MetadataCheckResult: Per-file validation result with status and fix hints
- MetadataReport: Aggregate report with counts and issue lists
- StoredMetadata: Bridges STAC item and versions.json data

Metadata includes: bbox, CRS, schema/bands, feature/pixel count.
"""

from portolan_cli.metadata.cog import (
    COGMetadata,
    extract_cog_metadata,
)
from portolan_cli.metadata.detection import (
    StoredMetadata,
    check_file_metadata,
    compute_schema_fingerprint,
    detect_changes,
    get_current_metadata,
    get_stored_metadata,
    is_stale,
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
from portolan_cli.metadata.validation import (
    ValidationMessage,
    ValidationResult,
    check_directory_metadata,
    validate_catalog_links,
    validate_collection_extent,
)

__all__ = [
    # Extraction
    "COGMetadata",
    "GeoParquetMetadata",
    "extract_cog_metadata",
    "extract_geoparquet_metadata",
    # Detection
    "StoredMetadata",
    "check_file_metadata",
    "compute_schema_fingerprint",
    "detect_changes",
    "get_current_metadata",
    "get_stored_metadata",
    "is_stale",
    # Validation
    "ValidationMessage",
    "ValidationResult",
    "check_directory_metadata",
    "validate_catalog_links",
    "validate_collection_extent",
    # Update functions
    "create_missing_item",
    "update_collection_extent",
    "update_item_metadata",
    "update_versions_tracking",
    # Check models
    "FileMetadataState",
    "MetadataCheckResult",
    "MetadataReport",
    "MetadataStatus",
]
