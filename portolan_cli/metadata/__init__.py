"""Metadata extraction and validation for cloud-native geospatial formats."""

from __future__ import annotations

from typing import TYPE_CHECKING

from portolan_cli.lazy_exports import lazy_exports

if TYPE_CHECKING:
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
    from portolan_cli.metadata.fix import (
        FixAction,
        FixReport,
        FixResult,
        fix_metadata,
    )
    from portolan_cli.metadata.flatgeobuf import (
        FlatGeobufMetadata,
        extract_flatgeobuf_metadata,
    )
    from portolan_cli.metadata.geoparquet import (
        GeoParquetMetadata,
        RewriteFidelity,
        SpatialLayout,
        extract_geoparquet_metadata,
        read_rewrite_fidelity,
        read_spatial_layout,
    )
    from portolan_cli.metadata.models import (
        FileMetadataState,
        MetadataCheckResult,
        MetadataReport,
        MetadataStatus,
    )
    from portolan_cli.metadata.pmtiles import (
        PMTilesMetadata,
        extract_pmtiles_metadata,
    )
    from portolan_cli.metadata.statistics import (
        BandStatistics,
        ColumnStatistics,
        extract_band_statistics,
        extract_parquet_statistics,
    )
    from portolan_cli.metadata.tabular import (
        TabularMetadata,
        extract_tabular_metadata,
    )
    from portolan_cli.metadata.update import (
        prepare_missing_item,
        update_item_metadata,
        update_versions_tracking,
    )

__all__ = [
    # Statistics
    "BandStatistics",
    # Extraction
    "COGMetadata",
    "ColumnStatistics",
    # Check models
    "FileMetadataState",
    # Fix functions
    "FixAction",
    "FixReport",
    "FixResult",
    "FlatGeobufMetadata",
    "GeoParquetMetadata",
    "MetadataCheckResult",
    "MetadataReport",
    "MetadataStatus",
    "PMTilesMetadata",
    "RewriteFidelity",
    "SpatialLayout",
    # Detection
    "StoredMetadata",
    "TabularMetadata",
    "check_file_metadata",
    "compute_schema_fingerprint",
    "detect_changes",
    "extract_band_statistics",
    "extract_cog_metadata",
    "extract_flatgeobuf_metadata",
    "extract_geoparquet_metadata",
    "extract_parquet_statistics",
    "extract_pmtiles_metadata",
    "extract_tabular_metadata",
    "fix_metadata",
    "get_current_metadata",
    "get_stored_metadata",
    "is_stale",
    # Update functions
    "prepare_missing_item",
    "read_rewrite_fidelity",
    "read_spatial_layout",
    "update_item_metadata",
    "update_versions_tracking",
]

# Lazy exports keep `portolan --help` from importing rasterio, pyogrio, and pandas.
_EXPORTS = {
    "COGMetadata": "portolan_cli.metadata.cog",
    "extract_cog_metadata": "portolan_cli.metadata.cog",
    "StoredMetadata": "portolan_cli.metadata.detection",
    "check_file_metadata": "portolan_cli.metadata.detection",
    "compute_schema_fingerprint": "portolan_cli.metadata.detection",
    "detect_changes": "portolan_cli.metadata.detection",
    "get_current_metadata": "portolan_cli.metadata.detection",
    "get_stored_metadata": "portolan_cli.metadata.detection",
    "is_stale": "portolan_cli.metadata.detection",
    "FixAction": "portolan_cli.metadata.fix",
    "FixReport": "portolan_cli.metadata.fix",
    "FixResult": "portolan_cli.metadata.fix",
    "fix_metadata": "portolan_cli.metadata.fix",
    "FlatGeobufMetadata": "portolan_cli.metadata.flatgeobuf",
    "extract_flatgeobuf_metadata": "portolan_cli.metadata.flatgeobuf",
    "GeoParquetMetadata": "portolan_cli.metadata.geoparquet",
    "RewriteFidelity": "portolan_cli.metadata.geoparquet",
    "SpatialLayout": "portolan_cli.metadata.geoparquet",
    "extract_geoparquet_metadata": "portolan_cli.metadata.geoparquet",
    "read_rewrite_fidelity": "portolan_cli.metadata.geoparquet",
    "read_spatial_layout": "portolan_cli.metadata.geoparquet",
    "FileMetadataState": "portolan_cli.metadata.models",
    "MetadataCheckResult": "portolan_cli.metadata.models",
    "MetadataReport": "portolan_cli.metadata.models",
    "MetadataStatus": "portolan_cli.metadata.models",
    "PMTilesMetadata": "portolan_cli.metadata.pmtiles",
    "extract_pmtiles_metadata": "portolan_cli.metadata.pmtiles",
    "BandStatistics": "portolan_cli.metadata.statistics",
    "ColumnStatistics": "portolan_cli.metadata.statistics",
    "extract_band_statistics": "portolan_cli.metadata.statistics",
    "extract_parquet_statistics": "portolan_cli.metadata.statistics",
    "TabularMetadata": "portolan_cli.metadata.tabular",
    "extract_tabular_metadata": "portolan_cli.metadata.tabular",
    "prepare_missing_item": "portolan_cli.metadata.update",
    "update_item_metadata": "portolan_cli.metadata.update",
    "update_versions_tracking": "portolan_cli.metadata.update",
}

__getattr__, __dir__ = lazy_exports(__name__, _EXPORTS, __all__, globals())
