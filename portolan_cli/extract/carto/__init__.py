"""Carto extract functionality.

Handles extraction of vector tables from Carto SQL API accounts into Portolan
catalogs. geoparquet-io performs the per-table extraction; this package adds
table discovery (CDB_UserTables), filtering, reporting, and catalog auto-init.
"""

from __future__ import annotations

from portolan_cli.extract.carto.discovery import (
    CartoDiscoveryError,
    CartoDiscoveryResult,
    CartoTableInfo,
    discover_carto_tables,
    normalize_sql_api_url,
    tables_from_names,
)
from portolan_cli.extract.carto.metadata import CartoMetadata, extract_carto_metadata
from portolan_cli.extract.carto.orchestrator import (
    ExtractionOptions,
    ExtractionProgress,
    extract_carto_catalog,
)
from portolan_cli.extract.common.filters import filter_layers
from portolan_cli.extract.common.report import (
    ExtractionReport,
    ExtractionSummary,
    LayerResult,
    MetadataExtracted,
    load_report,
    save_report,
)
from portolan_cli.extract.common.resume import (
    ResumeState,
    get_resume_state,
    should_process_layer,
)
from portolan_cli.extract.common.retry import (
    RetryConfig,
    RetryError,
    RetryResult,
    retry_with_backoff,
)

__all__ = [
    # Discovery
    "CartoDiscoveryError",
    "CartoDiscoveryResult",
    "CartoTableInfo",
    "discover_carto_tables",
    "normalize_sql_api_url",
    "tables_from_names",
    # Metadata
    "CartoMetadata",
    "extract_carto_metadata",
    # Orchestrator
    "ExtractionOptions",
    "ExtractionProgress",
    "extract_carto_catalog",
    # Filters (from common)
    "filter_layers",
    # Report models (from common)
    "ExtractionReport",
    "ExtractionSummary",
    "LayerResult",
    "MetadataExtracted",
    "load_report",
    "save_report",
    # Resume (from common)
    "ResumeState",
    "get_resume_state",
    "should_process_layer",
    # Retry (from common)
    "RetryConfig",
    "RetryError",
    "RetryResult",
    "retry_with_backoff",
]
