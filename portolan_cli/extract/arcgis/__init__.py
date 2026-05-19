"""ArcGIS extract functionality.

This module handles extraction of vector data from ArcGIS FeatureServer/MapServer
endpoints into Portolan catalogs.
"""

from __future__ import annotations

from portolan_cli.extract.arcgis.discovery import (
    ArcGISDiscoveryError,
    LayerInfo,
    ServiceDiscoveryResult,
    ServiceInfo,
    discover_layers,
    discover_services,
    fetch_layer_details,
)
from portolan_cli.extract.arcgis.metadata import ArcGISMetadata, extract_arcgis_metadata
from portolan_cli.extract.arcgis.orchestrator import (
    ExtractionOptions,
    ExtractionProgress,
    extract_arcgis_catalog,
)
from portolan_cli.extract.arcgis.url_parser import (
    ArcGISURLType,
    InvalidArcGISURLError,
    ParsedArcGISURL,
    parse_arcgis_url,
)
from portolan_cli.extract.common.filters import (
    apply_unified_filter,
    filter_layers,
    filter_services,
)
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
    "ArcGISDiscoveryError",
    "LayerInfo",
    "ServiceDiscoveryResult",
    "ServiceInfo",
    "discover_layers",
    "discover_services",
    "fetch_layer_details",
    # Filters
    "apply_unified_filter",
    "filter_layers",
    "filter_services",
    # Metadata
    "ArcGISMetadata",
    "extract_arcgis_metadata",
    # Report models
    "ExtractionReport",
    "ExtractionSummary",
    "LayerResult",
    "MetadataExtracted",
    # Report I/O
    "load_report",
    "save_report",
    # Resume logic
    "ResumeState",
    "get_resume_state",
    "should_process_layer",
    # Retry
    "RetryConfig",
    "RetryError",
    "RetryResult",
    "retry_with_backoff",
    # Orchestrator
    "ExtractionOptions",
    "ExtractionProgress",
    "extract_arcgis_catalog",
    # URL parsing
    "ArcGISURLType",
    "InvalidArcGISURLError",
    "ParsedArcGISURL",
    "parse_arcgis_url",
]
