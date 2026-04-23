"""WFS extract functionality.

This module handles extraction of vector data from OGC WFS (Web Feature Service)
endpoints into Portolan catalogs.
"""

from __future__ import annotations

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
from portolan_cli.extract.wfs.discovery import (
    LayerInfo,
    WFSDiscoveryError,
    WFSDiscoveryResult,
    discover_layers,
    list_layers,
)
from portolan_cli.extract.wfs.metadata import (
    WFSMetadata,
    extract_wfs_metadata,
)
from portolan_cli.extract.wfs.orchestrator import (
    ExtractionOptions,
    ExtractionProgress,
    extract_wfs_catalog,
)

__all__ = [
    # Discovery
    "LayerInfo",
    "WFSDiscoveryError",
    "WFSDiscoveryResult",
    "discover_layers",
    "list_layers",
    # Metadata
    "WFSMetadata",
    "extract_wfs_metadata",
    # Orchestrator
    "ExtractionOptions",
    "ExtractionProgress",
    "extract_wfs_catalog",
    # Filters (from common)
    "apply_unified_filter",
    "filter_layers",
    "filter_services",
    # Report models (from common)
    "ExtractionReport",
    "ExtractionSummary",
    "LayerResult",
    "MetadataExtracted",
    # Report I/O (from common)
    "load_report",
    "save_report",
    # Resume logic (from common)
    "ResumeState",
    "get_resume_state",
    "should_process_layer",
    # Retry (from common)
    "RetryConfig",
    "RetryError",
    "RetryResult",
    "retry_with_backoff",
]
