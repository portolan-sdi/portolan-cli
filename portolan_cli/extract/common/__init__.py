"""Common utilities for extraction backends.

This module provides shared functionality used across all extraction
backends (ArcGIS, WFS, etc.):

- filters: Glob-based filtering for services/layers
- metadata_seeding: Collection-level metadata seeding
- orchestrator_base: Shared post-extraction catalog lifecycle
- report: Extraction report models (LayerResult, ExtractionReport)
- retry: Retry logic with exponential backoff
- resume: Resume state for interrupted extractions
"""

from __future__ import annotations

from portolan_cli.extract.common.filters import (
    apply_unified_filter,
    filter_layers,
    filter_services,
)
from portolan_cli.extract.common.metadata_seeding import (
    seed_collection_metadata,
)
from portolan_cli.extract.common.orchestrator_base import (
    add_source_links,
    collect_successful_parquet_files,
    init_extracted_catalog,
    register_collection_styles,
    seed_catalog_metadata,
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
    # filters
    "apply_unified_filter",
    "filter_layers",
    "filter_services",
    # metadata_seeding
    "seed_collection_metadata",
    # orchestrator_base
    "add_source_links",
    "collect_successful_parquet_files",
    "init_extracted_catalog",
    "register_collection_styles",
    "seed_catalog_metadata",
    # report
    "ExtractionReport",
    "ExtractionSummary",
    "LayerResult",
    "MetadataExtracted",
    "load_report",
    "save_report",
    # resume
    "ResumeState",
    "get_resume_state",
    "should_process_layer",
    # retry
    "RetryConfig",
    "RetryError",
    "RetryResult",
    "retry_with_backoff",
]


# The common names that the arcgis, carto, and wfs packages re-export, by the
# module that defines each one. Each source package loads them on first access.
SHARED_EXPORTS: dict[str, str] = {
    "ExtractionProgress": "portolan_cli.extract.common.progress",
    "ExtractionReport": "portolan_cli.extract.common.report",
    "ExtractionSummary": "portolan_cli.extract.common.report",
    "LayerResult": "portolan_cli.extract.common.report",
    "MetadataExtracted": "portolan_cli.extract.common.report",
    "ResumeState": "portolan_cli.extract.common.resume",
    "RetryConfig": "portolan_cli.extract.common.retry",
    "RetryError": "portolan_cli.extract.common.retry",
    "RetryResult": "portolan_cli.extract.common.retry",
    "apply_unified_filter": "portolan_cli.extract.common.filters",
    "filter_layers": "portolan_cli.extract.common.filters",
    "filter_services": "portolan_cli.extract.common.filters",
    "get_resume_state": "portolan_cli.extract.common.resume",
    "load_report": "portolan_cli.extract.common.report",
    "retry_with_backoff": "portolan_cli.extract.common.retry",
    "save_report": "portolan_cli.extract.common.report",
    "should_process_layer": "portolan_cli.extract.common.resume",
}
