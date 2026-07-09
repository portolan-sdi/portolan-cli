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
