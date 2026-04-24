"""Common utilities for extraction backends.

This module provides shared functionality used across all extraction
backends (ArcGIS, WFS, etc.):

- filters: Glob-based filtering for services/layers
- metadata_seeding: Collection-level metadata seeding
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
