"""WFS extract functionality.

This module handles extraction of vector data from OGC WFS (Web Feature Service)
endpoints into Portolan catalogs.
"""

from __future__ import annotations

from portolan_cli.extract.common import SHARED_EXPORTS
from portolan_cli.lazy_exports import lazy_exports

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

# Each name loads on first access. The extract command imports this package,
# and its help must not pay for the HTTP clients that discovery loads.
_EXPORTS: dict[str, str] = {
    **{name: SHARED_EXPORTS[name] for name in __all__ if name in SHARED_EXPORTS},
    "ExtractionOptions": "portolan_cli.extract.wfs.orchestrator",
    "LayerInfo": "portolan_cli.extract.wfs.discovery",
    "WFSDiscoveryError": "portolan_cli.extract.wfs.discovery",
    "WFSDiscoveryResult": "portolan_cli.extract.wfs.discovery",
    "WFSMetadata": "portolan_cli.extract.wfs.metadata",
    "discover_layers": "portolan_cli.extract.wfs.discovery",
    "extract_wfs_catalog": "portolan_cli.extract.wfs.orchestrator",
    "extract_wfs_metadata": "portolan_cli.extract.wfs.metadata",
    "list_layers": "portolan_cli.extract.wfs.discovery",
}

__getattr__, __dir__ = lazy_exports(__name__, _EXPORTS, __all__, globals())
