"""Carto extract functionality.

Handles extraction of vector tables from Carto SQL API accounts into Portolan
catalogs. geoparquet-io performs the per-table extraction; this package adds
table discovery (CDB_UserTables), filtering, reporting, and catalog auto-init.
"""

from __future__ import annotations

from portolan_cli.extract.common import SHARED_EXPORTS
from portolan_cli.lazy_exports import lazy_exports

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

# Each name loads on first access. The extract command imports this package,
# and its help must not pay for the HTTP clients that discovery loads.
_EXPORTS: dict[str, str] = {
    **{name: SHARED_EXPORTS[name] for name in __all__ if name in SHARED_EXPORTS},
    "CartoDiscoveryError": "portolan_cli.extract.carto.discovery",
    "CartoDiscoveryResult": "portolan_cli.extract.carto.discovery",
    "CartoMetadata": "portolan_cli.extract.carto.metadata",
    "CartoTableInfo": "portolan_cli.extract.carto.discovery",
    "ExtractionOptions": "portolan_cli.extract.carto.orchestrator",
    "discover_carto_tables": "portolan_cli.extract.carto.discovery",
    "extract_carto_catalog": "portolan_cli.extract.carto.orchestrator",
    "extract_carto_metadata": "portolan_cli.extract.carto.metadata",
    "normalize_sql_api_url": "portolan_cli.extract.carto.discovery",
    "tables_from_names": "portolan_cli.extract.carto.discovery",
}

__getattr__, __dir__ = lazy_exports(__name__, _EXPORTS, __all__, globals())
