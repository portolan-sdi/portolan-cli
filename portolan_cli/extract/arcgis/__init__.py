"""ArcGIS extract functionality.

This module handles extraction of vector data from ArcGIS FeatureServer/MapServer
endpoints into Portolan catalogs.
"""

from __future__ import annotations

from portolan_cli.extract.common import SHARED_EXPORTS
from portolan_cli.lazy_exports import lazy_exports

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

# Each name loads on first access. The extract command imports this package,
# and its help must not pay for the HTTP clients that discovery loads.
_EXPORTS: dict[str, str] = {
    **{name: SHARED_EXPORTS[name] for name in __all__ if name in SHARED_EXPORTS},
    "ArcGISDiscoveryError": "portolan_cli.extract.arcgis.discovery",
    "ArcGISMetadata": "portolan_cli.extract.arcgis.metadata",
    "ArcGISURLType": "portolan_cli.extract.arcgis.url_parser",
    "ExtractionOptions": "portolan_cli.extract.arcgis.orchestrator",
    "InvalidArcGISURLError": "portolan_cli.extract.arcgis.url_parser",
    "LayerInfo": "portolan_cli.extract.arcgis.discovery",
    "ParsedArcGISURL": "portolan_cli.extract.arcgis.url_parser",
    "ServiceDiscoveryResult": "portolan_cli.extract.arcgis.discovery",
    "ServiceInfo": "portolan_cli.extract.arcgis.discovery",
    "discover_layers": "portolan_cli.extract.arcgis.discovery",
    "discover_services": "portolan_cli.extract.arcgis.discovery",
    "extract_arcgis_catalog": "portolan_cli.extract.arcgis.orchestrator",
    "extract_arcgis_metadata": "portolan_cli.extract.arcgis.metadata",
    "fetch_layer_details": "portolan_cli.extract.arcgis.discovery",
    "parse_arcgis_url": "portolan_cli.extract.arcgis.url_parser",
}

__getattr__, __dir__ = lazy_exports(__name__, _EXPORTS, __all__, globals())
