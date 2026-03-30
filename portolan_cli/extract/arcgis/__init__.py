"""ArcGIS extract functionality.

This module handles extraction of vector data from ArcGIS FeatureServer/MapServer
endpoints into Portolan catalogs.
"""

from __future__ import annotations

from portolan_cli.extract.arcgis.filters import (
    apply_unified_filter,
    filter_layers,
    filter_services,
)
from portolan_cli.extract.arcgis.metadata import ArcGISMetadata, extract_arcgis_metadata
from portolan_cli.extract.arcgis.report import (
    ExtractionReport,
    ExtractionSummary,
    LayerResult,
    MetadataExtracted,
    load_report,
    save_report,
)
from portolan_cli.extract.arcgis.resume import (
    ResumeState,
    get_resume_state,
    should_process_layer,
)
from portolan_cli.extract.arcgis.url_parser import (
    ArcGISURLType,
    InvalidArcGISURLError,
    ParsedArcGISURL,
    parse_arcgis_url,
)

__all__ = [
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
    # URL parsing
    "ArcGISURLType",
    "InvalidArcGISURLError",
    "ParsedArcGISURL",
    "parse_arcgis_url",
]
