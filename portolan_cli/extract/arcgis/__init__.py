"""ArcGIS extract functionality.

This module handles extraction of vector data from ArcGIS FeatureServer/MapServer
endpoints into Portolan catalogs.
"""

from __future__ import annotations

from portolan_cli.extract.arcgis.filters import filter_layers, filter_services
from portolan_cli.extract.arcgis.metadata import ArcGISMetadata, extract_arcgis_metadata
from portolan_cli.extract.arcgis.url_parser import (
    ArcGISURLType,
    InvalidArcGISURLError,
    ParsedArcGISURL,
    parse_arcgis_url,
)

__all__ = [
    "ArcGISMetadata",
    "ArcGISURLType",
    "InvalidArcGISURLError",
    "ParsedArcGISURL",
    "extract_arcgis_metadata",
    "filter_layers",
    "filter_services",
    "parse_arcgis_url",
]
