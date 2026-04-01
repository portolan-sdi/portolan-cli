"""ArcGIS ImageServer extract functionality.

This module handles discovery and extraction of raster data from ArcGIS
ImageServer endpoints into Portolan catalogs.
"""

from __future__ import annotations

from portolan_cli.extract.arcgis.imageserver.discovery import (
    ImageServerDiscoveryError,
    ImageServerMetadata,
    discover_imageserver,
)

__all__ = [
    "ImageServerDiscoveryError",
    "ImageServerMetadata",
    "discover_imageserver",
]
