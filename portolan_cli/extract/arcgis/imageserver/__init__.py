"""ArcGIS ImageServer extract functionality.

This module handles discovery and extraction of raster data from ArcGIS
ImageServer endpoints into Portolan catalogs.

Modules:
- discovery: Metadata discovery from ImageServer endpoints
- resume: Tile-based resume state tracking
"""

from __future__ import annotations

from portolan_cli.extract.arcgis.imageserver.discovery import (
    ImageServerDiscoveryError,
    ImageServerMetadata,
    discover_imageserver,
)
from portolan_cli.extract.arcgis.imageserver.resume import (
    ImageServerResumeState,
    load_resume_state,
    save_resume_state,
    should_process_tile,
)

__all__ = [
    "ImageServerDiscoveryError",
    "ImageServerMetadata",
    "discover_imageserver",
    "ImageServerResumeState",
    "load_resume_state",
    "save_resume_state",
    "should_process_tile",
]
