"""ArcGIS ImageServer extract functionality.

This module handles discovery and extraction of raster data from ArcGIS
ImageServer endpoints into Portolan catalogs.

Modules:
- discovery: Metadata discovery from ImageServer endpoints
- resume: Tile-based resume state tracking
- tiling: Tile grid calculation for partitioning large extents
- extractor: Full extraction pipeline orchestrator (COG files only)
- orchestrator: CLI-facing wrapper for Click commands

Note: STAC metadata is created via the Portolan API (init_catalog + add_files)
after extraction, not by the extractor itself (per ADR-0007, ADR-0031).
"""

from __future__ import annotations

from portolan_cli.extract.arcgis.imageserver.discovery import (
    ImageServerDiscoveryError,
    ImageServerMetadata,
    discover_imageserver,
    parse_imageserver_response,
)
from portolan_cli.extract.arcgis.imageserver.extractor import (
    ExtractionConfig,
    ExtractionResult,
    ImageServerExtractionError,
    download_tile,
    extract_imageserver,
)
from portolan_cli.extract.arcgis.imageserver.orchestrator import (
    ImageServerCLIOptions,
    run_imageserver_extraction,
    run_imageserver_extraction_sync,
)
from portolan_cli.extract.arcgis.imageserver.resume import (
    ImageServerResumeState,
    load_resume_state,
    save_resume_state,
    should_process_tile,
)
from portolan_cli.extract.arcgis.imageserver.tiling import (
    TileSpec,
    compute_tile_grid,
    tile_count,
)

__all__ = [
    # discovery
    "ImageServerDiscoveryError",
    "ImageServerMetadata",
    "discover_imageserver",
    "parse_imageserver_response",
    # extractor
    "ExtractionConfig",
    "ExtractionResult",
    "ImageServerExtractionError",
    "download_tile",
    "extract_imageserver",
    # resume
    "ImageServerResumeState",
    "load_resume_state",
    "save_resume_state",
    "should_process_tile",
    # tiling
    "TileSpec",
    "compute_tile_grid",
    "tile_count",
    # orchestrator
    "ImageServerCLIOptions",
    "run_imageserver_extraction",
    "run_imageserver_extraction_sync",
]
