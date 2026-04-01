"""ArcGIS ImageServer extract functionality.

This module handles discovery and extraction of raster data from ArcGIS
ImageServer endpoints into Portolan catalogs.

Modules:
- discovery: Metadata discovery from ImageServer endpoints
- resume: Tile-based resume state tracking
- tiling: Tile grid calculation for partitioning large extents
- metadata: STAC metadata generation
- extractor: Full extraction pipeline orchestrator
- orchestrator: CLI-facing wrapper for Click commands
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
from portolan_cli.extract.arcgis.imageserver.metadata import (
    create_collection_metadata,
    create_item_metadata,
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
    # metadata
    "create_collection_metadata",
    "create_item_metadata",
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
