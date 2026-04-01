"""ImageServer extraction orchestrator.

This module orchestrates the full extraction pipeline for ArcGIS ImageServer:
1. Discover service metadata (pixel type, extent, spatial reference)
2. Compute tile grid based on service limits and desired tile size
3. Download tiles via exportImage API (async, parallel)
4. Convert each tile to COG format using rio-cogeo
5. Generate STAC Items for each tile
6. Generate STAC Collection for the service
7. Save extraction report for resume support

Typical usage:
    from portolan_cli.extract.arcgis.imageserver.extractor import (
        extract_imageserver,
        ExtractionConfig,
    )

    result = await extract_imageserver(
        url="https://services.arcgis.com/.../ImageServer",
        output_dir=Path("./output"),
        config=ExtractionConfig(tile_size=4096),
    )
    print(f"Extracted {result.tiles_downloaded} tiles ({result.total_bytes} bytes)")
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

import httpx
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

from portolan_cli.extract.arcgis.imageserver.discovery import discover_imageserver
from portolan_cli.extract.arcgis.imageserver.resume import (
    ImageServerResumeState,
    load_resume_state,
    save_resume_state,
    should_process_tile,
)
from portolan_cli.extract.arcgis.imageserver.tiling import TileSpec, compute_tile_grid
from portolan_cli.output import detail, error, info, success

if TYPE_CHECKING:
    from portolan_cli.extract.arcgis.imageserver.discovery import ImageServerMetadata

logger = logging.getLogger(__name__)


class ImageServerExtractionError(Exception):
    """Error during ImageServer extraction."""

    pass


@dataclass
class ExtractionConfig:
    """Configuration for ImageServer extraction.

    Attributes:
        tile_size: Desired tile size in pixels (default 4096, per service limits).
        compression: COG compression method ("DEFLATE" or "JPEG"). DEFLATE is
            lossless and recommended per ADR-0019.
        max_retries: Maximum retry attempts per tile on failure.
        dry_run: If True, compute tiles but don't download anything.
        timeout: HTTP request timeout in seconds.
        max_concurrent: Maximum concurrent tile downloads.
    """

    tile_size: int = 4096
    compression: str = "DEFLATE"
    max_retries: int = 3
    dry_run: bool = False
    timeout: float = 120.0
    max_concurrent: int = 4


@dataclass
class ExtractionResult:
    """Result of an ImageServer extraction.

    Attributes:
        collection_path: Path to the generated STAC collection.json.
        items_created: Number of STAC items created.
        tiles_downloaded: Number of tiles successfully downloaded.
        tiles_skipped: Number of tiles skipped (from resume).
        tiles_failed: Number of tiles that failed after retries.
        total_bytes: Total bytes downloaded.
    """

    collection_path: Path
    items_created: int
    tiles_downloaded: int
    tiles_skipped: int
    tiles_failed: int = 0
    total_bytes: int = 0


def _build_export_url(
    service_url: str,
    tile: TileSpec,
    *,
    pixel_type: str = "U8",
) -> str:
    """Build exportImage URL for a tile.

    Args:
        service_url: ImageServer base URL.
        tile: Tile specification with bbox and dimensions.
        pixel_type: Pixel type for format selection.

    Returns:
        Full exportImage URL with parameters.
    """
    base_url = service_url.rstrip("/")
    minx, miny, maxx, maxy = tile.bbox

    params = {
        "bbox": f"{minx},{miny},{maxx},{maxy}",
        "size": f"{tile.width_px},{tile.height_px}",
        "format": "tiff",
        "f": "image",
    }

    return f"{base_url}/exportImage?{urlencode(params)}"


async def download_tile(
    url: str,
    tile: TileSpec,
    output_path: Path,
    client: httpx.AsyncClient,
    *,
    pixel_type: str = "U8",
) -> int:
    """Download a single tile via exportImage API.

    Args:
        url: ImageServer base URL.
        tile: Tile specification.
        output_path: Path to write the downloaded TIFF.
        client: Async HTTP client for connection pooling.
        pixel_type: Pixel type for format selection.

    Returns:
        Number of bytes downloaded.

    Raises:
        ImageServerExtractionError: On HTTP or I/O errors.
    """
    export_url = _build_export_url(url, tile, pixel_type=pixel_type)

    try:
        response = await client.get(export_url)
        response.raise_for_status()

        # Ensure parent directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Write to disk
        output_path.write_bytes(response.content)
        return len(response.content)

    except httpx.HTTPStatusError as e:
        msg = f"Tile download failed ({tile.get_id()}): HTTP {e.response.status_code}"
        raise ImageServerExtractionError(msg) from e
    except httpx.TimeoutException as e:
        msg = f"Tile download timeout ({tile.get_id()})"
        raise ImageServerExtractionError(msg) from e
    except httpx.RequestError as e:
        msg = f"Tile download failed ({tile.get_id()}): {e}"
        raise ImageServerExtractionError(msg) from e
    except OSError as e:
        msg = f"Failed to write tile ({tile.get_id()}): {e}"
        raise ImageServerExtractionError(msg) from e


async def _convert_to_cog(
    input_path: Path,
    output_path: Path,
    compression: str,
) -> None:
    """Convert a TIFF to COG format.

    Runs rio-cogeo in a thread executor since it's CPU-bound.

    Args:
        input_path: Path to input TIFF.
        output_path: Path for output COG.
        compression: Compression method ("deflate" or "jpeg").
    """
    loop = asyncio.get_event_loop()

    def _do_convert() -> None:
        profile = cog_profiles.get(compression.lower())  # type: ignore[no-untyped-call]
        cog_translate(
            str(input_path),
            str(output_path),
            profile,
            overview_resampling="nearest",
            quiet=True,
        )

    await loop.run_in_executor(None, _do_convert)


def _create_stac_item(
    tile: TileSpec,
    cog_path: Path,
    metadata: ImageServerMetadata,
    output_dir: Path,
) -> dict[str, Any]:
    """Create a STAC Item for a tile.

    Args:
        tile: Tile specification.
        cog_path: Path to the COG file.
        metadata: Service metadata.
        output_dir: Output directory root for relative paths.

    Returns:
        STAC Item as a dictionary.
    """
    minx, miny, maxx, maxy = tile.bbox
    now = datetime.now(timezone.utc).isoformat()

    # Relative path for asset href
    relative_path = cog_path.relative_to(output_dir)

    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": tile.get_id(),
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [minx, miny],
                    [maxx, miny],
                    [maxx, maxy],
                    [minx, maxy],
                    [minx, miny],
                ]
            ],
        },
        "bbox": [minx, miny, maxx, maxy],
        "properties": {
            "datetime": now,
            "created": now,
            "tile_col": tile.x,
            "tile_row": tile.y,
        },
        "links": [],
        "assets": {
            "data": {
                "href": str(relative_path),
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "title": f"Tile {tile.x},{tile.y}",
                "roles": ["data"],
            }
        },
    }


def _create_stac_collection(
    metadata: ImageServerMetadata,
    items: list[dict[str, Any]],
    output_dir: Path,
    service_url: str,
) -> dict[str, Any]:
    """Create a STAC Collection for the extracted imagery.

    Args:
        metadata: Service metadata.
        items: List of STAC Items.
        output_dir: Output directory for saving item files.
        service_url: Original ImageServer URL for provenance.

    Returns:
        STAC Collection as a dictionary.
    """
    minx, miny, maxx, maxy = metadata.get_bbox_tuple()
    now = datetime.now(timezone.utc).isoformat()

    # Build item links (items are already written to disk during extraction)
    item_links = [
        {
            "rel": "item",
            "href": f"./{item['id']}.json",
            "type": "application/geo+json",
        }
        for item in items
    ]

    return {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": metadata.name.lower().replace(" ", "_"),
        "title": metadata.name,
        "description": metadata.description or f"Imagery extracted from {metadata.name}",
        "license": "proprietary",
        "extent": {
            "spatial": {"bbox": [[minx, miny, maxx, maxy]]},
            "temporal": {"interval": [[now, None]]},
        },
        "links": [
            {"rel": "self", "href": "./collection.json", "type": "application/json"},
            {"rel": "root", "href": "./collection.json", "type": "application/json"},
            *item_links,
        ],
        "providers": [
            {
                "name": "ArcGIS ImageServer",
                "roles": ["producer"],
                "url": service_url,
            }
        ],
        "summaries": {
            "pixel_type": [metadata.pixel_type],
            "band_count": [metadata.band_count],
        },
    }


async def _process_tile(
    tile: TileSpec,
    url: str,
    output_dir: Path,
    config: ExtractionConfig,
    client: httpx.AsyncClient,
    metadata: ImageServerMetadata,
    resume_state: ImageServerResumeState,
    semaphore: asyncio.Semaphore,
) -> tuple[TileSpec, bool, int, dict[str, Any] | None]:
    """Process a single tile: download, convert to COG, create STAC item.

    Args:
        tile: Tile to process.
        url: ImageServer URL.
        output_dir: Output directory.
        config: Extraction configuration.
        client: HTTP client.
        metadata: Service metadata.
        resume_state: Resume state for tracking progress.
        semaphore: Concurrency limiter.

    Returns:
        Tuple of (tile, success, bytes_downloaded, stac_item or None).
    """
    async with semaphore:
        tile_dir = output_dir / "tiles"
        raw_path = tile_dir / f"tile_{tile.get_id()}_raw.tif"
        cog_path = tile_dir / f"tile_{tile.get_id()}.tif"

        for attempt in range(1, config.max_retries + 1):
            try:
                # Download raw tile
                bytes_downloaded = await download_tile(
                    url=url,
                    tile=tile,
                    output_path=raw_path,
                    client=client,
                    pixel_type=metadata.pixel_type,
                )

                # Convert to COG
                await _convert_to_cog(raw_path, cog_path, config.compression)

                # Remove raw file after successful conversion
                if raw_path.exists():
                    raw_path.unlink()

                # Create STAC item
                item = _create_stac_item(tile, cog_path, metadata, output_dir)

                return tile, True, bytes_downloaded, item

            except ImageServerExtractionError as e:
                if attempt < config.max_retries:
                    logger.warning(
                        "Tile %s failed (attempt %d/%d): %s",
                        tile.get_id(),
                        attempt,
                        config.max_retries,
                        e,
                    )
                    await asyncio.sleep(2**attempt)  # Exponential backoff
                else:
                    logger.error(
                        "Tile %s failed after %d attempts: %s",
                        tile.get_id(),
                        config.max_retries,
                        e,
                    )
                    return tile, False, 0, None
            except Exception as e:
                logger.error("Unexpected error processing tile %s: %s", tile.get_id(), e)
                return tile, False, 0, None

        return tile, False, 0, None


async def extract_imageserver(
    url: str,
    output_dir: Path,
    config: ExtractionConfig | None = None,
    resume: bool = False,
    bbox: tuple[float, float, float, float] | None = None,
) -> ExtractionResult:
    """Extract raster tiles from ImageServer to COG + STAC.

    This is the main orchestration function that:
    1. Discovers service metadata
    2. Computes tile grid (optionally filtered by bbox)
    3. Loads resume state if --resume
    4. For each tile (with concurrency control):
       - Skip if already succeeded
       - Download via exportImage API
       - Convert to COG with rio-cogeo
       - Create STAC Item
       - Update resume state
    5. Creates STAC Collection
    6. Returns extraction summary

    Args:
        url: ImageServer URL.
        output_dir: Directory to write extracted data.
        config: Extraction configuration (defaults to ExtractionConfig()).
        resume: If True, resume from previous extraction.
        bbox: Optional bbox to subset extraction (minx, miny, maxx, maxy).

    Returns:
        ExtractionResult with extraction statistics.

    Raises:
        ImageServerDiscoveryError: If service discovery fails.
    """
    if config is None:
        config = ExtractionConfig()

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    tiles_dir = output_dir / "tiles"
    tiles_dir.mkdir(exist_ok=True)
    portolan_dir = output_dir / ".portolan"
    portolan_dir.mkdir(exist_ok=True)

    # Discover service metadata
    info(f"Discovering ImageServer: {url}")
    metadata = await discover_imageserver(url, timeout=config.timeout)
    info(f"Service: {metadata.name} ({metadata.pixel_type}, {metadata.band_count} bands)")

    # Compute tile grid using correct API
    # compute_tile_grid expects extent dict and pixel sizes
    extent = metadata.full_extent
    if bbox:
        # Intersect user-provided bbox with service extent to avoid
        # generating tiles outside the actual data coverage
        service_xmin = extent["xmin"]
        service_ymin = extent["ymin"]
        service_xmax = extent["xmax"]
        service_ymax = extent["ymax"]

        # Compute intersection
        intersect_xmin = max(bbox[0], service_xmin)
        intersect_ymin = max(bbox[1], service_ymin)
        intersect_xmax = min(bbox[2], service_xmax)
        intersect_ymax = min(bbox[3], service_ymax)

        # Check if intersection is valid (non-empty)
        if intersect_xmin >= intersect_xmax or intersect_ymin >= intersect_ymax:
            info("User bbox does not intersect service extent - no tiles to extract")
            collection_path = output_dir / "collection.json"
            empty_collection = _create_stac_collection(metadata, [], output_dir, url)
            collection_path.write_text(json.dumps(empty_collection, indent=2))
            return ExtractionResult(
                collection_path=collection_path,
                items_created=0,
                tiles_downloaded=0,
                tiles_skipped=0,
                tiles_failed=0,
                total_bytes=0,
            )

        extent = {
            "xmin": intersect_xmin,
            "ymin": intersect_ymin,
            "xmax": intersect_xmax,
            "ymax": intersect_ymax,
        }
    tiles = list(
        compute_tile_grid(
            extent=extent,
            pixel_size_x=metadata.pixel_size_x,
            pixel_size_y=metadata.pixel_size_y,
            tile_size=config.tile_size,
        )
    )
    total_tiles = len(tiles)
    info(f"Computed {total_tiles} tiles to extract")

    if total_tiles == 0:
        success("No tiles to extract (bbox may not intersect service extent)")
        collection_path = output_dir / "collection.json"
        # Create empty collection
        empty_collection = _create_stac_collection(metadata, [], output_dir, url)
        collection_path.write_text(json.dumps(empty_collection, indent=2))
        return ExtractionResult(
            collection_path=collection_path,
            items_created=0,
            tiles_downloaded=0,
            tiles_skipped=0,
            tiles_failed=0,
            total_bytes=0,
        )

    # Handle dry run
    if config.dry_run:
        info(f"[DRY RUN] Would extract {total_tiles} tiles")
        collection_path = output_dir / "collection.json"
        return ExtractionResult(
            collection_path=collection_path,
            items_created=0,
            tiles_downloaded=0,
            tiles_skipped=0,
            tiles_failed=0,
            total_bytes=0,
        )

    # Load resume state
    resume_path = portolan_dir / "imageserver-resume.json"
    resume_state: ImageServerResumeState | None = None
    if resume:
        resume_state = load_resume_state(resume_path)
        if resume_state:
            info(f"Resuming: {len(resume_state.succeeded_tiles)} tiles already complete")

    # Initialize or update resume state
    if resume_state is None:
        resume_state = ImageServerResumeState(
            succeeded_tiles=set(),
            failed_tiles=set(),
            service_url=url,
            started_at=datetime.now(timezone.utc),
        )

    # Filter tiles based on resume state
    tiles_to_process = [tile for tile in tiles if should_process_tile(tile.x, tile.y, resume_state)]
    tiles_skipped = total_tiles - len(tiles_to_process)

    if tiles_skipped > 0:
        info(f"Skipping {tiles_skipped} already-completed tiles")

    # Extract tiles with concurrency control
    semaphore = asyncio.Semaphore(config.max_concurrent)
    tiles_downloaded = 0
    tiles_failed = 0
    total_bytes = 0
    stac_items: list[dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=config.timeout) as client:
        tasks = [
            _process_tile(
                tile=tile,
                url=url,
                output_dir=output_dir,
                config=config,
                client=client,
                metadata=metadata,
                resume_state=resume_state,
                semaphore=semaphore,
            )
            for tile in tiles_to_process
        ]

        for i, coro in enumerate(asyncio.as_completed(tasks)):
            tile, succeeded, bytes_downloaded, item = await coro
            progress = i + 1

            if succeeded:
                tiles_downloaded += 1
                total_bytes += bytes_downloaded

                # Write item JSON BEFORE marking success in resume state
                # This ensures if we crash after saving resume state, the item
                # JSON already exists on disk for collection reconstruction
                if item:
                    item_path = output_dir / f"{item['id']}.json"
                    item_path.write_text(json.dumps(item, indent=2))
                    stac_items.append(item)

                # Only mark success after item is persisted
                resume_state.succeeded_tiles.add((tile.x, tile.y))
                detail(
                    f"Tile {tile.get_id()}: {bytes_downloaded:,} bytes [{progress}/{len(tiles_to_process)}]"
                )
            else:
                tiles_failed += 1
                resume_state.failed_tiles.add((tile.x, tile.y))
                error(f"Tile {tile.get_id()}: failed [{progress}/{len(tiles_to_process)}]")

            # Save resume state after each tile
            save_resume_state(resume_state, resume_path)

    # Create STAC collection
    collection_path = output_dir / "collection.json"
    collection = _create_stac_collection(metadata, stac_items, output_dir, url)
    collection_path.write_text(json.dumps(collection, indent=2))

    success(f"Extracted {tiles_downloaded} tiles ({total_bytes:,} bytes)")
    if tiles_failed > 0:
        error(f"Failed: {tiles_failed} tiles")

    return ExtractionResult(
        collection_path=collection_path,
        items_created=len(stac_items),
        tiles_downloaded=tiles_downloaded,
        tiles_skipped=tiles_skipped,
        tiles_failed=tiles_failed,
        total_bytes=total_bytes,
    )
