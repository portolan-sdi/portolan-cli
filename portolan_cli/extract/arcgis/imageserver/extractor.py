"""ImageServer extraction orchestrator.

This module orchestrates the full extraction pipeline for ArcGIS ImageServer:
1. Discover service metadata (pixel type, extent, spatial reference)
2. Compute tile grid based on service limits and desired tile size
3. Download tiles via exportImage API (async, parallel with rate limiting)
4. Convert each tile to COG format using rio-cogeo
5. Generate STAC Items for each tile (streamed to disk)
6. Generate STAC Collection for the service
7. Save extraction report for resume support (with file locking)

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
import fcntl
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

import httpx
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

from portolan_cli.conversion_config import CogSettings, get_cog_settings
from portolan_cli.extract.arcgis.imageserver.discovery import discover_imageserver
from portolan_cli.extract.arcgis.imageserver.metadata import (
    create_collection_metadata,
    create_item_metadata,
)
from portolan_cli.extract.arcgis.imageserver.resume import (
    ImageServerResumeState,
    load_resume_state,
    should_process_tile,
)
from portolan_cli.extract.arcgis.imageserver.tiling import TileSpec, compute_tile_grid
from portolan_cli.output import detail, error, info, success, warn

if TYPE_CHECKING:
    from portolan_cli.extract.arcgis.imageserver.discovery import ImageServerMetadata

logger = logging.getLogger(__name__)

# TIFF magic bytes for validation
TIFF_MAGIC_LE = b"II\x2a\x00"  # Little-endian TIFF
TIFF_MAGIC_BE = b"MM\x00\x2a"  # Big-endian TIFF
BIGTIFF_MAGIC_LE = b"II\x2b\x00"  # Little-endian BigTIFF
BIGTIFF_MAGIC_BE = b"MM\x00\x2b"  # Big-endian BigTIFF

# Rate limiting defaults
DEFAULT_RATE_LIMIT_DELAY = 0.1  # 100ms between requests per concurrent slot
RATE_LIMIT_429_INITIAL_DELAY = 5.0  # Initial delay on 429 response
RATE_LIMIT_429_MAX_DELAY = 120.0  # Max delay on repeated 429s

# Resume state batching
RESUME_SAVE_INTERVAL = 10  # Save resume state every N tiles


class ImageServerExtractionError(Exception):
    """Error during ImageServer extraction."""

    pass


class RateLimitError(ImageServerExtractionError):
    """Server rate limit exceeded (HTTP 429)."""

    def __init__(self, retry_after: float | None = None) -> None:
        self.retry_after = retry_after
        super().__init__(f"Rate limited (retry after {retry_after}s)")


@dataclass
class ExtractionConfig:
    """Configuration for ImageServer extraction.

    Attributes:
        tile_size: Desired tile size in pixels (default 4096, per service limits).
        cog_settings: COG conversion settings (from config.yaml or defaults per ADR-0019).
        max_retries: Maximum retry attempts per tile on failure.
        dry_run: If True, compute tiles but don't download anything.
        timeout: HTTP request timeout in seconds.
        max_concurrent: Maximum concurrent tile downloads.
        rate_limit_delay: Minimum delay between requests per slot (seconds).
    """

    tile_size: int = 4096
    cog_settings: CogSettings = field(default_factory=CogSettings)
    max_retries: int = 3
    dry_run: bool = False
    timeout: float = 120.0
    max_concurrent: int = 4
    rate_limit_delay: float = DEFAULT_RATE_LIMIT_DELAY

    # Legacy compatibility: accept compression directly
    compression: str | None = None

    def __post_init__(self) -> None:
        """Handle legacy compression parameter."""
        if self.compression is not None and self.cog_settings.compression == "DEFLATE":
            # Legacy compression overrides default, not explicit cog_settings
            object.__setattr__(
                self,
                "cog_settings",
                CogSettings(
                    compression=self.compression.upper(),
                    quality=self.cog_settings.quality,
                    tile_size=self.cog_settings.tile_size,
                    predictor=self.cog_settings.predictor,
                    resampling=self.cog_settings.resampling,
                ),
            )


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


def _validate_tiff(data: bytes) -> bool:
    """Validate that data is a valid TIFF file.

    Checks magic bytes to ensure the downloaded data is actually a TIFF,
    not an HTML error page or other response.

    Args:
        data: Raw bytes to validate.

    Returns:
        True if data appears to be a valid TIFF, False otherwise.
    """
    if len(data) < 4:
        return False

    header = data[:4]
    return header in (TIFF_MAGIC_LE, TIFF_MAGIC_BE, BIGTIFF_MAGIC_LE, BIGTIFF_MAGIC_BE)


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
        RateLimitError: On HTTP 429 response.
    """
    export_url = _build_export_url(url, tile, pixel_type=pixel_type)

    try:
        response = await client.get(export_url)

        # Handle rate limiting (429)
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            retry_seconds = float(retry_after) if retry_after else None
            raise RateLimitError(retry_after=retry_seconds)

        response.raise_for_status()

        # Validate that response is actually a TIFF
        content = response.content
        if not _validate_tiff(content):
            # Check if it's an HTML error page
            if content.startswith(b"<!") or content.startswith(b"<html"):
                msg = f"Server returned HTML instead of TIFF for tile {tile.get_id()}"
            else:
                msg = f"Invalid TIFF data for tile {tile.get_id()} (bad magic bytes)"
            raise ImageServerExtractionError(msg)

        # Ensure parent directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Write to disk
        output_path.write_bytes(content)
        return len(content)

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
    cog_settings: CogSettings,
) -> None:
    """Convert a TIFF to COG format using settings from config.

    Runs rio-cogeo in a thread executor since it's CPU-bound.
    Uses settings from .portolan/config.yaml per ADR-0019.

    Args:
        input_path: Path to input TIFF.
        output_path: Path for output COG.
        cog_settings: COG conversion settings.
    """
    loop = asyncio.get_event_loop()

    def _do_convert() -> None:
        # Get base profile and customize with our settings
        profile = cog_profiles.get(cog_settings.compression.lower())  # type: ignore[no-untyped-call]

        # Apply predictor (for lossless compression)
        if cog_settings.compression.upper() not in ("JPEG", "WEBP"):
            profile["predictor"] = cog_settings.predictor

        # Apply quality for lossy compression
        if cog_settings.quality is not None and cog_settings.compression.upper() in (
            "JPEG",
            "WEBP",
        ):
            profile["quality"] = cog_settings.quality

        # Apply tile size
        profile["blockxsize"] = cog_settings.tile_size
        profile["blockysize"] = cog_settings.tile_size

        cog_translate(
            str(input_path),
            str(output_path),
            profile,
            # CogSettings.resampling is validated at config load time
            overview_resampling=cog_settings.resampling,  # type: ignore[arg-type]
            quiet=True,
        )

    await loop.run_in_executor(None, _do_convert)


def _intersect_bbox(
    bbox: tuple[float, float, float, float],
    extent: dict[str, float],
) -> dict[str, float] | None:
    """Intersect user bbox with service extent.

    Args:
        bbox: User-provided bbox (minx, miny, maxx, maxy).
        extent: Service extent dict with xmin, ymin, xmax, ymax.

    Returns:
        Intersected extent dict, or None if no intersection.
    """
    intersect_xmin = max(bbox[0], extent["xmin"])
    intersect_ymin = max(bbox[1], extent["ymin"])
    intersect_xmax = min(bbox[2], extent["xmax"])
    intersect_ymax = min(bbox[3], extent["ymax"])

    # Check if intersection is valid (non-empty)
    if intersect_xmin >= intersect_xmax or intersect_ymin >= intersect_ymax:
        return None

    return {
        "xmin": intersect_xmin,
        "ymin": intersect_ymin,
        "xmax": intersect_xmax,
        "ymax": intersect_ymax,
    }


def _save_resume_state_locked(state: ImageServerResumeState, path: Path) -> None:
    """Save resume state with file locking to prevent race conditions.

    Uses fcntl.flock for atomic writes when multiple concurrent tasks
    complete nearly simultaneously.

    Args:
        state: Resume state to save.
        path: Path to write the JSON file.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    # Write to temp file then rename for atomicity
    temp_path = path.with_suffix(".tmp")

    try:
        with open(temp_path, "w") as f:
            # Acquire exclusive lock
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                data = {
                    "extraction_type": "imageserver",
                    "service_url": state.service_url,
                    "started_at": state.started_at.isoformat().replace("+00:00", "Z"),
                    "tiles": {
                        "succeeded": sorted([list(coord) for coord in state.succeeded_tiles]),
                        "failed": sorted([list(coord) for coord in state.failed_tiles]),
                    },
                }
                json.dump(data, f, indent=2)
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)

        # Atomic rename
        temp_path.rename(path)
    except Exception:
        # Clean up temp file on error
        if temp_path.exists():
            temp_path.unlink()
        raise


def _count_item_files(output_dir: Path) -> int:
    """Count existing item JSON files in output directory.

    Used for resume to avoid loading all items into memory.

    Args:
        output_dir: Output directory containing item JSON files.

    Returns:
        Number of item JSON files found.
    """
    return len(list(output_dir.glob("tile_*.json")))


def _write_collection_from_items_on_disk(
    collection: dict[str, Any],
    output_dir: Path,
    collection_path: Path,
) -> int:
    """Write STAC collection with links to items found on disk.

    Instead of accumulating items in memory, scans disk for item files.
    This supports large extractions without OOM.

    Args:
        collection: Base collection metadata.
        output_dir: Directory containing item JSON files.
        collection_path: Path to write collection.json.

    Returns:
        Number of items linked.
    """
    # Scan for item files on disk
    item_files = sorted(output_dir.glob("tile_*.json"))

    item_links = [
        {
            "rel": "item",
            "href": f"./{item_file.name}",
            "type": "application/geo+json",
        }
        for item_file in item_files
    ]

    collection["links"].extend(item_links)
    collection_path.write_text(json.dumps(collection, indent=2))

    return len(item_files)


def _create_empty_result(collection_path: Path) -> ExtractionResult:
    """Create an empty ExtractionResult for early returns.

    Args:
        collection_path: Path to the collection.json file.

    Returns:
        ExtractionResult with all counts set to zero.
    """
    return ExtractionResult(
        collection_path=collection_path,
        items_created=0,
        tiles_downloaded=0,
        tiles_skipped=0,
        tiles_failed=0,
        total_bytes=0,
    )


@dataclass
class _ProcessingStats:
    """Mutable container for tile processing statistics."""

    tiles_downloaded: int = 0
    tiles_failed: int = 0
    total_bytes: int = 0
    tiles_since_last_save: int = 0


async def _process_tile(
    tile: TileSpec,
    url: str,
    output_dir: Path,
    config: ExtractionConfig,
    client: httpx.AsyncClient,
    metadata: ImageServerMetadata,
    collection_id: str,
    semaphore: asyncio.Semaphore,
    rate_limit_lock: asyncio.Lock,
    last_request_time: dict[str, float],
) -> tuple[TileSpec, bool, int, Path | None]:
    """Process a single tile: download, convert to COG, create STAC item.

    Writes item JSON directly to disk to avoid memory accumulation.

    Args:
        tile: Tile to process.
        url: ImageServer URL.
        output_dir: Output directory.
        config: Extraction configuration.
        client: HTTP client.
        metadata: Service metadata.
        collection_id: Collection ID for STAC item.
        semaphore: Concurrency limiter.
        rate_limit_lock: Lock for rate limiting coordination.
        last_request_time: Shared dict tracking last request time per slot.

    Returns:
        Tuple of (tile, success, bytes_downloaded, item_path or None).
    """
    async with semaphore:
        slot_id = str(id(asyncio.current_task()))
        tile_dir = output_dir / "tiles"
        raw_path = tile_dir / f"tile_{tile.get_id()}_raw.tif"
        cog_path = tile_dir / f"tile_{tile.get_id()}.tif"
        item_path = output_dir / f"tile_{tile.x}_{tile.y}.json"

        rate_limit_delay = config.rate_limit_delay
        bytes_downloaded = 0

        try:
            for attempt in range(1, config.max_retries + 1):
                try:
                    # Rate limiting: ensure minimum delay between requests
                    async with rate_limit_lock:
                        now = time.monotonic()
                        last_time = last_request_time.get(slot_id, 0)
                        wait_time = max(0, rate_limit_delay - (now - last_time))
                        if wait_time > 0:
                            await asyncio.sleep(wait_time)
                        last_request_time[slot_id] = time.monotonic()

                    # Download raw tile
                    bytes_downloaded = await download_tile(
                        url=url,
                        tile=tile,
                        output_path=raw_path,
                        client=client,
                        pixel_type=metadata.pixel_type,
                    )

                    # Convert to COG using config settings
                    await _convert_to_cog(raw_path, cog_path, config.cog_settings)

                    # Remove raw file after successful conversion
                    if raw_path.exists():
                        raw_path.unlink()

                    # Create and write STAC item directly to disk
                    relative_cog_path = str(cog_path.relative_to(output_dir))
                    item = create_item_metadata(tile, metadata, relative_cog_path, collection_id)
                    item_path.write_text(json.dumps(item, indent=2))

                    return tile, True, bytes_downloaded, item_path

                except RateLimitError as e:
                    # Handle 429 with exponential backoff
                    delay = e.retry_after or (RATE_LIMIT_429_INITIAL_DELAY * (2 ** (attempt - 1)))
                    delay = min(delay, RATE_LIMIT_429_MAX_DELAY)
                    warn(f"Rate limited on tile {tile.get_id()}, waiting {delay:.1f}s")
                    await asyncio.sleep(delay)
                    # Increase rate limit delay for future requests
                    rate_limit_delay = min(rate_limit_delay * 2, 2.0)

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

        finally:
            # Clean up raw file on any exit (success or failure)
            if raw_path.exists():
                try:
                    raw_path.unlink()
                except OSError:
                    pass  # Best effort cleanup


def _setup_extraction_dirs(output_dir: Path) -> tuple[Path, Path]:
    """Create extraction output directories.

    Args:
        output_dir: Base output directory.

    Returns:
        Tuple of (tiles_dir, portolan_dir).
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    tiles_dir = output_dir / "tiles"
    tiles_dir.mkdir(exist_ok=True)
    portolan_dir = output_dir / ".portolan"
    portolan_dir.mkdir(exist_ok=True)
    return tiles_dir, portolan_dir


def _load_effective_config(config: ExtractionConfig, output_dir: Path) -> ExtractionConfig:
    """Load COG settings from config.yaml if not explicitly provided.

    Args:
        config: Base extraction config.
        output_dir: Directory to check for .portolan/config.yaml.

    Returns:
        Config with COG settings from config.yaml, or original config.
    """
    if config.cog_settings != CogSettings():
        return config  # Explicit settings take precedence

    try:
        catalog_cog_settings = get_cog_settings(output_dir)
        if catalog_cog_settings != CogSettings():
            info(f"Using COG settings from config: {catalog_cog_settings.compression}")
            return ExtractionConfig(
                tile_size=config.tile_size,
                cog_settings=catalog_cog_settings,
                max_retries=config.max_retries,
                dry_run=config.dry_run,
                timeout=config.timeout,
                max_concurrent=config.max_concurrent,
                rate_limit_delay=config.rate_limit_delay,
            )
    except Exception as e:
        logger.debug("Could not load COG settings from config: %s", e)

    return config


def _write_empty_collection(
    metadata: ImageServerMetadata,
    url: str,
    output_dir: Path,
) -> ExtractionResult:
    """Write empty STAC collection and return empty result.

    Args:
        metadata: Service metadata.
        url: Service URL.
        output_dir: Output directory.

    Returns:
        Empty ExtractionResult.
    """
    collection_path = output_dir / "collection.json"
    empty_collection = create_collection_metadata(metadata, url)
    collection_path.write_text(json.dumps(empty_collection, indent=2))
    return _create_empty_result(collection_path)


async def _extract_all_tiles(
    tiles: list[TileSpec],
    url: str,
    output_dir: Path,
    config: ExtractionConfig,
    metadata: ImageServerMetadata,
    collection_id: str,
    resume_state: ImageServerResumeState,
    resume_path: Path,
) -> _ProcessingStats:
    """Extract all tiles with concurrency control.

    Args:
        tiles: List of tiles to process.
        url: Service URL.
        output_dir: Output directory.
        config: Extraction config.
        metadata: Service metadata.
        collection_id: STAC collection ID.
        resume_state: Resume state to update.
        resume_path: Path to save resume state.

    Returns:
        Processing statistics.
    """
    semaphore = asyncio.Semaphore(config.max_concurrent)
    rate_limit_lock = asyncio.Lock()
    last_request_time: dict[str, float] = {}
    stats = _ProcessingStats()

    async with httpx.AsyncClient(timeout=config.timeout) as client:
        tasks = [
            _process_tile(
                tile=tile,
                url=url,
                output_dir=output_dir,
                config=config,
                client=client,
                metadata=metadata,
                collection_id=collection_id,
                semaphore=semaphore,
                rate_limit_lock=rate_limit_lock,
                last_request_time=last_request_time,
            )
            for tile in tiles
        ]

        for i, coro in enumerate(asyncio.as_completed(tasks)):
            tile, succeeded, bytes_downloaded, _ = await coro
            _update_stats_and_state(
                tile, succeeded, bytes_downloaded, stats, resume_state, i, len(tiles)
            )

            # Batch resume state saves
            stats.tiles_since_last_save += 1
            if stats.tiles_since_last_save >= RESUME_SAVE_INTERVAL or not succeeded:
                _save_resume_state_locked(resume_state, resume_path)
                stats.tiles_since_last_save = 0

    return stats


def _update_stats_and_state(
    tile: TileSpec,
    succeeded: bool,
    bytes_downloaded: int,
    stats: _ProcessingStats,
    resume_state: ImageServerResumeState,
    index: int,
    total: int,
) -> None:
    """Update statistics and resume state after processing a tile.

    Args:
        tile: Processed tile.
        succeeded: Whether tile processing succeeded.
        bytes_downloaded: Bytes downloaded (0 if failed).
        stats: Statistics to update.
        resume_state: Resume state to update.
        index: Current tile index.
        total: Total tiles to process.
    """
    if succeeded:
        stats.tiles_downloaded += 1
        stats.total_bytes += bytes_downloaded
        resume_state.succeeded_tiles.add((tile.x, tile.y))
        detail(f"Tile {tile.get_id()}: {bytes_downloaded:,} bytes [{index + 1}/{total}]")
    else:
        stats.tiles_failed += 1
        resume_state.failed_tiles.add((tile.x, tile.y))
        error(f"Tile {tile.get_id()}: failed [{index + 1}/{total}]")


async def extract_imageserver(
    url: str,
    output_dir: Path,
    config: ExtractionConfig | None = None,
    resume: bool = False,
    bbox: tuple[float, float, float, float] | None = None,
) -> ExtractionResult:
    """Extract raster tiles from ImageServer to COG + STAC.

    This orchestrates the full extraction pipeline. See module docstring for details.

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

    # Setup
    _, portolan_dir = _setup_extraction_dirs(output_dir)
    config = _load_effective_config(config, output_dir)

    # Discover service
    info(f"Discovering ImageServer: {url}")
    metadata = await discover_imageserver(url, timeout=config.timeout)
    info(f"Service: {metadata.name} ({metadata.pixel_type}, {metadata.band_count} bands)")

    collection_id = metadata.name.lower().replace(" ", "_").replace("-", "_")
    collection_id = "".join(c for c in collection_id if c.isalnum() or c == "_")

    # Compute tiles
    extent = metadata.full_extent
    if bbox:
        intersected = _intersect_bbox(bbox, extent)
        if intersected is None:
            info("User bbox does not intersect service extent - no tiles to extract")
            return _write_empty_collection(metadata, url, output_dir)
        extent = intersected

    tiles = list(
        compute_tile_grid(
            extent=extent,
            pixel_size_x=metadata.pixel_size_x,
            pixel_size_y=metadata.pixel_size_y,
            tile_size=config.tile_size,
        )
    )
    info(f"Computed {len(tiles)} tiles to extract")

    if not tiles:
        success("No tiles to extract (bbox may not intersect service extent)")
        return _write_empty_collection(metadata, url, output_dir)

    if config.dry_run:
        info(f"[DRY RUN] Would extract {len(tiles)} tiles")
        return _create_empty_result(output_dir / "collection.json")

    # Resume state
    resume_path = portolan_dir / "imageserver-resume.json"
    resume_state = _load_or_create_resume_state(resume, resume_path, url)

    tiles_to_process = [t for t in tiles if should_process_tile(t.x, t.y, resume_state)]
    tiles_skipped = len(tiles) - len(tiles_to_process)
    if tiles_skipped > 0:
        info(f"Skipping {tiles_skipped} already-completed tiles")

    # Extract tiles
    stats = await _extract_all_tiles(
        tiles_to_process,
        url,
        output_dir,
        config,
        metadata,
        collection_id,
        resume_state,
        resume_path,
    )
    _save_resume_state_locked(resume_state, resume_path)

    # Write collection
    collection_path = output_dir / "collection.json"
    collection = create_collection_metadata(metadata, url)
    items_created = _write_collection_from_items_on_disk(collection, output_dir, collection_path)

    success(f"Extracted {stats.tiles_downloaded} tiles ({stats.total_bytes:,} bytes)")
    if stats.tiles_failed > 0:
        error(f"Failed: {stats.tiles_failed} tiles")

    return ExtractionResult(
        collection_path=collection_path,
        items_created=items_created,
        tiles_downloaded=stats.tiles_downloaded,
        tiles_skipped=tiles_skipped,
        tiles_failed=stats.tiles_failed,
        total_bytes=stats.total_bytes,
    )


def _load_or_create_resume_state(
    resume: bool,
    resume_path: Path,
    url: str,
) -> ImageServerResumeState:
    """Load existing resume state or create new one.

    Args:
        resume: Whether to attempt loading existing state.
        resume_path: Path to resume state file.
        url: Service URL for new state.

    Returns:
        Resume state (loaded or new).
    """
    if resume:
        state = load_resume_state(resume_path)
        if state:
            info(f"Resuming: {len(state.succeeded_tiles)} tiles already complete")
            return state

    return ImageServerResumeState(
        succeeded_tiles=set(),
        failed_tiles=set(),
        service_url=url,
        started_at=datetime.now(timezone.utc),
    )
