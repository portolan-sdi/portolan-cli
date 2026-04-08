"""ImageServer extraction orchestrator.

This module orchestrates the extraction pipeline for ArcGIS ImageServer:
1. Discover service metadata (pixel type, extent, spatial reference)
2. Compute tile grid based on service limits and desired tile size
3. Download tiles via exportImage API (async, parallel with rate limiting)
4. Convert each tile to COG format using rio-cogeo
5. Save extraction report for resume support (with file locking)
6. Auto-init Portolan catalog (unless raw mode) using standard API

The extractor does NOT create STAC metadata directly. Instead, it extracts
COG files and then calls the Portolan API (init_catalog + add_files) to
create proper STAC structure with items per raster (per ADR-0031).

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
import sys
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

# Cross-platform file locking
if sys.platform == "win32":
    import msvcrt

    def _lock_file(f: Any) -> None:
        """Lock file on Windows using msvcrt."""
        msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)

    def _unlock_file(f: Any) -> None:
        """Unlock file on Windows using msvcrt."""
        try:
            msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass  # May fail if not locked

else:
    import fcntl

    def _lock_file(f: Any) -> None:
        """Lock file on Unix using fcntl."""
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)

    def _unlock_file(f: Any) -> None:
        """Unlock file on Unix using fcntl."""
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)


import httpx
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

from portolan_cli.conversion_config import CogSettings, get_cog_settings
from portolan_cli.extract.arcgis.imageserver.discovery import discover_imageserver
from portolan_cli.extract.arcgis.imageserver.report import (
    ImageServerExtractionReport,
    TileResult,
    build_imageserver_report,
    save_imageserver_report,
)
from portolan_cli.extract.arcgis.imageserver.resume import (
    ImageServerResumeState,
    load_resume_state,
    should_process_tile,
)
from portolan_cli.extract.arcgis.imageserver.tiling import TileSpec, compute_tile_grid
from portolan_cli.metadata_seeding import seed_metadata_yaml
from portolan_cli.output import detail, error, info, success, warn

if TYPE_CHECKING:
    from collections.abc import Callable

    from portolan_cli.extract.arcgis.imageserver.discovery import ImageServerMetadata


@dataclass
class TileProgress:
    """Progress callback data for tile extraction.

    Matches FeatureServer's ExtractionProgress structure for consistency.

    Attributes:
        tile_index: Current tile index (0-based).
        total_tiles: Total number of tiles to extract.
        tile_id: ID of current tile (e.g., "0_0").
        status: Current status - one of "starting", "downloading",
            "converting", "success", "failed", or "skipped".
    """

    tile_index: int
    total_tiles: int
    tile_id: str
    status: str


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
        raw: If True, skip auto-init (only create COGs + report, no STAC catalog).
        timeout: HTTP request timeout in seconds.
        max_concurrent: Maximum concurrent tile downloads.
        rate_limit_delay: Minimum delay between requests per slot (seconds).
    """

    tile_size: int = 4096
    cog_settings: CogSettings = field(default_factory=CogSettings)
    max_retries: int = 3
    dry_run: bool = False
    raw: bool = False
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
        output_dir: Directory containing extracted COG files.
        tiles_downloaded: Number of tiles successfully downloaded.
        tiles_skipped: Number of tiles skipped (from resume).
        tiles_failed: Number of tiles that failed after retries.
        total_bytes: Total bytes downloaded.
        catalog_initialized: Whether Portolan catalog was auto-initialized.
        report: Full extraction report with metadata and tile results.
    """

    output_dir: Path
    tiles_downloaded: int
    tiles_skipped: int
    tiles_failed: int = 0
    total_bytes: int = 0
    catalog_initialized: bool = False
    report: ImageServerExtractionReport | None = None


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
            # Check if it's a JSON error response from ArcGIS
            elif content.startswith(b"{"):
                try:
                    error_data = json.loads(content.decode("utf-8"))
                    if "error" in error_data:
                        arcgis_error = error_data["error"]
                        code = arcgis_error.get("code", "unknown")
                        message = arcgis_error.get("message", "Unknown error")
                        msg = f"ArcGIS error for tile {tile.get_id()}: [{code}] {message}"
                    else:
                        msg = f"Unexpected JSON response for tile {tile.get_id()}: {content[:200].decode('utf-8', errors='replace')}"
                except (json.JSONDecodeError, UnicodeDecodeError):
                    msg = f"Invalid TIFF data for tile {tile.get_id()} (bad magic bytes)"
            else:
                msg = f"Invalid TIFF data for tile {tile.get_id()} (bad magic bytes)"
            raise ImageServerExtractionError(msg)

        # Ensure parent directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Write to disk
        output_path.write_bytes(content)
        return len(content)

    except httpx.HTTPStatusError as e:
        # Try to extract ArcGIS JSON error details from 4xx/5xx responses
        content = e.response.content
        if content.startswith(b"{"):
            try:
                error_data = json.loads(content.decode("utf-8"))
                if "error" in error_data:
                    arcgis_error = error_data["error"]
                    code = arcgis_error.get("code", e.response.status_code)
                    message = arcgis_error.get("message", "Unknown error")
                    msg = f"ArcGIS error for tile {tile.get_id()}: [{code}] {message}"
                    raise ImageServerExtractionError(msg) from e
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass  # Fall through to generic error
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


def _is_likely_wgs84(bbox: tuple[float, float, float, float]) -> bool:
    """Detect if bbox coordinates appear to be in WGS84 (EPSG:4326).

    WGS84 coordinates have characteristic ranges:
    - Longitude: -180 to 180
    - Latitude: -90 to 90

    This heuristic checks if all values fall within these ranges.
    False positives are possible for small-extent projected coordinates,
    but unlikely for typical imagery bboxes.

    Args:
        bbox: Bounding box (minx, miny, maxx, maxy).

    Returns:
        True if bbox appears to be in WGS84, False otherwise.
    """
    minx, miny, maxx, maxy = bbox
    return -180 <= minx <= 180 and -180 <= maxx <= 180 and -90 <= miny <= 90 and -90 <= maxy <= 90


def _reproject_bbox(
    bbox: tuple[float, float, float, float],
    from_crs: str,
    to_crs: str,
) -> tuple[float, float, float, float]:
    """Reproject a bounding box between coordinate reference systems.

    Args:
        bbox: Bounding box (minx, miny, maxx, maxy) in source CRS.
        from_crs: Source CRS (e.g., "EPSG:4326").
        to_crs: Target CRS (e.g., "EPSG:3857").

    Returns:
        Reprojected bounding box (minx, miny, maxx, maxy).

    Raises:
        ValueError: If CRS transformation fails.
    """
    from pyproj import CRS, Transformer

    try:
        transformer = Transformer.from_crs(
            CRS.from_string(from_crs),
            CRS.from_string(to_crs),
            always_xy=True,  # Ensure x=lon, y=lat order
        )
        minx, miny, maxx, maxy = bbox
        # Transform corners
        new_minx, new_miny = transformer.transform(minx, miny)
        new_maxx, new_maxy = transformer.transform(maxx, maxy)
        return (new_minx, new_miny, new_maxx, new_maxy)
    except Exception as e:
        raise ValueError(f"Failed to reproject bbox from {from_crs} to {to_crs}: {e}") from e


def reproject_bbox_if_needed(
    bbox: tuple[float, float, float, float],
    service_crs: str,
    bbox_crs: str | None = None,
) -> tuple[float, float, float, float]:
    """Auto-detect WGS84 bbox and reproject to service CRS if needed.

    If bbox_crs is explicitly provided, it is used directly. Otherwise,
    if the bbox appears to be in WGS84 (based on coordinate ranges) and the
    service uses a different CRS, the bbox is automatically reprojected.

    Args:
        bbox: User-provided bounding box (minx, miny, maxx, maxy).
        service_crs: Service CRS string (e.g., "EPSG:3857").
        bbox_crs: Optional explicit CRS of the bbox (e.g., "EPSG:4326").
            If provided, auto-detection is skipped and this CRS is used.

    Returns:
        Bbox in service CRS (reprojected if needed, original otherwise).
    """
    # If explicit bbox_crs provided, use it directly (no heuristics)
    if bbox_crs is not None:
        bbox_crs_upper = bbox_crs.upper()
        service_crs_upper = service_crs.upper()
        # Normalize CRS:84 to EPSG:4326 for comparison
        if bbox_crs_upper == "CRS:84":
            bbox_crs_upper = "EPSG:4326"
        if service_crs_upper == "CRS:84":
            service_crs_upper = "EPSG:4326"
        if bbox_crs_upper == service_crs_upper:
            return bbox
        logger.info(
            "Reprojecting bbox from %s to service CRS %s.",
            bbox_crs,
            service_crs,
        )
        return _reproject_bbox(bbox, bbox_crs, service_crs)

    # If service is already WGS84, no reprojection needed
    service_crs_upper = service_crs.upper()
    if service_crs_upper in ("EPSG:4326", "CRS:84"):
        return bbox

    # Check if bbox appears to be WGS84 (heuristic auto-detection)
    if _is_likely_wgs84(bbox):
        logger.info(
            "Bbox appears to be WGS84 (coordinates in -180/180, -90/90 range). "
            "Reprojecting to service CRS %s. Use --bbox-crs to override.",
            service_crs,
        )
        return _reproject_bbox(bbox, "EPSG:4326", service_crs)

    # Bbox doesn't look like WGS84, assume it's already in service CRS
    return bbox


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

    Uses platform-specific locking (fcntl on Unix, msvcrt on Windows)
    for atomic writes when multiple concurrent tasks complete simultaneously.

    Args:
        state: Resume state to save.
        path: Path to write the JSON file.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    # Write to temp file then rename for atomicity
    temp_path = path.with_suffix(".tmp")

    try:
        with open(temp_path, "w") as f:
            # Acquire exclusive lock (cross-platform)
            _lock_file(f)
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
                _unlock_file(f)

        # Atomic rename
        temp_path.rename(path)
    except Exception:
        # Clean up temp file on error
        if temp_path.exists():
            temp_path.unlink()
        raise


def _create_empty_result(output_dir: Path) -> ExtractionResult:
    """Create an empty ExtractionResult for early returns.

    Args:
        output_dir: Output directory.

    Returns:
        ExtractionResult with all counts set to zero.
    """
    return ExtractionResult(
        output_dir=output_dir,
        tiles_downloaded=0,
        tiles_skipped=0,
        tiles_failed=0,
        total_bytes=0,
        catalog_initialized=False,
    )


@dataclass
class _ProcessingStats:
    """Mutable container for tile processing statistics."""

    tiles_downloaded: int = 0
    tiles_failed: int = 0
    total_bytes: int = 0
    tiles_since_last_save: int = 0
    tile_results: list[TileResult] = field(default_factory=list)


@dataclass
class _TileProcessResult:
    """Result of processing a single tile."""

    tile: TileSpec
    success: bool
    bytes_downloaded: int
    duration_seconds: float
    error_msg: str | None
    attempts: int


async def _process_tile(
    tile: TileSpec,
    url: str,
    output_dir: Path,
    config: ExtractionConfig,
    client: httpx.AsyncClient,
    metadata: ImageServerMetadata,
    semaphore: asyncio.Semaphore,
    rate_limit_lock: asyncio.Lock,
    last_request_time: dict[str, float],
    collection_name: str = "tiles",
) -> _TileProcessResult:
    """Process a single tile: download and convert to COG.

    STAC metadata is NOT created here - that's handled by the Portolan API
    via _auto_init_catalog() after extraction completes (per ADR-0007, ADR-0031).

    Args:
        tile: Tile to process.
        url: ImageServer URL.
        output_dir: Output directory.
        config: Extraction configuration.
        client: HTTP client.
        metadata: Service metadata.
        semaphore: Concurrency limiter.
        rate_limit_lock: Lock for rate limiting coordination.
        last_request_time: Shared dict tracking last request time per slot.
        collection_name: Name for the collection directory (default: 'tiles').

    Returns:
        _TileProcessResult with tile, success status, bytes, duration, error, attempts.
    """
    start_time = time.monotonic()
    error_msg: str | None = None
    attempts_made = 0

    async with semaphore:
        slot_id = str(id(asyncio.current_task()))
        # Create proper STAC structure: collection/item/asset.tif
        # The collection is named per --collection-name flag (default: 'tiles')
        # tile.get_id() already returns "tile_X_Y" format
        tile_id = tile.get_id()
        item_dir = output_dir / collection_name / tile_id
        item_dir.mkdir(parents=True, exist_ok=True)
        raw_path = item_dir / f"{tile_id}_raw.tif"
        cog_path = item_dir / f"{tile_id}.tif"

        rate_limit_delay = config.rate_limit_delay
        bytes_downloaded = 0

        try:
            for attempt in range(1, config.max_retries + 1):
                attempts_made = attempt
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

                    duration = time.monotonic() - start_time
                    return _TileProcessResult(
                        tile=tile,
                        success=True,
                        bytes_downloaded=bytes_downloaded,
                        duration_seconds=duration,
                        error_msg=None,
                        attempts=attempts_made,
                    )

                except RateLimitError as e:
                    # Handle 429 with exponential backoff
                    delay = e.retry_after or (RATE_LIMIT_429_INITIAL_DELAY * (2 ** (attempt - 1)))
                    delay = min(delay, RATE_LIMIT_429_MAX_DELAY)
                    warn(f"Rate limited on tile {tile.get_id()}, waiting {delay:.1f}s")
                    await asyncio.sleep(delay)
                    # Increase rate limit delay for future requests
                    rate_limit_delay = min(rate_limit_delay * 2, 2.0)
                    error_msg = str(e)

                except ImageServerExtractionError as e:
                    error_msg = str(e)
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
                        duration = time.monotonic() - start_time
                        return _TileProcessResult(
                            tile=tile,
                            success=False,
                            bytes_downloaded=0,
                            duration_seconds=duration,
                            error_msg=error_msg,
                            attempts=attempts_made,
                        )

                except Exception as e:
                    error_msg = str(e)
                    logger.error("Unexpected error processing tile %s: %s", tile.get_id(), e)
                    duration = time.monotonic() - start_time
                    return _TileProcessResult(
                        tile=tile,
                        success=False,
                        bytes_downloaded=0,
                        duration_seconds=duration,
                        error_msg=error_msg,
                        attempts=attempts_made,
                    )

            # All retries exhausted
            duration = time.monotonic() - start_time
            return _TileProcessResult(
                tile=tile,
                success=False,
                bytes_downloaded=0,
                duration_seconds=duration,
                error_msg=error_msg or "Max retries exceeded",
                attempts=attempts_made,
            )

        finally:
            # Clean up raw file on any exit (success or failure)
            if raw_path.exists():
                try:
                    raw_path.unlink()
                except OSError:
                    pass  # Best effort cleanup


def _setup_extraction_dirs(output_dir: Path, collection_name: str = "tiles") -> tuple[Path, Path]:
    """Create extraction output directories.

    Args:
        output_dir: Base output directory.
        collection_name: Name for the collection directory (default: 'tiles').

    Returns:
        Tuple of (collection_dir, portolan_dir).
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    collection_dir = output_dir / collection_name
    collection_dir.mkdir(exist_ok=True)
    portolan_dir = output_dir / ".portolan"
    portolan_dir.mkdir(exist_ok=True)
    return collection_dir, portolan_dir


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


def _seed_metadata_from_report(
    output_dir: Path,
    report: ImageServerExtractionReport,
) -> None:
    """Seed metadata.yaml from extraction report.

    Converts the ImageServerMetadataExtracted to the common ExtractedMetadata
    format and seeds the metadata.yaml file. Does NOT overwrite existing files.

    Args:
        output_dir: Output directory containing .portolan/.
        report: Extraction report with metadata_extracted.
    """
    extracted = report.metadata_extracted.to_extracted()

    metadata_path = output_dir / ".portolan" / "metadata.yaml"
    if seed_metadata_yaml(extracted, metadata_path):
        info(f"Seeded metadata.yaml from {extracted.source_type}")


def _auto_init_catalog(
    output_dir: Path,
    service_name: str | None = None,
    collection_name: str = "tiles",
) -> bool:
    """Initialize a Portolan catalog and add extracted COG files.

    Called automatically after extraction unless raw=True.
    Uses the Portolan API (init_catalog + add_files) to create
    proper STAC structure with items per raster (per ADR-0031).

    Args:
        output_dir: Directory containing extracted COG files.
        service_name: Optional name for the catalog.
        collection_name: Name for the collection directory (default: 'tiles').

    Returns:
        True if catalog was initialized, False if no files to add.
    """
    from portolan_cli.catalog import init_catalog
    from portolan_cli.dataset import add_files

    # Get list of extracted COG files (nested in item directories)
    collection_dir = output_dir / collection_name
    cog_files = list(collection_dir.glob("*/*.tif"))

    if not cog_files:
        return False  # Nothing to add

    # Initialize the catalog
    init_catalog(output_dir, title=service_name)

    # Add all COG files - this creates items per raster (per ADR-0031)
    add_files(
        paths=cog_files,
        catalog_root=output_dir,
    )

    return True


async def _extract_all_tiles(
    tiles: list[TileSpec],
    url: str,
    output_dir: Path,
    config: ExtractionConfig,
    metadata: ImageServerMetadata,
    resume_state: ImageServerResumeState,
    resume_path: Path,
    on_progress: Callable[[TileProgress], None] | None = None,
    collection_name: str = "tiles",
) -> _ProcessingStats:
    """Extract all tiles with concurrency control.

    Args:
        tiles: List of tiles to process.
        url: Service URL.
        output_dir: Output directory.
        config: Extraction config.
        metadata: Service metadata.
        resume_state: Resume state to update.
        resume_path: Path to save resume state.
        on_progress: Optional progress callback (matches FeatureServer pattern).
        collection_name: Name for the collection directory (default: 'tiles').

    Returns:
        Processing statistics with tile results.
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
                semaphore=semaphore,
                rate_limit_lock=rate_limit_lock,
                last_request_time=last_request_time,
                collection_name=collection_name,
            )
            for tile in tiles
        ]

        for i, coro in enumerate(asyncio.as_completed(tasks)):
            result = await coro
            _update_stats_and_state(
                tile=result.tile,
                succeeded=result.success,
                bytes_downloaded=result.bytes_downloaded,
                stats=stats,
                resume_state=resume_state,
                index=i,
                total=len(tiles),
                output_dir=output_dir,
                duration=result.duration_seconds,
                error_msg=result.error_msg,
                attempts=result.attempts,
                on_progress=on_progress,
                collection_name=collection_name,
            )

            # Batch resume state saves
            stats.tiles_since_last_save += 1
            if stats.tiles_since_last_save >= RESUME_SAVE_INTERVAL or not result.success:
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
    output_dir: Path,
    duration: float,
    error_msg: str | None,
    attempts: int,
    on_progress: Callable[[TileProgress], None] | None = None,
    collection_name: str = "tiles",
) -> None:
    """Update statistics, resume state, and tile results after processing a tile.

    Args:
        tile: Processed tile.
        succeeded: Whether tile processing succeeded.
        bytes_downloaded: Bytes downloaded (0 if failed).
        stats: Statistics to update.
        resume_state: Resume state to update.
        index: Current tile index.
        total: Total tiles to process.
        output_dir: Output directory for computing relative paths.
        duration: Processing duration in seconds.
        error_msg: Error message if failed.
        attempts: Number of attempts.
        collection_name: Name for the collection directory (default: 'tiles').
        on_progress: Optional progress callback.
    """
    tile_id = tile.get_id()

    if succeeded:
        stats.tiles_downloaded += 1
        stats.total_bytes += bytes_downloaded
        resume_state.succeeded_tiles.add((tile.x, tile.y))

        # Compute relative output path (tile_id already includes "tile_" prefix)
        output_path = f"{collection_name}/{tile_id}/{tile_id}.tif"

        stats.tile_results.append(
            TileResult(
                tile_id=tile_id,
                status="success",
                size_bytes=bytes_downloaded,
                duration_seconds=duration,
                output_path=output_path,
                error=None,
                attempts=attempts,
            )
        )

        detail(f"Tile {tile_id}: {bytes_downloaded:,} bytes [{index + 1}/{total}]")

        if on_progress:
            on_progress(
                TileProgress(
                    tile_index=index,
                    total_tiles=total,
                    tile_id=tile_id,
                    status="success",
                )
            )
    else:
        stats.tiles_failed += 1
        resume_state.failed_tiles.add((tile.x, tile.y))

        stats.tile_results.append(
            TileResult(
                tile_id=tile_id,
                status="failed",
                size_bytes=None,
                duration_seconds=duration,
                output_path=None,
                error=error_msg,
                attempts=attempts,
            )
        )

        error(f"Tile {tile_id}: failed [{index + 1}/{total}]")

        if on_progress:
            on_progress(
                TileProgress(
                    tile_index=index,
                    total_tiles=total,
                    tile_id=tile_id,
                    status="failed",
                )
            )


def _validate_collection_name(name: str) -> str:
    """Validate and sanitize collection name to prevent path traversal.

    Args:
        name: User-provided collection name.

    Returns:
        Sanitized collection name (base name only, no path components).

    Raises:
        ValueError: If the sanitized name is empty or invalid.
    """
    # Extract just the base name (strips any path separators or .. components)
    sanitized = Path(name).name

    # Reject empty names or names that are just dots
    if not sanitized or sanitized in (".", ".."):
        raise ValueError(
            f"Invalid collection name: '{name}'. "
            "Collection name cannot be empty or contain path traversal sequences."
        )

    # Reject names with problematic characters for cross-platform compatibility
    invalid_chars = '<>:"|?*'
    for char in invalid_chars:
        if char in sanitized:
            raise ValueError(
                f"Invalid collection name: '{name}'. "
                f"Collection name cannot contain: {invalid_chars}"
            )

    return sanitized


async def extract_imageserver(
    url: str,
    output_dir: Path,
    config: ExtractionConfig | None = None,
    resume: bool = False,
    bbox: tuple[float, float, float, float] | None = None,
    on_progress: Callable[[TileProgress], None] | None = None,
    collection_name: str | None = None,
    bbox_crs: str | None = None,
) -> ExtractionResult:
    """Extract raster tiles from ImageServer to COG files.

    This orchestrates the extraction pipeline. STAC metadata is created
    via the Portolan API after extraction (unless raw=True).

    Args:
        url: ImageServer URL.
        output_dir: Directory to write extracted data.
        config: Extraction configuration (defaults to ExtractionConfig()).
        resume: If True, resume from previous extraction.
        bbox: Optional bbox to subset extraction (minx, miny, maxx, maxy).
        on_progress: Optional callback for progress updates (matches FeatureServer pattern).
        collection_name: Name for the collection directory (default: 'tiles').
        bbox_crs: Optional explicit CRS of the bbox (e.g., "EPSG:4326", "EPSG:3857").
            If provided, skips auto-detection and uses this CRS for reprojection.

    Returns:
        ExtractionResult with extraction statistics and full report.

    Raises:
        ImageServerDiscoveryError: If service discovery fails.
        ValueError: If collection_name contains path traversal sequences.
    """
    if config is None:
        config = ExtractionConfig()

    # Default collection name to 'tiles' if not provided, then validate
    if collection_name is None:
        collection_name = "tiles"
    else:
        collection_name = _validate_collection_name(collection_name)

    start_time = time.monotonic()

    # Setup
    _, portolan_dir = _setup_extraction_dirs(output_dir, collection_name)
    config = _load_effective_config(config, output_dir)

    # Discover service
    info(f"Discovering ImageServer: {url}")
    metadata = await discover_imageserver(url, timeout=config.timeout)
    info(f"Service: {metadata.name} ({metadata.pixel_type}, {metadata.band_count} bands)")

    # Validate tile size against service limits (proactive check per issue #335)
    max_tile_size = min(metadata.max_image_width, metadata.max_image_height)
    if config.tile_size > max_tile_size:
        warn(
            f"Requested tile size ({config.tile_size}px) exceeds service limit "
            f"({max_tile_size}px). Auto-adjusting to {max_tile_size}px."
        )
        # Use dataclasses.replace to preserve all fields (including compression)
        config = replace(config, tile_size=max_tile_size)

    # Get service CRS for bbox reprojection
    service_crs = metadata.get_crs_string()

    # Compute tiles
    extent = metadata.full_extent
    if bbox:
        # Reproject bbox to service CRS if needed (auto-detect or explicit via bbox_crs)
        bbox = reproject_bbox_if_needed(bbox, service_crs, bbox_crs=bbox_crs)
        intersected = _intersect_bbox(bbox, extent)
        if intersected is None:
            info("User bbox does not intersect service extent - no tiles to extract")
            return _create_empty_result(output_dir)
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
        return _create_empty_result(output_dir)

    if config.dry_run:
        info(f"[DRY RUN] Would extract {len(tiles)} tiles")
        return _create_empty_result(output_dir)

    # Resume state
    resume_path = portolan_dir / "imageserver-resume.json"
    resume_state = _load_or_create_resume_state(resume, resume_path, url)

    tiles_to_process = [t for t in tiles if should_process_tile(t.x, t.y, resume_state)]
    # Compute skipped tiles BEFORE extraction (resume_state changes during extraction)
    skipped_tile_specs = [t for t in tiles if not should_process_tile(t.x, t.y, resume_state)]
    tiles_skipped = len(skipped_tile_specs)
    if tiles_skipped > 0:
        info(f"Skipping {tiles_skipped} already-completed tiles")

    # Extract tiles (COG files only, no STAC metadata)
    stats = await _extract_all_tiles(
        tiles_to_process,
        url,
        output_dir,
        config,
        metadata,
        resume_state,
        resume_path,
        on_progress=on_progress,
        collection_name=collection_name,
    )
    _save_resume_state_locked(resume_state, resume_path)

    # Add skipped tiles to results (computed BEFORE extraction)
    for tile in skipped_tile_specs:
        tile_id = tile.get_id()
        stats.tile_results.append(
            TileResult(
                tile_id=tile_id,
                status="skipped",
                size_bytes=None,
                duration_seconds=None,
                output_path=f"{collection_name}/{tile_id}/{tile_id}.tif",
                error=None,
                attempts=0,
            )
        )

    total_duration = time.monotonic() - start_time

    # Build and save extraction report
    report = build_imageserver_report(
        url=url,
        metadata=metadata,
        tile_results=stats.tile_results,
        total_duration=total_duration,
    )
    report_path = portolan_dir / "extraction-report.json"
    save_imageserver_report(report, report_path)

    # Seed metadata.yaml from extracted service metadata
    _seed_metadata_from_report(output_dir, report)

    success(f"Extracted {stats.tiles_downloaded} tiles ({stats.total_bytes:,} bytes)")
    if stats.tiles_failed > 0:
        error(f"Failed: {stats.tiles_failed} tiles")
    info(f"Report: {report_path}")

    # Auto-init catalog using Portolan API (unless raw mode)
    catalog_initialized = False
    if not config.raw:
        info("Initializing Portolan catalog...")
        catalog_initialized = _auto_init_catalog(output_dir, metadata.name, collection_name)
        if catalog_initialized:
            success("Catalog initialized with STAC metadata")
        else:
            warn("No COG files found to add to catalog")

    return ExtractionResult(
        output_dir=output_dir,
        tiles_downloaded=stats.tiles_downloaded,
        tiles_skipped=tiles_skipped,
        tiles_failed=stats.tiles_failed,
        total_bytes=stats.total_bytes,
        catalog_initialized=catalog_initialized,
        report=report,
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
