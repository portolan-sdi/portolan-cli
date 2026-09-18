"""ImageServer extraction orchestrator.

This module orchestrates the extraction pipeline for ArcGIS ImageServer:
1. Discover service metadata (pixel type, extent, spatial reference)
2. Compute tile grid based on service limits and desired tile size
3. Download tiles via exportImage API (async, parallel with rate limiting)
4. Convert each tile to COG format using rio-cogeo
5. Save extraction report for resume support (atomic writes)
6. Auto-init Portolan catalog (unless raw mode) using standard API

The extractor does NOT create STAC metadata directly. Instead, it extracts
COG files and then calls the Portolan API (init_catalog + add_files) to
create proper STAC structure with items per raster.

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
import math
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

import httpx
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

from portolan_cli.conversion_config import CogSettings, get_cog_settings, resolve_cog_settings
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
from portolan_cli.extract.arcgis.imageserver.tilecache import (
    DEFAULT_CACHE_CONCURRENCY,
    LevelOfDetail,
    TileCacheError,
    TileCacheInfo,
    compute_cache_tile_grid,
    fetch_cache_tile,
    probe_block_empty,
    select_probe_lod,
)
from portolan_cli.extract.arcgis.imageserver.tiling import TileSpec, compute_tile_grid
from portolan_cli.json_io import write_json_atomic
from portolan_cli.licensing import (
    ResolvedLicense,
    license_url_from_text,
    resolve_harvest_license,
)
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

# Cache tile requests above which a tile cache run reports its own cost.
CACHE_REQUEST_WARNING_THRESHOLD = 10_000


class ImageServerExtractionError(Exception):
    """Error during ImageServer extraction."""

    pass


def _pool_limits(in_flight: int) -> httpx.Limits:
    """Build connection pool limits that match the request concurrency.

    The httpx default keeps 20 connections alive. A tile cache run holds more
    requests in flight than that, so the pool closes and reopens connections
    on every tile. Matching the two measured 112 to 280 requests per second
    against a live ArcGIS Online cache (issue #870).

    Args:
        in_flight: Maximum requests the caller runs at the same time.

    Returns:
        Limits for httpx.AsyncClient.
    """
    return httpx.Limits(max_connections=in_flight, max_keepalive_connections=in_flight)


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
        cog_settings: COG conversion settings (from config.yaml or defaults).
        max_retries: Maximum retry attempts per tile on failure.
        dry_run: If True, compute tiles but don't download anything.
        raw: If True, skip auto-init (only create COGs + report, no STAC catalog).
        timeout: HTTP request timeout in seconds.
        max_concurrent: Maximum concurrent tile downloads.
        rate_limit_delay: Minimum delay between requests per slot (seconds).
        catalog_id: Catalog id for the created catalog. None derives it from
            the output directory name, which is the behavior before issue #821.
        coarse_scan: Ask a coarse cache level which blocks hold data before
            reading them. It turns one request per cache tile into one request
            per block for the empty parts of a sparse cache (issue #870). It
            is a heuristic, because a cache pyramid can drop a thin feature at
            a coarse level. It is off by default.
    """

    tile_size: int = 4096
    cog_settings: CogSettings = field(default_factory=CogSettings)
    max_retries: int = 3
    dry_run: bool = False
    raw: bool = False
    timeout: float = 120.0
    max_concurrent: int = 4
    rate_limit_delay: float = DEFAULT_RATE_LIMIT_DELAY
    catalog_id: str | None = None
    coarse_scan: bool = False

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
        tiles_empty: Number of tiles that hold no valid pixel. A tile cache is
            sparse, so a service extent covers far more area than the data
            does. An empty tile writes no COG (issue #870).
        total_bytes: Total bytes downloaded.
        catalog_initialized: Whether Portolan catalog was auto-initialized.
        report: Full extraction report with metadata and tile results.
    """

    output_dir: Path
    tiles_downloaded: int
    tiles_skipped: int
    tiles_failed: int = 0
    tiles_empty: int = 0
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


# Longest response body excerpt that a tile error message quotes.
_ERROR_BODY_EXCERPT_CHARS = 200


def _describe_arcgis_error(
    tile: TileSpec,
    status_code: int,
    content: bytes,
    export_url: str,
) -> str | None:
    """Describe an ArcGIS JSON error body, or return None if it is not one.

    ArcGIS returns errors as ``{"error": {"code", "message", "details"}}``.
    The server sends this body with HTTP 200 for request errors and with
    HTTP 4xx/5xx for server errors. The details list often carries the real
    reason (issue #870), so the message includes it.

    Args:
        tile: Tile whose request failed.
        status_code: HTTP status of the response.
        content: Raw response body.
        export_url: Full exportImage URL that was requested.

    Returns:
        Error message, or None when the body is not an ArcGIS error.
    """
    if not content.lstrip().startswith(b"{"):
        return None
    try:
        error_data = json.loads(content.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(error_data, dict) or "error" not in error_data:
        return None

    arcgis_error = error_data["error"]
    if not isinstance(arcgis_error, dict):
        arcgis_error = {"message": str(arcgis_error)}
    code = arcgis_error.get("code", status_code)
    message = str(arcgis_error.get("message", "Unknown error")).rstrip(".")
    details_raw = arcgis_error.get("details") or []
    if isinstance(details_raw, str):
        details_raw = [details_raw]
    details = [str(d).strip() for d in details_raw if str(d).strip()]

    msg = f"ArcGIS error for tile {tile.get_id()}: HTTP {status_code} [{code}] {message}."
    if details:
        msg += f" Details: {'; '.join(details)}"
        if not msg.endswith("."):
            msg += "."
    return f"{msg} Request: {export_url}"


def _describe_http_error(
    tile: TileSpec,
    status_code: int,
    content: bytes,
    export_url: str,
) -> str:
    """Build the error message for a non-2xx exportImage response.

    Prefers the ArcGIS JSON error when the body has one. Otherwise quotes an
    excerpt of the body, or says the body was empty. Every message ends with
    the request URL so the user can reproduce the failure (issue #870).

    Args:
        tile: Tile whose request failed.
        status_code: HTTP status of the response.
        content: Raw response body.
        export_url: Full exportImage URL that was requested.

    Returns:
        Error message.
    """
    arcgis_msg = _describe_arcgis_error(tile, status_code, content, export_url)
    if arcgis_msg is not None:
        return arcgis_msg

    excerpt = content[:_ERROR_BODY_EXCERPT_CHARS].decode("utf-8", errors="replace").strip()
    excerpt = " ".join(excerpt.split())
    if not excerpt:
        return (
            f"Tile download failed ({tile.get_id()}): HTTP {status_code} "
            f"(empty response body). Request: {export_url}"
        )
    return (
        f"Tile download failed ({tile.get_id()}): HTTP {status_code}. "
        f"Response: {excerpt}. Request: {export_url}"
    )


def _describe_non_tiff_body(
    tile: TileSpec,
    status_code: int,
    content: bytes,
    export_url: str,
) -> str:
    """Build the error message for a 2xx response that is not a TIFF.

    Args:
        tile: Tile whose request failed.
        status_code: HTTP status of the response.
        content: Raw response body.
        export_url: Full exportImage URL that was requested.

    Returns:
        Error message.
    """
    # HTML error page
    if content.startswith(b"<!") or content.startswith(b"<html"):
        return (
            f"Server returned HTML instead of TIFF for tile {tile.get_id()}. Request: {export_url}"
        )
    # ArcGIS JSON error, sent with HTTP 200 for request errors
    arcgis_msg = _describe_arcgis_error(tile, status_code, content, export_url)
    if arcgis_msg is not None:
        return arcgis_msg
    # Other JSON
    if content.startswith(b"{"):
        try:
            json.loads(content.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass
        else:
            excerpt = content[:_ERROR_BODY_EXCERPT_CHARS].decode("utf-8", errors="replace")
            return f"Unexpected JSON response for tile {tile.get_id()}: {excerpt}"
    return f"Invalid TIFF data for tile {tile.get_id()} (bad magic bytes)"


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
            raise ImageServerExtractionError(
                _describe_non_tiff_body(tile, response.status_code, content, export_url)
            )

        # Ensure parent directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Write to disk
        output_path.write_bytes(content)
        return len(content)

    except httpx.HTTPStatusError as e:
        msg = _describe_http_error(tile, e.response.status_code, e.response.content, export_url)
        raise ImageServerExtractionError(msg) from e
    except httpx.TimeoutException as e:
        msg = f"Tile download timeout ({tile.get_id()}). Request: {export_url}"
        raise ImageServerExtractionError(msg) from e
    except httpx.RequestError as e:
        msg = f"Tile download failed ({tile.get_id()}): {e}. Request: {export_url}"
        raise ImageServerExtractionError(msg) from e
    except OSError as e:
        msg = f"Failed to write tile ({tile.get_id()}): {e}"
        raise ImageServerExtractionError(msg) from e


async def _convert_to_cog(
    input_path: Path,
    output_path: Path,
    cog_settings: CogSettings,
    add_mask: bool = False,
) -> None:
    """Convert a TIFF to COG format using settings from config.

    Runs rio-cogeo in a thread executor since it's CPU-bound.
    Uses settings from .portolan/config.yaml.

    Args:
        input_path: Path to input TIFF.
        output_path: Path for output COG.
        cog_settings: COG conversion settings.
        add_mask: Keep the input's validity mask in the COG. The tile cache
            path sets this, because a sparse cache leaves parts of a tile with
            no data and the pixel values alone do not say which (issue #870).
    """
    loop = asyncio.get_event_loop()

    def _do_convert() -> None:
        # Fill in any "auto" field from the downloaded tile (Issue #690)
        settings = resolve_cog_settings(cog_settings, input_path)

        # Get base profile and customize with our settings
        profile = cog_profiles.get(settings.compression.lower())  # type: ignore[no-untyped-call]

        # Apply predictor (for lossless compression)
        if settings.compression.upper() not in ("JPEG", "WEBP"):
            profile["predictor"] = settings.predictor

        # Apply quality for lossy compression
        if settings.quality is not None and settings.compression.upper() in (
            "JPEG",
            "WEBP",
        ):
            profile["quality"] = settings.quality

        # Apply tile size
        profile["blockxsize"] = settings.tile_size
        profile["blockysize"] = settings.tile_size

        cog_translate(
            str(input_path),
            str(output_path),
            profile,
            # CogSettings.resampling is validated at config load time
            overview_resampling=settings.resampling,  # type: ignore[arg-type]
            add_mask=add_mask,
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


def _save_resume_state(state: ImageServerResumeState, path: Path) -> None:
    """Save resume state atomically.

    Concurrent tile tasks each save the whole state, so two saves can overlap.
    :func:`write_json_atomic` gives each one its own temp file and lands it with
    ``os.replace``, so a reader sees one complete state and the last writer wins.
    That replaces the old shared-``.tmp``-plus-``flock`` dance, which serialized
    writers only after both had already truncated the same temp file.

    Args:
        state: Resume state to save.
        path: Path to write the JSON file.
    """
    data = {
        "extraction_type": "imageserver",
        "service_url": state.service_url,
        "started_at": state.started_at.isoformat().replace("+00:00", "Z"),
        "tiles": {
            "succeeded": sorted([list(coord) for coord in state.succeeded_tiles]),
            "failed": sorted([list(coord) for coord in state.failed_tiles]),
        },
    }
    write_json_atomic(path, data)


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
    tiles_empty: int = 0
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
    empty: bool = False


@dataclass(frozen=True)
class _TilePlan:
    """Tiles to extract, and the source they come from.

    Attributes:
        tiles: Output tiles, in row-major order.
        cache: Tile cache to read, or None when the extractor calls
            exportImage.
        lod: Cache level to read, or None when the extractor calls
            exportImage.
    """

    tiles: list[TileSpec]
    cache: TileCacheInfo | None = None
    lod: LevelOfDetail | None = None


def _extent_in_cache_crs(
    metadata: ImageServerMetadata,
    extent: dict[str, Any],
    cache: TileCacheInfo,
) -> tuple[dict[str, Any], float]:
    """Convert an extent and the service pixel size to the cache CRS.

    The cache grid arithmetic subtracts the cache origin from the extent, so
    both must use the same CRS. The pixel size keeps the pixel count across
    the extent, so the reader selects a level of the same detail.

    Args:
        metadata: Service metadata.
        extent: Extent to cover, in the service CRS.
        cache: Cache grid.

    Returns:
        Tuple of the extent and the pixel size, both in the cache CRS.

    Raises:
        ImageServerExtractionError: If the extent cannot be reprojected.
    """
    service_sr = metadata.full_extent.get("spatialReference") or {}
    if not (service_sr.get("latestWkid") or service_sr.get("wkid")):
        # get_crs_string() would guess EPSG:4326. Assume the cache CRS instead.
        return extent, metadata.pixel_size_x
    service_crs = metadata.get_crs_string()
    try:
        cache_crs = cache.crs_string()
    except TileCacheError as e:
        raise ImageServerExtractionError(str(e)) from e
    if service_crs == cache_crs:
        return extent, metadata.pixel_size_x

    from pyproj import CRS, Transformer

    try:
        transformer = Transformer.from_crs(
            CRS.from_string(service_crs), CRS.from_string(cache_crs), always_xy=True
        )
        xmin, ymin, xmax, ymax = transformer.transform_bounds(
            extent["xmin"], extent["ymin"], extent["xmax"], extent["ymax"], densify_pts=21
        )
    except Exception as e:
        raise ImageServerExtractionError(
            f"Cannot reproject the service extent from {service_crs} to the cache CRS "
            f"{cache_crs}: {e}"
        ) from e

    source_width = extent["xmax"] - extent["xmin"]
    pixel_size = metadata.pixel_size_x
    if source_width > 0 and pixel_size > 0:
        pixel_size = pixel_size * (xmax - xmin) / source_width
    return {"xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax}, pixel_size


def _plan_tiles(
    metadata: ImageServerMetadata,
    extent: dict[str, Any],
    config: ExtractionConfig,
) -> _TilePlan:
    """Choose the tile source and compute the tile grid.

    A service that lists the TilesOnly capability rejects exportImage with
    HTTP 400 at every size. The extractor reads its cache instead, at the
    level whose resolution matches the service pixel size (issue #870).

    Args:
        metadata: Service metadata.
        extent: Extent to cover, in the service CRS.
        config: Extraction configuration.

    Returns:
        The tile plan.

    Raises:
        ImageServerExtractionError: If the service rejects exportImage and
            publishes no tile cache, so no path can read it. Also if the
            extent cannot be reprojected to the cache CRS.
    """
    if metadata.export_image_supported:
        tiles = list(
            compute_tile_grid(
                extent=extent,
                pixel_size_x=metadata.pixel_size_x,
                pixel_size_y=metadata.pixel_size_y,
                tile_size=config.tile_size,
            )
        )
        return _TilePlan(tiles=tiles)

    cache = metadata.tile_cache
    if cache is None:
        raise ImageServerExtractionError(
            f"Service '{metadata.name}' reports the TilesOnly capability, so it "
            "rejects exportImage. It also publishes no tileInfo block, so Portolan "
            "has no way to read it."
        )

    extent, pixel_size = _extent_in_cache_crs(metadata, extent, cache)
    lod = cache.select_lod(pixel_size)
    info(
        f"Service serves only cached tiles. Reading level {lod.level} "
        f"({lod.resolution:g} units per pixel) from the {cache.tile_format} cache."
    )
    tiles = list(compute_cache_tile_grid(extent, cache, lod, tile_size=config.tile_size))
    return _TilePlan(tiles=tiles, cache=cache, lod=lod)


def _warn_on_cache_request_count(tiles: list[TileSpec], cache: TileCacheInfo) -> None:
    """Report how many cache requests the run still costs.

    The reader must ask for every cache tile of every block it keeps, because
    the cache reports an empty tile the same way at every level. A full-extent
    read of a continental service costs hundreds of thousands of requests, so
    the user must see the number before the reads start (issue #870). When
    the coarse scan runs, this counts only the blocks that hold data.

    Args:
        tiles: Output tiles the reader still has to read.
        cache: Cache grid.
    """
    requests = sum(
        math.ceil(tile.width_px / cache.tile_width) * math.ceil(tile.height_px / cache.tile_height)
        for tile in tiles
    )
    if requests < CACHE_REQUEST_WARNING_THRESHOLD:
        return
    warn(
        f"This run reads {requests:,} cache tiles. Use --bbox to name a smaller "
        "area, or --max-concurrent to run more requests at the same time."
    )


async def _download_one_tile(
    tile: TileSpec,
    url: str,
    raw_path: Path,
    client: httpx.AsyncClient,
    metadata: ImageServerMetadata,
    plan: _TilePlan,
) -> tuple[int, bool]:
    """Write one raw GeoTIFF, from exportImage or from the tile cache.

    Args:
        tile: Tile to read.
        url: ImageServer URL.
        raw_path: Path for the raw GeoTIFF.
        client: HTTP client.
        metadata: Service metadata.
        plan: Tile plan, which says which source to read.

    Returns:
        Tuple of the bytes read and whether the tile holds no valid pixel.

    Raises:
        ImageServerExtractionError: If the read fails.
    """
    if plan.cache is None or plan.lod is None:
        downloaded = await download_tile(
            url=url,
            tile=tile,
            output_path=raw_path,
            client=client,
            pixel_type=metadata.pixel_type,
        )
        return downloaded, False

    try:
        result = await fetch_cache_tile(
            url,
            tile,
            raw_path,
            client,
            plan.cache,
            plan.lod,
            plan.cache.crs_string(),
        )
    except TileCacheError as e:
        raise ImageServerExtractionError(str(e)) from e
    return result.bytes_downloaded, result.empty


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
    plan: _TilePlan | None = None,
) -> _TileProcessResult:
    """Process a single tile: download and convert to COG.

    STAC metadata is NOT created here - that's handled by the Portolan API
    via _auto_init_catalog() after extraction completes.

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
        plan: Tile plan, which says whether to call exportImage or read the
            tile cache. None calls exportImage.

    Returns:
        _TileProcessResult with tile, success status, bytes, duration, error, attempts.
    """
    if plan is None:
        plan = _TilePlan(tiles=[tile])
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
                    bytes_downloaded, is_empty = await _download_one_tile(
                        tile=tile,
                        url=url,
                        raw_path=raw_path,
                        client=client,
                        metadata=metadata,
                        plan=plan,
                    )

                    # An empty tile holds no valid pixel, so it gets no COG.
                    # The item directory would be empty, so remove it too.
                    if is_empty:
                        _remove_empty_item_dir(item_dir)
                        return _TileProcessResult(
                            tile=tile,
                            success=True,
                            bytes_downloaded=bytes_downloaded,
                            duration_seconds=time.monotonic() - start_time,
                            error_msg=None,
                            attempts=attempts_made,
                            empty=True,
                        )

                    # Convert to COG using config settings
                    await _convert_to_cog(
                        raw_path,
                        cog_path,
                        config.cog_settings,
                        add_mask=plan.cache is not None,
                    )

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


def _remove_empty_item_dir(item_dir: Path) -> None:
    """Remove the item directory of a tile that holds no data.

    Args:
        item_dir: Directory created for the tile.
    """
    try:
        item_dir.rmdir()
    except OSError:
        pass  # Best effort: a non-empty directory stays.


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
            # replace() keeps every other option, such as raw and catalog_id.
            return replace(config, cog_settings=catalog_cog_settings)
    except Exception as e:
        logger.debug("Could not load COG settings from config: %s", e)

    return config


def _seed_metadata_from_report(
    output_dir: Path,
    report: ImageServerExtractionReport,
    resolved_license: ResolvedLicense | None = None,
) -> None:
    """Seed metadata.yaml from extraction report.

    Converts the ImageServerMetadataExtracted to the common ExtractedMetadata
    format and seeds the metadata.yaml file. Does NOT overwrite existing files.

    Args:
        output_dir: Output directory containing .portolan/.
        report: Extraction report with metadata_extracted.
        resolved_license: License resolved before the download, which wins over
            anything the harvest found (issue #686). None in raw mode.
    """
    extracted = report.metadata_extracted.to_extracted()

    metadata_path = output_dir / ".portolan" / "metadata.yaml"
    if seed_metadata_yaml(extracted, metadata_path, license_override=resolved_license):
        info(f"Seeded metadata.yaml from {extracted.source_type}")


def _auto_init_catalog(
    output_dir: Path,
    service_name: str | None = None,
    collection_name: str = "tiles",
    catalog_id: str | None = None,
) -> bool:
    """Initialize a Portolan catalog and add extracted COG files.

    Called automatically after extraction unless raw=True.
    Uses the Portolan API (init_catalog + add_files) to create
    proper STAC structure with items per raster.

    Args:
        output_dir: Directory containing extracted COG files.
        service_name: Optional name for the catalog.
        collection_name: Name for the collection directory (default: 'tiles').
        catalog_id: Catalog id for the created catalog. None derives it from
            the output directory name, which is the behavior before issue #821.

    Returns:
        True if catalog was initialized, False if no files to add.
    """
    from portolan_cli.add import add_files
    from portolan_cli.catalog import CatalogState, detect_state, init_catalog

    # Get list of extracted COG files (nested in item directories)
    collection_dir = output_dir / collection_name
    cog_files = list(collection_dir.glob("*/*.tif"))

    if not cog_files:
        return False  # Nothing to add

    # A directory that is already a Portolan catalog gets the new rasters added to
    # it, not an abort after the tiles already downloaded (issue #767). init_catalog
    # raises CatalogAlreadyExistsError on a MANAGED directory, so skip it. The
    # catalog already carries a license, so the add license gate (issue #686) passes.
    if detect_state(output_dir) is not CatalogState.MANAGED:
        # Initialize the catalog. license_id=None because the ImageServer path seeds
        # metadata.yaml from --license or the harvested service licenseInfo before
        # this runs (issue #686, issue #870).
        # Print what init_catalog had to guess, the way `init` does, so a derived
        # id that names a tooling artifact does not reach a published catalog
        # unflagged (issue #821).
        _, init_warnings = init_catalog(
            output_dir, catalog_id=catalog_id, title=service_name, license_id=None
        )
        for message in init_warnings:
            warn(message)
    elif catalog_id is not None:
        # The catalog already exists and keeps the id it was created with. Say so,
        # rather than accept the flag and change nothing (issue #821).
        warn(
            f"Ignored --id '{catalog_id}'. {output_dir} is already a Portolan "
            "catalog and keeps the id it was created with."
        )

    # Add all COG files - this creates items per raster
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
    plan: _TilePlan | None = None,
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
        plan: Tile plan, which says whether to call exportImage or read the
            tile cache.

    Returns:
        Processing statistics with tile results.
    """
    semaphore = asyncio.Semaphore(config.max_concurrent)
    rate_limit_lock = asyncio.Lock()
    last_request_time: dict[str, float] = {}
    stats = _ProcessingStats()

    # The tile cache path runs DEFAULT_CACHE_CONCURRENCY requests inside every
    # output tile, so the pool must hold that many connections open.
    per_tile = DEFAULT_CACHE_CONCURRENCY if plan is not None and plan.cache else 1
    limits = _pool_limits(config.max_concurrent * per_tile)

    async with httpx.AsyncClient(timeout=config.timeout, limits=limits) as client:
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
                plan=plan,
            )
            for tile in tiles
        ]

        for i, coro in enumerate(asyncio.as_completed(tasks)):
            result = await coro
            _update_stats_and_state(
                tile=result.tile,
                succeeded=result.success,
                empty=result.empty,
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
                _save_resume_state(resume_state, resume_path)
                stats.tiles_since_last_save = 0

    return stats


def _record_empty_tile(
    tile: TileSpec,
    stats: _ProcessingStats,
    resume_state: ImageServerResumeState,
    index: int,
    total: int,
    duration: float,
    attempts: int,
    on_progress: Callable[[TileProgress], None] | None,
) -> None:
    """Record a tile that holds no valid pixel.

    A tile cache is sparse. The service extent covers far more area than the
    data does, so many tiles come back empty (issue #870). An empty tile is
    not a failure and it writes no COG. The resume state marks it complete, so
    a re-run with --resume does not read it again.

    Args:
        tile: Processed tile.
        stats: Statistics to update.
        resume_state: Resume state to update.
        index: Current tile index.
        total: Total tiles to process.
        duration: Processing duration in seconds.
        attempts: Number of attempts.
        on_progress: Optional progress callback.
    """
    tile_id = tile.get_id()
    stats.tiles_empty += 1
    resume_state.succeeded_tiles.add((tile.x, tile.y))
    stats.tile_results.append(
        TileResult(
            tile_id=tile_id,
            status="empty",
            size_bytes=None,
            duration_seconds=duration,
            output_path=None,
            error=None,
            attempts=attempts,
        )
    )
    detail(f"Tile {tile_id}: no data [{index + 1}/{total}]")
    if on_progress:
        on_progress(
            TileProgress(
                tile_index=index,
                total_tiles=total,
                tile_id=tile_id,
                status="empty",
            )
        )


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
    empty: bool = False,
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
        empty: True when the tile holds no valid pixel, so it wrote no COG.
    """
    tile_id = tile.get_id()

    if empty:
        _record_empty_tile(tile, stats, resume_state, index, total, duration, attempts, on_progress)
        return

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

        failure_line = f"Tile {tile_id}: failed [{index + 1}/{total}]"
        if error_msg:
            failure_line += f": {error_msg}"
        error(failure_line)

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


async def _scan_for_empty_blocks(
    url: str,
    tiles: list[TileSpec],
    plan: _TilePlan,
    config: ExtractionConfig,
) -> tuple[list[TileSpec], list[TileSpec]]:
    """Split output tiles into the ones to read and the ones with no data.

    A sparse cache answers an empty tile the same way at every level, so the
    reader must ask. Asking a coarse level costs one request per block instead
    of one per cache tile (issue #870). A block the coarse level calls empty is
    skipped. Any doubt reads the block in full, so a probe failure costs time
    rather than data.

    Args:
        url: ImageServer URL.
        tiles: Output tiles from the plan.
        plan: Tile plan, which must carry a cache and a level.
        config: Extraction configuration.

    Returns:
        Tuple of the tiles to read and the tiles the coarse level calls empty.
    """
    cache, read_lod = plan.cache, plan.lod
    if cache is None or read_lod is None or not config.coarse_scan:
        return tiles, []

    block_px = max((tile.width_px for tile in tiles), default=0)
    probe_lod = select_probe_lod(cache, read_lod, block_px)
    if probe_lod is None:
        return tiles, []

    info(
        f"Scanning level {probe_lod.level} to find the blocks that hold data ({len(tiles)} blocks)"
    )
    in_flight = config.max_concurrent * DEFAULT_CACHE_CONCURRENCY
    semaphore = asyncio.Semaphore(in_flight)

    async def _probe(tile: TileSpec, client: httpx.AsyncClient) -> bool:
        async with semaphore:
            return await probe_block_empty(url, tile, client, cache, probe_lod)

    async with httpx.AsyncClient(timeout=config.timeout, limits=_pool_limits(in_flight)) as client:
        verdicts = await asyncio.gather(*(_probe(tile, client) for tile in tiles))

    keep = [tile for tile, is_empty in zip(tiles, verdicts, strict=True) if not is_empty]
    empty = [tile for tile, is_empty in zip(tiles, verdicts, strict=True) if is_empty]
    if empty:
        info(
            f"Skipped {len(empty)} blocks that hold no data at level {probe_lod.level}. "
            "Pass --no-coarse-scan to read every cache tile."
        )
    return keep, empty


def _record_coarse_empty_tiles(
    tiles: list[TileSpec],
    stats: _ProcessingStats,
    resume_state: ImageServerResumeState,
) -> None:
    """Record the blocks the coarse scan called empty.

    Args:
        tiles: Blocks the coarse scan skipped.
        stats: Statistics to update.
        resume_state: Resume state to update, so --resume does not re-probe.
    """
    for tile in tiles:
        stats.tiles_empty += 1
        resume_state.succeeded_tiles.add((tile.x, tile.y))
        stats.tile_results.append(
            TileResult(
                tile_id=tile.get_id(),
                status="empty",
                size_bytes=None,
                duration_seconds=None,
                output_path=None,
                error=None,
                attempts=0,
            )
        )


def _clamp_tile_size(config: ExtractionConfig, metadata: ImageServerMetadata) -> ExtractionConfig:
    """Clamp the tile size to the exportImage limits of the service.

    This is the proactive check of issue #335. The limits describe exportImage.
    A cache-only service ignores them, because its tiles come at the size the
    cache stores (issue #870).
    """
    max_tile_size = min(metadata.max_image_width, metadata.max_image_height)
    if not metadata.export_image_supported or config.tile_size <= max_tile_size:
        return config
    warn(
        f"Requested tile size ({config.tile_size}px) exceeds service limit "
        f"({max_tile_size}px). Auto-adjusting to {max_tile_size}px."
    )
    # Use dataclasses.replace to preserve all fields (including compression)
    return replace(config, tile_size=max_tile_size)


def _report_tile_counts(stats: _ProcessingStats, report_path: Path) -> None:
    """Print the downloaded, empty, and failed tile counts of a run."""
    success(f"Extracted {stats.tiles_downloaded} tiles ({stats.total_bytes:,} bytes)")
    if stats.tiles_empty > 0:
        info(f"Skipped {stats.tiles_empty} tiles that hold no data")
    if stats.tiles_failed > 0:
        error(f"Failed: {stats.tiles_failed} tiles")
    info(f"Report: {report_path}")


async def extract_imageserver(
    url: str,
    output_dir: Path,
    config: ExtractionConfig | None = None,
    resume: bool = False,
    bbox: tuple[float, float, float, float] | None = None,
    on_progress: Callable[[TileProgress], None] | None = None,
    collection_name: str | None = None,
    bbox_crs: str | None = None,
    license_id: str | None = None,
    license_url: str | None = None,
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
        license_id: SPDX identifier from --license, or "other" with license_url.
            Overrides any license URL in the service's licenseInfo (issue #686).
        license_url: URL of the license text from --license-url.

    Returns:
        ExtractionResult with extraction statistics and full report.

    Raises:
        ImageServerDiscoveryError: If service discovery fails.
        MissingLicenseError: If neither the flags nor the service licenseInfo
            yield a license, unless config.raw is True. Raised before any tile
            downloads, so the failure costs a re-run rather than a download.
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

    config = _clamp_tile_size(config, metadata)

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

    plan = _plan_tiles(metadata, extent, config)
    tiles = plan.tiles
    info(f"Computed {len(tiles)} tiles to extract")

    if not tiles:
        success("No tiles to extract (bbox may not intersect service extent)")
        return _create_empty_result(output_dir)

    if config.dry_run:
        info(f"[DRY RUN] Would extract {len(tiles)} tiles")
        return _create_empty_result(output_dir)

    # Resolve the license before downloading anything, so a harvest that cannot be
    # licensed costs one command re-run rather than a whole download (issue #686).
    # Raw mode writes no catalog, so it has nothing to license.
    resolved_license = (
        None
        if config.raw
        else resolve_harvest_license(
            cli_license=license_id,
            cli_license_url=license_url,
            harvested_license_url=license_url_from_text(metadata.license_info),
        )
    )

    # Resume state
    resume_path = portolan_dir / "imageserver-resume.json"
    resume_state = _load_or_create_resume_state(resume, resume_path, url)

    # Ask a coarse cache level which blocks hold data, before reading any of
    # them at full resolution (issue #870).
    tiles, coarse_empty = await _scan_for_empty_blocks(url, tiles, plan, config)
    if plan.cache is not None:
        _warn_on_cache_request_count(tiles, plan.cache)

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
        plan=plan,
    )
    _record_coarse_empty_tiles(coarse_empty, stats, resume_state)
    _save_resume_state(resume_state, resume_path)

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
    _seed_metadata_from_report(output_dir, report, resolved_license)

    _report_tile_counts(stats, report_path)

    # Auto-init catalog using Portolan API (unless raw mode)
    catalog_initialized = False
    if not config.raw:
        info("Initializing Portolan catalog...")
        catalog_initialized = _auto_init_catalog(
            output_dir, metadata.name, collection_name, config.catalog_id
        )
        if catalog_initialized:
            success("Catalog initialized with STAC metadata")
        else:
            warn("No COG files found to add to catalog")

    return ExtractionResult(
        output_dir=output_dir,
        tiles_downloaded=stats.tiles_downloaded,
        tiles_skipped=tiles_skipped,
        tiles_failed=stats.tiles_failed,
        tiles_empty=stats.tiles_empty,
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
