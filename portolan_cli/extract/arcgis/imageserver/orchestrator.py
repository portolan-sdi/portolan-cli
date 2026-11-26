"""The ImageServer path of ``portolan extract arcgis``.

This module wraps the async ``extract_imageserver()`` function. It turns the
options into an extraction config, picks the exit code, and returns the
result. It prints nothing (#1005). Progress goes to ``on_progress`` and the
messages for the user go to ``on_message``. The CLI prints both.

Typical usage:
    from portolan_cli.extract.arcgis.imageserver.orchestrator import (
        ImageServerCLIOptions,
        run_imageserver_extraction_sync,
    )

    exit_code, report = run_imageserver_extraction_sync(
        url="https://services.arcgis.com/.../ImageServer",
        output_dir=Path("./output"),
        options=ImageServerCLIOptions(dry_run=True),
    )
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from portolan_cli.extract.arcgis.imageserver.extractor import (
    ExtractionConfig,
    TileProgress,
    extract_imageserver,
)
from portolan_cli.extract.common.progress import emit_message
from portolan_cli.extract.common.urls import redact_credentials

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from portolan_cli.extract.arcgis.imageserver.report import ImageServerExtractionReport


@dataclass
class ImageServerCLIOptions:
    """Options for ImageServer extraction.

    Attributes:
        tile_size: Desired tile size in pixels (default 4096).
        max_concurrent: Maximum concurrent tile downloads (default 4).
        max_retries: Attempts per tile before it counts as failed (default 3).
        dry_run: If True, compute tiles but don't download.
        resume: If True, resume from previous extraction.
        raw: If True, skip auto-init (only create COGs + report, no STAC catalog).
        bbox: Optional bounding box to subset extraction (minx, miny, maxx, maxy).
        bbox_crs: Optional explicit CRS of bbox (e.g., "EPSG:4326"). Skips auto-detection.
        timeout: HTTP request timeout in seconds (default 120).
        compression: COG compression method ("DEFLATE" or "JPEG").
        collection_name: Optional name for the collection (default: 'tiles').
        catalog_id: Catalog id for the created catalog. None derives it from
            the output directory name, which is the behavior before issue #821.
        license: SPDX identifier from --license, or "other" with license_url.
            Overrides any license URL in the service's licenseInfo (issue #686).
        license_url: URL of the license text from --license-url.
        coarse_scan: Ask a coarse tile cache level which blocks hold data
            before reading them (issue #870). Only a cache-only service uses
            it. It is off by default, because the scan can skip a thin feature.
    """

    tile_size: int = 4096
    max_concurrent: int = 4
    max_retries: int = 3
    dry_run: bool = False
    resume: bool = False
    raw: bool = False
    bbox: tuple[float, float, float, float] | None = None
    bbox_crs: str | None = None
    timeout: float = 120.0
    compression: str = "DEFLATE"
    collection_name: str | None = None
    catalog_id: str | None = None
    license: str | None = None
    license_url: str | None = None
    coarse_scan: bool = False


@dataclass(frozen=True)
class ImageServerRunResult:
    """One ImageServer run of ``portolan extract arcgis``.

    Attributes:
        url: The ImageServer URL.
        output_dir: The directory the run wrote, as the caller gave it.
        dry_run: True when the run computed the tiles and read none.
        exit_code: 0 for success, 1 when every tile failed or held no data.
        report: The extraction report, or None for a run that wrote none.
    """

    url: str
    output_dir: Path
    dry_run: bool
    exit_code: int
    report: ImageServerExtractionReport | None

    def to_dict(self) -> dict[str, Any]:
        """Return the envelope ``data`` of the command."""
        data: dict[str, Any] = {
            "source_url": redact_credentials(self.url),
            "output_dir": str(self.output_dir),
            "service_type": "ImageServer",
            "dry_run": self.dry_run,
        }
        if self.report:
            data["summary"] = self.report.summary.to_dict()
            data["metadata_extracted"] = self.report.metadata_extracted.to_dict()
            data["tiles"] = [tile.to_dict() for tile in self.report.tiles]
        return data


def _failure_hint(tiles_failed: int) -> str:
    """Tell the user how to recover from failed tiles (issue #870).

    Failed tiles stay in the resume state, so a second run with --resume
    retries them alone. HTTP 5xx responses from exportImage usually mean the
    server could not build a tile of the requested size under load, so a
    smaller tile or fewer parallel requests is the next thing to try.

    Args:
        tiles_failed: Number of tiles that failed after all retries.
    """
    return (
        f"Re-run the same command with --resume to retry the {tiles_failed} failed tiles. "
        "If the server returned HTTP 5xx, lower --tile-size or --max-concurrent. "
        "A new --tile-size builds another tile grid, so that run reads every tile again."
    )


async def run_imageserver_extraction(
    url: str,
    output_dir: Path,
    options: ImageServerCLIOptions | None = None,
    *,
    on_progress: Callable[[TileProgress], None] | None = None,
    on_message: Callable[[str, str], None] | None = None,
) -> tuple[int, ImageServerExtractionReport | None]:
    """Run ImageServer extraction and pick the exit code.

    Args:
        url: ImageServer URL.
        output_dir: Directory to write extracted data.
        options: Options (defaults to ImageServerCLIOptions()).
        on_progress: Called with the progress of each tile.
        on_message: Called with each message for the user, as
            ``on_message(level, text)``. See ``extract.common.progress.emit_message``.

    Returns:
        Tuple of (exit_code, report). Exit code is 0 for success, 1 when every
        tile failed or every tile is empty.

    Raises:
        MissingLicenseError: Before any download, when no license resolves.
        ImageServerDiscoveryError: If service discovery fails.
        ImageServerExtractionError: If no path can read the service.
    """
    if options is None:
        options = ImageServerCLIOptions()

    config = ExtractionConfig(
        catalog_id=options.catalog_id,
        tile_size=options.tile_size,
        max_concurrent=options.max_concurrent,
        max_retries=options.max_retries,
        coarse_scan=options.coarse_scan,
        dry_run=options.dry_run,
        raw=options.raw,
        timeout=options.timeout,
        compression=options.compression,
    )

    result = await extract_imageserver(
        url=url,
        output_dir=output_dir,
        config=config,
        resume=options.resume,
        bbox=options.bbox,
        on_progress=on_progress,
        collection_name=options.collection_name,
        bbox_crs=options.bbox_crs,
        license_id=options.license,
        license_url=options.license_url,
        on_message=on_message,
    )

    if options.dry_run:
        return 0, result.report

    if result.tiles_downloaded == 0 and result.tiles_failed > 0:
        emit_message(
            on_message, "error", f"Extraction failed: all {result.tiles_failed} tiles failed"
        )
        emit_message(on_message, "warn", _failure_hint(result.tiles_failed))
        return 1, result.report

    if result.tiles_downloaded == 0 and result.tiles_empty > 0 and result.tiles_skipped == 0:
        # The service answered every request, and every tile holds no data.
        # A resumed run that skips completed tiles holds data, so it is not
        # a failure.
        emit_message(
            on_message,
            "error",
            f"Extraction produced no data: all {result.tiles_empty} tiles are empty. "
            "The service extent covers more area than its data does. "
            "Use --bbox to name the area you want.",
        )
        return 1, result.report

    if result.tiles_failed > 0:
        emit_message(
            on_message,
            "info",
            f"Extraction completed with warnings: "
            f"{result.tiles_downloaded} succeeded, {result.tiles_failed} failed",
        )
        emit_message(on_message, "warn", _failure_hint(result.tiles_failed))
    else:
        emit_message(
            on_message,
            "success",
            f"Extraction complete: {result.tiles_downloaded} tiles ({result.total_bytes:,} bytes)",
        )
    return 0, result.report


def run_imageserver_extraction_sync(
    url: str,
    output_dir: Path,
    options: ImageServerCLIOptions | None = None,
) -> tuple[int, ImageServerExtractionReport | None]:
    """Run ``run_imageserver_extraction`` from synchronous code, with no callbacks.

    Args:
        url: ImageServer URL.
        output_dir: Directory to write extracted data.
        options: Options (defaults to ImageServerCLIOptions()).

    Returns:
        Tuple of (exit_code, report). Exit code is 0 for success, 1 for failure.
    """
    return asyncio.run(run_imageserver_extraction(url, output_dir, options))
