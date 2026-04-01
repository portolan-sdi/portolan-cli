"""CLI-facing orchestrator for ImageServer extraction.

This module wraps the async extract_imageserver() function for CLI use,
handling options, progress output, and exit codes.

Progress output matches FeatureServer pattern for consistency:
    [1/5] tile_0_0
      ✓ Done
    [2/5] tile_0_1
      ↪ Skipped (already extracted)

Typical usage from CLI:
    from portolan_cli.extract.arcgis.imageserver.orchestrator import (
        ImageServerCLIOptions,
        run_imageserver_extraction,
    )

    exit_code = await run_imageserver_extraction(
        url="https://services.arcgis.com/.../ImageServer",
        output_dir=Path("./output"),
        options=ImageServerCLIOptions(dry_run=True),
    )
    sys.exit(exit_code)
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from portolan_cli.extract.arcgis.imageserver.extractor import (
    ExtractionConfig,
    TileProgress,
    extract_imageserver,
)
from portolan_cli.output import detail, error, info, success

if TYPE_CHECKING:
    from portolan_cli.extract.arcgis.imageserver.report import ImageServerExtractionReport


@dataclass
class ImageServerCLIOptions:
    """CLI options for ImageServer extraction.

    Attributes:
        tile_size: Desired tile size in pixels (default 4096).
        max_concurrent: Maximum concurrent tile downloads (default 4).
        dry_run: If True, compute tiles but don't download.
        resume: If True, resume from previous extraction.
        raw: If True, skip auto-init (only create COGs + report, no STAC catalog).
        bbox: Optional bounding box to subset extraction (minx, miny, maxx, maxy).
        timeout: HTTP request timeout in seconds (default 120).
        compression: COG compression method ("DEFLATE" or "JPEG").
        use_json: If True, suppress progress output (for JSON mode).
    """

    tile_size: int = 4096
    max_concurrent: int = 4
    dry_run: bool = False
    resume: bool = False
    raw: bool = False
    bbox: tuple[float, float, float, float] | None = None
    timeout: float = 120.0
    compression: str = "DEFLATE"
    use_json: bool = False


def _create_progress_callback(
    use_json: bool,
) -> tuple[
    dict[str, str],
    Callable[[TileProgress], None] | None,
]:
    """Create a progress callback that matches FeatureServer output pattern.

    Args:
        use_json: If True, return None (no progress output in JSON mode).

    Returns:
        Tuple of (status_tracker dict, callback function or None).
    """
    if use_json:
        return {}, None

    # Track status per tile for output
    status_tracker: dict[str, str] = {}

    def on_progress(progress: TileProgress) -> None:
        """Progress callback matching FeatureServer pattern."""
        tile_id = progress.tile_id
        status = progress.status

        if status == "starting":
            info(f"[{progress.tile_index + 1}/{progress.total_tiles}] {tile_id}")
        elif status == "success":
            detail("  ✓ Done")
        elif status == "failed":
            error("  ✗ Failed")
        elif status == "skipped":
            detail("  ↪ Skipped (already extracted)")

        status_tracker[tile_id] = status

    return status_tracker, on_progress


async def run_imageserver_extraction(
    url: str,
    output_dir: Path,
    options: ImageServerCLIOptions | None = None,
) -> tuple[int, ImageServerExtractionReport | None]:
    """Run ImageServer extraction with CLI-appropriate output.

    This is the main entry point for CLI commands. It wraps
    extract_imageserver() and handles:
    - Progress output using portolan_cli.output helpers (matches FeatureServer)
    - Error handling with user-friendly messages
    - Exit code determination (0 for success, 1 for failure)

    Args:
        url: ImageServer URL.
        output_dir: Directory to write extracted data.
        options: CLI options (defaults to ImageServerCLIOptions()).

    Returns:
        Tuple of (exit_code, report). Exit code is 0 for success, 1 for failure.
        Report is None on complete failure.
    """
    if options is None:
        options = ImageServerCLIOptions()

    # Build extraction config from CLI options
    config = ExtractionConfig(
        tile_size=options.tile_size,
        max_concurrent=options.max_concurrent,
        dry_run=options.dry_run,
        raw=options.raw,
        timeout=options.timeout,
        compression=options.compression,
    )

    # Create progress callback (None in JSON mode)
    _, on_progress = _create_progress_callback(options.use_json)

    try:
        result = await extract_imageserver(
            url=url,
            output_dir=output_dir,
            config=config,
            resume=options.resume,
            bbox=options.bbox,
            on_progress=on_progress,
        )

        # Determine exit code based on results
        if options.dry_run:
            return 0, result.report

        if result.tiles_downloaded == 0 and result.tiles_failed > 0:
            # Complete failure - all tiles failed
            error(f"Extraction failed: all {result.tiles_failed} tiles failed")
            return 1, result.report

        # Success or partial success
        if result.tiles_failed > 0:
            info(
                f"Extraction completed with warnings: "
                f"{result.tiles_downloaded} succeeded, {result.tiles_failed} failed"
            )
        else:
            success(
                f"Extraction complete: {result.tiles_downloaded} tiles "
                f"({result.total_bytes:,} bytes)"
            )

        return 0, result.report

    except Exception as e:
        error(f"ImageServer extraction failed: {e}")
        return 1, None


def run_imageserver_extraction_sync(
    url: str,
    output_dir: Path,
    options: ImageServerCLIOptions | None = None,
) -> tuple[int, ImageServerExtractionReport | None]:
    """Synchronous wrapper for run_imageserver_extraction.

    Use this from Click commands which are synchronous.

    Args:
        url: ImageServer URL.
        output_dir: Directory to write extracted data.
        options: CLI options (defaults to ImageServerCLIOptions()).

    Returns:
        Tuple of (exit_code, report). Exit code is 0 for success, 1 for failure.
    """
    return asyncio.run(run_imageserver_extraction(url, output_dir, options))
