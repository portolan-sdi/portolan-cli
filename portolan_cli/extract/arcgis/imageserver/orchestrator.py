"""CLI-facing orchestrator for ImageServer extraction.

This module wraps the async extract_imageserver() function for CLI use,
handling options, progress output, and exit codes.

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
from dataclasses import dataclass
from pathlib import Path

from portolan_cli.extract.arcgis.imageserver.extractor import (
    ExtractionConfig,
    extract_imageserver,
)
from portolan_cli.output import error, info, success


@dataclass
class ImageServerCLIOptions:
    """CLI options for ImageServer extraction.

    Attributes:
        tile_size: Desired tile size in pixels (default 4096).
        max_concurrent: Maximum concurrent tile downloads (default 4).
        dry_run: If True, compute tiles but don't download.
        resume: If True, resume from previous extraction.
        bbox: Optional bounding box to subset extraction (minx, miny, maxx, maxy).
        timeout: HTTP request timeout in seconds (default 120).
        compression: COG compression method ("DEFLATE" or "JPEG").
    """

    tile_size: int = 4096
    max_concurrent: int = 4
    dry_run: bool = False
    resume: bool = False
    bbox: tuple[float, float, float, float] | None = None
    timeout: float = 120.0
    compression: str = "DEFLATE"


async def run_imageserver_extraction(
    url: str,
    output_dir: Path,
    options: ImageServerCLIOptions | None = None,
) -> int:
    """Run ImageServer extraction with CLI-appropriate output.

    This is the main entry point for CLI commands. It wraps
    extract_imageserver() and handles:
    - Progress output using portolan_cli.output helpers
    - Error handling with user-friendly messages
    - Exit code determination (0 for success, 1 for failure)

    Args:
        url: ImageServer URL.
        output_dir: Directory to write extracted data.
        options: CLI options (defaults to ImageServerCLIOptions()).

    Returns:
        Exit code: 0 for success (or partial success), 1 for complete failure.
    """
    if options is None:
        options = ImageServerCLIOptions()

    # Build extraction config from CLI options
    config = ExtractionConfig(
        tile_size=options.tile_size,
        max_concurrent=options.max_concurrent,
        dry_run=options.dry_run,
        timeout=options.timeout,
        compression=options.compression,
    )

    try:
        result = await extract_imageserver(
            url=url,
            output_dir=output_dir,
            config=config,
            resume=options.resume,
            bbox=options.bbox,
        )

        # Determine exit code based on results
        if options.dry_run:
            return 0

        if result.tiles_downloaded == 0 and result.tiles_failed > 0:
            # Complete failure - all tiles failed
            error(f"Extraction failed: all {result.tiles_failed} tiles failed")
            return 1

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

        return 0

    except Exception as e:
        error(f"ImageServer extraction failed: {e}")
        return 1


def run_imageserver_extraction_sync(
    url: str,
    output_dir: Path,
    options: ImageServerCLIOptions | None = None,
) -> int:
    """Synchronous wrapper for run_imageserver_extraction.

    Use this from Click commands which are synchronous.

    Args:
        url: ImageServer URL.
        output_dir: Directory to write extracted data.
        options: CLI options (defaults to ImageServerCLIOptions()).

    Returns:
        Exit code: 0 for success, 1 for failure.
    """
    return asyncio.run(run_imageserver_extraction(url, output_dir, options))
