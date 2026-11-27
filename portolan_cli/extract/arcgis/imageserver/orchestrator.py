"""The ImageServer path of ``portolan extract arcgis``."""

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
    from portolan_cli.extract.common.progress import MessageCallback


@dataclass
class ImageServerCLIOptions:
    """Options for ImageServer extraction."""

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
    """One ImageServer run of ``portolan extract arcgis``."""

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
    """Tell the user how to recover from failed tiles."""
    return (
        f"Re-run the same command with --resume to retry the {tiles_failed} failed tiles. "
        "If the server returned HTTP 5xx, lower --tile-size or --max-concurrent. "
        "A new --tile-size builds another tile grid, so that run reads every tile again."
    )


async def run_imageserver(
    url: str,
    output_dir: Path,
    options: ImageServerCLIOptions | None = None,
    *,
    on_progress: Callable[[TileProgress], None] | None = None,
    on_message: MessageCallback | None = None,
) -> tuple[int, ImageServerExtractionReport | None]:
    """Run ImageServer extraction and pick the exit code. Raise on an error."""
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
        # Every tile holds no data. A resumed run holds data, so it is not a failure.
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


async def run_imageserver_extraction(
    url: str, output_dir: Path, options: ImageServerCLIOptions | None = None
) -> tuple[int, ImageServerExtractionReport | None]:
    """Run ``run_imageserver`` and turn an error into exit code 1."""
    from portolan_core.errors import MissingLicenseError
    from portolan_core.licensing import CLI_LICENSE_REMEDIATION

    try:
        return await run_imageserver(url, output_dir, options)
    # The old contract maps every error to exit code 1, so the catch is broad.
    except Exception as e:
        emit_message(None, "error", f"ImageServer extraction failed: {redact_credentials(str(e))}")
        if isinstance(e, MissingLicenseError):
            emit_message(None, "info", CLI_LICENSE_REMEDIATION)
        return 1, None


def run_imageserver_extraction_sync(
    url: str,
    output_dir: Path,
    options: ImageServerCLIOptions | None = None,
) -> tuple[int, ImageServerExtractionReport | None]:
    """Run ``run_imageserver_extraction`` from synchronous code."""
    return asyncio.run(run_imageserver_extraction(url, output_dir, options))
