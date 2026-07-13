"""Conversion API for transforming files to cloud-native formats.

This module provides the core conversion functionality for Portolan CLI:
- ConversionStatus: Enum of possible conversion outcomes
- ConversionResult: Result of a single file conversion
- ConversionReport: Aggregate results from batch conversion
- convert_file(): Convert a single file to cloud-native format
- convert_directory(): Convert all files in a directory

Per ADR-0007, this module contains the logic; CLI commands are thin wrappers.
Per ADR-0010, actual conversion is delegated to geoparquet-io and rio-cogeo.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Literal, cast

from portolan_cli.constants import GEOSPATIAL_EXTENSIONS
from portolan_cli.conversion_config import (
    LOSSY_COMPRESSIONS,
    QUALITY_COMPRESSIONS,
    CogSettings,
    VectorSettings,
    get_cog_settings,
    get_vector_settings,
)
from portolan_cli.errors import (
    ConversionFailedError,
)
from portolan_cli.formats import (
    CloudNativeStatus,
    FormatType,
    detect_format,
    get_cloud_native_status,
)
from portolan_cli.viz.thumbnail import (
    ThumbnailConfig,
    generate_vector_thumbnail,
    get_thumbnail_config,
)

logger = logging.getLogger(__name__)

# Type alias for rio-cogeo resampling methods
ResamplingMethod = Literal[
    "nearest",
    "bilinear",
    "cubic",
    "cubic_spline",
    "lanczos",
    "average",
    "mode",
    "gauss",
    "rms",
]


class ConversionStatus(Enum):
    """Possible outcomes of a file conversion operation.

    Attributes:
        SUCCESS: File was converted successfully and validated.
        SKIPPED: File was already cloud-native, no conversion needed.
        FAILED: Conversion threw an exception (original file preserved).
        INVALID: Conversion completed but validation failed (output kept for inspection).
        UNSUPPORTED: Format is not supported for conversion (not an error).
    """

    SUCCESS = "success"
    SKIPPED = "skipped"
    FAILED = "failed"
    INVALID = "invalid"
    UNSUPPORTED = "unsupported"


@dataclass
class ConversionResult:
    """Result of a single file conversion operation.

    Attributes:
        source: Path to the source file that was converted.
        output: Path to the output file (None if conversion failed or skipped).
        format_from: Display name of the source format (e.g., "SHP", "GeoJSON").
        format_to: Display name of the target format (e.g., "GeoParquet", "COG").
            None if skipped.
        status: Outcome of the conversion operation.
        error: Error message if conversion failed or validation failed.
            None on success or skip.
        duration_ms: Time taken for the conversion in milliseconds.
    """

    source: Path
    output: Path | None
    format_from: str
    format_to: str | None
    status: ConversionStatus
    error: str | None
    duration_ms: int

    def to_dict(self) -> dict[str, Any]:
        """Convert to a JSON-serializable dictionary.

        Returns:
            Dictionary with all fields, paths converted to strings,
            and status converted to its string value.
        """
        return {
            "source": str(self.source),
            "output": str(self.output) if self.output else None,
            "format_from": self.format_from,
            "format_to": self.format_to,
            "status": self.status.value,
            "error": self.error,
            "duration_ms": self.duration_ms,
        }


@dataclass
class ConversionReport:
    """Aggregate results from batch conversion operations.

    Attributes:
        results: List of ConversionResult from processing each file.
    """

    results: list[ConversionResult]

    @property
    def succeeded(self) -> int:
        """Count of files successfully converted."""
        return sum(1 for r in self.results if r.status == ConversionStatus.SUCCESS)

    @property
    def failed(self) -> int:
        """Count of files that failed conversion."""
        return sum(1 for r in self.results if r.status == ConversionStatus.FAILED)

    @property
    def skipped(self) -> int:
        """Count of files skipped (already cloud-native)."""
        return sum(1 for r in self.results if r.status == ConversionStatus.SKIPPED)

    @property
    def invalid(self) -> int:
        """Count of files that converted but failed validation."""
        return sum(1 for r in self.results if r.status == ConversionStatus.INVALID)

    @property
    def unsupported(self) -> int:
        """Count of files with unsupported formats (not convertible)."""
        return sum(1 for r in self.results if r.status == ConversionStatus.UNSUPPORTED)

    @property
    def total(self) -> int:
        """Total number of files processed."""
        return len(self.results)

    def to_dict(self) -> dict[str, Any]:
        """Convert to a JSON-serializable dictionary.

        Returns:
            Dictionary with summary counts and full results array.
        """
        return {
            "summary": {
                "succeeded": self.succeeded,
                "failed": self.failed,
                "skipped": self.skipped,
                "invalid": self.invalid,
                "unsupported": self.unsupported,
                "total": self.total,
            },
            "results": [r.to_dict() for r in self.results],
        }


def generate_cog_thumbnail(
    cog_path: Path,
    max_size: int = 512,
    quality: int = 75,
    basemap_provider: str = "none",
) -> Path | None:
    """Generate a JPEG thumbnail from a COG file (Issue #372).

    Reads the lowest-resolution overview when available, downsamples to fit
    within ``max_size`` pixels on the longest edge, and writes a JPEG next to
    the COG. Per STAC best practices, this is the asset that should carry the
    ``thumbnail`` role.

    Args:
        cog_path: Path to the source COG file.
        max_size: Maximum pixel dimension for the longest edge (default 512).
        quality: JPEG quality 1-100 (default 75).
        basemap_provider: Unused. Raster thumbnails don't need basemaps because
            the raster data fills the entire extent — a basemap underneath would
            be invisible. Vector data needs basemaps because points/lines are
            sparse and benefit from geographic context. See ADR-0042.

    Returns:
        Path to the written JPEG thumbnail, or None if the source could not be
        read as a raster (e.g., corrupt file).
    """
    # Basemaps intentionally not supported for rasters (ADR-0042):
    # Raster data fills the extent, so a basemap would be hidden underneath.
    # Vector thumbnails need basemaps because points/lines are sparse.
    if basemap_provider != "none":
        logger.debug("Basemap ignored for raster thumbnail (not needed, see ADR-0042)")
    try:
        import numpy as np
        import rasterio
        from rasterio.enums import Resampling
    except ImportError:
        logger.debug("rasterio/numpy not available, skipping thumbnail generation")
        return None

    # Use ".thumb.jpg" rather than ".jpg" so we don't clobber a user-supplied
    # sibling thumbnail (e.g. hand-curated data.jpg next to data.tif). The
    # extension is still .jpg, so _scan_item_assets assigns role "thumbnail".
    thumb_path = cog_path.with_name(f"{cog_path.stem}.thumb.jpg")

    try:
        with rasterio.open(cog_path) as src:
            # Compute target shape preserving aspect ratio
            longest = max(src.width, src.height)
            scale = min(1.0, max_size / float(longest))
            out_w = max(1, int(round(src.width * scale)))
            out_h = max(1, int(round(src.height * scale)))

            # Pick up to 3 bands for RGB; fall back to first band for grayscale
            indexes: list[int] = [1, 2, 3] if src.count >= 3 else [1]

            # masked=True applies the dataset mask (nodata sentinels, alpha,
            # internal mask band) so percentile stretch on float/int rasters
            # ignores fill values like -9999.
            masked = src.read(
                indexes=indexes,
                out_shape=(len(indexes), out_h, out_w),
                resampling=Resampling.average,
                masked=True,
            )

            # Normalize to uint8 for JPEG
            if masked.dtype == np.uint8:
                data = masked.filled(0)
            else:
                valid = masked.compressed()
                # Drop non-finite samples (NaN/inf in float rasters)
                if valid.size:
                    valid = valid[np.isfinite(valid)]
                if valid.size == 0:
                    return None
                lo = float(np.percentile(valid, 2))
                hi = float(np.percentile(valid, 98))
                if hi <= lo:
                    hi = lo + 1.0
                stretched = (masked.astype("float32") - lo) / (hi - lo) * 255.0
                # Fill masked samples with 0 (black) after stretch
                data = np.clip(stretched.filled(0), 0, 255).astype("uint8")

            # JPEG needs 1 or 3 bands
            if data.shape[0] == 1:
                jpeg_data = data
                photometric = "minisblack"
            else:
                jpeg_data = data[:3]
                photometric = "rgb"

            profile = {
                "driver": "JPEG",
                "width": out_w,
                "height": out_h,
                "count": jpeg_data.shape[0],
                "dtype": "uint8",
                "quality": int(max(1, min(100, quality))),
                "photometric": photometric,
            }

            with rasterio.open(thumb_path, "w", **profile) as dst:
                dst.write(jpeg_data)
    except Exception as e:
        logger.debug("Could not generate thumbnail for %s: %s", cog_path, e)
        if thumb_path.exists():
            try:
                thumb_path.unlink()
            except OSError:
                pass
        return None

    logger.debug("Generated thumbnail %s (%dx%d)", thumb_path.name, out_w, out_h)
    return thumb_path


def _discover_style_for_thumbnail(collection_dir: Path) -> Path | None:
    """Find a style file for thumbnail generation.

    Searches for styles/default.json or styles/source.json in the collection
    directory, preferring default.json (the extracted style, per Issue #497).

    Args:
        collection_dir: Path to collection directory.

    Returns:
        Path to style file if found, None otherwise.
    """
    styles_dir = collection_dir / "styles"
    if not styles_dir.exists():
        return None

    # Prefer default.json (extracted style), then source.json
    for name in ("default.json", "source.json"):
        style_path = styles_dir / name
        if style_path.exists():
            return style_path

    return None


def _early_exit_result(
    source: Path,
    format_info: Any,
    *,
    force: bool,
    start_time: float,
) -> ConversionResult | None:
    """Return a SKIPPED/UNSUPPORTED result if no conversion should run, else None.

    Cloud-native files are skipped, EXCEPT when ``force`` is set and the file is a
    RASTER (a valid COG) — that case re-optimizes in place (issue #530), so this
    returns None to let conversion proceed. Vectors are never force-re-processed.
    Unsupported formats are accepted with no error (ADR-0014).
    """
    if format_info.status == CloudNativeStatus.CLOUD_NATIVE:
        force_raster = force and detect_format(source) == FormatType.RASTER
        if force_raster:
            return None
        duration_ms = int((time.perf_counter() - start_time) * 1000)
        return ConversionResult(
            source=source,
            output=None,
            format_from=format_info.display_name,
            format_to=None,
            status=ConversionStatus.SKIPPED,
            error=None,
            duration_ms=duration_ms,
        )

    if format_info.status == CloudNativeStatus.UNSUPPORTED:
        duration_ms = int((time.perf_counter() - start_time) * 1000)
        logger.info(
            "Unsupported format, skipping: %s (%s)",
            source,
            format_info.display_name,
        )
        return ConversionResult(
            source=source,
            output=None,
            format_from=format_info.display_name,
            format_to=None,
            status=ConversionStatus.UNSUPPORTED,
            error=None,  # Not an error - just unsupported
            duration_ms=duration_ms,
        )

    return None


def convert_file(
    source: Path,
    output_dir: Path | None = None,
    catalog_path: Path | None = None,
    cog_settings: CogSettings | None = None,
    vector_settings: VectorSettings | None = None,
    force: bool = False,
) -> ConversionResult:
    """Convert a single file to cloud-native format.

    Converts vector files to GeoParquet and raster files to COG.
    Files that are already cloud-native are skipped.

    Args:
        source: Path to the source file to convert.
        output_dir: Directory for the output file. If None, uses the same
            directory as the source file.
        catalog_path: Path to the catalog root for loading conversion config.
            If None, uses ADR-0019 defaults.
        cog_settings: Explicit COG settings override. Takes precedence over
            ``catalog_path``-loaded settings. If None, loads from
            ``catalog_path`` or falls back to ADR-0019 defaults.
        vector_settings: Explicit vector settings override. Takes precedence over
            ``catalog_path``-loaded settings. If None, loads from
            ``catalog_path`` or falls back to no spatial optimization.
        force: Re-optimize an already-cloud-native RASTER by re-applying current
            ``cog_settings`` (e.g. to add missing overviews). Raster-scoped only:
            cloud-native vectors are still skipped (issue #530). Has no effect on
            CONVERTIBLE or UNSUPPORTED files.

    Returns:
        ConversionResult with conversion outcome, timing, and paths.

    Raises:
        FileNotFoundError: If the source file does not exist.
    """
    start_time = time.perf_counter()

    if not source.exists():
        raise FileNotFoundError(f"Source file not found: {source}")

    # Get format info
    format_info = get_cloud_native_status(source)

    # Cloud-native (skip, unless forcing raster re-optimization) and unsupported
    # files exit early without conversion.
    early_exit = _early_exit_result(source, format_info, force=force, start_time=start_time)
    if early_exit is not None:
        return early_exit

    # Determine output directory and format type
    out_dir = output_dir if output_dir else source.parent
    format_type = detect_format(source)

    # Load COG settings: explicit arg > catalog config > defaults
    if cog_settings is None:
        cog_settings = get_cog_settings(catalog_path) if catalog_path else CogSettings()

    # Load vector settings: explicit arg > catalog config > defaults
    if vector_settings is None:
        vector_settings = get_vector_settings(catalog_path) if catalog_path else VectorSettings()

    # Convert based on format type
    try:
        if format_type == FormatType.VECTOR:
            output_path = _convert_vector(source, out_dir, vector_settings)
            target_format = "GeoParquet"
            # Validate output is valid GeoParquet
            validation_error = _validate_geoparquet(output_path)
            # Generate thumbnail next to the GeoParquet (Issue #13).
            # Only on a successful, valid conversion to avoid orphan thumbnails.
            # Thumbnail failure should not flip a successful conversion to FAILED.
            if validation_error is None:
                try:
                    thumb_config = (
                        get_thumbnail_config(catalog_path) if catalog_path else ThumbnailConfig()
                    )
                    if thumb_config.enabled and isinstance(output_path, Path):
                        # Discover style for thumbnail (Issue #495)
                        style_path = _discover_style_for_thumbnail(output_path.parent)
                        generate_vector_thumbnail(
                            pmtiles_path=None,  # PMTiles generated separately if enabled
                            geoparquet_path=output_path,
                            config=thumb_config,
                            style_path=style_path,
                        )
                except Exception as e:
                    logger.warning("Thumbnail generation failed for %s: %s", source.name, e)
        elif format_type == FormatType.RASTER:
            output_path = _convert_raster(source, out_dir, cog_settings)
            target_format = "COG"
            # Validate output is valid COG
            validation_error = _validate_cog(output_path)
            # Generate thumbnail next to the COG (Issue #372).
            # Only on a successful, valid conversion to avoid orphan thumbnails.
            if validation_error is None and cog_settings.generate_thumbnail:
                generate_cog_thumbnail(
                    output_path,
                    max_size=cog_settings.thumbnail_max_size,
                    quality=cog_settings.thumbnail_quality,
                )
        else:
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            return ConversionResult(
                source=source,
                output=None,
                format_from=format_info.display_name,
                format_to=None,
                status=ConversionStatus.FAILED,
                error=f"Unable to determine format type for {source.suffix}",
                duration_ms=duration_ms,
            )

        duration_ms = int((time.perf_counter() - start_time) * 1000)

        # Check validation result
        if validation_error is not None:
            logger.warning(
                "Conversion completed but validation failed for %s: %s",
                output_path,
                validation_error,
            )
            return ConversionResult(
                source=source,
                output=output_path,  # Keep output for inspection
                format_from=format_info.display_name,
                format_to=target_format,
                status=ConversionStatus.INVALID,
                error=validation_error,
                duration_ms=duration_ms,
            )

        return ConversionResult(
            source=source,
            output=output_path,
            format_from=format_info.display_name,
            format_to=target_format,
            status=ConversionStatus.SUCCESS,
            error=None,
            duration_ms=duration_ms,
        )

    except Exception as e:
        duration_ms = int((time.perf_counter() - start_time) * 1000)
        # Use structured error for logging context
        conversion_error = ConversionFailedError(str(source), e)
        logger.exception(
            "Conversion failed for %s [%s]: %s",
            source,
            conversion_error.code,
            e,
        )
        return ConversionResult(
            source=source,
            output=None,
            format_from=format_info.display_name,
            format_to=format_info.target_format,
            status=ConversionStatus.FAILED,
            error=str(e),
            duration_ms=duration_ms,
        )


#: Total attempts for a single vector conversion when it fails with a transient
#: DuckDB interrupt (1 initial try + retries). Small on purpose: a genuine
#: interrupt clears once raised, so one extra attempt almost always suffices.
_CONVERT_MAX_ATTEMPTS = 3


def _is_transient_conversion_error(exc: BaseException) -> bool:
    """Return True for a transient DuckDB interrupt that is safe to retry.

    ``geoparquet-io`` runs conversions through DuckDB, whose Python client raises
    ``duckdb.InterruptException('Query interrupted')`` when a pending process
    signal trips its per-query signal check. This is non-deterministic and
    environment-driven (observed ~1 in 1000 files during bulk nightly ``add``
    runs, on a different file each time) and the interrupt flag clears once the
    exception is raised, so re-running the same conversion reliably succeeds.

    We match by exception type name plus message rather than importing ``duckdb``
    directly: it is a transitive dependency (via ``geoparquet-io``) that we do
    not declare, and importing it here would break the deptry transitive-import
    contract. Matching the name keeps the coupling at the string level.
    """
    return type(exc).__name__ == "InterruptException" or "query interrupted" in str(exc).lower()


def _convert_vector(
    source: Path,
    output_dir: Path,
    settings: VectorSettings | None = None,
) -> Path:
    """Convert a vector file to GeoParquet with optional spatial optimization.

    Uses geoparquet-io's fluent Table API to apply spatial index columns,
    sorting, and bbox based on VectorSettings configuration.

    A transient DuckDB "Query interrupted" failure (see
    :func:`_is_transient_conversion_error`) is retried up to
    ``_CONVERT_MAX_ATTEMPTS`` times so a single interrupted query does not fail a
    whole bulk ``add``; any other error propagates on the first occurrence.

    Args:
        source: Source vector file.
        output_dir: Directory for output file.
        settings: Vector conversion settings. If None, uses defaults (no optimization).

    Returns:
        Path to the output GeoParquet file (or directory if partitioned).
    """
    import geoparquet_io as gpio  # type: ignore[import-untyped]

    if settings is None:
        settings = VectorSettings()
    resolved = settings

    output_path = output_dir / f"{source.stem}.parquet"

    def _run() -> Path:
        # Convert source to gpio Table
        table = gpio.convert(str(source))

        # Apply spatial optimizations based on settings
        table = _apply_vector_settings(table, resolved)

        # Write output (partitioned or single file)
        if resolved.partition and resolved.spatial_index != "none":
            # Partitioned output to directory
            partition_dir = output_dir / source.stem
            _write_partitioned(table, partition_dir, resolved)
            return partition_dir
        # Single file output
        table.write(str(output_path))
        return output_path

    for attempt in range(1, _CONVERT_MAX_ATTEMPTS + 1):
        try:
            return _run()
        except Exception as exc:
            is_last_attempt = attempt == _CONVERT_MAX_ATTEMPTS
            if is_last_attempt or not _is_transient_conversion_error(exc):
                raise
            logger.warning(
                "Transient interrupt converting %s (attempt %d/%d), retrying: %s",
                source.name,
                attempt,
                _CONVERT_MAX_ATTEMPTS,
                exc,
            )
    # Unreachable: the final attempt always returns or re-raises above. Present
    # only so the type checker sees every path terminates.
    raise AssertionError("vector conversion retry loop exited without returning")


def _apply_vector_settings(table: Any, settings: VectorSettings) -> Any:
    """Apply spatial optimization settings to a gpio Table.

    Args:
        table: geoparquet-io Table instance.
        settings: Vector conversion settings.

    Returns:
        Modified Table with optimizations applied.
    """
    # Add bbox column if requested
    if settings.add_bbox:
        table = table.add_bbox()

    # Add spatial index column if specified
    if settings.spatial_index != "none":
        resolution = _resolve_resolution(settings.spatial_index, settings.resolution)
        table = _add_spatial_index(table, settings.spatial_index, resolution)

    # Apply sorting
    if settings.sort == "hilbert":
        table = table.sort_hilbert()
    elif settings.sort == "quadkey":
        # sort_quadkey needs resolution; use quadkey default (13) if not set
        sort_resolution = _resolve_resolution("quadkey", settings.resolution)
        if sort_resolution is None:
            table = table.sort_quadkey()
        else:
            table = table.sort_quadkey(resolution=sort_resolution)

    return table


def _resolve_resolution(index_type: str, resolution: int | str) -> int | None:
    """Resolve resolution value for a spatial index type.

    Args:
        index_type: Spatial index type (h3, s2, quadkey, a5, kdtree).
        resolution: Either "auto" or explicit int.

    Returns:
        Resolution int, or None to use geoparquet-io defaults.
    """
    if resolution == "auto":
        return None  # Let gpio use its defaults
    return int(resolution)


def _add_spatial_index(table: Any, index_type: str, resolution: int | None) -> Any:
    """Add a spatial index column to the table.

    Args:
        table: geoparquet-io Table instance.
        index_type: Type of spatial index (h3, s2, quadkey, a5, kdtree).
        resolution: Resolution/level/iterations, or None for defaults.

    Returns:
        Table with spatial index column added.

    Raises:
        ValueError: If index_type is not recognized.
    """
    if index_type == "h3":
        return table.add_h3() if resolution is None else table.add_h3(resolution=resolution)
    elif index_type == "s2":
        return table.add_s2() if resolution is None else table.add_s2(level=resolution)
    elif index_type == "quadkey":
        return (
            table.add_quadkey() if resolution is None else table.add_quadkey(resolution=resolution)
        )
    elif index_type == "a5":
        return table.add_a5() if resolution is None else table.add_a5(resolution=resolution)
    elif index_type == "kdtree":
        return table.add_kdtree() if resolution is None else table.add_kdtree(iterations=resolution)
    else:
        raise ValueError(
            f"Unknown spatial index type: '{index_type}'. Valid types: h3, s2, quadkey, a5, kdtree"
        )


def _write_partitioned(table: Any, output_dir: Path, settings: VectorSettings) -> None:
    """Write table as hive-partitioned output.

    Args:
        table: geoparquet-io Table instance with spatial index column.
        output_dir: Output directory for partitioned files.
        settings: Vector settings with partition strategy.
    """
    resolution = _resolve_resolution(settings.spatial_index, settings.resolution)
    index_type = settings.spatial_index

    if index_type == "h3":
        if resolution is None:
            table.partition_by_h3(str(output_dir))
        else:
            table.partition_by_h3(str(output_dir), resolution=resolution)
    elif index_type == "s2":
        if resolution is None:
            table.partition_by_s2(str(output_dir))
        else:
            table.partition_by_s2(str(output_dir), level=resolution)
    elif index_type == "quadkey":
        if resolution is None:
            table.partition_by_quadkey(str(output_dir))
        else:
            table.partition_by_quadkey(str(output_dir), resolution=resolution)
    elif index_type == "a5":
        if resolution is None:
            table.partition_by_a5(str(output_dir))
        else:
            table.partition_by_a5(str(output_dir), resolution=resolution)
    elif index_type == "kdtree":
        if resolution is None:
            table.partition_by_kdtree(str(output_dir))
        else:
            table.partition_by_kdtree(str(output_dir), iterations=resolution)


def _convert_raster(source: Path, output_dir: Path, settings: CogSettings | None = None) -> Path:
    """Convert a raster file to COG.

    Uses COG settings from config if provided, otherwise ADR-0019 defaults:
    - DEFLATE compression
    - Predictor=2 (horizontal differencing)
    - 512x512 tiles
    - Nearest resampling

    Args:
        source: Source raster file.
        output_dir: Directory for output file.
        settings: COG conversion settings. If None, uses ADR-0019 defaults.

    Returns:
        Path to the output COG file.

    Note:
        If output_dir is the same directory as source, the original file
        will be replaced with the COG. This is by design for raster conversion.
    """
    import tempfile

    from rio_cogeo.cogeo import cog_translate
    from rio_cogeo.profiles import cog_profiles

    # Use defaults if no settings provided
    if settings is None:
        settings = CogSettings()

    output_path = output_dir / f"{source.stem}.tif"

    # Warn if we're about to overwrite the source file
    if output_path.resolve() == source.resolve():
        logger.info(
            "Source file will be replaced with COG (in-place conversion): %s",
            source,
        )

    # Select profile based on compression type
    compression_lower = settings.compression.lower()
    if compression_lower in ("jpeg", "webp"):
        profile = cog_profiles.get(compression_lower)  # type: ignore[no-untyped-call]
    else:
        # DEFLATE, LZW, ZSTD, etc. use the deflate profile as base
        profile = cog_profiles.get("deflate")  # type: ignore[no-untyped-call]
        profile["compress"] = settings.compression

    # Apply tile size settings
    profile["blockxsize"] = settings.tile_size
    profile["blockysize"] = settings.tile_size

    # Apply predictor only for non-lossy compression
    # Predictor is meaningless for JPEG/WEBP and can cause issues
    if settings.compression not in LOSSY_COMPRESSIONS:
        profile["predictor"] = settings.predictor

    # Apply quality for JPEG and WEBP compression
    if settings.quality is not None and settings.compression in QUALITY_COMPRESSIONS:
        profile["quality"] = settings.quality

    # Write to temp file first to avoid corrupting source if output_path == source
    # Use same directory as output for atomic rename across filesystems
    temp_fd, temp_path_str = tempfile.mkstemp(
        suffix=".tif", prefix=".portolan_cog_", dir=output_dir
    )
    temp_path = Path(temp_path_str)

    try:
        # Close the file descriptor - rio-cogeo will open it
        import os

        os.close(temp_fd)

        cog_translate(
            str(source),
            str(temp_path),
            profile,
            quiet=True,
            overview_resampling=cast(ResamplingMethod, settings.resampling),
        )

        # Atomic replace
        temp_path.replace(output_path)
    except Exception:
        # Clean up temp file on error
        if temp_path.exists():
            temp_path.unlink()
        raise

    return output_path


def _validate_geoparquet(path: Path) -> str | None:
    """Validate that the output is valid GeoParquet (file or partitioned directory).

    Args:
        path: Path to the parquet file or hive-partitioned directory.

    Returns:
        Error message if validation failed, None if valid.
    """
    try:
        from portolan_cli.formats import is_geoparquet

        if path.is_dir():
            return _validate_partitioned_geoparquet(path)

        if not is_geoparquet(path):
            return "Output file is not valid GeoParquet (missing geo metadata)"
        return None
    except Exception as e:
        return f"Failed to validate GeoParquet: {e}"


def _validate_partitioned_geoparquet(partition_dir: Path) -> str | None:
    """Validate a hive-partitioned GeoParquet directory.

    Checks that at least one parquet file exists and is valid GeoParquet.

    Args:
        partition_dir: Path to the partitioned output directory.

    Returns:
        Error message if validation failed, None if valid.
    """
    from portolan_cli.formats import is_geoparquet

    parquet_files = list(partition_dir.rglob("*.parquet"))
    if not parquet_files:
        return f"Partitioned directory contains no parquet files: {partition_dir}"

    # Validate first parquet file as representative sample
    sample_file = parquet_files[0]
    if not is_geoparquet(sample_file):
        return f"Partitioned output is not valid GeoParquet: {sample_file}"

    return None


def _validate_cog(path: Path) -> str | None:
    """Validate that the output file is a valid COG.

    Args:
        path: Path to the TIFF file to validate.

    Returns:
        Error message if validation failed, None if valid.
    """
    try:
        from rio_cogeo.cogeo import cog_validate

        is_valid, errors, _warnings = cog_validate(str(path))
        if not is_valid:
            return f"Output file is not valid COG: {'; '.join(errors)}"
        return None
    except ImportError:
        # If rio-cogeo isn't available, skip validation
        logger.debug("rio-cogeo not available for COG validation, skipping")
        return None
    except Exception as e:
        return f"Failed to validate COG: {e}"


# Note: GEOSPATIAL_EXTENSIONS imported from portolan_cli.constants


def convert_directory(
    path: Path,
    output_dir: Path | None = None,
    on_progress: Callable[[ConversionResult], None] | None = None,
    recursive: bool = True,
    file_paths: list[Path] | None = None,
    catalog_path: Path | None = None,
    workers: int | None = None,
    force: bool = False,
) -> ConversionReport:
    """Convert all geospatial files in a directory to cloud-native formats.

    Iterates through the directory, converts each geospatial file to its
    cloud-native equivalent (GeoParquet or COG), and returns an aggregate report.

    Args:
        path: Directory containing files to convert.
        output_dir: Directory for output files. If None, outputs are placed
            in the same directory as each source file.
        on_progress: Optional callback invoked after each file is processed.
            Receives the ConversionResult for streaming progress updates.
        recursive: If True (default), process subdirectories recursively.
        file_paths: Optional list of specific files to convert. If provided,
            skips directory scanning and converts only these files. Useful
            when the caller has already scanned and filtered the files.
        catalog_path: Path to the catalog root for loading conversion config.
            If None, uses ADR-0019 defaults for COG conversion.
        workers: Number of parallel worker processes. ``None`` or ``1`` runs
            serially (the default, so existing callers are unchanged). A value
            > 1 uses a ``ProcessPoolExecutor`` for CPU-bound raster re-encoding
            (issue #530). Callers wanting parallel-by-default resolve a concrete
            count (e.g. ``os.cpu_count()``) before calling.
        force: Re-optimize already-cloud-native rasters (passed through to
            :func:`convert_file`). Raster-scoped; valid vectors stay skipped.

    Returns:
        ConversionReport with results for all processed files.

    Raises:
        FileNotFoundError: If the directory does not exist.
        NotADirectoryError: If the path is a file, not a directory.
    """
    if not path.exists():
        raise FileNotFoundError(f"Directory not found: {path}")

    if not path.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {path}")

    # Use provided file list or scan for geospatial files
    if file_paths is not None:
        files = sorted(file_paths)
    else:
        files = []
        if recursive:
            for item in path.rglob("*"):
                if item.is_file() and item.suffix.lower() in GEOSPATIAL_EXTENSIONS:
                    files.append(item)
        else:
            for item in path.iterdir():
                if item.is_file() and item.suffix.lower() in GEOSPATIAL_EXTENSIONS:
                    files.append(item)
        files.sort()

    # Load conversion settings once and pass them explicitly to every file, so a
    # large batch does not re-read .portolan/config.yaml per file (4,200+ tiles).
    cog_settings = get_cog_settings(catalog_path) if catalog_path else None
    vector_settings = get_vector_settings(catalog_path) if catalog_path else None

    # Parallelize only when explicitly asked for (>1 worker) and there is more
    # than one file. A single file or workers<=1 stays serial to avoid pool
    # overhead and preserve the default behavior of every existing caller.
    effective_workers = workers if workers is not None else 1
    if effective_workers > 1 and len(files) > 1:
        results = _convert_files_parallel(
            files,
            output_dir=output_dir,
            catalog_path=catalog_path,
            cog_settings=cog_settings,
            vector_settings=vector_settings,
            force=force,
            on_progress=on_progress,
            workers=effective_workers,
        )
        return ConversionReport(results=results)

    # Serial path
    results = []
    for file_path in files:
        # Determine output directory for this file
        file_output_dir = output_dir if output_dir else file_path.parent

        result = convert_file(
            file_path,
            output_dir=file_output_dir,
            catalog_path=catalog_path,
            cog_settings=cog_settings,
            vector_settings=vector_settings,
            force=force,
        )
        results.append(result)

        # Invoke callback if provided
        if on_progress is not None:
            on_progress(result)

    return ConversionReport(results=results)


def _convert_files_parallel(
    files: list[Path],
    *,
    output_dir: Path | None,
    catalog_path: Path | None,
    cog_settings: CogSettings | None,
    vector_settings: VectorSettings | None,
    force: bool,
    on_progress: Callable[[ConversionResult], None] | None,
    workers: int,
) -> list[ConversionResult]:
    """Convert files concurrently with a ProcessPoolExecutor (issue #530).

    Conversion is CPU/GDAL-bound, so true parallelism needs separate processes,
    not threads. The ``on_progress`` callback is invoked in the PARENT process as
    each future completes (it is never pickled into a worker), preserving the
    streaming-progress contract (ADR-0040). A worker that dies or raises is
    captured into a FAILED result so one bad file does not abort the batch.
    """
    import multiprocessing
    from concurrent.futures import ProcessPoolExecutor, as_completed

    # Use a "spawn" context, not fork. GDAL/rasterio run worker threads, and
    # fork()-ing a multi-threaded process can deadlock the child (and aborts on
    # macOS). Spawn re-imports this module cleanly in each worker.
    mp_context = multiprocessing.get_context("spawn")

    results: list[ConversionResult] = []
    with ProcessPoolExecutor(max_workers=workers, mp_context=mp_context) as executor:
        future_to_file = {
            executor.submit(
                convert_file,
                file_path,
                output_dir=output_dir if output_dir else file_path.parent,
                catalog_path=catalog_path,
                cog_settings=cog_settings,
                vector_settings=vector_settings,
                force=force,
            ): file_path
            for file_path in files
        }
        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                result = future.result()
            except Exception as e:  # pragma: no cover - defensive (process crash/pickle)
                logger.exception("Parallel conversion worker failed for %s", file_path)
                result = ConversionResult(
                    source=file_path,
                    output=None,
                    format_from=file_path.suffix.lstrip(".").upper(),
                    format_to=None,
                    status=ConversionStatus.FAILED,
                    error=str(e),
                    duration_ms=0,
                )
            results.append(result)
            if on_progress is not None:
                on_progress(result)

    return results


# ---------------------------------------------------------------------------
# Multi-layer conversion (GeoPackage, FileGDB)
# ---------------------------------------------------------------------------


@dataclass
class LayerConversionResult:
    """Result of converting a single layer from a multi-layer file.

    Attributes:
        source: Path to the source multi-layer file.
        layer: Name of the layer that was converted.
        output: Path to the output GeoParquet file (None if failed).
        success: Whether the conversion succeeded.
        error: Error message if conversion failed.
    """

    source: Path
    layer: str
    output: Path | None
    success: bool
    error: str | None = None


def convert_multilayer_file(
    source: Path,
    output_dir: Path,
    settings: VectorSettings | None = None,
) -> list[LayerConversionResult]:
    """Convert all layers in a multi-layer file to separate GeoParquet files.

    For GeoPackage and FileGDB files that contain multiple layers, this function
    converts each layer to a separate GeoParquet file named:
        {source_stem}_{layer_name}.parquet

    Args:
        source: Path to the multi-layer file (GeoPackage or FileGDB).
        output_dir: Directory for output files.
        settings: Vector conversion settings. If None, uses defaults (no optimization).

    Returns:
        List of LayerConversionResult, one per layer.

    Raises:
        FileNotFoundError: If the source file does not exist.
        ValueError: If the file has no layers or layer listing fails.

    Note:
        Uses geoparquet-io's layer parameter for multi-layer format support.
    """
    from portolan_cli.formats import list_layers

    if not source.exists():
        raise FileNotFoundError(f"Source file not found: {source}")

    if settings is None:
        settings = VectorSettings()

    # Get all layers in the file
    layers = list_layers(source)
    if layers is None or len(layers) == 0:
        raise ValueError(
            f"Could not enumerate layers in {source}. "
            "FileGDB requires GDAL; GeoPackage should work with sqlite3."
        )

    results: list[LayerConversionResult] = []

    for layer_name in layers:
        output_path = output_dir / f"{source.stem}_{layer_name}.parquet"

        try:
            _convert_vector_layer(source, layer_name, output_path, settings)

            # Validate output
            validation_error = _validate_geoparquet(output_path)
            if validation_error:
                results.append(
                    LayerConversionResult(
                        source=source,
                        layer=layer_name,
                        output=output_path,
                        success=False,
                        error=validation_error,
                    )
                )
            else:
                results.append(
                    LayerConversionResult(
                        source=source,
                        layer=layer_name,
                        output=output_path,
                        success=True,
                    )
                )

        except Exception as e:
            logger.exception("Failed to convert layer %s from %s", layer_name, source)
            results.append(
                LayerConversionResult(
                    source=source,
                    layer=layer_name,
                    output=None,
                    success=False,
                    error=str(e),
                )
            )

    return results


def _convert_vector_layer(
    source: Path,
    layer: str,
    output: Path,
    settings: VectorSettings | None = None,
) -> None:
    """Convert a single layer from a multi-layer file to GeoParquet.

    Uses geoparquet-io's layer parameter for multi-layer format support.

    Args:
        source: Path to the multi-layer file.
        layer: Name of the layer to convert.
        output: Path for the output GeoParquet file.
        settings: Vector conversion settings. If None, uses defaults (no optimization).

    Raises:
        Exception: If conversion fails.
    """
    import geoparquet_io as gpio

    if settings is None:
        settings = VectorSettings()

    table = gpio.convert(source, layer=layer)
    table = _apply_vector_settings(table, settings)
    table.write(output)
