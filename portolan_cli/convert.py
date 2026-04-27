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
    get_cog_settings,
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


def generate_cog_thumbnail(cog_path: Path, max_size: int = 512, quality: int = 75) -> Path | None:
    """Generate a JPEG thumbnail from a COG file (Issue #372).

    Reads the lowest-resolution overview when available, downsamples to fit
    within ``max_size`` pixels on the longest edge, and writes a JPEG next to
    the COG. Per STAC best practices, this is the asset that should carry the
    ``thumbnail`` role.

    Args:
        cog_path: Path to the source COG file.
        max_size: Maximum pixel dimension for the longest edge (default 512).
        quality: JPEG quality 1-100 (default 75).

    Returns:
        Path to the written JPEG thumbnail, or None if the source could not be
        read as a raster (e.g., corrupt file).
    """
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


def convert_file(
    source: Path,
    output_dir: Path | None = None,
    catalog_path: Path | None = None,
    cog_settings: CogSettings | None = None,
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

    # Skip if already cloud-native
    if format_info.status == CloudNativeStatus.CLOUD_NATIVE:
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

    # Handle unsupported formats (not an error, just not convertible)
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

    # Determine output directory and format type
    out_dir = output_dir if output_dir else source.parent
    format_type = detect_format(source)

    # Load COG settings: explicit arg > catalog config > defaults
    if cog_settings is None:
        cog_settings = get_cog_settings(catalog_path) if catalog_path else CogSettings()

    # Convert based on format type
    try:
        if format_type == FormatType.VECTOR:
            output_path = _convert_vector(source, out_dir)
            target_format = "GeoParquet"
            # Validate output is valid GeoParquet
            validation_error = _validate_geoparquet(output_path)
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


def _convert_vector(source: Path, output_dir: Path) -> Path:
    """Convert a vector file to GeoParquet.

    Args:
        source: Source vector file.
        output_dir: Directory for output file.

    Returns:
        Path to the output GeoParquet file.
    """
    import geoparquet_io as gpio  # type: ignore[import-untyped]

    output_path = output_dir / f"{source.stem}.parquet"

    # Use geoparquet-io fluent API for conversion
    gpio.convert(str(source)).write(str(output_path))

    return output_path


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
    """Validate that the output file is valid GeoParquet.

    Args:
        path: Path to the parquet file to validate.

    Returns:
        Error message if validation failed, None if valid.
    """
    try:
        from portolan_cli.formats import is_geoparquet

        if not is_geoparquet(path):
            return "Output file is not valid GeoParquet (missing geo metadata)"
        return None
    except Exception as e:
        return f"Failed to validate GeoParquet: {e}"


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

    # Process each file
    results: list[ConversionResult] = []
    for file_path in files:
        # Determine output directory for this file
        file_output_dir = output_dir if output_dir else file_path.parent

        result = convert_file(file_path, output_dir=file_output_dir, catalog_path=catalog_path)
        results.append(result)

        # Invoke callback if provided
        if on_progress is not None:
            on_progress(result)

    return ConversionReport(results=results)


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
) -> list[LayerConversionResult]:
    """Convert all layers in a multi-layer file to separate GeoParquet files.

    For GeoPackage and FileGDB files that contain multiple layers, this function
    converts each layer to a separate GeoParquet file named:
        {source_stem}_{layer_name}.parquet

    Args:
        source: Path to the multi-layer file (GeoPackage or FileGDB).
        output_dir: Directory for output files.

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
            _convert_vector_layer(source, layer_name, output_path)

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


def _convert_vector_layer(source: Path, layer: str, output: Path) -> None:
    """Convert a single layer from a multi-layer file to GeoParquet.

    Uses geoparquet-io's layer parameter for multi-layer format support.

    Args:
        source: Path to the multi-layer file.
        layer: Name of the layer to convert.
        output: Path for the output GeoParquet file.

    Raises:
        Exception: If conversion fails.
    """
    import geoparquet_io as gpio

    gpio.convert(source, layer=layer).write(output)
