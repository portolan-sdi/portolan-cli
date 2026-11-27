"""Conversion API for transforming files to cloud-native formats."""

from __future__ import annotations

import contextlib
import json
import logging
import tempfile
import time
import warnings
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypeVar, cast

from portolan_cli.conversion_config import (
    GDAL_COMPRESS_NAMES,
    PREDICTOR_COMPRESSIONS,
    QUALITY_COMPRESSIONS,
    CogSettings,
    VectorSettings,
    get_cog_settings,
    get_vector_settings,
    resolve_cog_settings,
    settings_override,
)
from portolan_cli.formats import (
    CloudNativeStatus,
    current_run,
    get_cloud_native_status,
    in_run,
    invalidate,
    remember,
    run_scope,
)
from portolan_cli.report import emit
from portolan_cli.viz.thumbnail import (
    thumbnail_path_for,
)
from portolan_core.constants import GEOSPATIAL_EXTENSIONS
from portolan_core.errors import (
    ConversionFailedError,
    InvalidOutputError,
    OutputExistsError,
)
from portolan_core.trace import count, current_span, span

if TYPE_CHECKING:
    from collections.abc import Callable

    from portolan_cli.plugins.formats import FormatHandler
    from portolan_core.run_state import RunState

_RGB_BAND_COUNT = 3
_DBF_HEADER_MIN_BYTES = 32
_ASCII_MAX = 0x7F

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
    """Possible outcomes of a file conversion operation."""

    SUCCESS = "success"
    SKIPPED = "skipped"
    FAILED = "failed"
    INVALID = "invalid"
    UNSUPPORTED = "unsupported"


@dataclass
class ConversionResult:
    """Result of a single file conversion operation."""

    source: Path
    output: Path | None
    format_from: str
    format_to: str | None
    status: ConversionStatus
    error: str | None
    duration_ms: int

    def to_dict(self) -> dict[str, Any]:
        """Convert to a JSON-serializable dictionary."""
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
    """Aggregate results from batch conversion operations."""

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
        """Convert to a JSON-serializable dictionary."""
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
    """Generate a JPEG thumbnail from a COG file."""
    # No basemap for rasters: the raster fills the extent and hides it.
    if basemap_provider != "none":
        logger.debug("Basemap ignored for raster thumbnail (not needed)")
    try:
        import numpy as np
        import rasterio
        from rasterio.enums import Resampling
        from rasterio.errors import NotGeoreferencedWarning
    except ImportError:
        logger.debug("rasterio/numpy not available, skipping thumbnail generation")
        return None

    # `thumbnail_path_for` names it .thumb.jpg, so a user's data.jpg next to data.tif is left alone.
    thumb_path = thumbnail_path_for(cog_path)

    try:
        count("raster.opens")
        with rasterio.open(cog_path) as src:
            # Compute target shape preserving aspect ratio
            longest = max(src.width, src.height)
            scale = min(1.0, max_size / float(longest))
            out_w = max(1, round(src.width * scale))
            out_h = max(1, round(src.height * scale))

            # Pick up to 3 bands for RGB; fall back to first band for grayscale
            indexes: list[int] = [1, 2, 3] if src.count >= _RGB_BAND_COUNT else [1]

            # masked=True applies the dataset mask so the percentile stretch ignores nodata.
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

            # `indexes` reads 1 or 3 bands, the counts JPEG takes; the driver rejects PHOTOMETRIC.
            profile = {
                "driver": "JPEG",
                "width": out_w,
                "height": out_h,
                "count": data.shape[0],
                "dtype": "uint8",
                "quality": int(max(1, min(100, quality))),
            }

            # A thumbnail is a picture, not a map, so it has no georeference.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", NotGeoreferencedWarning)
                with rasterio.open(thumb_path, "w", **profile) as dst:
                    dst.write(data)
    except Exception as e:
        logger.debug("Could not generate thumbnail for %s: %s", cog_path, e)
        if thumb_path.exists():
            with contextlib.suppress(OSError):
                thumb_path.unlink()
        return None

    logger.debug("Generated thumbnail %s (%dx%d)", thumb_path.name, out_w, out_h)
    return thumb_path


def _discover_style_for_thumbnail(collection_dir: Path) -> Path | None:
    """Find a style file for thumbnail generation."""
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
    catalog_path: Path | None = None,
    state: RunState | None = None,
) -> ConversionResult | None:
    """Return a SKIPPED/UNSUPPORTED result if no conversion should run, else None."""
    if format_info.status == CloudNativeStatus.CLOUD_NATIVE:
        # force re-optimizes a file whose handler can rewrite it.
        from portolan_cli.plugins.formats import handler_for

        handler = handler_for(source, catalog_path, state=state) if force else None
        if handler is not None and hasattr(handler, "reoptimize"):
            return None
        return _result(
            source, None, format_info.display_name, None, ConversionStatus.SKIPPED, None, start_time
        )

    if format_info.status == CloudNativeStatus.UNSUPPORTED:
        logger.info(
            "Unsupported format, skipping: %s (%s)",
            source,
            format_info.display_name,
        )
        # Not an error: the format is unsupported.
        return _result(
            source,
            None,
            format_info.display_name,
            None,
            ConversionStatus.UNSUPPORTED,
            None,
            start_time,
        )

    return None


def convert_file(
    source: Path,
    output_dir: Path | None = None,
    catalog_path: Path | None = None,
    cog_settings: CogSettings | None = None,
    vector_settings: VectorSettings | None = None,
    force: bool = False,
    state: RunState | None = None,
) -> ConversionResult:
    """Convert a single file to cloud-native format."""
    # Each footer read of the file goes through the run memo.
    with span("convert.file", file=source.name), run_scope(state):
        start_time = time.perf_counter()

        if not source.exists():
            raise FileNotFoundError(f"Source file not found: {source}")

        from portolan_cli.plugins.formats import handler_for, plugin_handler_for

        # A plugin handler converts the formats it claims; built-in formats pass the cloud-native check first.
        handler = plugin_handler_for(source, catalog_path, state=state)
        reoptimize = False
        display_name = handler.display_name if handler is not None else ""
        format_info = (
            get_cloud_native_status(source, catalog_path, state=state)
            if handler is None or force
            else None
        )
        if format_info is not None and (
            handler is None or format_info.status == CloudNativeStatus.CLOUD_NATIVE
        ):
            early_exit = _early_exit_result(
                source,
                format_info,
                force=force,
                start_time=start_time,
                catalog_path=catalog_path,
                state=state,
            )
            if early_exit is not None:
                return early_exit
            reoptimize = format_info.status == CloudNativeStatus.CLOUD_NATIVE
            display_name = format_info.display_name
            handler = handler_for(source, catalog_path, state=state)
        if handler is None:
            return _result(
                source,
                None,
                display_name,
                None,
                ConversionStatus.FAILED,
                f"Unable to determine format type for {source.suffix}",
                start_time,
            )

        out_dir = output_dir or source.parent
        with settings_override(cog=cog_settings, vector=vector_settings):
            try:
                partitioned = (
                    None
                    if reoptimize
                    else _convert_partitioned_by_index(handler, source, out_dir, catalog_path)
                )
                # Size partitioning belongs to `add` only: this path reconverts every run and would collide.
                output_path = partitioned or convert_with_handler(
                    handler, source, out_dir, catalog_path, reoptimize=reoptimize
                )
            except InvalidOutputError as err:
                logger.warning("Conversion completed but validation failed: %s", err)
                return _result(
                    source,
                    Path(str(err.context["path"])),
                    display_name,
                    handler.display_name,
                    ConversionStatus.INVALID,
                    err.reason,
                    start_time,
                )
            except Exception as e:
                conversion_error = ConversionFailedError(str(source), e)
                logger.exception(
                    "Conversion failed for %s [%s]: %s", source, conversion_error.code, e
                )
                return _result(
                    source,
                    None,
                    display_name,
                    handler.display_name,
                    ConversionStatus.FAILED,
                    str(e),
                    start_time,
                )

        return _result(
            source,
            output_path,
            display_name,
            handler.display_name,
            ConversionStatus.SUCCESS,
            None,
            start_time,
        )


def _result(
    source: Path,
    output: Path | None,
    format_from: str,
    format_to: str | None,
    status: ConversionStatus,
    error: str | None,
    start_time: float,
) -> ConversionResult:
    result = ConversionResult(
        source=source,
        output=output,
        format_from=format_from,
        format_to=format_to,
        status=status,
        error=error,
        duration_ms=int((time.perf_counter() - start_time) * 1000),
    )
    current_span().set(
        format_from=format_from,
        format_to=format_to,
        status=status.value,
        duration_ms=result.duration_ms,
    )
    return result


def convert_with_handler(
    handler: FormatHandler,
    source: Path,
    output_dir: Path,
    catalog_root: Path | None,
    *,
    reoptimize: bool = False,
    target: Path | None = None,
    state: RunState | None = None,
) -> Path:
    """Convert ``source`` with its format handler. ``add`` and ``check --fix`` both call this."""
    reoptimize_method = getattr(handler, "reoptimize", None)
    if reoptimize and reoptimize_method is not None:
        output: Path = reoptimize_method(source, catalog_root)
        return output
    expected = handler.output_path(source, output_dir)
    if target is None:
        target = claim_output(source, expected, state=state)
    if target == expected:
        return handler.convert(source, output_dir, catalog_root)
    return _convert_to(handler, source, output_dir, catalog_root, target)


#: The Parquet footer key that names the source of a converted file. Its value
#: is JSON: the source path relative to the catalog (absolute outside a run) and the SHA-256 of
#: the source. GeoParquet defines no provenance key, so Portolan names its own.
PROVENANCE_KEY = "portolan:source"
#: The GDAL metadata tag that carries the same record in a COG.
COG_PROVENANCE_TAG = "PORTOLAN_SOURCE"


def provenance_record(source: Path) -> str:
    """The provenance value a converter writes into the output of ``source``."""
    state = current_run()
    checksum: Callable[[Path], str]
    if state is not None:
        checksum = state.checksum
    else:
        from portolan_core.checksums import compute_checksum, compute_dir_checksum

        # A FileGDB source is a directory.
        checksum = compute_dir_checksum if source.is_dir() else compute_checksum
    base, path = _provenance_path(source)
    return json.dumps({"base": base, "path": path, "sha256": checksum(source)}, sort_keys=True)


def _provenance_path(source: Path, state: RunState | None = None) -> tuple[str, str]:
    """Name a source unambiguously; catalog-relative identities survive moves."""
    from portolan_core.run_state import RunState

    active = current_run()
    if state is None and isinstance(active, RunState):
        state = active
    resolved = source.resolve()
    if state is not None and resolved.is_relative_to(state.catalog_root.resolve()):
        return "catalog", resolved.relative_to(state.catalog_root.resolve()).as_posix()
    return "absolute", resolved.as_posix()


def with_provenance(table: Any, source: Path, *, require_geometry: bool = False) -> Any:
    """Return a geoparquet-io table whose footer will name ``source``."""
    from geoparquet_io.api.table import Table

    if not isinstance(table, Table):
        return table
    if require_geometry and table.geometry_column is None:
        return table
    arrow = table.table
    metadata = dict(arrow.schema.metadata or {})
    metadata[PROVENANCE_KEY.encode()] = provenance_record(source).encode()
    return Table(
        arrow.replace_schema_metadata(metadata),
        geometry_column=table.geometry_column,
        crs=table.crs,
    )


def _recorded_source(output: Path) -> dict[str, object] | None:
    """The provenance record of ``output``, or None when absent or malformed."""
    raw: str | bytes | None = None
    suffix = output.suffix.lower()
    try:
        if suffix == ".parquet":
            import pyarrow.parquet as pq

            count("parquet.opens")
            raw = (pq.read_metadata(output).metadata or {}).get(PROVENANCE_KEY.encode())
        elif suffix in (".tif", ".tiff"):
            import rasterio

            count("raster.opens")
            with rasterio.open(output) as dataset:
                raw = dataset.tags().get(COG_PROVENANCE_TAG)
    except Exception:  # an unreadable file names no source
        return None
    if raw is None:
        return None
    try:
        recorded = json.loads(raw)
    except (ValueError, TypeError):
        return None
    return (
        recorded if isinstance(recorded, dict) and isinstance(recorded.get("path"), str) else None
    )


def _produced_by(output: Path, source: Path, state: RunState | None) -> bool | None:
    """Report whether ``output`` is a file that a conversion of ``source`` wrote."""
    memo = state or current_run()
    recorded = (
        memo.memo("conversion.provenance", output, _recorded_source)
        if memo is not None
        else _recorded_source(output)
    )
    if recorded is not None:
        base, path = _provenance_path(source, state)
        if recorded.get("base") is not None:
            if recorded["base"] == "absolute":
                owned = recorded["path"] == source.resolve().as_posix()
            else:
                owned = recorded["base"] == base and recorded["path"] == path
        # Early PR outputs named only a basename. It cannot establish ownership
        # across directories; their recorded checksum must prove the source.
        elif recorded["path"] != source.name:
            owned = False
        elif memo is not None:
            owned = recorded.get("sha256") == memo.checksum(source)
        else:
            from portolan_core.checksums import compute_checksum, compute_dir_checksum

            checksum = compute_dir_checksum if source.is_dir() else compute_checksum
            owned = recorded.get("sha256") == checksum(source)
        if owned:
            return True
        # A copied Parquet/COG retains its earlier conversion provenance. Its
        # bytes can still prove it is an exact copy of the current input.
    try:
        if output.stat().st_size != source.stat().st_size:
            return None
    except OSError:
        return None
    if memo is not None:
        same = memo.checksum(output) == memo.checksum(source)
    else:
        from portolan_core.checksums import compute_checksum

        same = compute_checksum(output) == compute_checksum(source)
    return True if same else (False if recorded is not None else None)


def distinct_output_path(source: Path, output: Path) -> Path:
    """The output name ``claim_output`` falls back to: ``<stem>-<ext><suffix>``."""
    extension = source.suffix.lstrip(".").lower()
    return output.with_name(f"{output.stem}-{extension}{output.suffix}")


def claim_output(source: Path, output: Path, *, state: RunState | None = None) -> Path:
    """Pick the path a conversion of ``source`` writes, so it replaces no other file."""
    # A multilayer conversion prepares each new layer as its own input after
    # the container reserved it. An in-place input keeps its own name.
    if output.resolve() == source.resolve():
        return output
    for candidate in (output, distinct_output_path(source, output)):
        available = (
            not candidate.exists()
            or candidate.resolve() == source.resolve()
            or _produced_by(candidate, source, state)
        )
        if available and (state is None or state.reserve_output(source, candidate)):
            return candidate
    raise OutputExistsError(source.name, output.name)


def _batch_has_output_inputs(paths: list[Path], catalog_root: Path, state: RunState | None) -> bool:
    """Report whether one batch input is also another input's possible output."""
    from portolan_cli.formats import list_layers
    from portolan_cli.plugins.formats import handler_for

    inputs = {path.resolve() for path in paths}
    for source in paths:
        try:
            handler = handler_for(source, catalog_root, state=state)
            outputs = [handler.output_path(source, source.parent)] if handler else []
            if source.suffix.lower() in (".gpkg", ".gdb"):
                layers = list_layers(source) or []
                if len(layers) > 1:
                    outputs.extend(
                        source.with_name(f"{source.stem}_{layer}.parquet") for layer in layers
                    )
        except Exception:  # preparation reports handler/input errors per file
            logger.debug("Could not inspect output dependencies for %s", source, exc_info=True)
            continue
        for output in outputs:
            if output.resolve() != source.resolve() and any(
                candidate.resolve() in inputs
                for candidate in (output, distinct_output_path(source, output))
            ):
                return True
    return False


def _convert_to(
    handler: FormatHandler,
    source: Path,
    output_dir: Path,
    catalog_root: Path | None,
    target: Path,
) -> Path:
    """Convert ``source`` with its handler and store the output at ``target``."""
    import shutil
    import tempfile

    stem = handler.output_path(source, output_dir).stem
    with tempfile.TemporaryDirectory(prefix=".portolan-convert-", dir=output_dir) as scratch:
        produced = handler.convert(source, Path(scratch), catalog_root)
        if produced.parent != Path(scratch):
            # The handler wrote outside the directory it was given.
            return produced
        for entry in sorted(Path(scratch).iterdir()):
            if entry.name == produced.name:
                shutil.move(entry, target)
                continue
            name = target.stem + entry.name[len(stem) :] if entry.name.startswith(stem) else ""
            if name and not (output_dir / name).exists():
                entry.replace(output_dir / name)
    return target


def _convert_partitioned_by_index(
    handler: FormatHandler, source: Path, output_dir: Path, catalog_root: Path | None
) -> Path | None:
    """Convert a vector source into Hive partitions by the configured spatial index."""
    from portolan_cli.plugins.formats import handler_named
    from portolan_cli.preparation import convert_vector

    if handler is not handler_named("geoparquet") or source.suffix.lower() == ".parquet":
        return None
    settings = get_vector_settings(catalog_root)
    if not (settings.partition and settings.spatial_index != "none"):
        return None
    output = convert_vector(
        source, output_dir, catalog_root, partition_dir=output_dir / source.stem
    )
    error = validate_geoparquet(output)
    if error is not None:
        raise InvalidOutputError(str(output), error)
    return output


# A transient interrupt clears after one attempt; if three fail, find the signal source.
# Do not raise this count.
_CONVERT_MAX_ATTEMPTS = 3


def write_raster_thumbnail(cog_path: Path, catalog_root: Path | None) -> None:
    """Write ``<stem>.thumb.jpg`` next to a COG when the COG settings ask for it.

    An existing thumbnail stays, so delete it to get a new one. A failure only warns.

    Args:
        cog_path: The COG.
        catalog_root: Catalog root, for the COG settings.
    """
    settings = get_cog_settings(catalog_root)
    if not settings.generate_thumbnail:
        return
    if cog_path.with_name(f"{cog_path.stem}.thumb.jpg").exists():
        return
    try:
        generate_cog_thumbnail(
            cog_path, max_size=settings.thumbnail_max_size, quality=settings.thumbnail_quality
        )
    except Exception as e:  # the thumbnail is optional
        logger.warning("Thumbnail generation failed for %s: %s", cog_path.name, e)


def _is_transient_conversion_error(exc: BaseException) -> bool:
    """Return True for a transient DuckDB interrupt that is safe to retry."""
    return type(exc).__name__ == "InterruptException" or "query interrupted" in str(exc).lower()


_ConvertT = TypeVar("_ConvertT")


def run_with_transient_convert_retry(
    operation: Callable[[], _ConvertT],
    *,
    source_name: str,
) -> _ConvertT:
    """Run a DuckDB-backed conversion, retrying transient interrupts."""
    for attempt in range(1, _CONVERT_MAX_ATTEMPTS + 1):
        try:
            return operation()
        except Exception as exc:
            is_last_attempt = attempt == _CONVERT_MAX_ATTEMPTS
            if is_last_attempt or not _is_transient_conversion_error(exc):
                raise
            logger.warning(
                "Transient interrupt converting %s (attempt %d/%d), retrying: %s",
                source_name,
                attempt,
                _CONVERT_MAX_ATTEMPTS,
                exc,
            )
    # Unreachable: the final attempt always returns or re-raises above. Present
    # only so the type checker sees every path terminates.
    raise AssertionError("vector conversion retry loop exited without returning")


def repair_shapefile_encoding_sidecar(shp: Path) -> Path | None:
    """Make a shapefile's ``.cpg`` sidecar match what its DBF records contain."""
    import struct

    # The notice is user-facing; the CLI prints it through the output writer (stderr in JSON mode).
    dbf, cpg = shp.with_suffix(".dbf"), shp.with_suffix(".cpg")
    if not dbf.exists():
        return None
    raw = dbf.read_bytes()
    if len(raw) <= _DBF_HEADER_MIN_BYTES:
        return None
    header_len = struct.unpack_from("<H", raw, 8)[0]
    records = raw[header_len:]
    try:
        records.decode("utf-8")
        records_are_utf8 = True
    except UnicodeDecodeError:
        records_are_utf8 = False
    has_non_ascii = any(b > _ASCII_MAX for b in records)
    declared = cpg.read_text(errors="replace").strip().upper() if cpg.exists() else None

    if records_are_utf8 and has_non_ascii and declared not in (None, "UTF-8", "UTF8"):
        cpg.write_text("UTF-8")
        emit("info", f"Corrected {cpg.name}: declared {declared}, DBF records are UTF-8")
        return cpg
    if not records_are_utf8 and (declared is None or "UTF" in (declared or "")):
        cpg.write_text("ISO-8859-1")
        what = f"corrected from {declared}" if declared else "written"
        emit("info", f"Encoding sidecar {cpg.name} {what}: DBF records are not valid UTF-8")
        return cpg
    return None


def has_geometry(table: Any) -> bool:
    """Report whether a gpio Table carries a geometry column."""
    return getattr(table, "geometry_column", "geometry") is not None


def apply_vector_settings(
    table: Any, settings: VectorSettings, *, partitioned: bool = False
) -> Any:
    """Apply spatial optimization settings to a gpio Table."""
    if not has_geometry(table):
        return table

    # Add bbox column if requested
    if settings.add_bbox:
        table = table.add_bbox()

    # geoparquet-io partitions on an existing column, so adding one here would bypass `auto` sizing.
    if settings.spatial_index != "none" and not partitioned:
        resolution = _resolve_resolution(settings.resolution)
        table = _add_spatial_index(table, settings.spatial_index, resolution)

    # Apply sorting
    if settings.sort == "hilbert":
        table = table.sort_hilbert()
    elif settings.sort == "quadkey":
        # sort_quadkey needs resolution; use quadkey default (13) if not set
        sort_resolution = _resolve_resolution(settings.resolution)
        if sort_resolution is None:
            table = table.sort_quadkey()
        else:
            table = table.sort_quadkey(resolution=sort_resolution)

    return table


def _resolve_resolution(resolution: int | str) -> int | None:
    """Resolve resolution value for a spatial index type."""
    if resolution == "auto":
        return None  # Let gpio use its defaults
    return int(resolution)


def _add_spatial_index(table: Any, index_type: str, resolution: int | None) -> Any:
    """Add a spatial index column to the table."""
    if index_type == "h3":
        return table.add_h3() if resolution is None else table.add_h3(resolution=resolution)
    if index_type == "s2":
        return table.add_s2() if resolution is None else table.add_s2(level=resolution)
    if index_type == "quadkey":
        return (
            table.add_quadkey() if resolution is None else table.add_quadkey(resolution=resolution)
        )
    if index_type == "a5":
        return table.add_a5() if resolution is None else table.add_a5(resolution=resolution)
    if index_type == "kdtree":
        # geoparquet-io 1.5 requires a size or `auto` for kdtree: https://github.com/geoparquet/geoparquet-io/pull/855
        if resolution is None:
            return table.add_kdtree(auto=True)
        return table.add_kdtree(iterations=resolution)
    raise ValueError(
        f"Unknown spatial index type: '{index_type}'. Valid types: h3, s2, quadkey, a5, kdtree"
    )


# The keyword each geoparquet-io partition method uses for its cell size.
_PARTITION_SIZE_KWARG = {
    "h3": "resolution",
    "s2": "level",
    "quadkey": "resolution",
    "a5": "resolution",
    "kdtree": "iterations",
}

# geoparquet-io 1.5 requires a prefix length beside an explicit resolution: https://github.com/geoparquet/geoparquet-io/pull/800
# 6 is the length geoparquet-io 1.4 used by default.
_QUADKEY_PARTITION_RESOLUTION = 6


def _write_partitioned(table: Any, output_dir: Path, settings: VectorSettings) -> None:
    """Write table as hive-partitioned output."""
    index_type = settings.spatial_index
    size_kwarg = _PARTITION_SIZE_KWARG.get(index_type)
    if size_kwarg is None:
        return

    resolution = _resolve_resolution(settings.resolution)
    sizing: dict[str, Any] = {"auto": True} if resolution is None else {size_kwarg: resolution}
    if index_type == "quadkey" and resolution is not None:
        sizing["partition_resolution"] = min(resolution, _QUADKEY_PARTITION_RESOLUTION)

    # geoparquet-io 1.4 changed the `hive` default to False: https://github.com/geoparquet/geoparquet-io/pull/661
    # Pass hive and the keep_*_column flags on every call; each partition directory must be a STAC Item.
    partition = getattr(table, f"partition_by_{index_type}")
    partition(str(output_dir), hive=True, **{f"keep_{index_type}_column": True}, **sizing)


#: The GTiff creation option that carries ``conversion.cog.quality`` for each
#: codec in ``QUALITY_COMPRESSIONS``.
_GTIFF_QUALITY_OPTION = {"JPEG": "jpeg_quality", "WEBP": "webp_level"}

#: Up to this many pixels (4096 x 4096), write_cog embeds exact band statistics.
#: Above it, GDAL computes them from a sample of the blocks, so a large source
#: costs no second full read. The band then carries STATISTICS_APPROXIMATE=YES.
EXACT_STATISTICS_PIXELS = 4096 * 4096


def _compute_band_statistics(src: Any, *, approx: bool) -> None:
    """Make GDAL compute the statistics of each band and keep them as band tags."""
    from rasterio.errors import RasterioDeprecationWarning, StatisticsError

    for bidx in src.indexes:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RasterioDeprecationWarning)
            with contextlib.suppress(StatisticsError):
                # Pixel edits need not invalidate a publisher's statistics tags.
                # clear_cache clears every band, so do it only on the first.
                src.statistics(bidx, approx=approx, clear_cache=bidx == src.indexes[0])


#: Marks a scratch file that an in-place rewrite swaps in. ``add`` collects a
#: directory before it converts anything, so the marker keeps a file left by a
#: killed run out of the next run's source list.
REWRITE_TEMP_INFIX = ".portolan-rewrite"

#: The ``conversion.cog.predictor`` values, as the GDAL COG driver names them.
_COG_PREDICTOR = {1: "NO", 2: "STANDARD", 3: "FLOATING_POINT"}

#: rio-cogeo turns the nodata value into a mask for JPEG and WEBP.
RIO_COGEO_CODECS = QUALITY_COMPRESSIONS


def write_cog(
    source: Path,
    output_dir: Path,
    settings: CogSettings | None = None,
    *,
    target: Path | None = None,
) -> Path:
    """Write a raster file as a COG with the given settings."""
    import rasterio

    settings = settings or CogSettings()
    output_path = (
        target if target is not None else claim_output(source, output_dir / f"{source.stem}.tif")
    )
    # A COG written in place is its own source, so it needs no record.
    record = output_path.resolve() != source.resolve()
    count("raster.opens")
    with rasterio.Env(GDAL_PAM_ENABLED="NO"), rasterio.open(source) as src:
        resolved = resolve_cog_settings(settings, src)
        if resolved.compression in RIO_COGEO_CODECS:
            scratch = _rio_cogeo_cog(src, source, output_path, resolved, record=record)
        else:
            options = _cog_options(resolved, src.width, src.height)
            # rio-cogeo wrote this tag, so a COG from either writer carries it.
            tags = {"OVERVIEW_RESAMPLING": resolved.resampling.upper()}
            if record:
                tags[COG_PROVENANCE_TAG] = provenance_record(source)
            scratch = copy_to_cog(src, output_path, options, tags)
    place_cog(scratch, output_path)
    return output_path


def _cog_options(settings: CogSettings, width: int, height: int) -> dict[str, Any]:
    """Return the GDAL COG driver options for a lossless codec."""
    from rasterio.rio.overview import get_maximum_overview_level

    options: dict[str, Any] = {
        "compress": GDAL_COMPRESS_NAMES.get(settings.compression, settings.compression),
        "blocksize": settings.tile_size,
        "resampling": settings.resampling.replace("_", "").upper(),
        "bigtiff": "IF_SAFER",
    }
    levels = get_maximum_overview_level(width, height, minsize=settings.tile_size)
    if levels:
        options["overview_count"] = levels
        # AUTO reuses source overviews, even when their resampling differs.
        options["overviews"] = "IGNORE_EXISTING"
    else:
        options["overviews"] = "NONE"
    # GDAL ignores PREDICTOR for any other codec and logs a warning per file.
    if settings.compression in PREDICTOR_COMPRESSIONS:
        options["predictor"] = _COG_PREDICTOR[int(settings.predictor)]
    return options


def copy_to_cog(src: Any, output: Path, options: dict[str, Any], tags: dict[str, str]) -> Path:
    """Write the open raster ``src`` to a scratch COG beside ``output``."""
    from rasterio.enums import MaskFlags
    from rasterio.shutil import copy as gdal_copy

    _compute_band_statistics(src, approx=src.width * src.height > EXACT_STATISTICS_PIXELS)
    scratch = _cog_scratch(output)
    vrt = scratch.with_suffix(".vrt")
    try:
        gdal_copy(src, str(vrt), driver="VRT")
        if tags:
            _tag_vrt(vrt, tags)
        # GDAL opens the VRT and its source internally. Neither goes through
        # rasterio.open, so the Python open-site gate cannot see these reads.
        count("raster.opens", 2)
        if options.get("compress") == "PACKBITS":
            _copy_packbits_cog(vrt, scratch, options)
        else:
            if options.get("overview_count"):
                # COG builds its pyramid in a temporary GTiff, then opens it
                # twice. An explicit mask gets a separate temporary pyramid.
                count("raster.opens", 2)
                flags = src.mask_flag_enums[0]
                if MaskFlags.per_dataset in flags and MaskFlags.alpha not in flags:
                    count("raster.opens", 2)
            gdal_copy(str(vrt), str(scratch), driver="COG", **options)
    except Exception:
        scratch.unlink(missing_ok=True)
        raise
    finally:
        vrt.unlink(missing_ok=True)
    return scratch


def _copy_packbits_cog(vrt: Path, scratch: Path, options: dict[str, Any]) -> None:
    """Copy through a tiled GTiff to avoid rio-cogeo's nodata-changing warp."""
    import rasterio
    from rasterio.enums import Resampling
    from rasterio.shutil import copy as gdal_copy

    tiled = scratch.with_suffix(".tiles.tif")
    profile = {
        "compress": "PACKBITS",
        "tiled": True,
        "blockxsize": options["blocksize"],
        "blockysize": options["blocksize"],
        "bigtiff": "IF_SAFER",
    }
    try:
        with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):
            # The caller counted the VRT and its internally opened source.
            gdal_copy(str(vrt), str(tiled), driver="GTiff", **profile)
            # rasterio probes the driver before opening an update handle.
            count("raster.opens", 2)
            with rasterio.open(tiled, "r+") as intermediate:
                levels = options.get("overview_count", 0)
                if levels:
                    resampling = next(
                        method
                        for method in Resampling
                        if method.name.replace("_", "").upper() == options["resampling"]
                    )
                    intermediate.build_overviews([2**i for i in range(1, levels + 1)], resampling)
            count("raster.opens")
            gdal_copy(str(tiled), str(scratch), driver="GTiff", copy_src_overviews=True, **profile)
    finally:
        tiled.unlink(missing_ok=True)


def _tag_vrt(vrt: Path, tags: dict[str, str]) -> None:
    """Set dataset metadata items in a VRT file. The COG copy keeps them."""
    import xml.etree.ElementTree as ET  # the usual name

    tree = ET.parse(vrt)  # noqa: S314 - GDAL wrote this file a moment ago
    root = tree.getroot()
    metadata = next((m for m in root.findall("Metadata") if not m.get("domain")), None)
    if metadata is None:
        metadata = ET.SubElement(root, "Metadata")
    for item in [m for m in metadata.findall("MDI") if m.get("key") in tags]:
        metadata.remove(item)
    for key, value in tags.items():
        ET.SubElement(metadata, "MDI", key=key).text = value
    tree.write(vrt)


def _rio_cogeo_cog(
    src: Any, source: Path, output: Path, settings: CogSettings, *, record: bool
) -> Path:
    """Write the open raster ``src`` to a scratch COG with a codec of ``RIO_COGEO_CODECS``."""
    from rio_cogeo.cogeo import cog_translate
    from rio_cogeo.profiles import cog_profiles

    profile = cog_profiles.get(settings.compression.lower())  # type: ignore[no-untyped-call]
    profile["blockxsize"] = settings.tile_size
    profile["blockysize"] = settings.tile_size
    # The GTiff driver has no QUALITY option. Each lossy codec takes its own.
    if settings.quality is not None and settings.compression in QUALITY_COMPRESSIONS:
        profile[_GTIFF_QUALITY_OPTION[settings.compression]] = settings.quality

    _compute_band_statistics(src, approx=src.width * src.height > EXACT_STATISTICS_PIXELS)
    scratch = _cog_scratch(output)
    try:
        cog_translate(
            src,
            str(scratch),
            profile,
            quiet=True,
            overview_resampling=cast("ResamplingMethod", settings.resampling),
            forward_band_tags=True,
            additional_cog_metadata=(
                {COG_PROVENANCE_TAG: provenance_record(source)} if record else {}
            ),
        )
    except Exception:
        scratch.unlink(missing_ok=True)
        raise
    return scratch


def _cog_scratch(output: Path) -> Path:
    """Exclusively create a unique sibling for each pending raster write."""
    with tempfile.NamedTemporaryFile(
        prefix=f".{output.stem}{REWRITE_TEMP_INFIX}-",
        suffix=".tif",
        dir=output.parent,
        delete=False,
    ) as scratch:
        return Path(scratch.name)


def place_cog(scratch: Path, output: Path) -> Path:
    """Check a scratch COG, move it over ``output``, and give the run what it read."""
    import rasterio

    from portolan_cli.metadata.statistics import embedded_statistics

    try:
        count("raster.opens")
        with rasterio.Env(GDAL_PAM_ENABLED="NO"), rasterio.open(scratch) as cog:
            if embedded_statistics(cog) != "present":
                raise InvalidOutputError(str(output), "a band has no statistics in the file")
            read = read_cog(cog)
        scratch.replace(output)
        invalidate(output)
    finally:
        scratch.unlink(missing_ok=True)
    remember_cog(output, read)
    return output


def read_cog(src: Any) -> tuple[Any, list[Any] | None]:
    """Read the item metadata and the embedded band statistics of an open COG."""
    from portolan_cli.metadata.cog import cog_metadata_of
    from portolan_cli.metadata.statistics import band_statistics_of

    statistics = band_statistics_of(src, mode="cached")
    return cog_metadata_of(src), statistics if len(statistics) == src.count else None


def remember_cog(output: Path, read: tuple[Any, list[Any] | None]) -> None:
    """Give the run memo what :func:`read_cog` read, for the COG now at ``output``."""
    metadata, statistics = read
    remember("cog.metadata", output, metadata)
    if statistics is not None:
        # Both modes read the tags first, so both give these values.
        remember("cog.statistics.cached", output, statistics)
        remember("cog.statistics.approx", output, statistics)


def validate_geoparquet(path: Path) -> str | None:
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
    """Validate a hive-partitioned GeoParquet directory."""
    from portolan_cli.formats import is_geoparquet

    parquet_files = list(partition_dir.rglob("*.parquet"))
    if not parquet_files:
        return f"Partitioned directory contains no parquet files: {partition_dir}"

    # Validate first parquet file as representative sample
    sample_file = parquet_files[0]
    if not is_geoparquet(sample_file):
        return f"Partitioned output is not valid GeoParquet: {sample_file}"

    return None


def validate_cog(path: Path) -> str | None:
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


# Note: GEOSPATIAL_EXTENSIONS imported from portolan_core.constants


def convert_directory(  # noqa: PLR0917 - each convert option is an argument
    path: Path,
    output_dir: Path | None = None,
    on_progress: Callable[[ConversionResult], None] | None = None,
    recursive: bool = True,
    file_paths: list[Path] | None = None,
    catalog_path: Path | None = None,
    workers: int | None = None,
    force: bool = False,
    state: RunState | None = None,
) -> ConversionReport:
    """Convert all geospatial files in a directory to cloud-native formats."""
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
    cog_settings = get_cog_settings(catalog_path, state=state) if catalog_path else None
    vector_settings = get_vector_settings(catalog_path, state=state) if catalog_path else None

    # Parallelize only for more than one worker and more than one file.
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
            state=state,
        )
        return ConversionReport(results=results)

    # Serial path
    results = _convert_files_serial(
        files,
        output_dir=output_dir,
        catalog_path=catalog_path,
        cog_settings=cog_settings,
        vector_settings=vector_settings,
        force=force,
        on_progress=on_progress,
        state=state,
    )
    return ConversionReport(results=results)


def _convert_files_serial(
    files: list[Path],
    *,
    output_dir: Path | None,
    catalog_path: Path | None,
    cog_settings: CogSettings | None,
    vector_settings: VectorSettings | None,
    force: bool,
    on_progress: Callable[[ConversionResult], None] | None,
    state: RunState | None = None,
) -> list[ConversionResult]:
    """Convert files one at a time in the current process."""
    results: list[ConversionResult] = []
    for file_path in files:
        file_output_dir = output_dir or file_path.parent
        result = convert_file(
            file_path,
            output_dir=file_output_dir,
            catalog_path=catalog_path,
            cog_settings=cog_settings,
            vector_settings=vector_settings,
            force=force,
            state=state,
        )
        results.append(result)
        if on_progress is not None:
            on_progress(result)
    return results


class _Worker:
    """Hold the run state of a conversion worker process. ``_init_worker_state`` sets it."""

    state: RunState | None = None


def _init_worker_state(catalog_path: Path | None, tracing: bool = False) -> None:
    """Give this worker process one run state, so it parses the config once."""
    from portolan_core import trace
    from portolan_core.run_state import RunState

    _Worker.state = RunState.open(catalog_path) if catalog_path is not None else None
    if tracing:
        trace.enable()
        # Spawn does not inherit patched library readers, so the perf harness supplies its own.
        import importlib
        import os

        if initializer := os.environ.get("PORTOLAN_PERF_WORKER_INIT"):
            module, name = initializer.split(":", 1)
            getattr(importlib.import_module(module), name)()


def _replay_worker_output(increments: dict[str, int], messages: list[tuple[str, str]]) -> None:
    """Add the trace counts of a worker to this process and print its messages."""
    for name, value in increments.items():
        if value:
            count(name, value)
    # Each message prints before the progress line of its file.
    for level, text in messages:
        emit(level, text)


def _convert_in_worker(
    source: Path, **kwargs: Any
) -> tuple[ConversionResult, dict[str, int], list[tuple[str, str]]]:
    """Return a conversion, its counters, and its messages to the parent process."""
    from portolan_cli.report import recording
    from portolan_core import trace

    before = trace.counters() if trace.is_enabled() else {}
    with recording() as messages:
        result = convert_file(source, state=_Worker.state, **kwargs)
    increments = (
        {name: value - before.get(name, 0) for name, value in trace.counters().items()}
        if trace.is_enabled()
        else {}
    )
    return result, increments, messages


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
    state: RunState | None = None,
) -> list[ConversionResult]:
    """Convert files concurrently with a ProcessPoolExecutor."""
    import multiprocessing
    from concurrent.futures import ProcessPoolExecutor, as_completed
    from concurrent.futures.process import BrokenProcessPool

    from portolan_core import trace

    # Spawn, not fork: GDAL threads can deadlock a forked child (and abort on macOS).
    mp_context = multiprocessing.get_context("spawn")

    # Keyed by source path so a mid-run pool failure can convert only the files
    # that have not completed yet, without re-firing on_progress for the rest.
    completed: dict[Path, ConversionResult] = {}

    def _record(file_path: Path, result: ConversionResult) -> None:
        completed[file_path] = result
        if on_progress is not None:
            on_progress(result)

    try:
        with ProcessPoolExecutor(
            max_workers=workers,
            mp_context=mp_context,
            initializer=_init_worker_state,
            initargs=(catalog_path, trace.is_enabled()),
        ) as executor:
            future_to_file = {
                executor.submit(
                    _convert_in_worker,
                    file_path,
                    output_dir=output_dir or file_path.parent,
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
                    result, increments, messages = future.result()
                    _replay_worker_output(increments, messages)
                except BrokenProcessPool:
                    # The pool itself died; let the outer handler fall back to
                    # serial rather than marking this file FAILED.
                    raise
                except Exception as e:  # pragma: no cover - defensive (worker crash/pickle)
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
                _record(file_path, result)
    except BrokenProcessPool:
        # A pool can fail to start in restricted environments; finish the batch serially.
        remaining = [file_path for file_path in files if file_path not in completed]
        logger.warning(
            "Process pool unavailable; converting %d remaining file(s) serially",
            len(remaining),
        )
        serial_results = _convert_files_serial(
            remaining,
            output_dir=output_dir,
            catalog_path=catalog_path,
            cog_settings=cog_settings,
            vector_settings=vector_settings,
            force=force,
            on_progress=on_progress,
            state=state,
        )
        completed.update(dict(zip(remaining, serial_results, strict=True)))

    # Preserve the caller's input order regardless of completion order.
    return [completed[file_path] for file_path in files]


@dataclass
class LayerConversionResult:
    """Result of converting a single layer from a multi-layer file."""

    source: Path
    layer: str
    output: Path | None
    success: bool
    error: str | None = None


def convert_multilayer_file(
    source: Path,
    output_dir: Path,
    settings: VectorSettings | None = None,
    *,
    state: RunState | None = None,
) -> list[LayerConversionResult]:
    """Convert all layers in a multi-layer file to separate GeoParquet files."""
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
        default_path = output_dir / f"{source.stem}_{layer_name}.parquet"
        try:
            output_path = claim_output(source, default_path, state=state)
        except OutputExistsError as err:
            results.append(LayerConversionResult(source, layer_name, None, False, str(err)))
            continue

        # The provenance record hashes the source through the run memo, so a
        # file of N layers is hashed once, not N times.
        hashing = in_run(state) if state is not None else contextlib.nullcontext()
        try:
            with hashing:
                _convert_vector_layer(source, layer_name, output_path, settings)

            # Validate output
            validation_error = validate_geoparquet(output_path)
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
    """Convert a single layer from a multi-layer file to GeoParquet."""
    import geoparquet_io as gpio

    if settings is None:
        settings = VectorSettings()

    resolved = settings

    def _run() -> None:
        table = gpio.convert(source, layer=layer)
        table = apply_vector_settings(table, resolved)
        with_provenance(table, source).write(output)

    run_with_transient_convert_retry(_run, source_name=f"{source.name}:{layer}")
