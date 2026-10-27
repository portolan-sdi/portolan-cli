"""Conversion API for transforming files to cloud-native formats.

This module provides the core conversion functionality for Portolan CLI:
- ConversionStatus: Enum of possible conversion outcomes
- ConversionResult: Result of a single file conversion
- ConversionReport: Aggregate results from batch conversion
- convert_file(): Convert a single file to cloud-native format
- convert_directory(): Convert all files in a directory

This module contains the logic; CLI commands are thin wrappers.
Actual conversion is delegated to geoparquet-io and rio-cogeo.
"""

from __future__ import annotations

import contextlib
import json
import logging
import time
import warnings
from contextvars import ContextVar
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypeVar, cast

from portolan_cli.constants import GEOSPATIAL_EXTENSIONS, TABULAR_EXTENSIONS
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
from portolan_cli.errors import (
    ConversionFailedError,
    InvalidOutputError,
    OutputExistsError,
)
from portolan_cli.formats import (
    CloudNativeStatus,
    get_cloud_native_status,
)
from portolan_cli.trace import count, current_span, span
from portolan_cli.versions import read_versions, versions_asset_key
from portolan_cli.viz.thumbnail import (
    thumbnail_path_for,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from portolan_cli.plugins.formats import FormatHandler
    from portolan_cli.run_state import RunState

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
            sparse and benefit from geographic context. See.

    Returns:
        Path to the written JPEG thumbnail, or None if the source could not be
        read as a raster (e.g., corrupt file).
    """
    # Basemaps intentionally not supported for rasters:
    # Raster data fills the extent, so a basemap would be hidden underneath.
    # Vector thumbnails need basemaps because points/lines are sparse.
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

    # `thumbnail_path_for` names it ".thumb.jpg" rather than ".jpg", so we leave
    # a user-supplied sibling alone (e.g. hand-curated data.jpg next to
    # data.tif). The extension stays .jpg, so _scan_item_assets assigns role
    # "thumbnail". The vector render calls the same helper. One convention then
    # names every thumbnail we write, and push recognizes it (Issue #735).
    thumb_path = thumbnail_path_for(cog_path)

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

            # `indexes` reads 1 or 3 bands, the two counts JPEG takes. The
            # driver reads the colour model from the band count, and rejects
            # a PHOTOMETRIC creation option.
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
    catalog_path: Path | None = None,
    state: RunState | None = None,
) -> ConversionResult | None:
    """Return a SKIPPED/UNSUPPORTED result if no conversion should run, else None.

    Cloud-native files are skipped, EXCEPT when ``force`` is set and the file is a
    RASTER (a valid COG) — that case re-optimizes in place (issue #530), so this
    returns None to let conversion proceed. Vectors are never force-re-processed.
    Unsupported formats are accepted with no error.
    """
    if format_info.status == CloudNativeStatus.CLOUD_NATIVE:
        # --force re-optimizes a file whose handler can rewrite it (issue #530).
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
    """Convert a single file to cloud-native format.

    Converts vector files to GeoParquet and raster files to COG.
    Files that are already cloud-native are skipped.

    Args:
        source: Path to the source file to convert.
        output_dir: Directory for the output file. If None, uses the same
            directory as the source file.
        catalog_path: Path to the catalog root for loading conversion config.
            If None, uses the built-in defaults.
        cog_settings: Explicit COG settings override. Takes precedence over
            ``catalog_path``-loaded settings. If None, loads from
            ``catalog_path`` or falls back to defaults.
        vector_settings: Explicit vector settings override. Takes precedence over
            ``catalog_path``-loaded settings. If None, loads from
            ``catalog_path`` or falls back to no spatial optimization.
        force: Re-optimize an already-cloud-native RASTER by re-applying current
            ``cog_settings`` (e.g. to add missing overviews). Raster-scoped only:
            cloud-native vectors are still skipped (issue #530). Has no effect on
            CONVERTIBLE or UNSUPPORTED files.
        state: The run state, which parses the config once per command.

    Returns:
        ConversionResult with conversion outcome, timing, and paths.

    Raises:
        FileNotFoundError: If the source file does not exist.
    """
    with span("convert.file", file=source.name):
        start_time = time.perf_counter()

        if not source.exists():
            raise FileNotFoundError(f"Source file not found: {source}")

        from portolan_cli.plugins.formats import handler_for, plugin_handler_for

        # A plugin format handler converts the formats it claims (#947). A built-in
        # format first passes the cloud-native check.
        handler = plugin_handler_for(source, catalog_path, state=state)
        reoptimize = False
        display_name = handler.display_name if handler is not None else ""
        if handler is None:
            format_info = get_cloud_native_status(source, catalog_path, state=state)
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
                # The size partition rule belongs to `add` only, as before #947.
                # This path converts a source again on every run, and a second
                # size partition into the same directory would collide.
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
    """Convert ``source`` with its format handler. ``add`` and ``check --fix`` both call this.

    The output never replaces a file that ``source`` did not write. See
    :func:`claim_output`.

    Args:
        handler: The handler ``handler_for`` picked.
        source: Source file.
        output_dir: Directory for the converted file.
        catalog_root: Catalog root, for settings.
        reoptimize: Rewrite a file that is already cloud-native. Only a handler
            with a ``reoptimize`` method supports it.
        target: The output path that :func:`claim_output` gave. None claims it here.
        state: The run state, for one hash per file and run.

    Returns:
        The converted file.

    Raises:
        InvalidOutputError: The converted file fails the handler's validation.
        OutputExistsError: No output name is free of another file.
    """
    reoptimize_method = getattr(handler, "reoptimize", None)
    if reoptimize and reoptimize_method is not None:
        output: Path = reoptimize_method(source, catalog_root)
        return output
    expected = handler.output_path(source, output_dir)
    if target is None:
        target = claim_output(source, expected, state=state)
    if target == expected:
        return handler.convert(source, output_dir, catalog_root)
    written = _convert_to(handler, source, output_dir, catalog_root, target)
    return settle_output(source, expected, written, state=state, geometry_required=True)


#: The Parquet footer key that names the source of a converted file. Its value
#: is JSON: the source path relative to the output directory and the SHA-256 of
#: the source. GeoParquet defines no provenance key, so Portolan names its own.
PROVENANCE_KEY = "portolan:source"
#: The GDAL metadata tag that carries the same record in a COG.
COG_PROVENANCE_TAG = "PORTOLAN_SOURCE"


#: The checksum function of the run that converts. A format handler takes no
#: run state, so ``prepare_item`` sets this around the conversion, as
#: ``conversion_config.settings_override`` does for the settings. The record
#: then reuses the run's hash of the source, and the asset scan hits the same
#: memo, so ``add`` hashes each source once.
_RUN_CHECKSUM: ContextVar[Callable[[Path], str] | None] = ContextVar(
    "portolan_run_checksum", default=None
)


@contextlib.contextmanager
def run_checksums(checksum: Callable[[Path], str]) -> Iterator[None]:
    """Make :func:`provenance_record` hash through ``checksum``, such as ``RunState.checksum``."""
    token = _RUN_CHECKSUM.set(checksum)
    try:
        yield
    finally:
        _RUN_CHECKSUM.reset(token)


def provenance_record(source: Path) -> str:
    """The provenance value a converter writes into the output of ``source``."""
    checksum = _RUN_CHECKSUM.get()
    if checksum is None:
        from portolan_cli.sync.checksums import compute_checksum, compute_dir_checksum

        # A FileGDB source is a directory.
        checksum = compute_dir_checksum if source.is_dir() else compute_checksum
    return json.dumps({"path": source.name, "sha256": checksum(source)}, sort_keys=True)


def with_provenance(table: Any, source: Path, *, require_geometry: bool = False) -> Any:
    """Return a geoparquet-io table whose footer will name ``source``.

    geoparquet-io 1.7.0 keeps the schema metadata keys of the table it writes
    (gpio#1280), so the key reaches the file with no second write.

    Args:
        table: The table to write.
        source: The file the table comes from.
        require_geometry: Add no record to a table with no geometry. ``add``
            deletes that output and tracks the source as a companion, so a
            record would only cost a hash of the source.
    """
    from geoparquet_io.api.table import Table  # type: ignore[import-untyped]

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


def _recorded_source(output: Path) -> str | None:
    """The source name that the provenance record of ``output`` holds, or None."""
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
    except Exception:  # noqa: BLE001 - an unreadable file names no source
        return None
    if raw is None:
        return None
    try:
        recorded = json.loads(raw)["path"]
    except (ValueError, KeyError, TypeError):
        return None
    return recorded if isinstance(recorded, str) else None


def _produced_by(output: Path, source: Path, state: RunState | None) -> bool | None:
    """Report whether ``output`` is a file that a conversion of ``source`` wrote.

    The provenance record decides. A file with no record is the source's when
    it is a byte copy of it, such as a COG that ``convert_raster`` copied to
    its ``.tif`` name.

    Returns:
        True or False when the file proves it, None when it carries no record.
    """
    recorded = _recorded_source(output)
    if recorded is not None:
        return recorded == source.name
    try:
        if output.stat().st_size != source.stat().st_size:
            return None
    except OSError:
        return None
    if state is not None:
        same = state.checksum(output) == state.checksum(source)
    else:
        from portolan_cli.sync.checksums import compute_checksum

        same = compute_checksum(output) == compute_checksum(source)
    return True if same else None


def _collection_dir_of(path: Path) -> Path | None:
    """The nearest directory above ``path`` that holds ``versions.json``.

    The search stops at the catalog root, whose ``versions.json`` is the
    catalog index and names no asset.
    """
    for directory in path.resolve().parents:
        if (directory / ".portolan").is_dir():
            return None
        if (directory / "versions.json").is_file():
            return directory
    return None


def _legacy_output_of(output: Path, source: Path, state: RunState | None) -> bool:
    """Report whether versions.json shows that an add of ``source`` wrote ``output``.

    Portolan 0.8.0 wrote no provenance record. Its versions.json tracks the
    output and the source side by side, and the output entry has no
    ``source_path``. Every output entry that an add writes now has one.
    """
    if output.parent != source.parent:
        return False
    collection_dir = _collection_dir_of(output)
    if collection_dir is None:
        return False
    try:
        record = (
            state.versions(collection_dir)
            if state is not None
            else read_versions(collection_dir / "versions.json")
        )
    except (ValueError, OSError):
        return False
    if not record.versions:
        return False
    assets = record.versions[-1].assets
    tracked = assets.get(versions_asset_key(output, collection_dir))
    return (
        tracked is not None
        and tracked.source_path is None
        and versions_asset_key(source, collection_dir) in assets
    )


def _footer_shape(path: Path) -> tuple[bool, frozenset[str]] | None:
    """Whether a Parquet file is GeoParquet, and its column names. None when unreadable."""
    import pyarrow.parquet as pq

    try:
        count("parquet.opens")
        metadata = pq.read_metadata(path)
    except Exception:  # noqa: BLE001 - an unreadable file has no shape
        return None
    has_geo = b"geo" in (metadata.metadata or {})
    return has_geo, frozenset(metadata.schema.to_arrow_schema().names)


def settle_output(
    source: Path,
    output: Path,
    written: Path,
    *,
    state: RunState | None = None,
    geometry_required: bool = False,
) -> Path:
    """Move the distinct output of a tabular source over the output 0.8.0 wrote.

    0.8.0 converted a CSV with geometry, and with ``tabular.convert`` any CSV,
    over the Parquet file of its stem. It tracked a CSV with no geometry
    beside a GeoParquet as a companion. The two pairs look the same in
    versions.json, so :func:`claim_output` sends a tabular source to the
    distinct name. This function then compares the file it wrote with the
    tracked file of the stem. The written file replaces it only when both are
    GeoParquet, or both are plain Parquet, and they have the same columns.
    Any other file stays, so a companion CSV that gains geometry beside a
    foreign GeoParquet keeps its distinct output.

    Args:
        source: The tabular source.
        output: The path the converter names, ``<stem>.parquet``.
        written: The path the conversion wrote.
        state: The run state, for one ``versions.json`` read per run.
        geometry_required: Replace only with a GeoParquet file.

    Returns:
        The path that holds the output now.
    """
    if written == output or source.suffix.lower() not in TABULAR_EXTENSIONS:
        return written
    if not output.is_file() or _recorded_source(output) is not None:
        return written
    if not _legacy_output_of(output, source, state):
        return written
    shape = _footer_shape(written)
    if shape is None or shape != _footer_shape(output) or (geometry_required and not shape[0]):
        return written
    written.replace(output)
    return output


def distinct_output_path(source: Path, output: Path) -> Path:
    """The output name ``claim_output`` falls back to: ``<stem>-<ext><suffix>``."""
    extension = source.suffix.lstrip(".").lower()
    return output.with_name(f"{output.stem}-{extension}{output.suffix}")


def claim_output(source: Path, output: Path, *, state: RunState | None = None) -> Path:
    """Pick the path a conversion of ``source`` writes, so it replaces no other file.

    A converter names its output after the source stem, so ``b1.csv`` and a
    GeoParquet ``b1.parquet`` claim one name. A file at that name is the
    source's to replace when its provenance record names the source, or when
    it has no record and versions.json shows that an add of the source wrote
    it (:func:`_legacy_output_of`). Any other file belongs to the operator. The
    output then takes the name ``<stem>-<source extension>``, such as
    ``b1-csv.parquet``, and the asset key follows the new stem.

    Args:
        source: The file to convert.
        output: The path the converter names.
        state: The run state, for one hash per file and run.

    Returns:
        ``output``, or the distinct name when ``output`` holds another file.

    Raises:
        OutputExistsError: Both names hold a file that ``source`` did not write.
    """
    if not output.exists() or output.resolve() == source.resolve():
        return output
    owned = _produced_by(output, source, state)
    if owned:
        return output
    distinct = distinct_output_path(source, output)
    if distinct.exists() and _produced_by(distinct, source, state):
        return distinct
    # A tabular source goes to the distinct name. settle_output decides after
    # the conversion, because only the output shows whether it has geometry.
    tabular = source.suffix.lower() in TABULAR_EXTENSIONS
    if owned is None and not tabular and _legacy_output_of(output, source, state):
        return output
    if not distinct.exists():
        return distinct
    raise OutputExistsError(source.name, output.name)


def _convert_to(
    handler: FormatHandler,
    source: Path,
    output_dir: Path,
    catalog_root: Path | None,
    target: Path,
) -> Path:
    """Convert ``source`` with its handler and store the output at ``target``.

    The handler writes the name it derives, so it converts into a scratch
    directory. Each file it writes moves to ``output_dir``, with the output
    stem changed to the stem of ``target``. A side file whose new name holds a
    file stays out, so the move replaces nothing but ``target``.
    """
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
                shutil.move(entry, output_dir / name)
    return target


def _convert_partitioned_by_index(
    handler: FormatHandler, source: Path, output_dir: Path, catalog_root: Path | None
) -> Path | None:
    """Convert a vector source into Hive partitions by the configured spatial index.

    Only ``check --fix`` does this, and only for the built-in GeoParquet
    handler. ``add`` keeps one file with the index column. The partitions go
    to ``<output_dir>/<stem>/``, so two sources in one directory cannot write
    to the same cell.

    Returns:
        The partition directory, or None when the settings ask for no
        partitions or the handler is not the built-in GeoParquet handler.

    Raises:
        InvalidOutputError: The partitions are not valid GeoParquet.
    """
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


# A transient interrupt clears after one attempt. If a file fails all three,
# the signal source is sustained. Find what delivers signals to the process
# during the run, such as subprocess reaping or a timeout. Do not raise this
# count.
_CONVERT_MAX_ATTEMPTS = 3


def write_raster_thumbnail(cog_path: Path, catalog_root: Path | None) -> None:
    """Write ``<stem>.thumb.jpg`` next to a COG when the COG settings ask for it.

    A thumbnail that exists stays, so a re-add keeps a hand-made thumbnail.
    Portolan cannot tell a hand-made thumbnail from one it wrote, so an old
    thumbnail also stays after the imagery changes. Delete ``<stem>.thumb.jpg``
    to make the next conversion write a new one. A thumbnail is optional, so a
    failure only warns.

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
    except Exception as e:  # noqa: BLE001 - the thumbnail is optional
        logger.warning("Thumbnail generation failed for %s: %s", cog_path.name, e)


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


_ConvertT = TypeVar("_ConvertT")


def run_with_transient_convert_retry(
    operation: Callable[[], _ConvertT],
    *,
    source_name: str,
) -> _ConvertT:
    """Run a DuckDB-backed conversion, retrying transient interrupts.

    ``geoparquet-io`` runs conversions through DuckDB, which can raise a
    transient ``InterruptException('Query interrupted')`` (see
    :func:`_is_transient_conversion_error`). ``operation`` is retried up to
    ``_CONVERT_MAX_ATTEMPTS`` times so a single interrupted query does not fail a
    whole bulk ``add`` (the interrupt flag clears once raised, and each attempt
    opens a fresh ``gpio.convert()`` connection); any other error propagates on
    the first occurrence.

    Shared by every conversion entry point the ``add`` pipeline uses
    (:func:`_convert_vector_layer`, ``preparation.convert_vector``, and
    ``preparation.convert_tabular``) so the
    retry is applied uniformly rather than on a single code path (Issue #339
    nightly ``test_add_1000_files_*`` flake).

    Args:
        operation: Zero-argument callable performing the conversion.
        source_name: Human-readable source name for retry log messages.

    Returns:
        Whatever ``operation`` returns.
    """
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
    """Make a shapefile's ``.cpg`` sidecar match what its DBF records contain.

    Legacy government shapefiles routinely ship Latin-1 attribute text with no
    ``.cpg`` (GDAL then passes raw bytes through and the GeoParquet writer's
    UTF-8 validation fails deep in the pipeline), and some ship a sidecar that
    LIES — UTF-8 records declared as ``1252``, which converts "successfully"
    into mojibake (issue #783; both defects observed in SITEX Extremadura
    data).

    The encoding test reads only the DBF *records region* (after the header
    length at bytes 8-9): the header is binary and would false-flag valid
    UTF-8 files. Rules, both safe by construction:

    - records are NOT valid UTF-8 and no/UTF-8 sidecar -> write ``ISO-8859-1``
      (the standard single-byte fallback; it always decodes);
    - records ARE valid UTF-8 with non-ASCII content but the sidecar declares
      something else -> correct it to ``UTF-8`` (non-ASCII UTF-8 is
      essentially unambiguous).

    Returns the sidecar path when one was written or corrected, else None.
    """
    import struct

    # The sidecar write changes the operator's files, so the notice is
    # user-facing. The output writer sends it to stderr in JSON mode (#1005).
    from portolan_cli.output import info as _output_info

    dbf, cpg = shp.with_suffix(".dbf"), shp.with_suffix(".cpg")
    if not dbf.exists():
        return None
    raw = dbf.read_bytes()
    if len(raw) <= 32:
        return None
    header_len = struct.unpack_from("<H", raw, 8)[0]
    records = raw[header_len:]
    try:
        records.decode("utf-8")
        records_are_utf8 = True
    except UnicodeDecodeError:
        records_are_utf8 = False
    has_non_ascii = any(b > 0x7F for b in records)
    declared = cpg.read_text(errors="replace").strip().upper() if cpg.exists() else None

    if records_are_utf8 and has_non_ascii and declared not in (None, "UTF-8", "UTF8"):
        cpg.write_text("UTF-8")
        _output_info(f"Corrected {cpg.name}: declared {declared}, DBF records are UTF-8")
        return cpg
    if not records_are_utf8 and (declared is None or "UTF" in (declared or "")):
        cpg.write_text("ISO-8859-1")
        what = f"corrected from {declared}" if declared else "written"
        _output_info(f"Encoding sidecar {cpg.name} {what}: DBF records are not valid UTF-8")
        return cpg
    return None


def has_geometry(table: Any) -> bool:
    """Report whether a gpio Table carries a geometry column.

    ``gpio.convert`` reads a geometry-less source, a CSV of records for
    example, as a plain table with ``geometry_column`` set to None. Every
    spatial operation below needs a geometry column, and DuckDB raises
    ``Binder Error: Column "geometry" referenced ...`` without one. A Table
    from an older geoparquet-io may not expose the attribute at all, so a
    missing attribute counts as "has geometry" and keeps the previous
    behavior.

    Args:
        table: geoparquet-io Table instance.

    Returns:
        False only when the Table reports no geometry column.
    """
    return getattr(table, "geometry_column", "geometry") is not None


def apply_vector_settings(
    table: Any, settings: VectorSettings, *, partitioned: bool = False
) -> Any:
    """Apply spatial optimization settings to a gpio Table.

    A table with no geometry column is returned untouched. Sorting and the
    bbox covering column are spatial operations, and the Portolan GeoParquet
    profile asks for them only of spatial tables (issue #805).

    Args:
        table: geoparquet-io Table instance.
        settings: Vector conversion settings.
        partitioned: True when the caller writes the table with
            ``_write_partitioned``. The partition call then adds the spatial
            index column itself, at the size it partitions by.

    Returns:
        Modified Table with optimizations applied.
    """
    if not has_geometry(table):
        return table

    # Add bbox column if requested
    if settings.add_bbox:
        table = table.add_bbox()

    # Add spatial index column if specified. geoparquet-io partitions on an
    # existing column at that column's resolution, so a column added here
    # would bypass the `auto` sizing of a partitioned write.
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
    """Resolve resolution value for a spatial index type.

    Args:
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
    if index_type == "s2":
        return table.add_s2() if resolution is None else table.add_s2(level=resolution)
    if index_type == "quadkey":
        return (
            table.add_quadkey() if resolution is None else table.add_quadkey(resolution=resolution)
        )
    if index_type == "a5":
        return table.add_a5() if resolution is None else table.add_a5(resolution=resolution)
    if index_type == "kdtree":
        # geoparquet-io 1.5 requires a size or `auto` for kdtree (geoparquet-io#855).
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

# Quadkey partitions group cells by a prefix of the quadkey column. geoparquet-io
# 1.5 made the prefix length required next to an explicit resolution
# (geoparquet-io#800). 6 is the length that geoparquet-io 1.4 used by default.
_QUADKEY_PARTITION_RESOLUTION = 6


def _write_partitioned(table: Any, output_dir: Path, settings: VectorSettings) -> None:
    """Write table as hive-partitioned output.

    ``resolution: auto`` passes ``auto=True``, which sizes the cells from the
    row count. geoparquet-io 1.5 refuses a partition call that names neither a
    size nor ``auto`` (geoparquet-io#800, #855).

    Args:
        table: geoparquet-io Table instance with spatial index column.
        output_dir: Output directory for partitioned files.
        settings: Vector settings with partition strategy.
    """
    index_type = settings.spatial_index
    size_kwarg = _PARTITION_SIZE_KWARG.get(index_type)
    if size_kwarg is None:
        return

    resolution = _resolve_resolution(settings.resolution)
    sizing: dict[str, Any] = {"auto": True} if resolution is None else {size_kwarg: resolution}
    if index_type == "quadkey" and resolution is not None:
        sizing["partition_resolution"] = min(resolution, _QUADKEY_PARTITION_RESOLUTION)

    # geoparquet-io 1.4.0 changed the Python API default for `hive` from True to
    # False (gpio#661). Portolan needs the Hive layout: each partition directory
    # becomes a STAC Item, and `partitioning.build_glob_pattern` matches
    # `<scheme>_cell=*`. Pass both flags at every call so a later default change
    # cannot alter the layout again. `keep_*_column` keeps partition pruning
    # available to a reader, which `partitioning.partition_file` already does.
    partition = getattr(table, f"partition_by_{index_type}")
    partition(str(output_dir), hive=True, **{f"keep_{index_type}_column": True}, **sizing)


#: The GTiff creation option that carries ``conversion.cog.quality`` for each
#: codec in ``QUALITY_COMPRESSIONS``.
_GTIFF_QUALITY_OPTION = {"JPEG": "jpeg_quality", "WEBP": "webp_level"}

#: Up to this many pixels (4096 x 4096), write_cog embeds exact band statistics.
#: Above it, GDAL computes them from a sample of the blocks, so a large source
#: costs no second full read. The band then carries STATISTICS_APPROXIMATE=YES.
EXACT_STATISTICS_PIXELS = 4096 * 4096


def write_cog(source: Path, output_dir: Path, settings: CogSettings | None = None) -> Path:
    """Write a raster file as a COG with the given settings.

    ``preparation.convert_raster`` decides when to call this. It keeps a
    source that is already a COG unless the operator asks to re-optimize.

    Uses COG settings from config if provided, otherwise the built-in defaults:
    - DEFLATE compression
    - 512x512 tiles
    - Predictor and overview resampling derived from the source raster's dtype
      (see derive_cog_defaults)

    Args:
        source: Source raster file.
        output_dir: Directory for output file.
        settings: COG conversion settings. If None, uses the built-in defaults.

    Returns:
        Path to the output COG file.

    Note:
        If output_dir is the same directory as source, the original file
        will be replaced with the COG. This is by design for raster conversion.
    """
    import tempfile

    import rasterio
    from rio_cogeo.cogeo import cog_translate
    from rio_cogeo.profiles import cog_profiles

    # Use defaults if no settings provided
    if settings is None:
        settings = CogSettings()

    # Fill in any "auto" field from the raster itself (Issue #690)
    settings = resolve_cog_settings(settings, source)

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
        profile["compress"] = GDAL_COMPRESS_NAMES.get(settings.compression, settings.compression)

    # Apply tile size settings
    profile["blockxsize"] = settings.tile_size
    profile["blockysize"] = settings.tile_size

    # GDAL ignores PREDICTOR for any other codec and logs a warning per file.
    if settings.compression in PREDICTOR_COMPRESSIONS:
        profile["predictor"] = settings.predictor

    # The GTiff driver has no QUALITY option. Each lossy codec takes its own.
    if settings.quality is not None and settings.compression in QUALITY_COMPRESSIONS:
        profile[_GTIFF_QUALITY_OPTION[settings.compression]] = settings.quality

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

        # The COG carries its band statistics, so a reader with PAM off sees
        # them (issue #748, PTL-DAT-009 and PTL-DAT-010). GDAL computes them on
        # the open source and keeps them as band tags, and rio-cogeo forwards
        # the band tags into the one write. PAM is off, so no .aux.xml appears.
        with rasterio.Env(GDAL_PAM_ENABLED="NO"), rasterio.open(source) as src:
            # Exact up to EXACT_STATISTICS_PIXELS. Above it, GDAL samples a
            # subset of the blocks and tags the result STATISTICS_APPROXIMATE.
            src.stats(approx=src.width * src.height > EXACT_STATISTICS_PIXELS)
            cog_translate(
                src,
                str(temp_path),
                profile,
                quiet=True,
                overview_resampling=cast("ResamplingMethod", settings.resampling),
                forward_band_tags=True,
                # A COG written in place is its own source, so it needs no record.
                additional_cog_metadata=(
                    {}
                    if output_path.resolve() == source.resolve()
                    else {COG_PROVENANCE_TAG: provenance_record(source)}
                ),
            )

        # Atomic replace
        temp_path.replace(output_path)
    except Exception:
        # Clean up temp file on error
        if temp_path.exists():
            temp_path.unlink()
        raise

    return output_path


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
    state: RunState | None = None,
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
            If None, uses the built-in defaults for COG conversion.
        workers: Number of parallel worker processes. ``None`` or ``1`` runs
            serially (the default, so existing callers are unchanged). A value
            > 1 uses a ``ProcessPoolExecutor`` for CPU-bound raster re-encoding
            (issue #530). Callers wanting parallel-by-default resolve a concrete
            count (e.g. ``os.cpu_count()``) before calling.
        force: Re-optimize already-cloud-native rasters (passed through to
            :func:`convert_file`). Raster-scoped; valid vectors stay skipped.
        state: The run state, which parses the config once per command.

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
    cog_settings = get_cog_settings(catalog_path, state=state) if catalog_path else None
    vector_settings = get_vector_settings(catalog_path, state=state) if catalog_path else None

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
    """Convert files one at a time in the current process.

    The default path for every existing caller (``workers <= 1`` or a single
    file), and the fallback used by :func:`_convert_files_parallel` when a
    process pool cannot start. Outputs land side-by-side with each source unless
    ``output_dir`` overrides it, matching the parallel path exactly.
    """
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


#: The run state of a conversion worker process. ``_init_worker_state`` sets it.
_WORKER_STATE: RunState | None = None


def _init_worker_state(catalog_path: Path | None) -> None:
    """Give this worker process one run state, so it parses the config once."""
    global _WORKER_STATE
    from portolan_cli.run_state import RunState

    _WORKER_STATE = RunState.open(catalog_path) if catalog_path is not None else None


def _convert_in_worker(source: Path, **kwargs: Any) -> ConversionResult:
    """Run :func:`convert_file` with the run state of this worker process."""
    return convert_file(source, state=_WORKER_STATE, **kwargs)


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
    """Convert files concurrently with a ProcessPoolExecutor (issue #530).

    Conversion is CPU/GDAL-bound, so true parallelism needs separate processes,
    not threads. The ``on_progress`` callback is invoked in the PARENT process as
    each future completes (it is never pickled into a worker), preserving the
    streaming-progress contract. A worker that dies or raises is
    captured into a FAILED result so one bad file does not abort the batch.

    If the pool itself cannot run (``BrokenProcessPool`` -- e.g. a restricted
    container with no ``/dev/shm``, a seccomp-filtered ``clone``, or a nested
    multiprocessing context such as mutmut's mutation runner), the remaining
    files are converted serially in this process instead of all being reported
    FAILED.

    Each worker process opens one run state for ``catalog_path``, so it parses
    the config once. A run state cannot cross a process boundary. The serial
    fallback uses ``state``.
    """
    import multiprocessing
    from concurrent.futures import ProcessPoolExecutor, as_completed
    from concurrent.futures.process import BrokenProcessPool

    # Use a "spawn" context, not fork. GDAL/rasterio run worker threads, and
    # fork()-ing a multi-threaded process can deadlock the child (and aborts on
    # macOS). Spawn re-imports this module cleanly in each worker.
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
            initargs=(catalog_path,),
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
                    result = future.result()
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
        # A ProcessPoolExecutor can fail to start or keep workers in restricted
        # environments (no /dev/shm, seccomp-filtered clone, or a nested
        # multiprocessing context such as mutmut's mutation runner). Rather than
        # report every file FAILED, finish the batch serially in this process.
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
    *,
    state: RunState | None = None,
) -> list[LayerConversionResult]:
    """Convert all layers in a multi-layer file to separate GeoParquet files.

    For GeoPackage and FileGDB files that contain multiple layers, this function
    converts each layer to a separate GeoParquet file named:
        {source_stem}_{layer_name}.parquet

    A layer output never replaces a file that ``source`` did not write. It
    takes a distinct name instead (see :func:`claim_output`).

    Args:
        source: Path to the multi-layer file (GeoPackage or FileGDB).
        output_dir: Directory for output files.
        settings: Vector conversion settings. If None, uses defaults (no optimization).
        state: The run state, for one hash per file and run.

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
            output_path = claim_output(source, output_path, state=state)
        except OutputExistsError as err:
            results.append(LayerConversionResult(source, layer_name, None, False, str(err)))
            continue

        try:
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
    import geoparquet_io as gpio  # type: ignore[import-untyped]

    if settings is None:
        settings = VectorSettings()

    resolved = settings

    def _run() -> None:
        table = gpio.convert(source, layer=layer)
        table = apply_vector_settings(table, resolved)
        with_provenance(table, source).write(output)

    run_with_transient_convert_retry(_run, source_name=f"{source.name}:{layer}")
