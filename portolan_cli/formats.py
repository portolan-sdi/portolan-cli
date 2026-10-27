"""Format detection for routing to appropriate conversion library.

This module provides minimal format detection to route inputs to either
geoparquet-io (vector) or rio-cogeo (raster). Actual
validation and conversion are delegated to these upstream libraries.

Additionally, it provides cloud-native status classification (see issue #10):
- CLOUD_NATIVE: Already cloud-optimized (GeoParquet, COG, FlatGeobuf, etc.)
- CONVERTIBLE: Can be converted (Shapefile, GeoJSON, GeoPackage, etc.)
- UNSUPPORTED: Not yet supported (NetCDF, HDF5, non-COPC LAS/LAZ)
"""

from __future__ import annotations

import contextlib
import json
import logging
from contextvars import ContextVar
from dataclasses import dataclass, replace
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

from portolan_cli import extension_registry as _reg

# The extension-only detection lives in a leaf module that loads no third-party
# package. The read modules import it directly (issue #944). This module
# re-exports it, so `portolan_cli.formats.detect_format` keeps working.
from portolan_cli.format_families import (
    FORMAT_DISPLAY_NAMES as FORMAT_DISPLAY_NAMES,
)
from portolan_cli.format_families import (
    RASTER as RASTER,
)
from portolan_cli.format_families import (
    RASTER_EXTENSIONS as RASTER_EXTENSIONS,
)
from portolan_cli.format_families import (
    VECTOR as VECTOR,
)
from portolan_cli.format_families import (
    VECTOR_EXTENSIONS as VECTOR_EXTENSIONS,
)
from portolan_cli.format_families import (
    detect_family,
    is_geojson,
)
from portolan_cli.trace import count

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    import pyarrow.parquet as pq

    from portolan_cli.config import ConfigSource
    from portolan_cli.conversion_config import ConversionOverrides
    from portolan_cli.plugins.formats import FormatHandler

logger = logging.getLogger(__name__)

_T = TypeVar("_T")


class RunMemo(Protocol):
    """The memo and checksum methods of ``run_state.RunState``.

    This module does not import ``run_state``. The ``viz`` modules import
    this module, and ``run_state`` reaches ``sync``, which ``viz`` must not
    reach.
    """

    def memo(self, kind: str, path: Path, loader: Callable[[Path], _T]) -> _T:
        """Load a value derived from one file once per run."""
        ...

    def checksum(self, path: Path) -> str:
        """The SHA-256 of one file, hashed once per run."""
        ...

    def invalidate(self, path: Path) -> None:
        """Drop values derived from a file after a writer replaces it."""
        ...


#: The run state of the command that runs. A format handler and a ``viz``
#: function take no run state, so ``add`` and ``prepare_item`` set this. A
#: footer read, the provenance record, and the COG reads then go through the
#: run memo. The memo key holds the file's stat, so a rewrite reads again.
_RUN: ContextVar[RunMemo | None] = ContextVar("portolan_run", default=None)


@contextlib.contextmanager
def in_run(state: RunMemo) -> Iterator[None]:
    """Make the reads and the converters below go through the memo of ``state``."""
    token = _RUN.set(state)
    try:
        yield
    finally:
        _RUN.reset(token)


def run_scope(state: RunMemo | None) -> contextlib.AbstractContextManager[None]:
    """:func:`in_run` for ``state``, or a scope that does nothing when it is None."""
    return in_run(state) if state is not None else contextlib.nullcontext()


def current_run() -> RunMemo | None:
    """The run state that :func:`in_run` set, or None."""
    return _RUN.get()


def invalidate(path: Path) -> None:
    """Drop the current run's cached reads after writing ``path``.

    A filesystem can reuse a stat key, so writers cannot rely on stat changes
    alone to invalidate a footer or checksum.
    """
    state = _RUN.get()
    if state is not None:
        state.invalidate(path)


def remember(kind: str, path: Path, value: Any) -> None:
    """Put a value read from ``path`` in the memo of the current run, if one runs.

    A writer that read its output before it moved the output into place gives
    the value here. The next ``state.memo(kind, path, ...)`` returns it while
    the file stays as it is.
    """
    state = _RUN.get()
    if state is not None:
        state.memo(kind, path, lambda _path: value)


def parquet_footer(path: Path) -> pq.ParquetFile:
    """Read the footer of a Parquet file, once per run.

    The file closes before this returns. The closed file still answers
    ``metadata`` and ``schema_arrow``, which is all a footer reader needs.
    Inside :func:`in_run`, the run memo keeps the footer while the file
    stays as it is, so each footer reader of one file shares one read.

    Raises:
        Exception: The error of pyarrow for a file that is not Parquet.
    """
    state = _RUN.get()
    if state is not None and path.is_file():
        return state.memo("parquet.footer", path, _read_footer)
    return _read_footer(path)


def _read_footer(path: Path) -> pq.ParquetFile:
    import pyarrow.parquet as pq

    count("parquet.opens")
    footer = pq.ParquetFile(path)
    footer.close()
    return footer


# =============================================================================
# Cloud-Native Status Classification (Issue #10)
# =============================================================================


class CloudNativeStatus(Enum):
    """Classification of file format for cloud-native data handling.

    Used to determine how to handle a file during add():
    - CLOUD_NATIVE: Accept silently, no conversion needed
    - CONVERTIBLE: Warn then convert to cloud-native format
    - UNSUPPORTED: Reject with helpful error message
    """

    CLOUD_NATIVE = "cloud_native"
    CONVERTIBLE = "convertible"
    UNSUPPORTED = "unsupported"


@dataclass(frozen=True)
class FormatInfo:
    """Information about a detected file format.

    Provides metadata needed for user-facing messages during add.

    Attributes:
        status: Cloud-native classification status.
        display_name: Human-readable format name (e.g., "SHP", "GeoJSON").
        target_format: Target cloud-native format if convertible (e.g., "GeoParquet").
        error_message: Error message if unsupported.
    """

    status: CloudNativeStatus
    display_name: str
    target_format: str | None
    error_message: str | None


# =============================================================================
# Cloud-Native Format Constants
# =============================================================================

# The extension vocabulary below is DERIVED from portolan_cli.extension_registry
# (the single source). Edit rows there, not these frozensets.

# Cloud-native formats that pass through without conversion. Statically
# cloud-native single-suffix files only: .parquet/.tif are content-inspected
# ("inspect"), and .zarr/.copc.laz are cloud-native but handled by the
# directory / compound-extension branches in get_cloud_native_status().
CLOUD_NATIVE_EXTENSIONS: frozenset[str] = _reg.cloud_native_extensions()

# Convertible vector formats (.gdb is a FileGDB directory, handled specially).
CONVERTIBLE_VECTOR_EXTENSIONS: frozenset[str] = _reg.convertible_extensions("GeoParquet")

# Convertible raster formats.
CONVERTIBLE_RASTER_EXTENSIONS: frozenset[str] = _reg.convertible_extensions("COG")

# Explicitly unsupported formats.
UNSUPPORTED_EXTENSIONS: frozenset[str] = _reg.unsupported_extensions()

# Display names and unsupported error messages.
UNSUPPORTED_ERROR_MESSAGES: dict[str, str] = _reg.field_map("unsupported_message")


# =============================================================================
# Cloud-Native Detection Functions
# =============================================================================


def is_valid_parquet(path: Path) -> bool:
    """Check if a file is a valid Parquet file (can be read by PyArrow).

    Args:
        path: Path to the file to check.

    Returns:
        True if the file is a valid Parquet file, False if corrupted or not Parquet.
    """
    try:
        import pyarrow.parquet as pq
    except ImportError:
        logger.warning(
            "pyarrow not installed; cannot validate Parquet. Install with: pip install pyarrow"
        )
        return False

    try:
        # Just read metadata - fast and validates the file structure
        count("parquet.opens")
        pq.read_metadata(str(path))
        return True
    except Exception:
        # File is corrupted or not a valid Parquet file
        return False


def read_geo_metadata(source: Path | pq.ParquetFile) -> dict[str, Any] | None:
    """Read and parse the ``geo`` footer key of a GeoParquet file.

    Args:
        source: Path to a Parquet file, or a file the caller already opened.
            A path goes through :func:`parquet_footer`, so the readers of one
            file in one run share one footer read.

    Returns:
        The parsed GeoParquet metadata object. None for a plain Parquet file,
        for a ``geo`` value that is not a JSON object, and for an unreadable
        file.
    """
    try:
        footer = parquet_footer(source) if isinstance(source, Path) else source
        geo = json.loads((footer.metadata.metadata or {}).get(b"geo", b"null"))
    except Exception:  # noqa: BLE001 - an unreadable file is not a GeoParquet
        logger.debug("Failed to read GeoParquet metadata from %s", source, exc_info=True)
        return None
    return geo if isinstance(geo, dict) else None


def is_geoparquet(path: Path) -> bool:
    """Check if a Parquet file has GeoParquet metadata.

    The one GeoParquet test. It reads the raw footer through
    :func:`read_geo_metadata`, so a ``geo`` key that sits after ``ARROW:schema``
    counts (issue #864).

    Args:
        path: Path to the Parquet file.

    Returns:
        True if the file is a valid GeoParquet, False otherwise.
        Note: Returns False for both non-geo Parquet AND invalid/corrupted files.
        Use is_valid_parquet() first if you need to distinguish these cases.
    """
    return read_geo_metadata(path) is not None


def is_cloud_optimized_geotiff(path: Path, *, quiet: bool = True) -> bool:
    """Check if a TIFF file is a Cloud-Optimized GeoTIFF.

    Uses rio-cogeo's validation to determine if the file meets COG requirements.
    This delegates to upstream library rather than reimplementing.

    Args:
        path: Path to the TIFF file.
        quiet: If True, suppress rio-cogeo's warning output to stdout.
            Default is True to avoid polluting JSON output.

    Returns:
        True if the file is a valid COG, False otherwise.
    """
    try:
        from rio_cogeo.cogeo import cog_validate
    except ImportError:
        logger.warning(
            "rio-cogeo not installed; cannot validate COG. Install with: pip install rio-cogeo"
        )
        return False

    try:
        # Use quiet=True to suppress rio-cogeo's warning output to stdout
        is_valid, _errors, _warnings = cog_validate(str(path), quiet=quiet)
        return is_valid
    except Exception:
        logger.exception("Failed to validate COG for %s", path)
        return False


def _status_by_content(path: Path, extension: str) -> FormatInfo | None:
    """Classify a file whose extension alone does not give the status.

    A .parquet file may or may not carry geo metadata. A .tif file may or may
    not be a COG. A .json file may or may not be GeoJSON. Each case reads the
    file to decide.

    Args:
        path: Path to the file to classify.
        extension: Lowercase file extension, with the leading dot.

    Returns:
        FormatInfo for a content-inspected extension, or None for any other
        extension.
    """
    if extension == ".parquet":
        if not is_valid_parquet(path):
            return FormatInfo(
                status=CloudNativeStatus.UNSUPPORTED,
                display_name="Corrupted Parquet",
                target_format=None,
                error_message="File has .parquet extension but is not a valid Parquet file",
            )
        if is_geoparquet(path):
            return FormatInfo(
                status=CloudNativeStatus.CLOUD_NATIVE,
                display_name="GeoParquet",
                target_format=None,
                error_message=None,
            )
        # Plain Parquet is cloud-native too. It is not geo-aware.
        return FormatInfo(
            status=CloudNativeStatus.CLOUD_NATIVE,
            display_name="Parquet",
            target_format=None,
            error_message=None,
        )

    if extension in (".tif", ".tiff"):
        if is_cloud_optimized_geotiff(path):
            return FormatInfo(
                status=CloudNativeStatus.CLOUD_NATIVE,
                display_name="COG",
                target_format=None,
                error_message=None,
            )
        # A TIFF that is not a COG converts to one.
        return FormatInfo(
            status=CloudNativeStatus.CONVERTIBLE,
            display_name="TIFF",
            target_format="COG",
            error_message=None,
        )

    if extension == ".json":
        if is_geojson(path):
            return FormatInfo(
                status=CloudNativeStatus.CONVERTIBLE,
                display_name="GeoJSON",
                target_format="GeoParquet",
                error_message=None,
            )
        return FormatInfo(
            status=CloudNativeStatus.UNSUPPORTED,
            display_name="JSON",
            target_format=None,
            error_message=(
                "JSON file does not appear to be GeoJSON. "
                "Rename to .geojson if it contains geospatial data, "
                "or use a supported format."
            ),
        )

    return None


def detect_format(
    path: Path, catalog_root: Path | None = None, *, state: ConfigSource | None = None
) -> str | None:
    """Return the format family of a file, asking the plugin format handlers first.

    Args:
        path: Path to the file or directory to detect.
        catalog_root: Catalog root, for a ``conversion.handlers`` pin.
        state: The run state, which parses the config once per run.

    Returns:
        The family of the handler that claims the file, for example
        ``vector`` or ``raster``. None when no handler claims it.

    Raises:
        FileNotFoundError: If the file does not exist.
        IsADirectoryError: If the path is a non-geospatial directory.
    """
    from portolan_cli.plugins.formats import extension_of, handler_for, plugin_extensions

    if extension_of(path) in plugin_extensions():
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        handler = handler_for(path, catalog_root, state=state)
        if handler is not None:
            return handler.family
    return detect_family(path)


def _plugin_handler(
    path: Path, catalog_root: Path | None = None, state: ConfigSource | None = None
) -> tuple[FormatHandler | None, bool]:
    """Return the plugin handler for ``path``, and whether only plugins register its extension.

    The lookup runs for every extension that a plugin registers, so an
    ambiguous claim or a pin to a missing handler raises here, before
    ``check --fix`` converts a file. A ``conversion.handlers`` pin that selects
    a built-in hides the plugin.

    Raises:
        AmbiguousFormatError: Two plugin handlers claim the file and no pin decides.
        UnknownFormatHandlerError: A pin names a handler that does not exist.
    """
    from portolan_cli.plugins.formats import (
        builtin_extensions,
        extension_of,
        plugin_extensions,
        plugin_handler_for,
    )

    extension = extension_of(path)
    if extension not in plugin_extensions():
        return None, False
    handler = plugin_handler_for(path, catalog_root, state=state)
    return handler, extension not in builtin_extensions()


def get_cloud_native_status(
    path: Path, catalog_root: Path | None = None, *, state: ConfigSource | None = None
) -> FormatInfo:
    """Determine cloud-native status and format info for a file.

    Classifies a file as CLOUD_NATIVE, CONVERTIBLE, or UNSUPPORTED based on
    its format. Uses content inspection for ambiguous cases (e.g., .tif files
    that may or may not be COGs, .parquet files that may or may not have geo metadata).

    Args:
        path: Path to the file to classify.
        catalog_root: Catalog root, for a ``conversion.handlers`` pin.
        state: The run state, which parses the config once per run.

    Returns:
        FormatInfo with status, display name, target format, and error message.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    info = _default_status(path, catalog_root, state)
    if catalog_root is None:
        return info
    # The catalog's conversion.extensions and conversion.paths lists win over
    # the default status, so convert and check agree on a file (#1020).
    from portolan_cli.conversion_config import get_conversion_overrides

    overrides = get_conversion_overrides(catalog_root, state=state)
    return _apply_overrides(path, info, overrides, catalog_root)


def _default_status(
    path: Path, catalog_root: Path | None, state: ConfigSource | None = None
) -> FormatInfo:
    """The status of a file by format alone. See ``get_cloud_native_status``."""
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    # Handle Zarr directories (special case: directory with .zarr extension)
    if path.is_dir():
        if path.suffix.lower() == ".zarr":
            return FormatInfo(
                status=CloudNativeStatus.CLOUD_NATIVE,
                display_name="Zarr",
                target_format=None,
                error_message=None,
            )
        raise IsADirectoryError(f"Path is a directory: {path}")

    # A plugin format handler converts the formats it claims (#947). For an
    # extension that a built-in handler also registers, the built-in checks
    # decide the status, so a valid COG stays CLOUD_NATIVE when a plugin
    # claims .tif. The plugin still names the target format.
    handler, plugin_only = _plugin_handler(path, catalog_root, state)
    if handler is not None and plugin_only:
        return FormatInfo(
            status=CloudNativeStatus.CONVERTIBLE,
            display_name=handler.display_name,
            target_format=handler.display_name,
            error_message=None,
        )
    info = _builtin_status(path)
    if handler is not None and info.status == CloudNativeStatus.CONVERTIBLE:
        return replace(info, target_format=handler.display_name)
    return info


def _builtin_status(path: Path) -> FormatInfo:
    """Classify a file by the built-in format rules. See ``get_cloud_native_status``."""
    # Get the file extension (lowercase)
    extension = path.suffix.lower()

    # Handle compound extensions like .copc.laz
    if path.name.lower().endswith(".copc.laz"):
        return FormatInfo(
            status=CloudNativeStatus.CLOUD_NATIVE,
            display_name="COPC",
            target_format=None,
            error_message=None,
        )

    # Check for cloud-native formats (extension-based)
    if extension in CLOUD_NATIVE_EXTENSIONS:
        display_name = FORMAT_DISPLAY_NAMES.get(extension, extension.upper().lstrip("."))
        return FormatInfo(
            status=CloudNativeStatus.CLOUD_NATIVE,
            display_name=display_name,
            target_format=None,
            error_message=None,
        )

    content_info = _status_by_content(path, extension)
    if content_info is not None:
        return content_info

    # Check for unsupported formats (before convertible to catch .laz)
    if extension in UNSUPPORTED_EXTENSIONS:
        display_name = FORMAT_DISPLAY_NAMES.get(extension, extension.upper().lstrip("."))
        error_message = UNSUPPORTED_ERROR_MESSAGES.get(
            extension,
            f"{display_name} is not yet supported. Support coming soon.",
        )
        return FormatInfo(
            status=CloudNativeStatus.UNSUPPORTED,
            display_name=display_name,
            target_format=None,
            error_message=error_message,
        )

    # Check for convertible vector formats
    if extension in CONVERTIBLE_VECTOR_EXTENSIONS:
        display_name = FORMAT_DISPLAY_NAMES.get(extension, extension.upper().lstrip("."))
        return FormatInfo(
            status=CloudNativeStatus.CONVERTIBLE,
            display_name=display_name,
            target_format="GeoParquet",
            error_message=None,
        )

    # Check for convertible raster formats
    if extension in CONVERTIBLE_RASTER_EXTENSIONS:
        display_name = FORMAT_DISPLAY_NAMES.get(extension, extension.upper().lstrip("."))
        return FormatInfo(
            status=CloudNativeStatus.CONVERTIBLE,
            display_name=display_name,
            target_format="COG",
            error_message=None,
        )

    # Unknown format - treat as unsupported
    if extension:
        error_msg = f"Unknown format '{extension}' is not supported."
    else:
        error_msg = "File has no extension and format could not be detected."
    return FormatInfo(
        status=CloudNativeStatus.UNSUPPORTED,
        display_name=extension.upper().lstrip(".") if extension else "Unknown",
        target_format=None,
        error_message=error_msg,
    )


# =============================================================================
# Effective Status with Conversion Overrides
# =============================================================================


def get_effective_status(
    path: Path,
    overrides: ConversionOverrides | None = None,
    *,
    root: Path | None = None,
    state: ConfigSource | None = None,
) -> FormatInfo:
    """Get cloud-native status with conversion overrides applied.

    This function wraps get_cloud_native_status() and applies user-configured
    overrides to modify the classification:
    - Force-convert: Cloud-native formats can be marked as CONVERTIBLE
    - Preserve: Convertible formats can be marked as CLOUD_NATIVE

    Path-based overrides take precedence over extension-based overrides.

    Args:
        path: Path to the file to classify.
        overrides: Optional ConversionOverrides with user configuration.
            If None, returns the same result as get_cloud_native_status().
        root: Catalog root for resolving relative paths in glob patterns
            and for a ``conversion.handlers`` pin. Required if using
            path-based overrides.
        state: The run state, which parses the config once per run.

    Returns:
        FormatInfo with potentially modified status based on overrides.

    Raises:
        FileNotFoundError: If the file does not exist.

    Example:
        >>> from portolan_cli.conversion_config import ConversionOverrides
        >>> overrides = ConversionOverrides(extensions_convert=frozenset({".fgb"}))
        >>> result = get_effective_status(Path("data.fgb"), overrides=overrides)
        >>> result.status  # CONVERTIBLE instead of CLOUD_NATIVE
    """
    info = _default_status(path, root, state)
    if overrides is None:
        return info
    return _apply_overrides(path, info, overrides, root)


def _apply_overrides(
    path: Path, info: FormatInfo, overrides: ConversionOverrides, root: Path | None
) -> FormatInfo:
    """Apply the preserve and force-convert lists to a default status."""
    # UNSUPPORTED formats cannot be overridden - they're genuinely unsupported
    if info.status == CloudNativeStatus.UNSUPPORTED:
        return info

    # Check path-based preserve first (highest precedence)
    if overrides.paths_preserve and root is not None and overrides.should_preserve(path, root=root):
        # Override to CLOUD_NATIVE (preserve)
        return FormatInfo(
            status=CloudNativeStatus.CLOUD_NATIVE,
            display_name=info.display_name,
            target_format=None,
            error_message=None,
        )

    # Check extension-based preserve
    if overrides.should_preserve(path):
        return FormatInfo(
            status=CloudNativeStatus.CLOUD_NATIVE,
            display_name=info.display_name,
            target_format=None,
            error_message=None,
        )

    # Check extension-based force-convert (only for CLOUD_NATIVE)
    if info.status == CloudNativeStatus.CLOUD_NATIVE and overrides.should_force_convert(path):
        return FormatInfo(
            status=CloudNativeStatus.CONVERTIBLE,
            display_name=info.display_name,
            target_format="GeoParquet",  # All force-converts go to GeoParquet
            error_message=None,
        )

    return info


# =============================================================================
# Format Type Detection (existing functionality)
# =============================================================================


# =============================================================================
# Multi-Layer Format Support (Issue #265)
# =============================================================================

# Formats that can contain multiple layers (derived from the registry).
MULTILAYER_EXTENSIONS: frozenset[str] = _reg.extensions_where(is_multilayer=True)


def list_layers(path: Path) -> list[str] | None:
    """List all layers in a multi-layer geospatial file.

    For multi-layer formats (GeoPackage, FileGDB), returns all layer names.
    For single-layer formats (GeoJSON, Shapefile), returns None.

    Delegates to geoparquet-io which uses DuckDB's spatial extension internally.
    No external GDAL installation required.

    Args:
        path: Path to the geospatial file.

    Returns:
        List of layer names for multi-layer formats, None for single-layer formats.

    Raises:
        FileNotFoundError: If the file does not exist.

    Example:
        >>> list_layers(Path("multi.gpkg"))
        ['points', 'lines', 'polygons']
        >>> list_layers(Path("single.geojson"))
        None
    """
    import geoparquet_io as gpio  # type: ignore[import-untyped]

    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    extension = path.suffix.lower()

    # Single-layer formats return None
    # Also check for directory-based formats
    if extension not in MULTILAYER_EXTENSIONS and not (path.is_dir() and extension == ".gdb"):
        return None

    # A GeoPackage declares its own layers: gpkg_contents is spec-mandated and
    # names each table's data_type. Reading it directly returns only feature
    # layers, so non-spatial tables (QGIS ``layer_styles``, attribute-only
    # tables) never become collection assets (#784) — and it keeps working on
    # files where the delegated listing comes back empty.
    if extension == ".gpkg":
        feature_layers = _gpkg_feature_layers(path)
        if feature_layers is not None:
            return feature_layers or None

    # Delegate to geoparquet-io for both GeoPackage and FileGDB
    try:
        layers = gpio.list_layers(str(path.resolve()))
        if not layers:
            return None
        # Filter out empty strings and internal tables (Workspace, feature datasets)
        # Only return actual feature layers
        feature_layers = [
            layer
            for layer in layers
            if layer
            and layer not in ("", "Workspace")
            and not layer.startswith("fd")  # feature dataset containers
            or "_" in layer  # but keep fd1_lyr1 style names
        ]
        return feature_layers or None
    except Exception as e:
        logger.error("Failed to list layers in %s: %s", path, e)
        return None


def _gpkg_feature_layers(path: Path) -> list[str] | None:
    """Feature layers of a GeoPackage, straight from ``gpkg_contents``.

    Returns None when the file cannot be read as a GeoPackage (caller falls
    back to the delegated listing); an empty list means the file genuinely
    has no feature layers.
    """
    import sqlite3

    try:
        con = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
        try:
            rows = con.execute(
                "SELECT table_name FROM gpkg_contents WHERE data_type = 'features'"
                " ORDER BY table_name"
            ).fetchall()
        finally:
            con.close()
    except sqlite3.Error as e:
        logger.debug("Could not read gpkg_contents from %s: %s", path, e)
        return None
    return [row[0] for row in rows]


def is_multilayer(path: Path) -> bool:
    """Check if a file contains multiple layers.

    Args:
        path: Path to the geospatial file.

    Returns:
        True if the file has more than one layer, False otherwise.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    layers = list_layers(path)
    if layers is None:
        return False
    return len(layers) > 1
