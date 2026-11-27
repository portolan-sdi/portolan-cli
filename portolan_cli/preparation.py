"""Per-item preparation and conversion routing for the add pipeline."""

from __future__ import annotations

import contextlib
import fnmatch
import functools
import logging
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

import pystac
from pyproj import CRS

from portolan_cli.collection_id import normalize_collection_id, validate_collection_id
from portolan_cli.conversion_config import (
    VectorSettings,
    get_cog_settings,
    get_vector_settings,
    settings_override,
)
from portolan_cli.convert import (
    REWRITE_TEMP_INFIX,
    apply_vector_settings,
    claim_output,
    convert_with_handler,
    distinct_output_path,
    run_with_transient_convert_retry,
    with_provenance,
)
from portolan_cli.crs import _is_wgs84, measure_wgs84_bbox, transform_bbox_to_wgs84
from portolan_cli.formats import (
    current_run,
    in_run,
    invalidate,
    is_cloud_optimized_geotiff,
    is_geoparquet,
    remember,
)
from portolan_cli.metadata import (
    extract_band_statistics,
    extract_parquet_statistics,
    read_rewrite_fidelity,
    read_spatial_layout,
)
from portolan_cli.metadata.cog import COGMetadata
from portolan_cli.metadata.flatgeobuf import FlatGeobufMetadata
from portolan_cli.metadata.geoparquet import (
    GeoParquetMetadata,
    RewriteFidelity,
    geoparquet_metadata_of,
    open_footer,
)
from portolan_cli.metadata.pmtiles import PMTilesMetadata
from portolan_cli.metadata.statistics import embedded_statistics
from portolan_cli.metadata_yaml import (
    NodataMismatchError,
    apply_raster_nodata_defaults,
    apply_temporal_defaults,
    validate_metadata,
)
from portolan_cli.plugins.formats import FormatContext, extension_of, handler_for
from portolan_cli.readme import README_FILENAME
from portolan_cli.report import emit
from portolan_cli.scan.detect import is_filegdb
from portolan_cli.stac import (
    STEM_RANK_SOURCE,
    _require_wgs84,
    add_projection_extension,
    add_raster_extension,
    add_vector_extension,
    create_item,
    stem_rank,
)
from portolan_cli.stac_parquet import (
    PARQUET_FILENAME,
    declare_extensions_in_use,
)
from portolan_cli.viz.pmtiles_links import PMTILES_MEDIA_TYPE, pmtiles_asset_key
from portolan_cli.viz.style import enrich_cog_assets
from portolan_core import extension_registry as _reg
from portolan_core.checksums import (
    compute_checksum,
    compute_dir_checksum,
    compute_dir_size,
    file_fields_from,
)
from portolan_core.config import get_ignored_files, get_setting
from portolan_core.constants import ROLE_VISUAL
from portolan_core.errors import (
    AssetKeyCollisionError,
    CRSMismatchError,
    CRSTransformError,
    InvalidBboxError,
    NoGeometryError,
    RewriteFidelityError,
)
from portolan_core.format_families import RASTER, VECTOR
from portolan_core.run_state import RunState
from portolan_core.stac_links import GDAL_PAM_SUFFIX
from portolan_core.trace import count, span
from portolan_core.versions import versions_asset_key

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    import pyarrow.parquet as pq

    from portolan_cli.plugins.formats import FormatHandler
    from portolan_core.config import ConfigSource

logger = logging.getLogger(__name__)


# Structural STAC/Portolan files, not user data. AGENTS.md is a rel="agents" link, not an asset.
IGNORED_FILES: frozenset[str] = frozenset(
    {
        "catalog.json",
        "collection.json",
        "versions.json",
        "AGENTS.md",
    }
)


# Extension-to-MIME-type mapping for asset files. Derived from the extension
# registry (the single source). Edit rows there, not this map.
_MEDIA_TYPE_MAP: dict[str, str] = _reg.field_map("media_type")


# Asset keys reserved for well-known roles, preferred over filename stems. Order sets
# collision priority: a taken key falls back to the stem.
_ROLE_KEYS: dict[str, str] = {
    "thumbnail": "thumbnail",
    "metadata": "metadata",
    "documentation": "documentation",
}


# Extension-to-role mapping from the extension registry; unknown extensions are "data".
_ROLE_MAP: dict[str, str] = _reg.field_map("role")


def _get_media_type(path: Path) -> str:
    """Determine MIME type from file extension."""
    return _MEDIA_TYPE_MAP.get(path.suffix.lower(), "application/octet-stream")


def _get_asset_role(path: Path) -> str:
    """Determine STAC asset role from file extension."""
    return _ROLE_MAP.get(path.suffix.lower(), "data")


def _is_never_an_asset(file_path: Path) -> bool:
    """Report whether a directory entry can never be a tracked asset."""
    if file_path.name.startswith("."):
        return True
    if file_path.is_symlink():
        return True
    if file_path.name.lower().endswith(GDAL_PAM_SUFFIX):
        return True
    return is_rewrite_temp(file_path)


@dataclass(frozen=True)
class _ScannedFile:
    """One file of an item directory, measured once."""

    path: Path
    checksum: str
    size: int
    media_type: str
    role: str


def ignored_by(file_path: Path, patterns: list[str]) -> str | None:
    """Return the first ``ignored_files`` pattern the file name matches, or None."""
    return next((p for p in patterns if fnmatch.fnmatch(file_path.name, p)), None)


def _scan_item_assets(
    item_dir: Path,
    item_id: str,
    primary_file: Path,
    collection_dir: Path,
    *,
    collection_id: str,
    exclude_names: frozenset[str] = frozenset(),
    collection_level: bool = False,
    ignored_patterns: list[str] | None = None,
    state: RunState | None = None,
) -> tuple[dict[str, pystac.Asset], dict[str, tuple[Path, str, int]], list[str]]:
    """Scan an item directory for all trackable assets."""
    in_collection_dir = item_dir.resolve() == collection_dir.resolve()
    # Prune cross-item siblings only for collection-level (flat) scans. Resolve once, not per file.
    is_flat = bool(exclude_names) and in_collection_dir
    # _create_and_save_item writes the item JSON into item_dir, so an item's hrefs are bare names.
    href_prefix = "./" if collection_level else ""

    scanned: list[_ScannedFile] = []
    # Sorted, so the keys never depend on the filesystem listing order.
    for file_path in sorted(item_dir.iterdir()):
        if _is_never_an_asset(file_path):
            continue
        # Skip siblings that belong to OTHER items in this batch; keep the primary, its same-stem
        # sidecars, and loose companions.
        if (
            is_flat
            and primary_file.stem not in (file_path.stem, _distinct_stem(file_path))
            and _in_batch(file_path.name, exclude_names)
        ):
            continue
        if file_path.name in IGNORED_FILES or file_path.name == f"{item_id}.json":
            continue
        # The primary is the file this add publishes. The input filter already
        # accepted its source, so a pattern never drops it here.
        if file_path != primary_file and ignored_by(file_path, ignored_patterns or []):
            continue
        # The collection README is a rel="describedby" link, never an asset. The item mirror has
        # one writer, register_mirror_asset.
        if in_collection_dir and file_path.name in (README_FILENAME, PARQUET_FILENAME):
            continue
        measured = _measure(file_path, state)
        if measured is not None:
            scanned.append(measured)

    stac_assets: dict[str, pystac.Asset] = {}
    for key, (scanned_file, role) in _resolve_assets(
        scanned, primary_file, collection_id=collection_id, collection_level=collection_level
    ).items():
        stac_assets[key] = pystac.Asset(
            href=f"{href_prefix}{scanned_file.path.name}",
            media_type=scanned_file.media_type,
            roles=[role],
            # Titles come from metadata.yaml or survive from existing metadata via the merge.
            # file_fields_from, not file_fields: a FileGDB asset is a directory, measured by compute_dir_*.
            extra_fields=file_fields_from(scanned_file.checksum, scanned_file.size),
        )
    asset_files = {f.path.name: (f.path, f.checksum, f.size) for f in scanned}
    return stac_assets, asset_files, [str(f.path) for f in scanned]


def _distinct_stem(source: Path) -> str:
    """The stem of the distinct output name of ``source`` (``convert.claim_output``)."""
    return distinct_output_path(source, source.with_suffix(".parquet")).stem


def _in_batch(name: str, batch_names: frozenset[str]) -> bool:
    """Report whether ``name`` is a batch source, its output, or its distinct output."""
    if name in batch_names:
        return True
    stem = name.rpartition(".")[0]
    base, dash, extension = stem.rpartition("-")
    return bool(dash) and f"{base}.{extension}" in batch_names


def _measure(file_path: Path, state: RunState | None = None) -> _ScannedFile | None:
    """Checksum and type one directory entry, or None when it is no asset."""
    if file_path.is_dir():
        # FileGDB directories are single container assets.
        if not is_filegdb(file_path):
            return None
        return _ScannedFile(
            file_path,
            state.checksum(file_path) if state is not None else compute_dir_checksum(file_path),
            compute_dir_size(file_path),
            "application/x-filegdb",
            "data",
        )
    if not file_path.is_file():
        # Sockets, devices, and the like.
        return None
    return _ScannedFile(
        file_path,
        state.checksum(file_path) if state is not None else compute_checksum(file_path),
        file_path.stat().st_size,
        _get_media_type(file_path),
        _get_asset_role(file_path),
    )


ROLE_SOURCE = "source"


def _stem_rank(scanned_file: _ScannedFile, *, read_footer: bool) -> int:
    """Rank one content file. ``read_footer`` tells GeoParquet from a plain table."""
    return stem_rank(
        scanned_file.media_type,
        scanned_file.path.name,
        lambda: read_footer and not is_geoparquet(scanned_file.path),
    )


def _resolve_assets(
    scanned: list[_ScannedFile], primary_file: Path, *, collection_id: str, collection_level: bool
) -> dict[str, tuple[_ScannedFile, str]]:
    """Give every scanned file one asset key and one role."""
    groups: dict[str, list[_ScannedFile]] = {}
    role_files: list[_ScannedFile] = []
    stems = {scanned_file.path.stem for scanned_file in scanned}
    for scanned_file in scanned:
        if scanned_file.role in _ROLE_KEYS:
            role_files.append(scanned_file)
            continue
        # A source whose output took the distinct name joins that output.
        distinct = _distinct_stem(scanned_file.path)
        stem = distinct if distinct in stems else scanned_file.path.stem
        groups.setdefault(stem, []).append(scanned_file)

    resolved: dict[str, tuple[_ScannedFile, str]] = {}

    def claim(key: str, scanned_file: _ScannedFile, role: str) -> None:
        if key in resolved:
            key = scanned_file.path.name
        if key in resolved:
            holder = resolved[key][0].path.name
            raise AssetKeyCollisionError(collection_id, key, holder, scanned_file.path.name)
        resolved[key] = (scanned_file, role)

    # The primary's group goes first, so an item's primary gets ``data``.
    for stem in sorted(groups, key=lambda stem: stem != primary_file.stem):
        members = groups[stem]
        ranks = {f.path: _stem_rank(f, read_footer=len(members) > 1) for f in members}
        owner = min(
            (f for f in members if f.media_type != PMTILES_MEDIA_TYPE),
            default=None,
            key=lambda f: (
                not collection_level and f.path != primary_file,
                ranks[f.path],
                f.path != primary_file,
                f.path.name != f"{stem}.parquet",
                f.path.name,
            ),
        )
        converted = owner is not None and ranks[owner.path] < STEM_RANK_SOURCE
        for member in members:
            if member.media_type == PMTILES_MEDIA_TYPE:
                claim(pmtiles_asset_key(stem), member, ROLE_VISUAL)
            elif member is owner:
                is_primary = member.path == primary_file
                claim("data" if is_primary and not collection_level else stem, member, "data")
            elif converted and ranks[member.path] == STEM_RANK_SOURCE:
                claim(member.path.name, member, ROLE_SOURCE)
            else:
                claim(member.path.name, member, member.role)

    for scanned_file in role_files:
        role_key = _ROLE_KEYS[scanned_file.role]
        key = role_key if role_key not in resolved else scanned_file.path.stem
        claim(key, scanned_file, scanned_file.role)
    return resolved


@dataclass
class PreparedItem:
    """Result of prepare_item() — metadata extracted, ready for finalization."""

    item_id: str
    collection_id: str
    format_type: str | None
    bbox: list[float]
    asset_files: dict[str, tuple[Path, str, int]]
    item_json_path: Path | None  # None for collection-level vector assets
    is_collection_level_asset: bool = False
    stac_item: pystac.Item | None = None
    stac_assets: dict[str, pystac.Asset] | None = None  # For collection-level addition
    metadata: AllMetadata | None = None
    partition_metadata: dict[str, object] | None = None
    format_name: str | None = None
    # True when this run wrote the output and no file was there before. Only
    # such a file is safe to remove when a later step fails.
    created_output: bool = False
    # Versions.json source_path of each output, by output path: its source key, or its own key
    # if added as is. convert.claim_output reads it to tell a 0.8.0 output (no field) from an operator file.
    source_paths: dict[Path, str] = field(default_factory=dict)


def _pre_validate_geometry(path: Path) -> None:
    """Pre-validate that a file has valid geometry BEFORE any filesystem operations."""
    ext = path.suffix.lower()

    # Parquet: check GeoParquet metadata
    if ext == ".parquet":
        if not is_geoparquet(path):
            raise NoGeometryError(
                path=path.stem,
                reason="The source file may have no valid geometry.",
            )
        return

    # GeoJSON: check for features with geometry
    if ext in {".geojson", ".json"}:
        import json

        try:
            # Per RFC 7946: GeoJSON MUST be encoded as UTF-8
            with Path(path).open(encoding="utf-8") as f:
                data = json.load(f)

            # Check for features
            if data.get("type") == "FeatureCollection":
                features = data.get("features", [])
                if not features:
                    raise NoGeometryError(
                        path=path.stem,
                        reason="The source file has no features.",
                    )
                # Check that at least one feature has geometry
                has_geometry = any(f.get("geometry") is not None for f in features)
                if not has_geometry:
                    raise NoGeometryError(
                        path=path.stem,
                        reason="No features have geometry.",
                    )
            elif data.get("type") == "Feature":
                if data.get("geometry") is None:
                    raise NoGeometryError(
                        path=path.stem,
                        reason="Feature has no geometry.",
                    )
        except json.JSONDecodeError as err:
            raise ValueError(f"Invalid JSON in '{path}': {err}") from err
        return


def _cleanup_orphaned_output(output_path: Path, source_path: Path) -> None:
    """Remove a conversion output that gets no STAC record, and its GDAL sidecar."""
    if not output_path.exists() or output_path == source_path:
        return

    sidecar = output_path.with_name(output_path.name + GDAL_PAM_SUFFIX)
    try:
        output_path.unlink()
        logger.debug("Cleaned up orphaned conversion output: %s", output_path)
        # Never delete the source file.
        if sidecar.resolve() != source_path.resolve():
            sidecar.unlink(missing_ok=True)
    except OSError as cleanup_err:
        # Log but don't swallow the original error
        logger.warning("Failed to clean up orphaned file %s: %s", output_path, cleanup_err)


def _derive_item_id_and_asset_level(
    path: Path,
    collection_dir: Path,
    item_id: str | None,
) -> tuple[str, bool]:
    """Derive item ID and detect if asset is collection-level."""
    from portolan_cli.scan.detect import is_hive_partition_dir

    # If item_id is explicitly provided, treat as item-level (not collection-level)
    # This ensures --item-id creates a subdirectory structure
    if item_id is not None:
        # Validate item_id is a safe single path segment
        if not item_id or "/" in item_id or "\\" in item_id or item_id in {".", ".."}:
            raise ValueError(f"Invalid item_id '{item_id}': must be a single path segment")
        return item_id, False  # Explicit item_id = item-level structure

    # Auto-detect: collection-level if file is directly in collection directory
    is_collection_level_asset = path.parent.resolve() == collection_dir.resolve()

    # Hive partition directories in the path relative to the collection.
    try:
        relative_parts = list(path.parent.resolve().relative_to(collection_dir.resolve()).parts)
    except ValueError:
        relative_parts = []

    # Separate Hive partitions from regular directories
    hive_partitions: list[tuple[str, str]] = []  # (key, value) pairs
    non_hive_parts: list[str] = []
    for part in relative_parts:
        partition = is_hive_partition_dir(part)
        if partition is not None:
            hive_partitions.append(partition)
        else:
            non_hive_parts.append(part)

    # If path contains Hive partitions, apply special handling
    if hive_partitions:
        # A multi-level Hive path (year=2023/month=01/) needs the full relative path as item_id,
        # because path.parent.name repeats across branches.
        if len(hive_partitions) > 1 or non_hive_parts:
            # Multi-level partitions or mixed structure: use full relative path
            # e.g., year=2023/month=01/data.parquet -> item_id = "year=2023_month=01"
            item_id = "_".join(relative_parts)
        else:
            # Single-level Hive partition (most common case, e.g., kdtree):
            # Use parent directory name as item_id (existing behavior)
            item_id = path.parent.name
    elif is_collection_level_asset:
        # Item boundaries are directories: item_id is the parent directory name.
        # Collection-level assets use the file stem, to avoid collection/collection/ nesting.
        item_id = path.stem
    else:
        # Use parent directory name for item-level organization
        item_id = path.parent.name

    # Validate derived item_id
    if not item_id or "/" in item_id or "\\" in item_id or item_id in {".", ".."}:
        raise ValueError(f"Invalid item_id '{item_id}': must be a single path segment")

    return item_id, is_collection_level_asset


def _validate_collection_id(collection_id: str) -> None:
    """Validate collection ID for security and STAC compliance."""
    # First check: reject unsafe collection IDs (security check)
    # Forward slashes allowed for nested catalogs
    if (
        not collection_id
        or "\\" in collection_id
        or collection_id in {".", ".."}
        or any(part in {".", ".."} for part in collection_id.split("/"))
    ):
        raise ValueError(
            f"Invalid collection_id '{collection_id}': backslashes and. or .. segments not allowed"
        )

    # Second check: validate collection ID format per STAC spec
    is_valid, error_msg = validate_collection_id(collection_id)
    if not is_valid:
        suggestion = ""
        try:
            normalized = normalize_collection_id(collection_id)
            suggestion = f" Suggested: '{normalized}'"
        except ValueError:
            # Cannot normalize (e.g., all special characters)
            pass
        raise ValueError(f"Invalid collection ID '{collection_id}': {error_msg}.{suggestion}")


# Type alias for all supported metadata types
VectorMetadata = GeoParquetMetadata | PMTilesMetadata | FlatGeobufMetadata


AllMetadata = VectorMetadata | COGMetadata


def wgs84_bbox(metadata: AllMetadata, data_path: Path | None = None) -> list[float]:
    """Return the WGS84 bbox of a metadata object, for a STAC Item or Collection.

    Args:
        metadata: Metadata object with bbox attribute.
        data_path: Converted data file, when available, to measure the extent from instead of reprojecting the stored bbox.

    Returns:
        Bounding box as [min_x, min_y, max_x, max_y] in WGS84.
    """
    if isinstance(metadata, PMTilesMetadata):
        # PMTiles store bounds in WGS84 (4326), no transformation needed
        return list(metadata.bbox)  # type: ignore[arg-type]

    # The CRS is an EPSG/WKT string or a PROJJSON dict (GeoParquet stores a CRS with no
    # authority code as PROJJSON). pyproj reads both, so pass it through.
    crs_raw = getattr(metadata, "crs", None)
    source_crs = crs_raw if isinstance(crs_raw, (str, dict)) else None

    geometry_column = getattr(metadata, "geometry_column", None)
    if data_path is not None and geometry_column:
        measured = measure_wgs84_bbox(data_path, geometry_column, source_crs)
        if measured is not None:
            return list(measured)

    return list(transform_bbox_to_wgs84(metadata.bbox, source_crs))  # type: ignore[arg-type]


def _warn_if_source_newer(source_path: Path, output_path: Path) -> None:
    """Warn if source file is newer than output (suggests --reconvert)."""
    if source_path.stat().st_mtime > output_path.stat().st_mtime:
        emit(
            "warn",
            f"Source file '{source_path.name}' is newer than converted output. "
            "Use --reconvert to re-convert from source.",
        )


def _convert_and_extract_metadata(
    path: Path,
    item_dir: Path,
    handler: FormatHandler,
    *,
    target: Path | None = None,
    catalog_root: Path | None = None,
    force: bool = False,
    reconvert: bool = False,
    state: RunState | None = None,
) -> tuple[Path, AllMetadata]:
    """Convert with the format handler and read the metadata."""
    if target is None:
        target = claim_output(path, handler.output_path(path, item_dir), state=state)
    in_place = target.resolve() == path.resolve()
    reconvert_method = getattr(handler, "reconvert", None)
    override = (
        settings_override(
            cog=get_cog_settings(catalog_root, state=state),
            vector=get_vector_settings(catalog_root, state=state),
        )
        if state is not None
        else contextlib.nullcontext()
    )

    with override:
        if force and not reconvert and target.exists():
            if not in_place:
                _warn_if_source_newer(path, target)
            output = target
            if extension_of(target) in handler.extensions:
                output = handler.convert(target, item_dir, catalog_root)
        elif reconvert and in_place and reconvert_method is not None:
            output = reconvert_method(path, catalog_root)
        else:
            output = convert_with_handler(
                handler, path, item_dir, catalog_root, target=target, state=state
            )
    # A converter that read its output put the metadata in the run memo.
    kind = f"{handler.name}.metadata"
    metadata = (
        state.memo(kind, output, handler.read_metadata)
        if state is not None
        else handler.read_metadata(output)
    )
    return output, cast("AllMetadata", metadata)


def _extract_statistics_best_effort(
    output_path: Path,
    format_type: str | None,
    catalog_root: Path,
    collection_path: Path | None = None,
    state: ConfigSource | None = None,
) -> tuple[list[Any], dict[str, Any]]:
    """Extract statistics with best-effort error handling."""
    band_stats: list[Any] = []
    parquet_stats: dict[str, Any] = {}
    stats_enabled = _setting("statistics.enabled", catalog_root, collection_path, state)
    if not stats_enabled:
        return band_stats, parquet_stats

    try:
        if format_type == RASTER:
            raster_mode = _setting("statistics.raster_mode", catalog_root, collection_path, state)
            mode = cast(
                "Literal['cached', 'approx', 'exact']",
                raster_mode if raster_mode in ("cached", "approx", "exact") else "approx",
            )
            # The conversion put the embedded statistics in the run memo.
            # ``exact`` computes them again, so it reads the file.
            read = functools.partial(extract_band_statistics, mode=mode)
            band_stats = (
                state.memo(f"cog.statistics.{mode}", output_path, read)
                if isinstance(state, RunState) and mode != "exact"
                else read(output_path)
            )
        else:
            parquet_stats = extract_parquet_statistics(output_path)
    except Exception:
        # Statistics are optional. Continue without them and record why.
        logger.debug("Statistics extraction failed for %s", output_path, exc_info=True)
    return band_stats, parquet_stats


def _add_statistics_to_properties(
    stac_properties: dict[str, Any],
    format_type: str | None,
    band_stats: list[Any],
    parquet_stats: dict[str, Any],
    stats_enabled: bool,
) -> None:
    """Add statistics to STAC properties in-place."""
    if not stats_enabled:
        return

    if format_type == RASTER and band_stats:
        for i, stats in enumerate(band_stats):
            if i < len(stac_properties.get("bands", [])):
                stac_properties["bands"][i]["statistics"] = stats.to_stac_dict()
    elif format_type == VECTOR and parquet_stats:
        col_stats = {
            name: stat.to_stac_dict() for name, stat in parquet_stats.items() if stat.to_stac_dict()
        }
        if col_stats:
            stac_properties["table:column_statistics"] = col_stats


def _create_and_save_item(
    *,
    item_id: str,
    bbox: list[float],
    item_datetime: datetime | None,
    stac_properties: dict[str, Any],
    stac_assets: dict[str, pystac.Asset],
    describe: Callable[[pystac.Item], None],
    metadata: AllMetadata,
    item_dir: Path,
) -> tuple[pystac.Item, Path]:
    """Create a STAC item, let the format handler describe it, and save it."""
    item = create_item(
        item_id=item_id,
        bbox=bbox,
        datetime=item_datetime,
        properties=stac_properties,
        assets=stac_assets,
    )
    add_projection_extension(item, metadata)
    describe(item)
    # The item carries file:* on its assets and may carry table:* statistics, so it declares those extensions.
    document = item.to_dict(include_self_link=False, transform_hrefs=False)
    if declare_extensions_in_use(document):
        item.stac_extensions = document["stac_extensions"]

    item_json_path = item_dir / f"{item_id}.json"
    item.set_self_href(str(item_json_path))
    item.save_object()

    return item, item_json_path


def _apply_nodata_defaults_to_bands(
    stac_properties: dict[str, Any],
    metadata: COGMetadata,
    defaults: dict[str, Any],
    source_path: Path,
) -> None:
    """Apply nodata defaults from metadata.yaml to STAC band properties."""
    bands = stac_properties.get("bands", [])
    if not bands:
        return

    # Get current nodatavals from metadata extraction
    current_nodatavals = metadata.nodatavals or tuple(None for _ in range(len(bands)))

    # Apply defaults with strict checking (raises NodataMismatchError on mismatch)
    try:
        updated_nodatavals = apply_raster_nodata_defaults(
            defaults, current_nodatavals, band_count=len(bands), strict=True
        )
    except NodataMismatchError as e:
        raise NodataMismatchError(
            f"Error applying nodata defaults to '{source_path.name}': {e}"
        ) from e

    # Update bands with defaults where extraction returned None
    for i, band in enumerate(bands):
        # Only set if band doesn't already have nodata
        if (
            i < len(updated_nodatavals)
            and updated_nodatavals[i] is not None
            and band.get("nodata") is None
        ):
            band["nodata"] = updated_nodatavals[i]


def _setting(
    key: str, catalog_root: Path, collection_path: Path | None, state: ConfigSource | None
) -> Any:
    """Resolve a setting, through the run state when there is one."""
    if state is not None:
        return state.setting(key, collection_dir=collection_path)
    return get_setting(key, catalog_path=catalog_root, collection_path=collection_path)


def _stats_enabled(context: FormatContext) -> bool:
    return bool(
        _setting("statistics.enabled", context.catalog_root, context.collection_dir, context.state)
    )


def describe_raster_item(
    item: pystac.Item, output: Path, metadata: Any, context: FormatContext
) -> None:
    """Add the raster fields to a new item: render, band statistics, nodata, extension.

    The built-in COG handler calls this from ``describe_item``.

    Args:
        item: The new item.
        output: The COG.
        metadata: What ``extract_cog_metadata`` returned.
        context: Where the file sits.
    """
    # Enrich COG assets with render extension properties.
    enrich_cog_assets(item.assets, context.catalog_root, state=context.state)
    band_stats, _ = _extract_statistics_best_effort(
        output,
        RASTER,
        context.catalog_root,
        collection_path=context.collection_dir,
        state=context.state,
    )
    _add_statistics_to_properties(item.properties, RASTER, band_stats, {}, _stats_enabled(context))
    # Raster nodata defaults: applied to bands missing nodata values
    if context.defaults and isinstance(metadata, COGMetadata) and context.source is not None:
        _apply_nodata_defaults_to_bands(item.properties, metadata, context.defaults, context.source)
    add_raster_extension(item, metadata)


def describe_vector_item(
    item: pystac.Item, output: Path, metadata: Any, context: FormatContext
) -> None:
    """Add the vector fields to a new item: column statistics and the vector extension.

    The built-in GeoParquet handler calls this from ``describe_item``.

    Args:
        item: The new item.
        output: The GeoParquet file.
        metadata: What ``extract_geoparquet_metadata`` returned.
        context: Where the file sits.
    """
    _, parquet_stats = _extract_statistics_best_effort(
        output,
        VECTOR,
        context.catalog_root,
        collection_path=context.collection_dir,
        state=context.state,
    )
    _add_statistics_to_properties(
        item.properties, VECTOR, [], parquet_stats, _stats_enabled(context)
    )
    add_vector_extension(item, metadata)


def _item_bbox(
    metadata: AllMetadata,
    path: Path,
    output_path: Path,
    *,
    created_output: bool,
    collection_level: bool,
) -> list[float]:
    """Return the WGS84 bbox of an item, or remove a new output and raise."""
    try:
        if not metadata.bbox:
            raise NoGeometryError(
                path=metadata.id if hasattr(metadata, "id") else path.stem,
                reason="The source file may have no valid geometry.",
            )
        bbox = wgs84_bbox(metadata, output_path)
        # Collection extents filter invalid asset bboxes when they aggregate.
        return bbox if collection_level else _require_wgs84(bbox)
    except (NoGeometryError, CRSTransformError, CRSMismatchError, InvalidBboxError):
        if created_output:
            _cleanup_orphaned_output(output_path, path)
        raise


def prepare_item(  # noqa: PLR0914 - the whole per-file pipeline in one sequence
    *,
    path: Path,
    catalog_root: Path,
    collection_id: str,
    title: str | None = None,
    description: str | None = None,
    item_id: str | None = None,
    item_datetime: datetime | None = None,
    force: bool = False,
    reconvert: bool = False,
    exclude_sibling_names: frozenset[str] = frozenset(),
    state: RunState | None = None,
) -> PreparedItem:
    """Prepare files for addition (convert, extract metadata, create STAC item)."""
    # Step 1: Validate inputs
    _validate_collection_id(collection_id)

    handler = handler_for(path, catalog_root, state=state)
    if handler is None:
        raise ValueError(f"Unsupported format: {path.suffix}")
    format_type = handler.family

    # Every footer read of this item goes through the run memo, so the source is hashed once.
    if state is None:
        state = RunState.open(catalog_root)
    with in_run(state):
        _pre_validate_geometry(path)

    # Step 2: Set up paths
    collection_dir = catalog_root / Path(*collection_id.split("/"))
    item_id_resolved, is_collection_level_asset = _derive_item_id_and_asset_level(
        path=path,
        collection_dir=collection_dir,
        item_id=item_id,
    )
    item_dir = path.parent

    # Verify item_dir is inside collection_dir (security check)
    try:
        item_dir.resolve().relative_to(collection_dir.resolve())
    except ValueError as err:
        raise ValueError(
            f"File '{path}' is not inside collection '{collection_id}'. "
            f"Expected path under '{collection_dir}'."
        ) from err

    # Step 3: Convert and extract metadata. The output never replaces a file
    # that the source did not make (claim_output).
    target = claim_output(path, handler.output_path(path, item_dir), state=state)
    created_output = not target.exists()
    with in_run(state):
        output_path, metadata = _convert_and_extract_metadata(
            path,
            item_dir,
            handler,
            target=target,
            catalog_root=catalog_root,
            force=force,
            reconvert=reconvert,
            state=state,
        )
    created_output = created_output and output_path == target
    source_paths = {output_path: versions_asset_key(path, collection_dir)}
    if isinstance(metadata, GeoParquetMetadata) and output_path.is_file():
        # The handler API takes no state. Give the run the footer the handler
        # read, so finalization does not read it again.
        state.memo("geoparquet.metadata", output_path, lambda _path: metadata)

    # Step 3b: Load metadata.yaml defaults (for temporal/nodata when source lacks them)
    metadata_yaml = state.metadata(collection_dir)
    defaults = metadata_yaml.get("defaults", {})

    # Validate defaults section if present (fail fast on invalid config)
    if defaults:
        validation_errors = validate_metadata({"defaults": defaults})
        # Filter to only defaults-related errors
        defaults_errors = [e for e in validation_errors if "defaults" in e.lower()]
        if defaults_errors:
            raise ValueError(
                "Invalid metadata.yaml defaults configuration:\n"
                + "\n".join(f" - {e}" for e in defaults_errors)
            )

    # Step 4: Extract and transform bbox
    collection_level = is_collection_level_asset and handler.asset_level == "collection"
    bbox = _item_bbox(
        metadata,
        path,
        output_path,
        created_output=created_output,
        collection_level=collection_level,
    )

    # Step 5: Scan assets and compute statistics
    try:
        stac_assets, asset_files, _asset_paths = _scan_item_assets(
            item_dir=item_dir,
            item_id=item_id_resolved,
            primary_file=output_path,
            collection_dir=collection_dir,
            collection_id=collection_id,
            exclude_names=exclude_sibling_names,
            collection_level=collection_level,
            ignored_patterns=get_ignored_files(catalog_root, state=state),
            state=state,
        )
    except AssetKeyCollisionError:
        # The add stops before any STAC or versions.json write. A file this
        # run converted goes too, so the add leaves the directory as it was.
        if created_output and output_path != path:
            output_path.unlink(missing_ok=True)
        raise

    # Step 6: build STAC properties; the handler adds its format fields when it describes the item.
    stac_properties = metadata.to_stac_properties()
    if title:
        stac_properties["title"] = title
    if description:
        stac_properties["description"] = description

    # Step 6b: Apply metadata.yaml defaults
    # Temporal defaults: applied when no --datetime flag was provided
    effective_datetime = item_datetime
    if effective_datetime is None and defaults:
        effective_datetime = apply_temporal_defaults(defaults)

    # Step 7: collection-level vector assets go straight into collection.json; others get an item.json.
    if collection_level:
        # Collection-level vector asset: no item.json
        return PreparedItem(
            item_id=item_id_resolved,
            collection_id=collection_id,
            format_type=format_type,
            bbox=bbox,
            asset_files=asset_files,
            item_json_path=None,  # No item.json for collection-level vector
            is_collection_level_asset=True,
            stac_item=None,
            stac_assets=stac_assets,
            metadata=metadata,
            format_name=handler.name,
            created_output=created_output,
            source_paths=source_paths,
        )

    # Item-level: create STAC item and save item.json
    item, item_json_path = _create_and_save_item(
        item_id=item_id_resolved,
        bbox=bbox,
        item_datetime=effective_datetime,
        stac_properties=stac_properties,
        stac_assets=stac_assets,
        describe=lambda item: handler.describe_item(
            item,
            output_path,
            metadata,
            FormatContext(
                catalog_root=catalog_root,
                collection_dir=collection_dir,
                source=path,
                defaults=defaults,
                state=state,
            ),
        ),
        metadata=metadata,
        item_dir=item_dir,
    )

    return PreparedItem(
        item_id=item_id_resolved,
        collection_id=collection_id,
        format_type=format_type,
        bbox=bbox,
        asset_files=asset_files,
        item_json_path=item_json_path,
        is_collection_level_asset=is_collection_level_asset,
        stac_item=item,
        metadata=metadata,
        format_name=handler.name,
        created_output=created_output,
        source_paths=source_paths,
    )


def is_rewrite_temp(path: Path) -> bool:
    """Report whether a path is the scratch file of an in-place rewrite."""
    return REWRITE_TEMP_INFIX in path.name


def _needs_spatial_rewrite(
    source: Path | pq.ParquetFile, settings: VectorSettings, *, reconvert: bool = False
) -> bool:
    """Decide whether an existing Parquet file must be rewritten on ``add``."""
    layout = read_spatial_layout(source)
    if not layout.is_geoparquet:
        return False
    if reconvert:
        return True
    if not settings.add_bbox:
        return False
    return not layout.has_bbox_covering


def convert_vector(
    source: Path,
    dest_dir: Path,
    catalog_root: Path | None = None,
    *,
    reconvert: bool = False,
    strict: bool = False,
    settings: VectorSettings | None = None,
    partition_dir: Path | None = None,
) -> Path:
    """Convert a vector file to GeoParquet.

    Args:
        source: Source vector file.
        dest_dir: Destination directory.
        catalog_root: Catalog root, for reading ``conversion.vector`` settings. None uses the built-in defaults.
        reconvert: True when the operator asked to re-convert from source, which rewrites a Parquet file whatever its footer already declares.
        strict: Raise when a Parquet rewrite fails, and do not keep the file with a warning.
        settings: Explicit vector settings. None reads them from the catalog.
        partition_dir: Directory for Hive partitions by the configured spatial index. It applies only to a non-Parquet source, when the settings set ``partition`` and a spatial index.

    Returns:
        Path to the output GeoParquet file, or ``partition_dir`` when the
        table was written as partitions.
    """
    import geoparquet_io as gpio

    from portolan_cli.convert import _write_partitioned, repair_shapefile_encoding_sidecar

    claim_state = (
        RunState.open(catalog_root) if catalog_root is not None and current_run() is None else None
    )
    output_path = claim_output(source, dest_dir / f"{source.stem}.parquet", state=claim_state)
    if settings is None:
        settings = get_vector_settings(catalog_root)

    # A shapefile with a missing or wrong .cpg converts with the right encoding.
    if source.suffix.lower() == ".shp":
        repair_shapefile_encoding_sidecar(source)

    # Check if already GeoParquet
    if source.suffix.lower() == ".parquet":
        same_file = source.resolve() == output_path.resolve()
        # One footer read answers both questions. The file closes before the rewrite swaps a sibling
        # over it, because an open handle blocks the replace on Windows.
        footer = open_footer(source)
        try:
            needs = footer is not None and _needs_spatial_rewrite(
                footer, settings, reconvert=reconvert
            )
            before = read_rewrite_fidelity(footer) if footer is not None and needs else None
        finally:
            if footer is not None:
                footer.close()
        if not needs:
            if not same_file:
                shutil.copy2(source, output_path)
                invalidate(output_path)
            return output_path

        reason = "re-convert requested" if reconvert else "it carries no bbox covering column"
        _announce_rewrite(source, reason)
        if same_file:
            # A single-file collection keeps its data in place: write a sibling and swap it in,
            # because geoparquet-io cannot write the file it reads.
            _rewrite_or_keep(source, settings, before, strict=strict)
        else:
            # Copy first, then rewrite the copy. Converting straight into `output_path` would skip
            # `_assert_rewrite_kept_everything`, so a `.parquet` source could lose its CRS.
            shutil.copy2(source, output_path)
            invalidate(output_path)
            _rewrite_or_keep(output_path, settings, before, strict=strict)
        return output_path

    # A non-Parquet source: convert with the geoparquet-io fluent API inside the shared retry,
    # so a transient DuckDB "Query interrupted" does not fail a bulk add.
    if not (settings.partition and settings.spatial_index != "none"):
        partition_dir = None
    resolved = settings

    def _run() -> None:
        table = apply_vector_settings(
            gpio.convert(str(source)), resolved, partitioned=partition_dir is not None
        )
        if partition_dir is None:
            with_provenance(table, source, require_geometry=True).write(str(output_path))
        else:
            _write_partitioned(table, partition_dir, resolved)

    run_with_transient_convert_retry(_run, source_name=source.name)

    return output_path if partition_dir is None else partition_dir


# Above this size the rewrite is slow enough that the operator should know why `add`
# stalls and that it needs free space. The rewrite still runs.
LARGE_REWRITE_WARN_BYTES = 1024 * 1024 * 1024


def _announce_rewrite(source: Path, reason: str) -> None:
    """Tell the operator a rewrite starts, and how much work it is."""
    # The notice is user-facing and goes through the output writer (stderr in JSON mode).
    # Do not demote it to logging.info.
    from portolan_cli.utils import format_size

    size_bytes = source.stat().st_size
    size = format_size(size_bytes)
    emit("info", f"Rewriting {source.name} ({size}): {reason}")
    if size_bytes >= LARGE_REWRITE_WARN_BYTES:
        emit(
            "warn",
            f"{source.name} is {size}. The rewrite reads, sorts, and writes the "
            f"whole file, and needs {size} of free space while it runs. Set "
            f"add_bbox: false in .portolan/config.yaml to skip it.",
        )


def _rewrite_or_keep(
    source: Path, settings: VectorSettings, before: RewriteFidelity | None, *, strict: bool = False
) -> None:
    """Rewrite a GeoParquet in place, or keep it and say why the rewrite failed."""
    try:
        _rewrite_parquet_in_place(source, settings, before)
    except RewriteFidelityError as err:
        if strict:
            raise
        emit("warn", f"Kept {source.name} as it is: it would lose {err.lost}")
    except Exception as err:  # a copy is better than a failed add
        if strict:
            raise
        logger.info("Rewrite of %s failed: %s", source, err, exc_info=True)
        emit("warn", f"Kept {source.name} as it is: geoparquet-io could not rewrite it ({err})")


def _rewrite_parquet_in_place(
    source: Path, settings: VectorSettings, before: RewriteFidelity | None
) -> None:
    """Rewrite a GeoParquet file with the vector settings applied."""
    import geoparquet_io as gpio

    temp_path = source.with_name(f".{source.stem}{REWRITE_TEMP_INFIX}.parquet")

    def _run() -> None:
        table = apply_vector_settings(gpio.convert(str(source)), settings)
        table.write(str(temp_path))

    try:
        # A prior attempt can leave a memo entry after deleting its scratch
        # file. Its next incarnation may reuse the same stat key.
        invalidate(temp_path)
        run_with_transient_convert_retry(_run, source_name=source.name)
        # One footer read gives the gate and the item metadata. The footer closes before the
        # replace, because an open handle blocks it on Windows.
        footer = open_footer(temp_path)
        try:
            rewritten = footer if footer is not None else temp_path
            _assert_rewrite_kept_everything(source, rewritten, before)
            metadata = geoparquet_metadata_of(footer) if footer is not None else None
        finally:
            if footer is not None:
                footer.close()
        temp_path.replace(source)
        invalidate(source)
        if footer is not None and metadata is not None:
            remember("parquet.footer", source, footer)
            remember("geoparquet.metadata", source, metadata)
    finally:
        temp_path.unlink(missing_ok=True)


def _same_crs(before: Any, after: Any) -> bool:
    """Report whether two GeoParquet ``crs`` values mean the same thing."""
    if before == after:
        return True
    try:
        return bool(CRS.from_user_input(before) == CRS.from_user_input(after))
    except Exception:  # an unparsable CRS is not a proven match
        return False


def _means_wgs84(crs: Any) -> bool:
    """Report whether a GeoParquet ``crs`` value means WGS84."""
    try:
        return _is_wgs84(CRS.from_user_input(crs))
    except Exception:  # an unparsable CRS is not provably WGS84
        return False


def _assert_rewrite_kept_everything(
    source: Path, rewritten: Path | pq.ParquetFile, before: RewriteFidelity | None
) -> None:
    """Refuse a rewrite that would replace good data with worse data.

    Keep it: it guards a destructive rewrite against any cause, not one fixed upstream bug.
    """
    if before is None:
        return
    after = read_rewrite_fidelity(rewritten)
    if after is None:
        raise RewriteFidelityError(source.name, "the ability to be read back")

    if after.row_count != before.row_count:
        raise RewriteFidelityError(
            source.name, f"rows: {before.row_count} became {after.row_count}"
        )

    dropped = before.columns - after.columns
    if dropped:
        raise RewriteFidelityError(source.name, f"the columns {sorted(dropped)}")

    lost_keys = before.extra_keys - after.extra_keys
    if lost_keys:
        names = sorted(key.decode(errors="replace") for key in lost_keys)
        raise RewriteFidelityError(source.name, f"the schema metadata keys {names}")

    if before.crs is not None and after.crs is None and not _means_wgs84(before.crs):
        raise RewriteFidelityError(source.name, "the CRS it declared")

    # A written CRS that differs from the source is corruption. Compare the meaning, not the
    # PROJJSON text, because an equivalent CRS can serialize two ways.
    if before.crs is not None and after.crs is not None and not _same_crs(before.crs, after.crs):
        raise RewriteFidelityError(source.name, "its CRS, which the rewrite changed")

    lost = _lost_geo_fields(before, after) or _lost_field_metadata(before, after)
    if lost:
        raise RewriteFidelityError(source.name, lost)


def _lost_geo_fields(before: RewriteFidelity, after: RewriteFidelity) -> str | None:
    """Name a publisher field of a geometry column that the rewrite lost, or None."""
    for name, fields in (before.geo_columns or {}).items():
        kept = (after.geo_columns or {}).get(name)
        if kept is None:
            return f"the geometry column {name}"
        for key in ("edges", "epoch", "orientation"):
            if fields.get(key) != kept.get(key):
                return f"the {key} of the geometry column {name}"
        if not _crs_kept(fields, kept):
            return f"the CRS of the geometry column {name}"
    return None


def _crs_kept(before: dict[str, Any], after: dict[str, Any]) -> bool:
    """Report whether two ``geo`` column entries declare the same CRS."""
    if "crs" in before and before["crs"] is None:
        return "crs" in after and after["crs"] is None
    if "crs" not in after:
        return "crs" not in before or _means_wgs84(before["crs"])
    if after["crs"] is None:
        return False
    if "crs" not in before:
        return _means_wgs84(after["crs"])
    return _same_crs(before["crs"], after["crs"])


def _lost_field_metadata(before: RewriteFidelity, after: RewriteFidelity) -> str | None:
    """Name a column whose field metadata the rewrite lost or changed, or None."""
    for name, metadata in (before.field_metadata or {}).items():
        if (after.field_metadata or {}).get(name) != metadata:
            return f"the field metadata of the column {name}"
    return None


def convert_tabular(source: Path, dest_dir: Path, *, state: RunState | None = None) -> Path:
    """Convert tabular file to Parquet using geoparquet-io."""
    import geoparquet_io as gpio

    default_path = dest_dir / f"{source.stem}.parquet"
    output_path = claim_output(source, default_path, state=state)

    # If already Parquet, just copy (no conversion needed)
    if source.suffix.lower() == ".parquet":
        if source.resolve() == output_path.resolve():
            return output_path
        shutil.copy2(source, output_path)
        return output_path

    # gpio.convert() reads plain tables with geometry_column=None (gpio v1.2.0+ writes them
    # correctly). The shared retry covers a transient DuckDB "Query interrupted".
    def _run() -> None:
        table = gpio.convert(str(source))
        with_provenance(table, source).write(str(output_path))

    if state is None:
        run_with_transient_convert_retry(_run, source_name=source.name)
    else:
        with in_run(state):
            run_with_transient_convert_retry(_run, source_name=source.name)

    return output_path


def convert_raster(
    source: Path,
    dest_dir: Path,
    catalog_root: Path | None = None,
    *,
    reoptimize: bool = False,
) -> Path:
    """Convert a raster to a COG. ``add`` and ``check --fix`` both use this.

    Args:
        source: Source raster file.
        dest_dir: Destination directory.
        catalog_root: Catalog root, for the COG settings. None uses the defaults.
        reoptimize: Rewrite a source that is already a COG.

    Returns:
        Path to the output COG file.
    """
    import rasterio

    from portolan_cli.conversion_config import get_cog_settings
    from portolan_cli.convert import place_cog, read_cog, remember_cog, write_cog

    claim_state = (
        RunState.open(catalog_root) if catalog_root is not None and current_run() is None else None
    )
    output_path = claim_output(source, dest_dir / f"{source.stem}.tif", state=claim_state)
    is_cog = source.suffix.lower() in (".tif", ".tiff") and is_cloud_optimized_geotiff(source)
    if not is_cog or reoptimize:
        return write_cog(source, dest_dir, get_cog_settings(catalog_root), target=output_path)

    scratch = None
    count("raster.opens")
    with rasterio.Env(GDAL_PAM_ENABLED="NO"), rasterio.open(source) as src:
        embedded = embedded_statistics(src)
        # Read before the rewrite. It computes statistics that src then holds.
        read = read_cog(src)
        if embedded == "missing":
            scratch = _embed_statistics(src, source, output_path)
    if scratch is not None and _warn_on_failure(source, lambda: place_cog(scratch, output_path)):
        return output_path
    if embedded == "lossy":
        emit(
            "warn",
            f"{source.name} has no band statistics in the file (PTL-DAT-009). "
            "Portolan keeps it as it is, because its tiles use lossy compression "
            "and a rewrite would re-encode them.",
        )
    if source.resolve() != output_path.resolve():
        shutil.copy2(source, output_path)
        invalidate(output_path)
    remember_cog(output_path, read)
    return output_path


#: The codecs that store each pixel exactly and that the GDAL COG driver
#: writes. The statistics rewrite keeps one. PACKBITS is lossless, but the COG
#: driver has no PACKBITS, so the rewrite writes DEFLATE.
_LOSSLESS_COG_CODECS = frozenset({"DEFLATE", "LZW", "ZSTD", "LZMA", "NONE"})

#: The PREDICTOR tag values of a GeoTIFF, as the GDAL COG driver names them.
_COG_PREDICTOR_NAMES = {"1": "NO", "2": "STANDARD", "3": "FLOATING_POINT"}


def _embed_statistics(src: Any, source: Path, output: Path) -> Path | None:
    """Write a lossless COG again beside ``output``, so that it carries its band statistics."""
    from portolan_cli.convert import COG_PROVENANCE_TAG, copy_to_cog, provenance_record

    structure = src.tags(ns="IMAGE_STRUCTURE")
    codec = structure.get("COMPRESSION", "NONE").upper()
    block_width = src.block_shapes[0][1]
    options: dict[str, Any] = {
        "compress": "DEFLATE",
        # Small striped TIFFs pass COG validation, but their strip width need
        # not be a legal tile size (a multiple of 16).
        "blocksize": block_width if block_width % 16 == 0 else 512,
        # Adding statistics must retain the source pyramid, including no overviews.
        "overviews": "FORCE_USE_EXISTING",
    }
    if codec in _LOSSLESS_COG_CODECS:
        options["compress"] = codec
        if structure.get("PREDICTOR") in _COG_PREDICTOR_NAMES:
            options["predictor"] = _COG_PREDICTOR_NAMES[structure["PREDICTOR"]]
    # A COG written in place is its own source, so it needs no record.
    in_place = output.resolve() == source.resolve()
    return _warn_on_failure(
        source,
        lambda: copy_to_cog(
            src,
            output,
            options,
            {} if in_place else {COG_PROVENANCE_TAG: provenance_record(source)},
        ),
    )


def _warn_on_failure(source: Path, step: Callable[[], Path]) -> Path | None:
    """Run one step of the statistics rewrite. On failure, warn and return None."""
    try:
        with span("convert.embed_statistics", file=source.name):
            return step()
    except Exception as err:  # the original file stays usable
        emit(
            "warn",
            f"{source.name} has no band statistics in the file (PTL-DAT-009), "
            f"and the rewrite that adds them failed: {err}",
        )
        return None
