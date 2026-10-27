"""Per-item preparation and conversion routing for the add pipeline.

Extracted from ``add.py`` (issue #623). This module owns the parallelizable,
GDAL-bound phase of ``add``: format-specific conversion to cloud-native outputs
(GeoParquet / COG / plain Parquet), metadata + statistics extraction, asset
scanning, and STAC item construction. It writes item.json but performs no
versions.json or collection-link updates — those are batched in
``add.finalize_items`` to keep versioning O(n) (see Issue #281).

the CLI stays a thin wrapper; ``add.py`` orchestrates on top of the
routines here. This module deliberately imports nothing from ``add`` so the
dependency edge is one-directional (add -> preparation).
"""

from __future__ import annotations

import contextlib
import fnmatch
import glob
import logging
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pystac
from pyproj import CRS

from portolan_cli import extension_registry as _reg
from portolan_cli.collection_id import normalize_collection_id, validate_collection_id
from portolan_cli.config import get_ignored_files, get_setting, load_merged_metadata
from portolan_cli.constants import ROLE_VISUAL
from portolan_cli.conversion_config import (
    VectorSettings,
    get_cog_settings,
    get_vector_settings,
    settings_override,
)
from portolan_cli.convert import (
    apply_vector_settings,
    claim_output,
    convert_with_handler,
    distinct_output_path,
    run_with_transient_convert_retry,
    settle_output,
    with_provenance,
)
from portolan_cli.crs import _is_wgs84, measure_wgs84_bbox, transform_bbox_to_wgs84
from portolan_cli.errors import AssetKeyCollisionError, NoGeometryError, RewriteFidelityError
from portolan_cli.format_families import RASTER, VECTOR
from portolan_cli.formats import is_cloud_optimized_geotiff, is_geoparquet
from portolan_cli.metadata import (
    extract_band_statistics,
    extract_parquet_statistics,
    read_rewrite_fidelity,
    read_spatial_layout,
)
from portolan_cli.metadata.cog import COGMetadata
from portolan_cli.metadata.flatgeobuf import FlatGeobufMetadata
from portolan_cli.metadata.geoparquet import GeoParquetMetadata, RewriteFidelity
from portolan_cli.metadata.pmtiles import PMTilesMetadata
from portolan_cli.metadata_yaml import (
    NodataMismatchError,
    apply_raster_nodata_defaults,
    apply_temporal_defaults,
    validate_metadata,
)
from portolan_cli.plugins.formats import FormatContext, extension_of, handler_for
from portolan_cli.readme import README_FILENAME
from portolan_cli.scan.detect import is_filegdb
from portolan_cli.stac import (
    STEM_RANK_SOURCE,
    add_projection_extension,
    add_raster_extension,
    add_vector_extension,
    create_item,
    stem_rank,
)
from portolan_cli.stac_links import GDAL_PAM_SUFFIX
from portolan_cli.stac_parquet import (
    PARQUET_FILENAME,
    declare_extensions_in_use,
)
from portolan_cli.sync.checksums import (
    compute_checksum,
    compute_dir_checksum,
    compute_dir_size,
    file_fields_from,
)
from portolan_cli.versions import versions_asset_key
from portolan_cli.viz.pmtiles_links import PMTILES_MEDIA_TYPE, pmtiles_asset_key
from portolan_cli.viz.style import enrich_cog_assets

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from portolan_cli.config import ConfigSource
    from portolan_cli.plugins.formats import FormatHandler
    from portolan_cli.run_state import RunState

logger = logging.getLogger(__name__)


# Files to ignore when scanning item directories for assets.
# These are STAC/Portolan structural files, not user data.
# AGENTS.md is referenced via a rel="agents" link, not tracked as an asset
# ("AGENTS.md is a link, not an asset").
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


# Asset keys reserved for well-known roles. _resolve_assets prefers these
# keys over filename-derived stems so STAC consumers can find assets by role
# without inspecting file paths. Order matters for collision priority: an
# asset with role "thumbnail" prefers key "thumbnail"; if it's already taken
# (e.g. by a user-named thumbnail.png), the second asset falls back to its
# stem.
_ROLE_KEYS: dict[str, str] = {
    "thumbnail": "thumbnail",
    "metadata": "metadata",
    "documentation": "documentation",
}


# Extension-to-role mapping for asset files (data / thumbnail / metadata /
# documentation). Derived from the extension registry. Unknown
# extensions fall back to "data" in _get_asset_role().
_ROLE_MAP: dict[str, str] = _reg.field_map("role")


def _get_media_type(path: Path) -> str:
    """Determine MIME type from file extension.

    Args:
        path: Path to the file.

    Returns:
        MIME type string. Defaults to "application/octet-stream" for
        unknown extensions.
    """
    return _MEDIA_TYPE_MAP.get(path.suffix.lower(), "application/octet-stream")


def _get_asset_role(path: Path) -> str:
    """Determine STAC asset role from file extension.

    Args:
        path: Path to the file.

    Returns:
        Role string: "data", "visual", "thumbnail", "metadata", or
        "documentation". Defaults to "data" for unknown extensions.
        :func:`_resolve_assets` then gives each scanned file its final role.
    """
    return _ROLE_MAP.get(path.suffix.lower(), "data")


def _is_never_an_asset(file_path: Path) -> bool:
    """Report whether a directory entry can never be a tracked asset.

    Four entries qualify, and none of them depend on the item being scanned:

    - A hidden file. Editors and tools leave them beside the data.
    - A symlink. This check must precede any ``is_dir()`` branching, because
      ``is_dir()`` follows symlinks, so a symlinked ``.gdb`` directory would
      otherwise be checksummed as a container asset and escape the intended
      item boundary.
    - A GDAL ``.aux.xml`` statistics sidecar. GDAL writes it after the first
      scan, when it computes band statistics, so a later ``add --force``
      registered it and the rebuild differed. The spec does not count it as
      the embedded statistics (PORTO-FMT-026), so it is a cache, not an asset.
    - The scratch file of an in-place GeoParquet rewrite. The hidden test
      already covers the name the current writer uses. A killed run of an
      earlier version can leave a visible one, and tracking that as an asset
      makes every later ``add`` read a truncated file (issue #805).

    Args:
        file_path: Directory entry to test.

    Returns:
        True when the scan must skip the entry.
    """
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
    """Return the first ``ignored_files`` pattern the file name matches, or None.

    ``add`` applies it to the files it prepares, and the asset scan applies it
    to their companions, so an ignored file never enters the catalog by
    sitting beside an accepted one.
    """
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
) -> tuple[dict[str, pystac.Asset], dict[str, tuple[Path, str, int]], list[str]]:
    """Scan an item directory for all trackable assets.

    Per issue #133, ALL files in item directories are tracked as assets.
    FileGDB directories (.gdb) are treated as single container assets (Issue #174).
    Skips: non-FileGDB directories, symlinks, hidden files, STAC structural files.
    :func:`_resolve_assets` gives each file its key and role.

    Args:
        item_dir: Path to the item directory (where files are).
        item_id: Item identifier (for skipping item.json).
        primary_file: Path to the primary data file.
        collection_dir: Path to the collection directory.
        collection_id: The collection id, for the collision error.
        exclude_names: Base filenames of OTHER items being added in the same
            batch (their sources and converted outputs). For a collection-level
            asset (``item_dir == collection_dir``) the flat collection directory
            also holds every sibling asset, so without this the scan re-checksums
            all siblings on every file, O(n^2) (issue #465). Files here that do
            not share the primary's stem are skipped. Each is tracked by its own
            ``prepare_item``. Loose companions (not in this set) are kept.
            Ignored for item-level (subdirectory) scans.
        collection_level: The assets go into collection.json. Hrefs are then
            ``./<name>`` and keys follow the stem groups. For an item, the
            primary is keyed ``data`` and hrefs are ``<name>``, relative to the
            item JSON in ``item_dir``.
        ignored_patterns: The ``ignored_files`` patterns of the catalog. A
            companion that matches one is not an asset (#1044).

    Returns:
        Tuple of (stac_assets, asset_files, asset_paths):
        - stac_assets: Dict mapping asset key to pystac.Asset
        - asset_files: Dict mapping filename to (path, checksum, size) tuples
        - asset_paths: List of absolute path strings

    Raises:
        AssetKeyCollisionError: Two files claim one asset key.
    """
    in_collection_dir = item_dir.resolve() == collection_dir.resolve()
    # Issue #465: only prune cross-item siblings for collection-level (flat)
    # scans. Resolve once, not per file: a flat collection holds n files.
    is_flat = bool(exclude_names) and in_collection_dir
    # _create_and_save_item writes the item JSON into item_dir, whatever the
    # item id, so an item's hrefs are bare names. A "../" prefix assumed a JSON
    # in {collection_dir}/{item_id}/ and broke --item-id (issue #840).
    href_prefix = "./" if collection_level else ""

    scanned: list[_ScannedFile] = []
    # Sorted, so the keys never depend on the filesystem listing order.
    for file_path in sorted(item_dir.iterdir()):
        if _is_never_an_asset(file_path):
            continue
        # Issue #465: skip siblings that belong to OTHER items in this batch.
        # Keep the primary and its own same-stem source/sidecars; keep loose
        # companions (not in exclude_names) so tracking is preserved.
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
        # The collection README is a rel="describedby" link that `readme`
        # rewrites, never an asset. The first add runs before the README exists,
        # so a scan that registered it made add --force differ (issue #1041).
        # The item mirror has one writer, register_mirror_asset. The scan gave
        # it the data role, so the PMTiles side-step drew tiles of it.
        if in_collection_dir and file_path.name in (README_FILENAME, PARQUET_FILENAME):
            continue
        measured = _measure(file_path)
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
            # Titles are human-enrichable (Issue #446). They come from
            # metadata.yaml or survive from existing metadata via the merge.
            # file_fields_from, not file_fields: a FileGDB asset is a directory,
            # already measured by compute_dir_checksum/compute_dir_size.
            extra_fields=file_fields_from(scanned_file.checksum, scanned_file.size),
        )
    asset_files = {f.path.name: (f.path, f.checksum, f.size) for f in scanned}
    return stac_assets, asset_files, [str(f.path) for f in scanned]


def _distinct_stem(source: Path) -> str:
    """The stem of the distinct output name of ``source`` (``convert.claim_output``)."""
    return distinct_output_path(source, source.with_suffix(".parquet")).stem


def _in_batch(name: str, batch_names: frozenset[str]) -> bool:
    """Report whether ``name`` is a batch source, its output, or its distinct output.

    The distinct output of ``b1.csv`` is ``b1-csv.<suffix>``. The test reads
    the name only, so it costs no ``stat`` per sibling (issue #465).
    """
    if name in batch_names:
        return True
    stem = name.rpartition(".")[0]
    base, dash, extension = stem.rpartition("-")
    return bool(dash) and f"{base}.{extension}" in batch_names


def _measure(file_path: Path) -> _ScannedFile | None:
    """Checksum and type one directory entry, or None when it is no asset."""
    if file_path.is_dir():
        # FileGDB directories are single container assets (Issue #174).
        if not is_filegdb(file_path):
            return None
        return _ScannedFile(
            file_path,
            compute_dir_checksum(file_path),
            compute_dir_size(file_path),
            "application/x-filegdb",
            "data",
        )
    if not file_path.is_file():
        # Sockets, devices, and the like.
        return None
    return _ScannedFile(
        file_path,
        compute_checksum(file_path),
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
    """Give every scanned file one asset key and one role.

    Content files (every role but thumbnail, metadata and documentation) are
    grouped by stem. In each group:

    - A PMTiles never owns the stem. portolan-spec formats.md "PMTiles" makes
      it the visualization derivative of the GeoParquet. It gets the
      ``visual`` role and the key the PMTiles writer uses (Issue #929).
    - Of the other files, the file of the lowest rank owns the stem and gets
      the ``data`` role: GeoParquet, then other cloud-native data (COG,
      FlatGeobuf), then Parquet without GeoParquet metadata, then everything
      else. The primary wins a tie, then ``<stem>.parquet``, then the first
      name. An item keys its primary ``data`` and always lets it own its group.
    - When the owner is cloud-native, a file that is not is the file the owner
      was converted from, such as a GeoJSON or a shapefile and its sidecars. It
      gets the ``source`` role (Issue #960).
    - Any other file keeps its role and takes its filename as key.

    A file with a well-known role takes the role name as key, then its stem,
    then its filename. A file whose key is taken takes its filename. When a
    file has the filename of a key that is taken, two files claim one key.
    The resolver then raises, because a dict holds one of them and
    versions.json tracks both. The answer reads the files, never the listing order or,
    for a collection, the choice of primary, because every item of a flat
    collection scans the same files (Issue #933).

    Args:
        scanned: The files of the directory, sorted by name.
        primary_file: The file this scan registers.
        collection_id: The collection id, for the collision error.
        collection_level: Key the owner by its stem, not ``data``.

    Returns:
        ``(file, role)`` by asset key.

    Raises:
        AssetKeyCollisionError: Two files claim one asset key.
    """
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
    """Result of prepare_item() — metadata extracted, ready for finalization.

    This dataclass holds all the information needed to finalize an item
    (write versions.json, update collection links) without any I/O happening
    during the prepare phase.

    The prepare/finalize separation enables O(n) versioning instead of O(n²)
    by batching all version writes at the end. See Issue #281.

    Attributes:
        item_id: STAC item identifier (for item-level) or asset key (for collection-level).
        collection_id: Collection identifier (may include '/' for nested).
        format_type: Vector or raster format.
        bbox: Bounding box [min_x, min_y, max_x, max_y] in WGS84.
        asset_files: Dict mapping filename to (path, checksum, size) tuples.
        item_json_path: Path to item.json (None for collection-level vector assets).
        is_collection_level_asset: If True, asset is at collection level.
        stac_item: The PySTAC Item object (None for collection-level vector assets).
        stac_assets: Assets to add to collection.json (for collection-level assets).
        metadata: Extracted metadata (GeoParquet or COG) for table extension (Issue #304).
        partition_metadata: Partition extension fields from get_partition_metadata() (Issue #232).
    """

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
    format_name: str | None = None  # The format handler that converted the file (#947)
    # True when this run wrote the output and no file was there before. Only
    # such a file is safe to remove when a later step fails.
    created_output: bool = False
    # The versions.json source_path of each output, by output path: the key of
    # the source it was converted from, or its own key for a file added as it
    # is. convert.claim_output reads the field to tell a 0.8.0 output, which
    # has none, from a file of the operator.
    source_paths: dict[Path, str] = field(default_factory=dict)


def _pre_validate_geometry(path: Path) -> None:
    """Pre-validate that a file has valid geometry BEFORE any filesystem operations.

    Issue #163: Failed add operations should be atomic. This function checks for
    geometry/features before any conversion or copying happens, preventing partial
    artifacts from being created.

    Args:
        path: Path to the source file.

    Raises:
        ValueError: If the file has no valid geometry/features.
    """
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


def _cleanup_orphaned_output(output_path: Path, item_dir: Path, source_path: Path) -> None:
    """Clean up orphaned conversion output when geometry extraction fails.

    Called when conversion succeeds but produces no geometry (empty bbox).
    Removes the output file and the sidecars of the output to avoid leaving
    orphaned files in the item directory.

    A sidecar of the output carries the full output name, such as
    ``scene.tif.aux.xml``. A file that only shares the stem belongs to the
    user: a pre-built ``zoning.pmtiles`` or the ``.dbf`` of a shapefile
    source. The cleanup deleted those too.

    Args:
        output_path: Path to the converted output file.
        item_dir: Directory containing the item files.
        source_path: Original source file path (won't be deleted if same).
    """
    if not output_path.exists() or output_path == source_path:
        return

    # Resolve source_path for comparison (Issue #432: don't delete source file)
    resolved_source = source_path.resolve()

    try:
        output_path.unlink()
        logger.debug("Cleaned up orphaned conversion output: %s", output_path)
        for sidecar in item_dir.glob(f"{glob.escape(output_path.name)}.*"):
            if sidecar.resolve() != resolved_source:
                sidecar.unlink()
                logger.debug("Cleaned up orphaned sidecar: %s", sidecar)
    except OSError as cleanup_err:
        # Log but don't swallow the original error
        logger.warning("Failed to clean up orphaned file %s: %s", output_path, cleanup_err)


def _derive_item_id_and_asset_level(
    path: Path,
    collection_dir: Path,
    item_id: str | None,
) -> tuple[str, bool]:
    """Derive item ID and detect if asset is collection-level.

    Args:
        path: Path to the asset file.
        collection_dir: Collection directory path.
        item_id: Optional explicit item ID.

    Returns:
        Tuple of (item_id, is_collection_level_asset).

    Raises:
        ValueError: If derived or provided item_id is invalid.

    Note:
        For nested collections (e.g., collection_id="a/b"), a file at
        catalog_root/a/file.parquet will NOT be detected as collection-level
        for collection "a/b" (since path.parent != catalog_root/a/b).
        This is intentional - the file would belong to parent collection "a".

    Note:
        Per Issue #443: Files in Hive partition directories (key=value) are
        handled specially to avoid duplicate item IDs. The item ID comes from
        the partition values, and the format type does not change the result.
    """
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

    # Check for Hive partition directories in path relative to collection
    # Per Issue #443: Handle Hive partitions consistently with collection_id filtering
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
        # Issue #443: For multi-level Hive partitions (e.g., year=2023/month=01/),
        # using path.parent.name would give "month=01" for ALL year branches,
        # causing duplicate item IDs. Instead, use the full relative path as item_id.
        #
        # For single-level partitions (e.g., kdtree_cell=XXX/), path.parent.name
        # is unique, so no special handling needed - fall through to normal logic.
        if len(hive_partitions) > 1 or non_hive_parts:
            # Multi-level partitions or mixed structure: use full relative path
            # e.g., year=2023/month=01/data.parquet -> item_id = "year=2023_month=01"
            item_id = "_".join(relative_parts)
        else:
            # Single-level Hive partition (most common case, e.g., kdtree):
            # Use parent directory name as item_id (existing behavior)
            item_id = path.parent.name
    elif is_collection_level_asset:
        # Generate item ID from PARENT DIRECTORY name (Issue #163)
        # Item boundaries are directories, not filenames.
        # Example: collection/item_dir/file.parquet -> item_id = "item_dir"
        # For collection-level assets, use file stem to avoid duplicate directory name
        # Use file stem for collection-level assets to avoid collection/collection/ nesting
        item_id = path.stem
    else:
        # Use parent directory name for item-level organization
        item_id = path.parent.name

    # Validate derived item_id
    if not item_id or "/" in item_id or "\\" in item_id or item_id in {".", ".."}:
        raise ValueError(f"Invalid item_id '{item_id}': must be a single path segment")

    return item_id, is_collection_level_asset


def _validate_collection_id(collection_id: str) -> None:
    """Validate collection ID for security and STAC compliance.

    Args:
        collection_id: The collection ID to validate.

    Raises:
        ValueError: If the collection ID is invalid.
    """
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


def _extract_bbox_wgs84(metadata: AllMetadata, data_path: Path | None = None) -> list[float]:
    """Extract bbox from metadata, transforming to WGS84 if needed.

    PMTiles bbox is already in WGS84 (4326). Other formats may need
    CRS transformation.

    For GeoParquet in a projected CRS the extent is *measured* from the
    geometry when ``data_path`` is given. Reprojecting the stored bbox instead
    answers a different question — it gives the WGS84 envelope of a rectangle,
    which for a projected CRS is strictly larger than the envelope of the data
    inside it, so the declared extent overstates the data and `check` rejects it
    (PTL-DAT-005). Measurement falls back to the reprojected bbox whenever it
    cannot be made.

    Args:
        metadata: Metadata object with bbox attribute.
        data_path: Converted data file, when available, to measure the extent
            from instead of reprojecting the stored bbox.

    Returns:
        Bounding box as [min_x, min_y, max_x, max_y] in WGS84.
    """
    if isinstance(metadata, PMTilesMetadata):
        # PMTiles store bounds in WGS84 (4326), no transformation needed
        return list(metadata.bbox)  # type: ignore[arg-type]

    # Other formats may need CRS transformation. The CRS is an EPSG/WKT string
    # or a PROJJSON dict: GeoParquet stores a CRS with no authority code as
    # PROJJSON, which is what an ESRI .prj such as POSGAR 1994 produces. pyproj
    # reads both, so pass the value through rather than reject it (Issue #810).
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
    from portolan_cli.output import warn as warn_output

    if source_path.stat().st_mtime > output_path.stat().st_mtime:
        warn_output(
            f"Source file '{source_path.name}' is newer than converted output. "
            "Use --reconvert to re-convert from source."
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
    """Convert with the format handler and read the metadata (#947).

    The same rule applies to every handler, built-in or plugin:

    - ``--force`` without ``--reconvert`` keeps an output that exists
      (issue #386). The handler converts it again only when it reads that
      format itself, which brings an existing GeoParquet up to the profile
      (issue #805).
    - ``--reconvert`` on a source that is its own output rewrites it with the
      handler's ``reconvert``, when it has one.
    - Otherwise the handler converts the source into ``target``.

    ``target`` comes from :func:`convert.claim_output`, so no branch writes over
    a file that the source did not make.

    Args:
        path: Source file path.
        item_dir: Item directory for output.
        handler: The format handler that claims the file.
        target: The output path from :func:`convert.claim_output`. None claims it here.
        catalog_root: Catalog root, for reading conversion settings.
        force: If True, bypass change detection (Issue #386).
        reconvert: If True, re-convert from source (requires force=True).
        state: The run state. The handler takes no state, so the conversion
            settings reach it through ``settings_override``. The state also
            gives one hash and one ``versions.json`` read per run.

    Returns:
        Tuple of (output_path, metadata). A plugin's FormatMetadata provides
        the bbox, crs, and to_stac_properties that the add path reads.
    """
    if target is None:
        target = claim_output(path, handler.output_path(path, item_dir))
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
    return output, cast("AllMetadata", handler.read_metadata(output))


def _extract_statistics_best_effort(
    output_path: Path,
    format_type: str | None,
    catalog_root: Path,
    collection_path: Path | None = None,
    state: ConfigSource | None = None,
) -> tuple[list[Any], dict[str, Any]]:
    """Extract statistics with best-effort error handling.

    Args:
        output_path: Path to the converted file.
        format_type: Format type (RASTER or VECTOR).
        catalog_root: Catalog root for config lookup.
        collection_path: Collection directory for hierarchical config.
        state: The run state, which parses the config once per run.

    Returns:
        Tuple of (band_stats, parquet_stats). Empty if disabled or failed.
    """
    band_stats: list[Any] = []
    parquet_stats: dict[str, Any] = {}
    stats_enabled = _setting("statistics.enabled", catalog_root, collection_path, state)
    if not stats_enabled:
        return band_stats, parquet_stats

    try:
        if format_type == RASTER:
            raster_mode = _setting("statistics.raster_mode", catalog_root, collection_path, state)
            mode = raster_mode if raster_mode in ("cached", "approx", "exact") else "approx"
            band_stats = extract_band_statistics(output_path, mode=mode)  # type: ignore[arg-type]
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
    """Add statistics to STAC properties in-place.

    Args:
        stac_properties: Properties dict to modify.
        format_type: Format type (RASTER or VECTOR).
        band_stats: Band statistics (for rasters).
        parquet_stats: Parquet column statistics (for vectors).
        stats_enabled: Whether stats are enabled.
    """
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
    """Create a STAC item, let the format handler describe it, and save it.

    Helper to reduce complexity in prepare_item().

    Args:
        item_id: STAC item identifier.
        bbox: Bounding box [min_x, min_y, max_x, max_y].
        item_datetime: Acquisition/creation datetime.
        stac_properties: Properties to include in the item.
        stac_assets: Assets to attach to the item.
        describe: Calls the format handler's ``describe_item`` (#947).
        metadata: Extracted metadata for the projection extension.
        item_dir: Directory where item.json will be saved.

    Returns:
        Tuple of (item, item_json_path).
    """
    item = create_item(
        item_id=item_id,
        bbox=bbox,
        datetime=item_datetime,
        properties=stac_properties,
        assets=stac_assets,
    )
    add_projection_extension(item, metadata)
    describe(item)
    # The item carries file:* on its assets and may carry table:* statistics,
    # so it declares those extensions itself (issue #1037).
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
    """Apply nodata defaults from metadata.yaml to STAC band properties.

    Only applies defaults to bands that don't already have nodata values.
    Modifies stac_properties["bands"] in-place.

    Args:
        stac_properties: Properties dict to modify.
        metadata: COGMetadata with extraction results.
        defaults: The 'defaults' section from metadata.yaml.
        source_path: Path to source file (for error messages).

    Raises:
        NodataMismatchError: If per-band nodata list doesn't match band count.
    """
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

    The built-in COG handler calls this from ``describe_item`` (#947).

    Args:
        item: The new item.
        output: The COG.
        metadata: What ``extract_cog_metadata`` returned.
        context: Where the file sits.
    """
    # Enrich COG assets with render extension properties (Issue #13)
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

    The built-in GeoParquet handler calls this from ``describe_item`` (#947).

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


def prepare_item(
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
    """Prepare files for addition (convert, extract metadata, create STAC item).

    This function does the GDAL-bound work (conversion, metadata extraction) but
    does NOT write to versions.json or update collection.json links. This enables
    O(n) versioning instead of O(n²) by batching writes in finalize_items().

    Per Issue #281: This is the parallelizable phase of the add workflow.
    Per Issue #386: force/reconvert control conversion skip behavior.

    Args:
        path: Path to the source file.
        catalog_root: Root directory of the catalog.
        collection_id: Collection to add the data to.
        title: Optional display title for the item.
        description: Optional description.
        item_id: Optional item ID (defaults to parent directory name).
        item_datetime: Optional acquisition/creation datetime.
        force: If True, bypass change detection (Issue #386).
        reconvert: If True, re-convert from source (requires force=True).
        exclude_sibling_names: Base filenames of other batch items (sources +
            converted outputs) to prune from a collection-level asset scan
            (issue #465). Forwarded to _scan_item_assets; see its docstring.
        state: The run state, which parses the config once per run and hashes
            each file once. None reads each shared file here.

    Returns:
        PreparedItem with all metadata needed for finalization.

    Raises:
        ValueError: If the format is unsupported or collection_id is invalid.
        FileNotFoundError: If the source file doesn't exist.
        NoGeometryError: If the file has no valid geometry.
        OutputExistsError: Both output names hold a file that the source did
            not write.
    """
    # Step 1: Validate inputs
    _validate_collection_id(collection_id)

    handler = handler_for(path, catalog_root, state=state)
    if handler is None:
        raise ValueError(f"Unsupported format: {path.suffix}")
    format_type = handler.family

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
    # settle_output can move a tabular output over the file 0.8.0 wrote.
    created_output = created_output and output_path == target
    source_paths = {output_path: versions_asset_key(path, collection_dir)}

    # Step 3b: Load metadata.yaml defaults (for temporal/nodata when source lacks them)
    metadata_yaml = (
        state.metadata(collection_dir)
        if state is not None
        else load_merged_metadata(collection_dir, catalog_root)
    )
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
    if not metadata.bbox:
        if created_output:
            _cleanup_orphaned_output(output_path, item_dir, path)
        raise NoGeometryError(
            path=metadata.id if hasattr(metadata, "id") else path.stem,
            reason="The source file may have no valid geometry.",
        )
    bbox = _extract_bbox_wgs84(metadata, output_path)

    # Step 5: Scan assets and compute statistics
    collection_level = is_collection_level_asset and handler.asset_level == "collection"
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
        )
    except AssetKeyCollisionError:
        # The add stops before any STAC or versions.json write. A file this
        # run converted goes too, so the add leaves the directory as it was.
        if created_output and output_path != path:
            output_path.unlink(missing_ok=True)
        raise

    # Step 6: Build STAC properties. The handler adds its format fields when it
    # describes the item (#947).
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

    # Step 7: Create STAC item or collection-level assets
    # Collection-level vector assets: no item.json, assets go directly in collection.json
    # Item-level assets (rasters, partitioned vectors): create item.json as usual
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


# Marks the temporary file `_rewrite_parquet_in_place` swaps in. `add` collects
# a directory before it converts anything, so the marker keeps a file left by a
# killed run out of the next run's source list (issue #805).
REWRITE_TEMP_INFIX = ".portolan-rewrite"


def is_rewrite_temp(path: Path) -> bool:
    """Report whether a path is the scratch file of an in-place rewrite.

    :func:`_rewrite_parquet_in_place` writes a hidden sibling and swaps it in.
    A killed run skips the cleanup, so ``add`` must recognize a leftover and
    refuse to ingest it as a source (issue #805).

    Args:
        path: Candidate file path.

    Returns:
        True when the file name carries the rewrite marker.
    """
    return REWRITE_TEMP_INFIX in path.name


def _needs_spatial_rewrite(
    source: Path, settings: VectorSettings, *, reconvert: bool = False
) -> bool:
    """Decide whether an existing Parquet file must be rewritten on ``add``.

    ``add`` used to copy every ``.parquet`` source through untouched, which
    produced a catalog that failed Portolan's own ``check``: the file carried
    no bbox covering column, so it failed PTL-DAT-007 and left PTL-DAT-006
    unevaluated (issue #805).

    Three cases keep the copy:

    - The file is tabular Parquet, with no ``geo`` metadata. ``add_bbox()``
      needs a geometry column.
    - ``add_bbox`` is off. The footer is the only thing this function can read
      cheaply, so with the bbox column disabled there is nothing it can detect
      and no outcome a rewrite would change. Rewriting anyway would repeat on
      every ``add`` and report a reason the rewrite cannot fix.
    - The file already declares a bbox covering column. Rewriting a large
      conformant file would cost time and change nothing.

    The footer says nothing about row order, so a file with the column but
    unordered rows still fails PTL-DAT-006. ``reconvert`` is the operator's
    answer to that, and to a sort-only configuration: ``add --force
    --reconvert`` rewrites the file whatever its footer says.

    Args:
        source: Source Parquet file.
        settings: Vector conversion settings.
        reconvert: True when the operator asked to re-convert from source.

    Returns:
        True when the file must go through geoparquet-io again.
    """
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
    settings: VectorSettings | None = None,
    partition_dir: Path | None = None,
) -> Path:
    """Convert a vector file to GeoParquet. ``add`` and ``check --fix`` both use this (#947).

    Applies the catalog's ``conversion.vector`` settings, which sort rows and
    add a bbox covering column by default (issue #805). A ``.parquet`` source
    is copied unless :func:`_needs_spatial_rewrite` says it must be rewritten.

    ``add`` writes one file. A configured spatial index becomes a column of
    that file, whatever ``partition`` says. ``check --fix`` passes
    ``partition_dir`` instead, and the table is written as Hive partitions by
    that index. Each source gets its own ``partition_dir``, because two
    sources in one directory share cells, and geoparquet-io does not write over
    a cell that exists.

    Args:
        source: Source vector file.
        dest_dir: Destination directory.
        catalog_root: Catalog root, for reading ``conversion.vector`` settings.
            None uses the built-in defaults.
        reconvert: True when the operator asked to re-convert from source, which
            rewrites a Parquet file whatever its footer already declares.
        settings: Explicit vector settings. None reads them from the catalog.
        partition_dir: Directory for Hive partitions by the configured spatial
            index. It applies only to a non-Parquet source, when the settings
            set ``partition`` and a spatial index.

    Returns:
        Path to the output GeoParquet file, or ``partition_dir`` when the
        table was written as partitions.
    """
    import geoparquet_io as gpio  # type: ignore[import-untyped]

    from portolan_cli.convert import _write_partitioned, repair_shapefile_encoding_sidecar

    output_path = dest_dir / f"{source.stem}.parquet"
    if settings is None:
        settings = get_vector_settings(catalog_root)

    # A shapefile with a missing or wrong .cpg converts with the right encoding.
    if source.suffix.lower() == ".shp":
        repair_shapefile_encoding_sidecar(source)

    # Check if already GeoParquet
    if source.suffix.lower() == ".parquet":
        same_file = source.resolve() == output_path.resolve()
        if not _needs_spatial_rewrite(source, settings, reconvert=reconvert):
            if not same_file:
                shutil.copy2(source, output_path)
            return output_path

        reason = "re-convert requested" if reconvert else "it carries no bbox covering column"
        _announce_rewrite(source, reason)
        if same_file:
            # A single-file collection keeps its data where the operator put it,
            # so there is no separate destination to write to. Write a sibling
            # and swap it in, because geoparquet-io cannot write the file it
            # reads.
            _rewrite_or_keep(source, settings)
        else:
            # Copy first, then rewrite the copy. Converting straight into
            # `output_path` would skip `_assert_rewrite_kept_everything`, so a
            # `.parquet` source would silently lose its CRS on this path while
            # the same file kept it on the path above.
            shutil.copy2(source, output_path)
            _rewrite_or_keep(output_path, settings)
        return output_path

    # A non-Parquet source. Convert with the geoparquet-io fluent API, wrapped
    # in the shared retry so a transient DuckDB "Query interrupted" does not
    # fail a bulk add (Issue #339 nightly test_add_1000_files_* flake); this is
    # the code path add and check --fix use. A partitioned write adds the index
    # column at the size it partitions by, so the converter leaves it out.
    if not (settings.partition and settings.spatial_index != "none"):
        partition_dir = None
    resolved = settings

    def _run() -> None:
        table = apply_vector_settings(
            gpio.convert(str(source)), resolved, partitioned=partition_dir is not None
        )
        if partition_dir is None:
            with_provenance(table, source).write(str(output_path))
        else:
            _write_partitioned(table, partition_dir, resolved)

    run_with_transient_convert_retry(_run, source_name=source.name)

    return output_path if partition_dir is None else partition_dir


# Above this size, the rewrite is slow enough that the operator should know why
# `add` appears to stall, and should know it needs the free space. The rewrite
# still runs: `add` produces conforming output without asking (issue #805).
LARGE_REWRITE_WARN_BYTES = 1024 * 1024 * 1024


def _announce_rewrite(source: Path, reason: str) -> None:
    """Tell the operator a rewrite starts, and how much work it is.

    The rewrite is a full geoparquet-io read, sort, and write, not the copy
    this path used to do. It needs room for a second copy of the file while it
    runs. A large file gets an explicit warning, because the read and the sort
    take long enough to look like a hang.

    Args:
        source: The file about to be rewritten.
        reason: Why the rewrite runs, in the operator's terms.
    """
    # A rewrite changes the operator's file, so the notice is user-facing and
    # shows by default. It goes through the output writer, which sends it to
    # stderr in JSON mode (#1005). Do not demote it to logging.info.
    from portolan_cli.output import info as info_output
    from portolan_cli.output import warn
    from portolan_cli.utils import format_size

    size_bytes = source.stat().st_size
    size = format_size(size_bytes)
    info_output(f"Rewriting {source.name} ({size}): {reason}")
    if size_bytes >= LARGE_REWRITE_WARN_BYTES:
        warn(
            f"{source.name} is {size}. The rewrite reads, sorts, and writes the "
            f"whole file, and needs {size} of free space while it runs. Set "
            f"add_bbox: false in .portolan/config.yaml to skip it."
        )


def _rewrite_or_keep(source: Path, settings: VectorSettings) -> None:
    """Rewrite a GeoParquet in place, or keep it and say why the rewrite failed.

    ``add`` used to copy a ``.parquet`` source through untouched, so a file
    geoparquet-io cannot read or cannot write faithfully still reached the
    catalog. The rewrite must not turn that into a failed ``add``, and it must
    not replace the operator's data with worse data (issue #805).

    A file kept this way is still non-conformant, and ``check`` reports it. The
    operator sees the reason on the spot rather than a stack trace.

    Args:
        source: GeoParquet file to rewrite.
        settings: Vector conversion settings to apply.
    """
    from portolan_cli.output import warn

    try:
        _rewrite_parquet_in_place(source, settings)
    except RewriteFidelityError as err:
        warn(f"Kept {source.name} as it is: it would lose {err.lost}")
    except Exception as err:  # noqa: BLE001 - a copy is better than a failed add
        logger.info("Rewrite of %s failed: %s", source, err, exc_info=True)
        warn(f"Kept {source.name} as it is: geoparquet-io could not rewrite it ({err})")


def _rewrite_parquet_in_place(source: Path, settings: VectorSettings) -> None:
    """Rewrite a GeoParquet file with the vector settings applied.

    Writes a sibling temporary file and swaps it in with :meth:`Path.replace`,
    which is atomic within one filesystem. The source keeps its bytes if the
    write raises, so a failed rewrite cannot destroy the operator's data.

    The temporary file is a hidden sibling and carries
    :data:`REWRITE_TEMP_INFIX` in its name. ``add`` collects the source
    directory before it converts anything, and a hard kill skips the cleanup
    below, so a leftover file would otherwise be collected as a source on the
    next run. :func:`is_rewrite_temp` lets ``add`` skip it.

    geoparquet-io 1.7.0 writes a fresh ``geo`` key and keeps every other footer
    key a publisher set, such as a provenance key (gpio#1280). It drops
    ``pandas`` on purpose, because that key describes the columns of the input
    (gpio#1268). So the rewrite writes the file once.
    :func:`_assert_rewrite_kept_everything` refuses a rewrite that drops a
    publisher key.

    Args:
        source: GeoParquet file to rewrite.
        settings: Vector conversion settings to apply.
    """
    import geoparquet_io as gpio

    temp_path = source.with_name(f".{source.stem}{REWRITE_TEMP_INFIX}.parquet")
    before = read_rewrite_fidelity(source)

    def _run() -> None:
        table = apply_vector_settings(gpio.convert(str(source)), settings)
        table.write(str(temp_path))

    try:
        run_with_transient_convert_retry(_run, source_name=source.name)
        _assert_rewrite_kept_everything(source, temp_path, before)
        temp_path.replace(source)
    finally:
        temp_path.unlink(missing_ok=True)


def _same_crs(before: Any, after: Any) -> bool:
    """Report whether two GeoParquet ``crs`` values mean the same thing.

    The values are PROJJSON, and two encodings of one CRS can differ in text,
    so pyproj decides equivalence. A value pyproj cannot parse falls back to
    equality, which keeps an unreadable pair from passing as a match.

    Args:
        before: The source file's ``crs`` value.
        after: The rewritten file's ``crs`` value.

    Returns:
        True when both describe the same CRS.
    """
    if before == after:
        return True
    try:
        return bool(CRS.from_user_input(before) == CRS.from_user_input(after))
    except Exception:  # noqa: BLE001 - an unparsable CRS is not a proven match
        return False


def _means_wgs84(crs: Any) -> bool:
    """Report whether a GeoParquet ``crs`` value means WGS84.

    GeoParquet reads an absent ``crs`` as OGC:CRS84, which is WGS84 with
    longitude and latitude in that axis order. A writer that omits the key for
    WGS84 data therefore loses no meaning, and geoparquet-io omits it.

    Without this test the fidelity gate refuses every rewrite of a WGS84 file.
    That is the common case, so the file never gains its bbox covering column,
    and a partitioned add rewrites each partition on its own instead (#805).

    Args:
        crs: The source file's ``crs`` value, as PROJJSON or a string.

    Returns:
        True when the value describes WGS84 or CRS84.
    """
    try:
        return _is_wgs84(CRS.from_user_input(crs))
    except Exception:  # noqa: BLE001 - an unparsable CRS is not provably WGS84
        return False


def _assert_rewrite_kept_everything(
    source: Path, rewritten: Path, before: RewriteFidelity | None
) -> None:
    """Refuse a rewrite that would replace good data with worse data.

    The rewrite overwrites the operator's own file, so it has to prove it lost
    nothing before the swap. A GeoParquet that comes back declaring no CRS
    reads as OGC:CRS84 per the specification, so projected coordinates would be
    mislabeled as longitude and latitude.

    geoparquet-io 1.4.0 fixed the write that dropped the CRS (gpio#625), so
    this gate is silent on the normal path. Keep it. It guards a destructive
    operation against any cause, and it is not a workaround for one dependency
    bug.

    This is a gate, not a repair. Portolan does not put the CRS back. It keeps
    the operator's file and says why, and ``check`` still reports the missing
    covering column.

    Args:
        source: The file the rewrite would replace.
        rewritten: The candidate replacement.
        before: The source's fidelity fields, read before the rewrite ran.

    Raises:
        RewriteFidelityError: When the rewrite loses rows, columns, schema
            metadata keys, or the CRS.
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

    # A written CRS that differs from the source is corruption, not a drop, and
    # relabels every coordinate in the file. Compare the meaning rather than the
    # PROJJSON text, because an equivalent CRS can serialize two ways.
    if before.crs is not None and after.crs is not None and not _same_crs(before.crs, after.crs):
        raise RewriteFidelityError(source.name, "its CRS, which the rewrite changed")


def convert_tabular(source: Path, dest_dir: Path, *, state: RunState | None = None) -> Path:
    """Convert tabular file to Parquet using geoparquet-io (Issue #432).

    Routes CSV/TSV/XLSX through gpio.convert().write() — the same pipeline
    as geo files but with geometry_column=None. This ensures consistent
    compression and row-group sizing across all Parquet outputs.

    For plain Parquet files, copies them directly (no re-conversion needed).

    Args:
        source: Source tabular file (CSV, TSV, XLSX, or plain Parquet).
        dest_dir: Destination directory.
        state: The run state, for one hash per file and run.

    Returns:
        Path to the output Parquet file.

    Raises:
        OutputExistsError: Both output names hold a file that ``source`` did
            not write (see :func:`convert.claim_output`).
    """
    import geoparquet_io as gpio

    default_path = dest_dir / f"{source.stem}.parquet"
    output_path = claim_output(source, default_path, state=state)

    # If already Parquet, just copy (no conversion needed)
    if source.suffix.lower() == ".parquet":
        if source.resolve() == output_path.resolve():
            return output_path
        shutil.copy2(source, output_path)
        return output_path

    # Convert CSV/TSV/XLSX using geoparquet-io
    # gpio.convert() auto-detects format and handles non-geo files correctly
    # (logs "Reading as plain table" and returns Table with geometry_column=None)
    # Write with standard Parquet settings (compression, row groups); gpio v1.2.0+
    # handles geometry_column=None correctly in all write strategies. Wrapped in
    # the shared retry so a transient DuckDB "Query interrupted" does not fail a
    # bulk add on the tabular path (Issue #339).
    def _run() -> None:
        table = gpio.convert(str(source))
        with_provenance(table, source).write(str(output_path))

    run_with_transient_convert_retry(_run, source_name=source.name)

    return settle_output(source, default_path, output_path, state=state)


def convert_raster(
    source: Path,
    dest_dir: Path,
    catalog_root: Path | None = None,
    *,
    reoptimize: bool = False,
) -> Path:
    """Convert a raster to a COG. ``add`` and ``check --fix`` both use this (#947).

    A source that is already a COG is kept, unless ``reoptimize`` asks to
    rewrite it with the current settings (issue #530). Otherwise
    ``convert.write_cog`` writes the COG with the catalog's ``conversion.cog``
    settings: DEFLATE, 512x512 tiles, and a predictor and overview resampling
    derived from the source dtype by default (issue #690).

    Args:
        source: Source raster file.
        dest_dir: Destination directory.
        catalog_root: Catalog root, for the COG settings. None uses the defaults.
        reoptimize: Rewrite a source that is already a COG.

    Returns:
        Path to the output COG file.
    """
    from portolan_cli.conversion_config import get_cog_settings
    from portolan_cli.convert import write_cog

    output_path = dest_dir / f"{source.stem}.tif"
    is_cog = source.suffix.lower() in (".tif", ".tiff") and is_cloud_optimized_geotiff(source)
    if is_cog and not reoptimize:
        if source.resolve() != output_path.resolve():
            shutil.copy2(source, output_path)
        return output_path
    return write_cog(source, dest_dir, get_cog_settings(catalog_root))
