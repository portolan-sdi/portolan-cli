"""Add pipeline module - manages the add/list/info/remove workflow.

This module orchestrates the complete workflow for managing collections in a
Portolan catalog:
1. Format detection (route to vector or raster handler)
2. Conversion to cloud-native format (GeoParquet or COG)
3. Metadata extraction
4. STAC item/collection creation
5. versions.json update
6. File staging

All logic lives here; the CLI is a thin wrapper.
"""

from __future__ import annotations

import contextlib
import json
import logging
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pystac

from portolan_cli.collection import (
    _compute_union_bbox,
    _get_metadata_yaml_bbox,
    _get_sibling_collection_bboxes,
)
from portolan_cli.collection_id import (
    infer_nested_collection_id,
    resolve_collection_id,  # noqa: F401
)
from portolan_cli.config import get_ignored_files, load_merged_metadata
from portolan_cli.constants import (
    GEOSPATIAL_EXTENSIONS,
    TABULAR_EXTENSIONS,
)
from portolan_cli.conversion_config import (
    VectorSettings,
    get_vector_settings,
    resolve_partition_settings,
    settings_override,
)
from portolan_cli.convert import convert_multilayer_file
from portolan_cli.discovery import get_sidecars, iter_files_with_sidecars, iter_geospatial_files
from portolan_cli.errors import (
    AssetKeyCollisionError,
    MissingLicenseError,
    NoGeometryError,
    PartitionCollisionError,
)
from portolan_cli.finalization import (
    CompanionAsset,
    add_companion_assets,
    apply_human_metadata_to_dict,
    stage_companions,
)

# Batch finalization (STAC-write + backend coordination) was extracted to
# finalization.py (issue #624). add.py orchestrates on top of it: add_files /
# add / add_directory call finalize_items, and _ensure_tabular_collection reuses
# _update_catalog_links. These names are re-exported so the finalize helpers
# resolve through add's namespace (keeping test patches of
# ``portolan_cli.add.finalize_items`` effective) and the test-suite keeps
# importing them from this module. The edge is one-directional (add ->
# finalization); finalization imports nothing from add.
from portolan_cli.finalization import (  # noqa: F401
    _asset_freshness_fields as _asset_freshness_fields,  # noqa: PLC0414
)
from portolan_cli.finalization import (  # noqa: F401
    _ensure_partition_metadata as _ensure_partition_metadata,  # noqa: PLC0414
)
from portolan_cli.finalization import (  # noqa: F401
    _finalize_with_backend as _finalize_with_backend,  # noqa: PLC0414
)
from portolan_cli.finalization import (  # noqa: F401
    _fix_collection_links as _fix_collection_links,  # noqa: PLC0414
)
from portolan_cli.finalization import (  # noqa: F401
    _get_or_create_collection as _get_or_create_collection,  # noqa: PLC0414
)
from portolan_cli.finalization import (  # noqa: F401
    _save_collection_with_links as _save_collection_with_links,  # noqa: PLC0414
)
from portolan_cli.finalization import (
    _update_catalog_links as _update_catalog_links,  # noqa: PLC0414
)
from portolan_cli.finalization import (
    apply_human_metadata as apply_human_metadata,  # noqa: PLC0414
)
from portolan_cli.finalization import (
    finalize_items as finalize_items,  # noqa: PLC0414
)
from portolan_cli.format_families import VECTOR
from portolan_cli.formats import (
    in_run,
    is_geoparquet,
    is_multilayer,
    list_layers,
)
from portolan_cli.hookspecs import EnrichContext
from portolan_cli.humanize import humanize_slug
from portolan_cli.json_io import write_json_atomic
from portolan_cli.licensing import LICENSE_REL, license_gap, resolve_license
from portolan_cli.plugins.formats import handler_for, plugin_extensions
from portolan_cli.preparation import (
    _MEDIA_TYPE_MAP as _MEDIA_TYPE_MAP,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    _ROLE_MAP as _ROLE_MAP,  # noqa: PLC0414
)

# Per-item preparation and conversion routing were extracted to preparation.py
# (issue #623). add.py orchestrates on top of it: some names below are used
# directly by the finalize/asset-update code that stays here, others are
# re-exported so external callers (external.py) and the test-suite keep importing
# them from this module. The dependency edge is one-directional (add ->
# preparation); preparation imports nothing from add.
from portolan_cli.preparation import (  # noqa: F401
    IGNORED_FILES as IGNORED_FILES,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    ROLE_SOURCE,
    ignored_by,
    is_rewrite_temp,
)
from portolan_cli.preparation import (
    PreparedItem as PreparedItem,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    _convert_and_extract_metadata as _convert_and_extract_metadata,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    _create_and_save_item as _create_and_save_item,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    _derive_item_id_and_asset_level as _derive_item_id_and_asset_level,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    _get_asset_role as _get_asset_role,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    _get_media_type as _get_media_type,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    _pre_validate_geometry as _pre_validate_geometry,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    _scan_item_assets as _scan_item_assets,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    _warn_if_source_newer as _warn_if_source_newer,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    convert_raster as convert_raster,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    convert_tabular as convert_tabular,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    convert_vector as convert_vector,  # noqa: PLC0414
)
from portolan_cli.preparation import (
    prepare_item as prepare_item,  # noqa: PLC0414
)
from portolan_cli.query import ItemInfo, get_item_info, is_current, list_items  # noqa: F401
from portolan_cli.remove import remove_files  # noqa: F401
from portolan_cli.stac import (
    MergeStrategy,
    create_collection,
    update_catalog_provenance,
)
from portolan_cli.stac_enrich import enrich_collection_file, enrich_item_file
from portolan_cli.stac_parquet import PARQUET_FILENAME
from portolan_cli.trace import span, submit
from portolan_cli.versions import Asset, versions_asset_key
from portolan_cli.viz.pmtiles_links import PMTILES_SUFFIX
from portolan_cli.viz.style import enrich_cog_assets

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from portolan_cli.run_state import RunState


logger = logging.getLogger(__name__)

# Error message patterns for parquet files without geometry (Issue #177).
# These patterns indicate a parquet file lacks GeoParquet metadata (no 'geo' key),
# meaning it's tabular data that should be tracked as an auxiliary asset.
_PARQUET_NO_GEOMETRY_PATTERNS: tuple[str, ...] = (
    "missing bounding box",
    "no valid geometry",
)


def _is_parquet_no_geometry_error(err: ValueError) -> bool:
    """Check if a ValueError indicates a parquet file lacks geometry (Issue #177).

    This handles the case where a parquet file is valid but has no GeoParquet
    metadata (no 'geo' key in schema). Such files should be tracked as auxiliary
    assets, not rejected.

    Args:
        err: The ValueError to check.

    Returns:
        True if the error is specifically about missing geometry in a parquet file.
    """
    err_msg = str(err).lower()
    return any(pattern in err_msg for pattern in _PARQUET_NO_GEOMETRY_PATTERNS)


def _batch_sibling_names(sources: list[Path]) -> frozenset[str]:
    """Base filenames of a batch's source files plus their converted outputs.

    Used to prune cross-item re-scanning of collection-level assets (issue #465).
    For each source we include its filename and its deterministic converted
    output ``{stem}.parquet`` (which equals the source for cloud-native parquet).

    Names, not paths, so the per-sibling membership check in ``_scan_item_assets``
    is a plain set lookup — no ``resolve()``/``stat`` syscall per (file × sibling),
    which would reintroduce O(n²) I/O. This is collision-safe: siblings only ever
    share the one collection directory, every batch name carries a geospatial or
    ``.parquet`` extension, and any real file with such a name is itself tracked
    by its own ``prepare_item`` (or the deferred non-geo pass). Loose companions
    that rely solely on the scan (``.txt``/``.png``/``.xml`` …) can never match.

    Multi-layer sources (GeoPackage/FileGDB) expand to one
    ``{stem}_{layer}.parquet`` output per layer (``convert_multilayer_file``),
    each tracked as its own layer item; their names are included so sibling
    layers skip each other too. ``list_layers`` is cheap for single-layer
    formats (extension check, no GDAL open) and reuses the exact naming
    ``convert_multilayer_file`` applies.
    """
    names: set[str] = set()
    for src in sources:
        names.add(src.name)
        names.add(f"{src.stem}.parquet")
        try:
            layers = list_layers(src)
        except Exception:
            # Pruning is a best-effort optimization; a listing failure only
            # means a few extra sibling scans, never incorrect output.
            layers = None
        if layers:
            for layer in layers:
                names.add(f"{src.stem}_{layer}.parquet")
    return frozenset(names)


@dataclass
class AddFailure:
    """Information about a failed add operation.

    Used by add_files to report files that could not be processed.

    Attributes:
        path: Path to the file that failed to add.
        error: Human-readable error message describing the failure.
    """

    path: Path
    error: str


def _maybe_partition_large_file(
    prepared: PreparedItem,
    catalog_root: Path,
    item_datetime: datetime | None,
    skip_partitioning: bool = False,
    state: RunState | None = None,
) -> list[PreparedItem]:
    """Partition a large GeoParquet file if it exceeds the size threshold.

    Issue #352: Files > 2GB should be spatially partitioned.
    Each partition becomes a STAC Item with its own bbox.

    Args:
        prepared: The prepared item to potentially partition.
        catalog_root: Root directory of the catalog.
        item_datetime: Optional datetime for created items.
        skip_partitioning: If True, skip partitioning even if file exceeds threshold.
            Used when user declines interactive prompt.
        state: The run state, which parses the config once per run.

    Raises:
        PartitionCollisionError: The collection already holds partitions.
            A converted file that this run wrote is removed first, so no row
            appears twice. A file that existed before the run stays.

    Returns:
        List of PreparedItems. If partitioning occurred, contains multiple
        items (one per partition). Otherwise, returns [prepared] unchanged.
    """
    from portolan_cli.partitioning import (
        build_glob_pattern,
        get_partition_metadata,
        partition_columns_of,
        strategy_of_column,
    )
    from portolan_cli.plugins.formats import handler_named

    if skip_partitioning:
        return [prepared]

    # The format handler owns the size partition rule (#947).
    handler = handler_named(prepared.format_name) if prepared.format_name else None
    partition = getattr(handler, "partition", None)
    if partition is None:
        return [prepared]

    parquet_files = [
        path
        for filename, (path, _checksum, _size) in prepared.asset_files.items()
        if filename.endswith(".parquet")
    ]
    if not parquet_files:
        return [prepared]

    primary_parquet = parquet_files[0]
    partition_output_dir = primary_parquet.parent
    # The handler takes no state, so the settings reach it through the override.
    override = (
        settings_override(partition=resolve_partition_settings(partition_output_dir, state))
        if state is not None
        else contextlib.nullcontext()
    )
    try:
        with override:
            partition_files: list[Path] = partition(primary_parquet, catalog_root)
    except PartitionCollisionError:
        if prepared.created_output:
            primary_parquet.unlink(missing_ok=True)
        raise
    if not partition_files:
        return [prepared]
    columns = partition_columns_of(partition_files)
    strategy = strategy_of_column(columns[0]) if columns else "kdtree"

    if prepared.item_json_path and prepared.item_json_path.exists():
        prepared.item_json_path.unlink()

    # Delete original large file (now replaced by partitions)
    if primary_parquet.exists():
        primary_parquet.unlink()

    # Create PreparedItem for each partition
    partitioned_items: list[PreparedItem] = []

    for partition_path in partition_files:
        # Create STAC item for this partition
        # item_id auto-derived from partition_path.parent.name (e.g., "kdtree_cell=0000000000")
        partition_prepared = prepare_item(
            path=partition_path,
            catalog_root=catalog_root,
            collection_id=prepared.collection_id,
            item_datetime=item_datetime,
            state=state,
        )
        partitioned_items.append(partition_prepared)

    # Add collection-level glob asset for bulk access (Issue #351)
    # This provides a single glob URL for DuckDB/PyArrow/GDAL to read all partitions
    glob_pattern = build_glob_pattern(strategy, partition_columns=columns)
    glob_asset = pystac.Asset(
        href=glob_pattern,
        media_type="application/vnd.apache.parquet",
        roles=["data"],
        title="Partitioned GeoParquet",
        description=f"Glob pattern for {len(partition_files)} spatial partitions",
        # partition:glob will be populated on push with the remote URL
    )

    # Extract partition metadata for STAC partition extension (Issue #232)
    partition_meta = get_partition_metadata(partition_output_dir, strategy)

    # Create a PreparedItem for the glob asset (collection-level)
    # Use original item_id as base to avoid collisions across collections
    glob_item_id = f"{prepared.item_id}_partitioned"
    glob_prepared = PreparedItem(
        item_id=glob_item_id,
        collection_id=prepared.collection_id,
        format_type=VECTOR,
        bbox=prepared.bbox,
        asset_files={},  # No physical files - glob is a pattern reference
        item_json_path=None,
        is_collection_level_asset=True,
        stac_item=None,
        stac_assets={glob_item_id: glob_asset},
        metadata=None,
        partition_metadata=partition_meta,
    )
    partitioned_items.append(glob_prepared)

    return partitioned_items


def add(
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
) -> ItemInfo:
    """Add files to a Portolan catalog.

    This is a convenience wrapper around prepare_item() + finalize_items()
    for adding a single file. For batch operations, use those functions directly
    to achieve O(n) versioning instead of O(n²). See Issue #281.

    Args:
        path: Path to the source file.
        catalog_root: Root directory of the catalog.
        collection_id: Collection to add the data to.
        title: Optional display title for the item.
        description: Optional description.
        item_id: Optional item ID (defaults to parent directory name).
        item_datetime: Optional acquisition/creation datetime.
            If None, uses null datetime with open interval.
        force: If True, bypass change detection and re-process (Issue #386).
        reconvert: If True, re-convert from source (requires force=True).

    Returns:
        ItemInfo with details about the added item.

    Raises:
        ValueError: If the format is unsupported or collection_id is invalid.
        FileNotFoundError: If the source file doesn't exist.
    """
    from portolan_cli.version_ops import owned_run

    with owned_run(catalog_root) as state:
        # Prepare: extract metadata, convert, create STAC item
        prepared = prepare_item(
            path=path,
            catalog_root=catalog_root,
            collection_id=collection_id,
            title=title,
            description=description,
            item_id=item_id,
            item_datetime=item_datetime,
            force=force,
            reconvert=reconvert,
            state=state,
        )

        # Finalize: write collection.json and stage the version
        results = finalize_items(catalog_root, [prepared], state=state)

    # Return the single result with title/description preserved
    result = results[0]
    return ItemInfo(
        item_id=result.item_id,
        collection_id=result.collection_id,
        format_type=result.format_type,
        bbox=result.bbox,
        asset_paths=result.asset_paths,
        title=title,
        description=description,
    )


def _new_tabular_collection(
    catalog_root: Path,
    collection_id: str,
    collection_dir: Path,
    state: RunState,
) -> pystac.Collection:
    """Build the collection of standalone tabular data in memory (Issue #432).

    For tabular-only collections (no geometry), the spatial extent comes from
    (in priority order):
    1. Explicit bbox in metadata.yaml (manual override)
    2. Inherited from sibling geo collections (AOI inheritance)
    3. Global fallback [-180, -90, 180, 90]

    Per design decision: companion tabular data is almost always about the same
    area as the catalog's geo data, so inheriting the AOI is correct and zero-friction.
    The caller writes the collection once, with its assets.

    Args:
        catalog_root: Root directory of the catalog.
        collection_id: Collection identifier.
        collection_dir: Path to the collection directory.
        state: The run state, which parses each metadata.yaml once per run.

    Returns:
        The new collection, not yet on disk.
    """
    # Priority 1: Check metadata.yaml for explicit bbox
    explicit_bbox = _get_metadata_yaml_bbox(collection_dir)
    if explicit_bbox is not None:
        bbox_source = "metadata.yaml"
        final_bbox = explicit_bbox
        sibling_count = 0
    else:
        # Priority 2: AOI inheritance from sibling geo collections
        sibling_bboxes = _get_sibling_collection_bboxes(catalog_root)
        final_bbox = _compute_union_bbox(sibling_bboxes)  # Falls back to global if empty
        sibling_count = len(sibling_bboxes)
        bbox_source = "sibling" if sibling_count > 0 else "global"
    logger.info(
        "Creating tabular collection %s with extent from %s (%d sibling collection(s))",
        collection_id,
        bbox_source,
        sibling_count,
    )

    # Issue #502: human-readable title; description defaults to it.
    tabular_title = humanize_slug(collection_id)
    collection = create_collection(
        collection_id=collection_id,
        description=tabular_title,
        title=tabular_title,
        bbox=final_bbox,
    )

    # A tabular-only add never reaches finalize_items, so apply the same
    # human-authored title, license, providers, and derived provenance
    # metadata.yaml carries for geo collections (issue #654 / issue #684 / #755).
    apply_human_metadata(collection, collection_dir, catalog_root, state)
    return collection


def _sweep_catalog(catalog_root: Path, state: RunState) -> None:
    """Run the catalog-wide conformance sweeps after a new tabular collection.

    ``finalize_items`` runs the same sweeps for the collections it writes. A
    tabular collection that phase 3 creates needs them too: the root title
    from metadata.yaml (issue #815), the child link titles (issue #502), the
    AGENTS.md and README.md files with their links and the profile schema URI
    (issue #654), and the root sync stamp of an all-mirror tree (issue #684).
    """
    from portolan_cli.agents_md import ensure_agents_md_tree
    from portolan_cli.catalog import (
        apply_catalog_human_titles,
        ensure_link_titles,
        ensure_schema_uris,
    )
    from portolan_cli.readme import ensure_readmes

    apply_catalog_human_titles(catalog_root, state=state)
    ensure_link_titles(catalog_root)
    ensure_agents_md_tree(catalog_root)
    ensure_schema_uris(catalog_root)
    ensure_readmes(catalog_root, state=state)
    update_catalog_provenance(catalog_root)


def add_directory(
    *,
    path: Path,
    catalog_root: Path,
    collection_id: str,
    recursive: bool = True,
    force: bool = False,
    reconvert: bool = False,
) -> list[ItemInfo]:
    """Add all geospatial files in a directory to a collection.

    Uses batch versioning (Issue #281) for O(n) instead of O(n²) performance.

    Args:
        path: Directory containing geospatial files.
        catalog_root: Root directory containing .portolan/.
        collection_id: Collection to add files to.
        recursive: If True, process subdirectories recursively.
        force: If True, bypass change detection and re-process (Issue #386).
        reconvert: If True, re-convert from source (requires force=True).

    Returns:
        List of ItemInfo for each added item.
    """
    files = list(iter_geospatial_files(path, recursive=recursive))

    # Issue #465: skip cross-item siblings when scanning collection-level assets
    # so per-file work stays O(n) instead of O(n²).
    batch_exclude_names = _batch_sibling_names(files)

    from portolan_cli.version_ops import owned_run

    with owned_run(catalog_root) as state:
        # Phase 1: Prepare all items (GDAL work, parallelizable)
        prepared = [
            prepare_item(
                path=file_path,
                catalog_root=catalog_root,
                collection_id=collection_id,
                force=force,
                reconvert=reconvert,
                exclude_sibling_names=batch_exclude_names,
                state=state,
            )
            for file_path in files
        ]

        # Phase 2: Finalize (collection.json once, one staged version)
        return finalize_items(catalog_root=catalog_root, prepared=prepared, state=state)


# ─────────────────────────────────────────────────────────────────────────────
# Sidecar auto-detection (per issue #97)
# ─────────────────────────────────────────────────────────────────────────────

# Note: SIDECAR_PATTERNS imported from portolan_cli.constants


def _copy_non_geo_to_item_dir(
    file_path: Path,
    item_dir: Path,
) -> Path:
    """Copy a non-geospatial file to an item directory as a companion asset.

    ALL files in item directories should be tracked as STAC assets.
    Non-geospatial CSV/TSV files are copied (not converted) and tracked alongside
    the primary geospatial data.

    Args:
        file_path: Source file path.
        item_dir: Destination item directory.

    Returns:
        Path to the copied file in item_dir.
    """
    dest_path = item_dir / file_path.name
    if dest_path.exists() and dest_path.resolve() == file_path.resolve():
        # Already in place
        return dest_path
    shutil.copy2(file_path, dest_path)
    return dest_path


def _ensure_nested_catalogs(
    collection_id: str, catalog_root: Path, setup_collections: set[str]
) -> None:
    """Ensure intermediate catalogs exist for nested collection IDs.

    Args:
        collection_id: The collection ID (may be nested like "climate/hittekaart").
        catalog_root: Root directory of the catalog.
        setup_collections: Set of already-setup collection IDs (mutated).
    """
    if collection_id in setup_collections:
        return

    from portolan_cli.catalog import create_intermediate_catalogs

    create_intermediate_catalogs(collection_id, catalog_root)
    setup_collections.add(collection_id)


#: Skip reasons for a file that is in the catalog after the add. Every other
#: reason means the file stayed out of the catalog.
SKIP_UNCHANGED = "unchanged since the last add"
SKIP_COMPANION = "tracked as an asset next to its geospatial file, not converted"
SKIP_ITEM_MIRROR = "it is the STAC-GeoParquet item mirror"
SKIP_REWRITE_TEMP = "it is a scratch file of an interrupted rewrite"
SKIP_LONE_PMTILES = (
    "PMTiles is a visualization derivative. "
    "Add the GeoParquet it renders, and keep the tiles beside it."
)
SKIPS_THAT_TRACK = frozenset({SKIP_UNCHANGED, SKIP_COMPANION, SKIP_ITEM_MIRROR})


def _ignore_reason(
    file_path: Path, catalog_root: Path, patterns: list[str], state: RunState | None = None
) -> str | None:
    """Return why ``ignored_files`` excludes ``file_path`` from the add, or None.

    A pattern that matches the converted output would leave the source as the
    data asset, so the add stops before conversion and names the output.
    """
    pattern = ignored_by(file_path, patterns)
    if pattern is not None:
        return f"it matches ignored_files pattern {pattern!r}"
    handler = handler_for(file_path, catalog_root, state=state)
    if handler is None:
        return None
    output = handler.output_path(file_path, file_path.parent)
    pattern = ignored_by(output, patterns)
    if pattern is None:
        return None
    return f"its output {output.name} matches ignored_files pattern {pattern!r}"


def _unchanged_for_add(
    file_path: Path, collection_dir: Path, catalog_root: Path, state: RunState | None
) -> bool:
    """Check a tabular item companion against its owner's record as well.

    Format-based inference places a CSV at collection level. When it already
    belongs to a raster item, the exact item-relative key in the parent's
    version record supplies its baseline before any preparation or writes.
    """
    if is_current(file_path, collection_dir / "versions.json", state=state):
        return True
    owner = file_path.parent.parent
    if (
        state is None
        or file_path.suffix.lower() not in TABULAR_EXTENSIONS
        or owner.resolve() == catalog_root.resolve()
    ):
        return False
    record = state.tracked_assets(owner).get(versions_asset_key(file_path, owner))
    return record is not None and state.unchanged(file_path, record)


def _collect_files_for_add(
    paths: list[Path],
    catalog_root: Path,
    collection_id: str | None,
    skipped: list[Path],
    setup_collections: set[str],
    *,
    force: bool = False,
    skip_reasons: dict[Path, str] | None = None,
    state: RunState | None = None,
) -> list[tuple[Path, str]]:
    """Collect and filter files for add operation (Phase 1).

    This is the fast, sequential phase that doesn't involve GDAL.
    Extracts from add_files() to reduce cyclomatic complexity.

    Args:
        paths: List of paths to add (files or directories).
        catalog_root: Root directory of the catalog.
        collection_id: Optional explicit collection ID.
        skipped: List to append skipped paths to (mutated).
        setup_collections: Set to track which collections have been set up (mutated).
        force: If True, bypass change detection (Issue #386).
        skip_reasons: Receives the reason for each file this phase skips (mutated).
        state: The run state, which parses the config once per run.

    Returns:
        List of (file_path, collection_id) tuples to process.
    """
    processed_paths: set[Path] = set()
    files_to_process: list[tuple[Path, str]] = []
    reasons = skip_reasons if skip_reasons is not None else {}

    def skip(file_path: Path, reason: str) -> None:
        skipped.append(file_path)
        reasons[file_path] = reason

    geo_extensions = GEOSPATIAL_EXTENSIONS | plugin_extensions()
    ignored_patterns = get_ignored_files(catalog_root, state=state)

    for path in paths:
        files = iter_files_with_sidecars(path) if path.is_dir() else [path] + get_sidecars(path)

        for file_path in files:
            # Resolve symlinks to track the real file
            if file_path.is_symlink():
                file_path = file_path.resolve()

            if file_path in processed_paths:
                continue
            processed_paths.add(file_path)

            # Skip non-geospatial files. A plugin format handler adds its
            # extensions (#947).
            if file_path.suffix.lower() not in geo_extensions:
                continue

            # `ignored_files` in config.yaml excludes a file from the catalog,
            # with the basename match rule of `portolan list` (#1044).
            ignore_reason = _ignore_reason(file_path, catalog_root, ignored_patterns, state)
            if ignore_reason is not None:
                skip(file_path, ignore_reason)
                continue

            # Never re-ingest the item mirror the add flow itself publishes.
            # The spec reserves items.parquet in the collection root for the
            # STAC-GeoParquet mirror (PORTO-FMT-040), and with default-on
            # generation every item-bearing collection carries one (#654).
            if file_path.name == PARQUET_FILENAME:
                skip(file_path, SKIP_ITEM_MIRROR)
                continue

            # Never ingest the scratch file the in-place GeoParquet rewrite
            # swaps in. It is hidden and cleaned up on the normal path, but a
            # killed run leaves one behind, and this phase reads the directory
            # before conversion starts (issue #805).
            if is_rewrite_temp(file_path):
                skip(file_path, SKIP_REWRITE_TEMP)
                continue

            # Determine collection ID (use full nested path)
            coll_id = collection_id
            if coll_id is None:
                try:
                    coll_id = infer_nested_collection_id(file_path, catalog_root, state=state)
                except ValueError as err:
                    skip(file_path, str(err))
                    continue

            # Check if unchanged (skip this check when force=True per Issue #386)
            if not force:
                collection_dir = catalog_root / Path(*coll_id.split("/"))
                # The run state reads each versions.json once (issue #995).
                if _unchanged_for_add(file_path, collection_dir, catalog_root, state):
                    skip(file_path, SKIP_UNCHANGED)
                    continue

            # Set up nested catalog structure if needed. A tabular file can be
            # the companion of an item, so the collection that phase 1 infers
            # for it can be an item directory. Phase 2 and phase 3 set up the
            # catalogs once they know where the file goes.
            if file_path.suffix.lower() not in TABULAR_EXTENSIONS:
                _ensure_nested_catalogs(coll_id, catalog_root, setup_collections)

            files_to_process.append((file_path, coll_id))

    return _without_lone_pmtiles(files_to_process, catalog_root, skip, state)


def _without_lone_pmtiles(
    files: list[tuple[Path, str]],
    catalog_root: Path,
    skip: Callable[[Path, str], None],
    state: RunState | None = None,
) -> list[tuple[Path, str]]:
    """Skip each PMTiles that renders no GeoParquet.

    portolan-spec formats.md "PMTiles" makes the tiles the visualization
    derivative of the GeoParquet data asset. The spec has no PMTiles that is
    the data of a collection. The GeoParquet is the same-stem file beside the
    tiles, or a same-stem file of this add that converts to GeoParquet.

    Args:
        files: The ``(file, collection_id)`` pairs of phase 1.
        catalog_root: Root directory of the catalog.
        skip: Records a skipped file and its reason.
        state: The run state, which parses the config once per run.

    Returns:
        The pairs without the skipped PMTiles.
    """

    def gives_geoparquet(path: Path) -> bool:
        if path.suffix.lower() == ".parquet":
            return is_geoparquet(path)
        handler = handler_for(path, catalog_root, state=state)
        return (
            handler is not None
            and handler.output_path(path, path.parent).suffix.lower() == ".parquet"
        )

    def is_tiles(path: Path) -> bool:
        return path.suffix.lower() == PMTILES_SUFFIX

    rendered = {(p.parent, p.stem) for p, _ in files if not is_tiles(p) and gives_geoparquet(p)}
    kept: list[tuple[Path, str]] = []
    for file_path, coll_id in files:
        if is_tiles(file_path) and (file_path.parent, file_path.stem) not in rendered:
            parquet = file_path.with_suffix(".parquet")
            if not (parquet.is_file() and is_geoparquet(parquet)):
                skip(file_path, SKIP_LONE_PMTILES)
                continue
        kept.append((file_path, coll_id))
    return kept


@dataclass(frozen=True)
class _PrepareOptions:
    """Immutable per-batch inputs threaded through the phase-2 prep helpers.

    Bundling these keeps the prep functions module-level (not a closure over
    ``add_files``) so nesting stays shallow and each helper is independently
    unit-testable. The instance is read-only, so it is safe to share across
    worker threads in the parallel path.
    """

    catalog_root: Path
    item_id: str | None
    item_datetime: datetime | None
    force: bool
    reconvert: bool
    skip_partitioning: bool
    batch_exclude_names: frozenset[str]
    state: RunState | None = None
    #: The ``conversion.vector`` settings of the catalog, resolved once per batch.
    vector_settings: VectorSettings | None = None


@dataclass
class _ProcessResult:
    """Accumulated output of phase 2 (per-item preparation).

    ``prepared_items`` feed ``finalize_items``; ``deferred_non_geo`` plus the
    ``source_to_*`` maps drive phase 3 (deferred non-geo companion assets);
    ``failures`` are surfaced to the caller (Issue #175). ``skips`` holds the
    reason for each file that stays out because two files claim one asset key.
    """

    prepared_items: list[PreparedItem] = field(default_factory=list)
    failures: list[AddFailure] = field(default_factory=list)
    skips: dict[Path, str] = field(default_factory=dict)
    deferred_non_geo: list[tuple[Path, Path, str]] = field(default_factory=list)
    source_to_item_dir: dict[Path, tuple[Path, str, str]] = field(default_factory=dict)
    source_to_collection_dir: dict[Path, tuple[Path, str]] = field(default_factory=dict)


def _prepare_multilayer_file(
    file_path: Path,
    coll_id: str,
    opts: _PrepareOptions,
) -> tuple[list[PreparedItem], list[AddFailure], dict[Path, str]]:
    """Convert + prepare every layer of a multi-layer file (Issue #265).

    GeoPackage / FileGDB are split into one parquet (and one PreparedItem) per
    layer. A failure preparing a single layer is recorded and the remaining
    layers continue (Issue #175); a failure of the conversion itself fails the
    whole file with a single AddFailure. A layer output that two files claim
    one asset key for is a skip, as a single file is.
    """
    prepared_list: list[PreparedItem] = []
    failure_list: list[AddFailure] = []
    skips: dict[Path, str] = {}

    try:
        # The batch resolves the vector settings once. Then convert all layers.
        vector_settings = opts.vector_settings or get_vector_settings(
            opts.catalog_root, state=opts.state
        )
        # prepare_item sees each layer file after it exists, so it cannot tell
        # a new layer file from an old one. Record the old ones here.
        existing = set(file_path.parent.glob("*.parquet"))
        results = convert_multilayer_file(
            file_path, file_path.parent, settings=vector_settings, state=opts.state
        )

        for result in results:
            if not (result.success and result.output):
                failure_list.append(
                    AddFailure(path=file_path, error=f"Layer {result.layer}: {result.error}")
                )
                continue
            try:
                prepared = prepare_item(
                    path=result.output,
                    catalog_root=opts.catalog_root,
                    collection_id=coll_id,
                    item_id=None,  # Derive from output filename
                    item_datetime=opts.item_datetime,
                    force=opts.force,
                    reconvert=opts.reconvert,
                    exclude_sibling_names=opts.batch_exclude_names,
                    state=opts.state,
                )
                prepared.created_output = result.output not in existing
                # Apply partitioning to each layer (Issue #352)
                prepared_list.extend(
                    _maybe_partition_large_file(
                        prepared=prepared,
                        catalog_root=opts.catalog_root,
                        item_datetime=opts.item_datetime,
                        skip_partitioning=opts.skip_partitioning,
                        state=opts.state,
                    )
                )
            except AssetKeyCollisionError as err:
                # prepare_item keeps a layer file that existed before its scan.
                # A layer file this run wrote goes, as for a single file.
                if result.output not in existing:
                    result.output.unlink(missing_ok=True)
                skips[result.output] = err.message
            except Exception as err:
                failure_list.append(
                    AddFailure(path=result.output, error=f"Layer {result.layer}: {err}")
                )

        return (prepared_list, failure_list, skips)

    except Exception as err:
        return ([], [AddFailure(path=file_path, error=str(err))], {})


#: What phase 2 makes of one source file: the prepared items, the failures,
#: the deferred non-geo file, and the skips with their reasons.
_FileOutcome = tuple[
    list[PreparedItem], list[AddFailure], tuple[Path, Path, str] | None, dict[Path, str]
]


def _prepare_single_file(
    file_path: Path,
    coll_id: str,
    opts: _PrepareOptions,
) -> _FileOutcome:
    """Prepare one source file inside an ``add.file`` span (issue #991)."""
    with span("add.file", file=file_path.name, collection=coll_id):
        return _prepare_file(file_path, coll_id, opts)


def _prepare_file(
    file_path: Path,
    coll_id: str,
    opts: _PrepareOptions,
) -> _FileOutcome:
    """Prepare one source file for finalization (phase 2, parallelizable).

    Runs ``prepare_item()`` (GDAL work + item.json) but writes no versions.json
    or collection.json — those are batched in ``finalize_items`` (Issue #281).
    Returns ``(prepared_items, failures, deferred, skips)`` where ``deferred``
    is a ``(file, source_dir, collection_id)`` tuple for a non-geo tabular file
    to be tracked as a companion asset in phase 3, else ``None``. Two files
    that claim one asset key make a skip, not a failure. The scan raises
    before any write, so the catalog stays as it was.
    """
    # Multi-layer files (GeoPackage, FileGDB) split into one item per layer.
    if is_multilayer(file_path):
        prepared_list, failure_list, skips = _prepare_multilayer_file(file_path, coll_id, opts)
        return (prepared_list, failure_list, None, skips)

    # Single-layer file - original behavior
    try:
        prepared = prepare_item(
            path=file_path,
            catalog_root=opts.catalog_root,
            collection_id=coll_id,
            item_id=opts.item_id,
            item_datetime=opts.item_datetime,
            force=opts.force,
            reconvert=opts.reconvert,
            exclude_sibling_names=opts.batch_exclude_names,
            state=opts.state,
        )
        # Check if file should be partitioned (Issue #352)
        # Returns multiple PreparedItems if partitioned, else [prepared]
        partitioned = _maybe_partition_large_file(
            prepared=prepared,
            catalog_root=opts.catalog_root,
            item_datetime=opts.item_datetime,
            skip_partitioning=opts.skip_partitioning,
            state=opts.state,
        )
        return (partitioned, [], None, {})

    except AssetKeyCollisionError as err:
        return ([], [], None, {file_path: err.message})

    except NoGeometryError as err:
        if file_path.suffix.lower() in TABULAR_EXTENSIONS:
            return ([], [], (file_path, file_path.parent, coll_id), {})
        return ([], [AddFailure(path=file_path, error=str(err))], None, {})

    except ValueError as err:
        if _is_parquet_no_geometry_error(err) and file_path.suffix.lower() in TABULAR_EXTENSIONS:
            return ([], [], (file_path, file_path.parent, coll_id), {})
        return ([], [AddFailure(path=file_path, error=str(err))], None, {})

    except Exception as err:
        return ([], [AddFailure(path=file_path, error=str(err))], None, {})


def _fold_prepared(
    result: _ProcessResult,
    outcome: _FileOutcome,
    file_path: Path,
    coll_id: str,
    catalog_root: Path,
) -> None:
    """Fold the outcome of one file's preparation into the phase-2 accumulator."""
    prepared_list, failure_list, deferred, skips = outcome
    _record_prepared(result, prepared_list, file_path, coll_id, catalog_root)
    result.failures.extend(failure_list)
    result.skips.update(skips)
    if deferred is not None:
        result.deferred_non_geo.append(deferred)


def _record_prepared(
    result: _ProcessResult,
    prepared_list: list[PreparedItem],
    file_path: Path,
    coll_id: str,
    catalog_root: Path,
) -> None:
    """Fold one file's prepared items into the running phase-2 accumulator.

    Maps the file's source dir to its item dir (item-level) or collection dir
    (collection-level, Issue #383) so phase 3 can place any deferred non-geo
    companion in the right location.
    """
    source_dir = file_path.parent
    collection_dir = catalog_root / Path(*coll_id.split("/"))
    for prepared in prepared_list:
        result.prepared_items.append(prepared)
        if prepared.is_collection_level_asset:
            # Collection-level: map source to collection dir (Issue #383)
            result.source_to_collection_dir[source_dir] = (collection_dir, coll_id)
        else:
            # Item-level: map source to item dir
            item_dir = collection_dir / prepared.item_id
            result.source_to_item_dir[source_dir] = (item_dir, coll_id, prepared.item_id)


def _phase_process(
    files_to_process: list[tuple[Path, str]],
    opts: _PrepareOptions,
    *,
    workers: int,
    on_progress: Callable[[Path], None] | None,
) -> _ProcessResult:
    """Phase 2: prepare every collected file, sequentially or across workers.

    Each file is prepared independently (writes only its own item.json), so the
    work parallelizes cleanly (Issue #281); results are folded into a single
    ``_ProcessResult`` on the main thread. GDAL preparation dominates, so worker
    threads overlap those reads.
    """
    result = _ProcessResult()

    if workers == 1:
        # Sequential execution (original behavior)
        for file_path, coll_id in files_to_process:
            if on_progress is not None:
                on_progress(file_path)
            _fold_prepared(
                result,
                _prepare_single_file(file_path, coll_id, opts),
                file_path,
                coll_id,
                opts.catalog_root,
            )
        return result

    # Parallel execution with ThreadPoolExecutor
    from concurrent.futures import ThreadPoolExecutor, as_completed

    logger.info("Using %d parallel workers for %d files", workers, len(files_to_process))

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_file = {
            submit(executor, _prepare_single_file, fp, cid, opts): (fp, cid)
            for fp, cid in files_to_process
        }
        # Process results as they complete (main thread)
        for future in as_completed(future_to_file):
            file_path, coll_id = future_to_file[future]
            # Call progress callback from the main thread (thread-safe) so the
            # CLI's AddProgressReporter works in parallel mode.
            if on_progress is not None:
                on_progress(file_path)
            _fold_prepared(result, future.result(), file_path, coll_id, opts.catalog_root)

    return result


def _collection_license_on_disk(collection_json_path: Path) -> tuple[str | None, bool]:
    """Read the license and license-link state off a collection.json already on disk.

    A re-add inherits both: ``apply_human_license`` leaves the license alone when
    metadata.yaml declares none, and an existing ``rel="license"`` link survives
    every link rewrite. So a collection a human already licensed by hand stays
    licensed, and the gate must see that rather than demand metadata.yaml repeat it.

    Args:
        collection_json_path: Path to the collection.json, which need not exist.

    Returns:
        Tuple of (license id or None, whether a rel="license" link is present).
    """
    try:
        data = json.loads(collection_json_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None, False

    if not isinstance(data, dict):
        return None, False

    license_id = data.get("license")
    links = data.get("links")
    has_link = isinstance(links, list) and any(
        isinstance(link, dict) and link.get("rel") == LICENSE_REL for link in links
    )
    return (license_id if isinstance(license_id, str) else None), has_link


def _require_licenses(
    catalog_root: Path, collection_ids: set[str], state: RunState | None = None
) -> None:
    """Stop the add when a target collection would end up with no usable license.

    Runs after phase 1, so it costs one metadata read per collection and fires
    before any conversion work, and long before ``finalize_items`` writes a
    collection.json. Raising here rather than at the write seam keeps a refused
    add from leaving converted files behind. Nested catalog scaffolding from phase
    1 may already exist, which carries no license and is written idempotently.

    The license and the link are predicted exactly as ``apply_human_license``
    would apply them: metadata.yaml wins over what is on disk, and either source
    can supply the link.

    Args:
        catalog_root: Root directory of the catalog.
        collection_ids: Collections this add will write.
        state: The run state, which parses each metadata.yaml once per run.

    Raises:
        MissingLicenseError: When a collection would fail PTL-LIC-001, PTL-LIC-002,
            or PTL-LIC-003.
    """
    for coll_id in sorted(collection_ids):
        collection_dir = catalog_root / Path(*coll_id.split("/"))
        on_disk_license, on_disk_link = _collection_license_on_disk(
            collection_dir / "collection.json"
        )
        metadata = (
            state.metadata(collection_dir)
            if state is not None
            else load_merged_metadata(collection_dir, catalog_root)
        )
        declared = resolve_license(metadata)

        license_id = declared.license_id if declared is not None else on_disk_license
        has_license_link = on_disk_link or (
            declared is not None and declared.license_url is not None
        )

        gap = license_gap(license_id, has_license_link=has_license_link)
        if gap is not None:
            raise MissingLicenseError(f"collection '{coll_id}'", gap)


def add_files(
    *,
    paths: list[Path],
    catalog_root: Path,
    collection_id: str | None = None,
    item_id: str | None = None,
    item_datetime: datetime | None = None,
    on_progress: Callable[[Path], None] | None = None,
    workers: int = 1,
    force: bool = False,
    reconvert: bool = False,
    skip_partitioning: bool = False,
    merge_strategy: MergeStrategy = MergeStrategy.SMART,
    skip_reasons: dict[Path, str] | None = None,
    state: RunState | None = None,
) -> tuple[list[ItemInfo], list[Path], list[AddFailure]]:
    r"""Add files to a Portolan catalog.

    This is the main entry point for the `portolan add` command.
    Handles single files, directories, and sidecar auto-detection.

    - Geospatial files (with geometry) are converted to cloud-native format
    - Non-geospatial CSV/TSV files are tracked as companion assets (no conversion)
    - Files must be in a directory with at least one geospatial file to be tracked

    Per Issue #175 ("Continue on errors and report all failures at end"):
    - Continues processing all files even when some fail
    - Collects all errors and reports them at the end
    - Enables batch processing without stopping on first error

    Per Issue #386 ("--force flag for re-tracking files"):
    - force=True bypasses mtime-based change detection
    - reconvert=True also re-converts from source (requires force=True)

    Args:
        paths: List of paths to add (files or directories).
        catalog_root: Root directory of the catalog.
        collection_id: Optional explicit collection ID.
            If not provided (None), the collection is inferred per-file from
            the first directory component relative to catalog_root. This is
            used by `portolan add .` to process multiple collections at once.
            Files at the catalog root level (not in a subdirectory) are skipped
            with a warning when collection_id=None.
        item_id: Optional explicit item ID. If provided, overrides automatic
            derivation from parent directory name. Must be a single path segment
            (no '/', '\\', '.', or '..').
        item_datetime: Optional acquisition/creation datetime.
            If None, defaults to current time but marks item as provisional.
        on_progress: Optional callback invoked before processing each geo file.
            Receives the file path being processed. Use for progress display.
        workers: Number of parallel workers for metadata extraction.
            Default is 1 (sequential). Higher values parallelize GDAL reads.
        force: If True, bypass change detection and re-process all files.
        reconvert: If True, re-convert from source files (requires force=True).
        skip_partitioning: If True, skip partitioning even when a file exceeds
            the size threshold.
        merge_strategy: How to reconcile an existing item's metadata with the
            values this run detects. See ``MergeStrategy`` in ``stac.py``.
        skip_reasons: Receives the reason for each skipped path (mutated). A
            path in ``SKIPS_THAT_TRACK`` is in the catalog. Any other reason
            means the file stayed out of it.
        state: The run state. Without one, this opens a state and commits it.

    Returns:
        Tuple of (added_items, skipped_paths, failures).
        added_items: List of ItemInfo for newly added/updated files.
        skipped_paths: List of paths that were skipped (unchanged or non-geospatial).
        failures: List of AddFailure for files that could not be processed.
    """
    from portolan_cli.version_ops import owned_run

    # Every footer read of the run goes through the run memo (formats.in_run).
    with owned_run(catalog_root, state) as state, in_run(state):
        skipped: list[Path] = []

        # Track which nested collections have had their catalogs set up
        setup_collections: set[str] = set()

        # Phase 1: Collect + filter the files to process (fast, no GDAL).
        with span("add.collect") as stage:
            reasons = skip_reasons if skip_reasons is not None else {}
            files_to_process = _collect_files_for_add(
                paths,
                catalog_root,
                collection_id,
                skipped,
                setup_collections,
                force=force,
                skip_reasons=reasons,
                state=state,
            )
            stage.set(files=len(files_to_process), skipped=len(skipped))
        if not files_to_process:
            return [], skipped, []

        # Phase 1.5: refuse the add before any GDAL work when a target collection would
        # end up with no usable license (issue #686).
        with span("add.licenses"):
            _require_licenses(catalog_root, {coll_id for _, coll_id in files_to_process}, state)

        # Issue #465: filenames of every batch item (sources + converted outputs) so a
        # collection-level asset scan can skip siblings that are tracked as their own
        # items instead of re-scanning the whole flat collection dir (O(n²) → O(n)).
        opts = _PrepareOptions(
            catalog_root=catalog_root,
            item_id=item_id,
            item_datetime=item_datetime,
            force=force,
            reconvert=reconvert,
            skip_partitioning=skip_partitioning,
            batch_exclude_names=_batch_sibling_names([fp for fp, _ in files_to_process]),
            state=state,
            vector_settings=get_vector_settings(catalog_root, state=state),
        )

        # Phase 2: Prepare each file (GDAL work), accumulating prepared items (Issue #281).
        with span("add.process", files=len(files_to_process), workers=workers) as stage:
            proc = _phase_process(
                files_to_process,
                opts,
                workers=workers,
                on_progress=on_progress,
            )
            stage.set(prepared=len(proc.prepared_items), failures=len(proc.failures))
        failures = proc.failures
        for path, reason in proc.skips.items():
            skipped.append(path)
            reasons[path] = reason
        # A tabular file with geometry is a collection of its own. Phase 1 left
        # its catalogs to this point.
        for coll_id in sorted({p.collection_id for p in proc.prepared_items}):
            _ensure_nested_catalogs(coll_id, catalog_root, setup_collections)

        # Phase 2.5: plan the deferred non-geo files, so that a collection that
        # finalize writes takes its companions in the same write.
        plan = _plan_deferred(proc, catalog_root, failures, state)

        # Phase 2.6: finalize writes each collection once (Issue #281).
        with span("add.finalize", items=len(proc.prepared_items)):
            added: list[ItemInfo] = finalize_items(
                catalog_root,
                proc.prepared_items,
                merge_strategy,
                state=state,
                companions=plan.companions,
            )

        # Phase 3: write the deferred files that finalize did not write.
        touched_items: list[tuple[str, str]] = []
        with span("add.deferred_non_geo", files=len(proc.deferred_non_geo)):
            affected_collections = _write_deferred(
                plan,
                {p.collection_id for p in proc.prepared_items},
                catalog_root,
                added=added,
                skipped=skipped,
                merge_strategy=merge_strategy,
                touched_items=touched_items,
                skip_reasons=reasons,
                state=state,
            )

        # Phase 4: run the enrichment hooks on the documents phase 3 changed (#947).
        with span("add.enrich", items=len(touched_items)):
            _enrich_deferred_documents(catalog_root, affected_collections, touched_items)

        # Phase 3 tracks a non-geospatial file next to a geo file as an asset and
        # lists it as skipped, because it is not converted.
        for path in skipped:
            reasons.setdefault(path, SKIP_COMPANION)
        return added, skipped, failures


def _enrich_deferred_documents(
    catalog_root: Path,
    affected_collections: set[Path],
    touched_items: list[tuple[str, str]],
) -> None:
    """Run the enrichment hooks on the items and collections phase 3 changed.

    Args:
        catalog_root: Root directory of the catalog.
        affected_collections: Collection directories that received assets.
        touched_items: ``(collection_id, item_id)`` of each updated item.
    """
    for coll_id, item_id in dict.fromkeys(touched_items):
        item_path = catalog_root / coll_id / item_id / f"{item_id}.json"
        if item_path.exists():
            enrich_item_file(
                item_path,
                EnrichContext(
                    catalog_root=catalog_root,
                    collection_id=coll_id,
                    document_path=item_path,
                    item_id=item_id,
                ),
            )
    for collection_dir in sorted(affected_collections):
        collection_path = collection_dir / "collection.json"
        if collection_path.exists():
            enrich_collection_file(
                collection_path,
                EnrichContext(
                    catalog_root=catalog_root,
                    collection_id=collection_dir.relative_to(catalog_root).as_posix(),
                    document_path=collection_path,
                ),
            )


def _collection_aoi_bbox(collection_dir: Path) -> list[float]:
    """Read a tabular collection's area-of-interest bbox from collection.json.

    The spec requires every collection to carry ``extent.spatial.bbox``. For a
    tabular collection it is the area the data pertains to, not a geometry
    footprint, and validators treat it as informational (portolan-spec
    ``specs/portolan/formats.md``, Tabular). ``_ensure_tabular_collection``
    inherits it from siblings, so it is already on disk by the time we report.

    Args:
        collection_dir: Directory holding the collection.json.

    Returns:
        The collection's spatial bbox, or the global extent if it is absent.
    """
    collection_json = collection_dir / "collection.json"
    if collection_json.exists():
        with Path(collection_json).open(encoding="utf-8") as f:
            bboxes = json.load(f).get("extent", {}).get("spatial", {}).get("bbox", [])
        if bboxes and isinstance(bboxes[0], list):
            return [float(v) for v in bboxes[0]]
    return [-180.0, -90.0, 180.0, 90.0]


#: Tracked files that make a directory a home for companions. A tabular file
#: counts only when it is GeoParquet. Tiles render data and own no companion.
_COMPANION_HOST_EXTENSIONS = GEOSPATIAL_EXTENSIONS - TABULAR_EXTENSIONS - {PMTILES_SUFFIX}


def _hosts_companions(path: Path) -> bool:
    """Report whether a tracked file is geospatial data that companions attach to."""
    suffix = path.suffix.lower()
    if suffix in _COMPANION_HOST_EXTENSIONS or suffix in plugin_extensions():
        return path.exists()
    return suffix == ".parquet" and path.is_file() and is_geoparquet(path)


def _map_recorded_geo_dirs(
    deferred_non_geo: list[tuple[Path, Path, str]],
    source_to_item_dir: dict[Path, tuple[Path, str, str]],
    source_to_collection_dir: dict[Path, tuple[Path, str]],
    catalog_root: Path,
    state: RunState,
) -> None:
    """Map each companion directory that holds an unchanged, tracked geo file.

    Phase 1 skips an unchanged geo file, so phase 2 maps no directory for it.
    A companion that changed beside it still belongs to it. The geo file is in
    the latest version of its collection, so that record names the directory.
    The collection is the directory itself, or its parent for an item. Phase 1
    infers the collection of a lone CSV in an item directory as the item
    directory, so the inferred id is not the answer. The catalog root is never
    a collection, and its ``versions.json`` is the catalog index.

    Args:
        deferred_non_geo: The ``(file, source_dir, collection_id)`` of phase 2.
        source_to_item_dir: The item map of phase 2 (mutated).
        source_to_collection_dir: The collection map of phase 2 (mutated).
        catalog_root: Root directory of the catalog.
        state: The run state, which reads each ``versions.json`` once.
    """
    root = catalog_root.resolve()
    for _file, source_dir, _coll_id in deferred_non_geo:
        if source_dir in source_to_item_dir or source_dir in source_to_collection_dir:
            continue
        for collection_dir in (source_dir, source_dir.parent):
            if collection_dir.resolve() == root:
                break
            if not (collection_dir / "versions.json").exists():
                continue
            hosts = [
                collection_dir / key
                for key in state.tracked_assets(collection_dir)
                if (collection_dir / key).parent == source_dir
            ]
            if not any(_hosts_companions(path) for path in hosts):
                continue
            coll_id = collection_dir.relative_to(catalog_root).as_posix()
            if collection_dir == source_dir:
                source_to_collection_dir[source_dir] = (collection_dir, coll_id)
            else:
                source_to_item_dir[source_dir] = (source_dir, coll_id, source_dir.name)
            break


@dataclass
class _DeferredPlan:
    """Where each deferred file without geometry goes (phase 3).

    Attributes:
        item_companions: ``(file, (item_dir, collection_id, item_id))`` of each
            file that joins an item.
        companions: Collection id to the collection-level files it gains. The
            files of a collection that ``finalize_items`` writes go in that
            write. ``_write_deferred`` writes the others.
        tabular: Collections of standalone tabular data, to create when absent.
        tabular_added: Collection id to the ``(asset file, file names)`` that
            ``add`` reports as added (#712).
        companion_files: The deferred files that phase 3 tracks as companions.
            ``add`` lists them as skipped, because it converts none of them.
    """

    item_companions: list[tuple[Path, tuple[Path, str, str]]] = field(default_factory=list)
    companions: dict[str, list[CompanionAsset]] = field(default_factory=dict)
    tabular: set[str] = field(default_factory=set)
    tabular_added: dict[str, list[tuple[Path, list[str]]]] = field(default_factory=dict)
    companion_files: list[Path] = field(default_factory=list)


def _plan_deferred(
    proc: _ProcessResult,
    catalog_root: Path,
    failures: list[AddFailure],
    state: RunState,
) -> _DeferredPlan:
    """Decide where each deferred file goes, before any collection is written.

    A file in an item directory joins the item. A file beside a
    collection-level geo file is a collection asset (Issue #383). Any other
    file is standalone tabular data: a failure while ``tabular.enabled`` is
    off, and its own collection asset otherwise (Issue #432). With
    ``tabular.convert``, the Parquet conversion runs here, and the source file
    becomes a companion with the ``source`` role (#960).

    Args:
        proc: The result of phase 2.
        catalog_root: Root directory of the catalog.
        failures: Receives each file that cannot be tracked (mutated).
        state: The run state.

    Returns:
        The plan that ``finalize_items`` and ``_write_deferred`` carry out.
    """
    plan = _DeferredPlan()
    source_to_item_dir = dict(proc.source_to_item_dir)
    source_to_collection_dir = dict(proc.source_to_collection_dir)
    _map_recorded_geo_dirs(
        proc.deferred_non_geo, source_to_item_dir, source_to_collection_dir, catalog_root, state
    )
    for file_path, source_dir, coll_id in proc.deferred_non_geo:
        try:
            if source_dir in source_to_item_dir:
                plan.item_companions.append((file_path, source_to_item_dir[source_dir]))
            elif source_dir in source_to_collection_dir:
                _, owner_id = source_to_collection_dir[source_dir]
                logger.info("Tracking %s as a collection-level companion asset", file_path)
                plan.companions.setdefault(owner_id, []).append(CompanionAsset(file_path))
                plan.companion_files.append(file_path)
            else:
                _plan_tabular(plan, file_path, coll_id, catalog_root, failures, state)
        except Exception as err:
            # Record failure and continue (Issue #175).
            failures.append(AddFailure(path=file_path, error=str(err)))
    return plan


def _plan_tabular(
    plan: _DeferredPlan,
    file_path: Path,
    coll_id: str,
    catalog_root: Path,
    failures: list[AddFailure],
    state: RunState,
) -> None:
    """Plan a file with no geo file beside it as standalone tabular data (#432)."""
    collection_dir = catalog_root / Path(*coll_id.split("/"))
    if not state.setting("tabular.enabled", collection_dir=collection_dir):
        failures.append(
            AddFailure(
                path=file_path,
                error=(
                    f"Tabular data support is disabled. "
                    f"File '{file_path.name}' has no geometry and no companion "
                    f"geospatial file in the same directory. "
                    f"To track standalone tabular data as collection-level assets, "
                    f"set 'tabular.enabled: true' in .portolan/config.yaml"
                ),
            )
        )
        return
    convert = state.setting("tabular.convert", collection_dir=collection_dir)
    if convert and file_path.suffix.lower() != ".parquet":
        # Convert CSV/TSV/XLSX to Parquet via gpio (Issue #432), and track both
        # files side by side, as the vector path does. The plan takes the
        # collection only after the conversion, so a failed one writes nothing.
        asset_path = convert_tabular(file_path, collection_dir, state=state)
        new = [CompanionAsset(asset_path), CompanionAsset(file_path, ROLE_SOURCE)]
        names = [asset_path.name, file_path.name]
    else:
        asset_path = file_path
        new = [CompanionAsset(file_path)]
        names = [file_path.name]
    plan.companions.setdefault(coll_id, []).extend(new)
    plan.tabular.add(coll_id)
    logger.info("Tracking %s as standalone tabular collection-level asset", asset_path)
    plan.tabular_added.setdefault(coll_id, []).append((asset_path, names))


def _write_deferred(
    plan: _DeferredPlan,
    finalized: set[str],
    catalog_root: Path,
    *,
    added: list[ItemInfo],
    skipped: list[Path],
    merge_strategy: MergeStrategy,
    touched_items: list[tuple[str, str]],
    skip_reasons: dict[Path, str],
    state: RunState,
) -> set[Path]:
    """Write the deferred files that ``finalize_items`` did not write (phase 3).

    Each collection gets one ``collection.json`` write for all its files, and
    one staged version for all of them.

    Args:
        plan: The plan of :func:`_plan_deferred`.
        finalized: The collections that ``finalize_items`` wrote, with their
            companions.
        catalog_root: Root directory of the catalog.
        added: Receives each standalone tabular asset (mutated, #712).
        skipped: Receives each companion file (mutated).
        merge_strategy: The strategy of the ``add`` run.
        touched_items: Receives the ``(collection_id, item_id)`` of each
            updated item (mutated).
        skip_reasons: Receives the reason of a companion that two files claim
            one asset key for in its item (mutated).
        state: The run state, which holds the staged versions.

    Returns:
        The collection directories that received deferred assets.
    """
    affected: set[Path] = set()
    for file_path, item in plan.item_companions:
        try:
            _track_companion_in_item(file_path, item, catalog_root, touched_items, state)
        except AssetKeyCollisionError as err:
            skip_reasons[file_path] = err.message
        else:
            affected.add(catalog_root / item[1])
        skipped.append(file_path)

    created = False
    for coll_id, companions in plan.companions.items():
        collection_dir = catalog_root / Path(*coll_id.split("/"))
        if coll_id not in finalized:
            created |= _write_companion_collection(
                catalog_root, coll_id, companions, merge_strategy, coll_id in plan.tabular, state
            )
            stage_companions(state, coll_id, [c.path for c in companions])
        affected.add(collection_dir)
    if created:
        _sweep_catalog(catalog_root, state)
    elif affected:
        # A tabular collection takes its providers from metadata.yaml (#813),
        # so it can turn the whole catalog into a mirror. The root then needs
        # its sync time (rashid PTL-PRO-003).
        update_catalog_provenance(catalog_root)

    skipped.extend(plan.companion_files)
    for coll_id, assets in plan.tabular_added.items():
        collection_dir = catalog_root / Path(*coll_id.split("/"))
        added.extend(
            ItemInfo(
                item_id=asset_path.stem,
                collection_id=coll_id,
                format_type=None,
                bbox=_collection_aoi_bbox(collection_dir),
                asset_paths=names,
            )
            for asset_path, names in assets
        )
    return affected


def _write_companion_collection(
    catalog_root: Path,
    coll_id: str,
    companions: list[CompanionAsset],
    merge_strategy: MergeStrategy,
    create: bool,
    state: RunState,
) -> bool:
    """Write the companions of one collection in one ``collection.json`` write.

    Args:
        catalog_root: Root directory of the catalog.
        coll_id: Collection identifier.
        companions: The files to add.
        merge_strategy: The strategy of the ``add`` run.
        create: Create the collection when it is absent (standalone tabular).
        state: The run state.

    Returns:
        True when this created the collection.
    """
    collection_dir = catalog_root / Path(*coll_id.split("/"))
    collection_json_path = collection_dir / "collection.json"
    if collection_json_path.exists():
        data = json.loads(collection_json_path.read_text(encoding="utf-8"))
        # metadata.yaml applies here too, so an ``add --force`` of a tabular
        # collection carries an edited title, license, or providers into STAC
        # as the spatial path does (issue #813).
        apply_human_metadata_to_dict(data, collection_dir, catalog_root, state)
        add_companion_assets(data, collection_dir, companions, merge_strategy, state)
        write_json_atomic(collection_json_path, data)
        return False
    if not create:
        logger.warning("collection.json not found: %s", collection_json_path)
        return False
    collection = _new_tabular_collection(catalog_root, coll_id, collection_dir, state)
    collection_dir.mkdir(parents=True, exist_ok=True)
    # Phase 1 sets up no catalogs for a tabular file (see _collect_files_for_add).
    from portolan_cli.catalog import create_intermediate_catalogs

    create_intermediate_catalogs(coll_id, catalog_root)

    def finish(data: dict[str, Any]) -> None:
        add_companion_assets(data, collection_dir, companions, merge_strategy, state)

    _save_collection_with_links(collection, collection_dir, catalog_root, coll_id, finish)
    return True


def _track_companion_in_item(
    file_path: Path,
    item: tuple[Path, str, str],
    catalog_root: Path,
    touched_items: list[tuple[str, str]] | None,
    state: RunState,
) -> None:
    """Copy a non-geo file into an item directory and add it to the item.

    Args:
        file_path: The non-geo file.
        item: ``(item_dir, collection_id, item_id)`` of the item.
        catalog_root: Root directory of the catalog.
        touched_items: List to append ``(collection_id, item_id)`` to.
        state: The run state, which holds the staged version.

    Raises:
        AssetKeyCollisionError: Two files claim one asset key in the item. The
            copy goes again, so the item stays as it was.
    """
    item_dir, coll_id, item_id = item
    dest_path = _copy_non_geo_to_item_dir(file_path, item_dir)
    logger.info(
        "Tracking %s as non-geospatial %s asset (no conversion): %s",
        file_path,
        file_path.suffix.upper().lstrip("."),
        dest_path.name,
    )
    try:
        _update_item_with_asset(
            catalog_root=catalog_root, collection_id=coll_id, item_id=item_id, state=state
        )
    except AssetKeyCollisionError:
        if dest_path.resolve() != file_path.resolve():
            dest_path.unlink(missing_ok=True)
        raise
    if touched_items is not None:
        touched_items.append((coll_id, item_id))


def _update_item_with_asset(
    catalog_root: Path,
    collection_id: str,
    item_id: str,
    state: RunState,
) -> None:
    """Update a STAC item to include a new asset file.

    Re-scans the item directory and updates the item.json with all assets.
    This is used to add non-geospatial companion files to existing items.
    The scanned files go in the staged version of the collection.

    Args:
        catalog_root: Root directory of the catalog.
        collection_id: Collection identifier.
        item_id: Item identifier.
        state: The run state, which holds the staged version.
    """
    collection_dir = catalog_root / collection_id
    item_dir = collection_dir / item_id
    item_json_path = item_dir / f"{item_id}.json"

    if not item_json_path.exists():
        logger.warning("Item JSON not found: %s", item_json_path)
        return

    # Load existing item
    with Path(item_json_path).open(encoding="utf-8") as f:
        item_data = json.load(f)

    # Find the primary data file by checking existing assets first (Issue #190).
    # Prefer the existing "data" asset to avoid reselecting a tabular parquet
    # that was just copied as the primary geo-asset.
    primary_file: Path | None = None

    # First: Check existing assets for one with "data" role
    existing_assets = item_data.get("assets", {})
    for asset_info in existing_assets.values():
        roles = asset_info.get("roles", [])
        if "data" in roles:
            # Found existing primary asset - use its href
            href = asset_info.get("href", "")
            if href:
                candidate = item_dir / href
                if candidate.exists():
                    primary_file = candidate
                    break

    # Fallback: scan directory for .parquet or .tif (original behavior)
    if primary_file is None:
        for file in item_dir.iterdir():
            if file.suffix.lower() in {".parquet", ".tif", ".tiff"}:
                primary_file = file
                break

    if primary_file is None:
        # Use the first non-json file as primary
        for file in item_dir.iterdir():
            if file.is_file() and file.suffix.lower() != ".json":
                primary_file = file
                break

    if primary_file is None:
        logger.warning("No primary file found in item directory: %s", item_dir)
        return

    # Re-scan assets
    stac_assets, asset_files, _ = _scan_item_assets(
        item_dir=item_dir,
        item_id=item_id,
        primary_file=primary_file,
        collection_dir=collection_dir,
        collection_id=collection_id,
        ignored_patterns=get_ignored_files(catalog_root, state=state),
    )

    # Enrich COG assets with render extension properties (Issue #13)
    enrich_cog_assets(stac_assets, catalog_root, state=state)

    # Update item assets - include extra_fields for style properties
    # Merge with existing asset metadata to preserve title/description
    existing_assets = item_data.get("assets", {})
    item_data["assets"] = {
        key: {
            **existing_assets.get(key, {}),  # Preserve existing metadata
            "href": asset.href,
            "type": asset.media_type,
            "roles": asset.roles,
            **(asset.extra_fields or {}),
        }
        for key, asset in stac_assets.items()
    }

    # Write updated item
    write_json_atomic(item_json_path, item_data)

    # Stage the files of the item. The scan already hashed them. A companion
    # bumps the minor, as it did when add published it through the backend.
    state.stage(
        collection_id,
        minor=True,
        assets={
            versions_asset_key(path, collection_dir): Asset(
                sha256=checksum,
                size_bytes=size,
                href=f"{collection_id}/{versions_asset_key(path, collection_dir)}",
                mtime=path.stat().st_mtime,
            )
            for path, checksum, size in asset_files.values()
        },
    )
