"""Add pipeline module - manages the add/list/info/remove workflow."""

from __future__ import annotations

import contextlib
import json
import logging
import shutil
from dataclasses import dataclass, field, replace
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
    resolve_collection_id,  # noqa: F401 - re-exported
)
from portolan_cli.conversion_config import (
    VectorSettings,
    get_vector_settings,
    resolve_partition_settings,
    settings_override,
)
from portolan_cli.convert import convert_multilayer_file
from portolan_cli.discovery import get_sidecars, iter_files_with_sidecars, iter_geospatial_files
from portolan_cli.finalization import (
    CompanionAsset,
    add_companion_assets,
    apply_human_metadata_to_dict,
    stage_companions,
    stage_deferred_post_add,
)

# Re-exported so finalize helpers resolve through add's namespace: tests patch
# ``portolan_cli.add.finalize_items``. The edge is one-way: finalization imports nothing from add.
from portolan_cli.finalization import (
    _asset_freshness_fields as _asset_freshness_fields,
)
from portolan_cli.finalization import (
    _ensure_partition_metadata as _ensure_partition_metadata,
)
from portolan_cli.finalization import (
    _finalize_with_backend as _finalize_with_backend,
)
from portolan_cli.finalization import (
    _fix_collection_links as _fix_collection_links,
)
from portolan_cli.finalization import (
    _get_or_create_collection as _get_or_create_collection,
)
from portolan_cli.finalization import (
    _save_collection_with_links as _save_collection_with_links,
)
from portolan_cli.finalization import (
    _update_catalog_links as _update_catalog_links,
)
from portolan_cli.finalization import (
    apply_human_metadata as apply_human_metadata,
)
from portolan_cli.finalization import (
    finalize_items as finalize_items,
)
from portolan_cli.formats import (
    in_run,
    is_geoparquet,
    is_multilayer,
    list_layers,
)
from portolan_cli.hookspecs import EnrichContext
from portolan_cli.humanize import humanize_slug
from portolan_cli.plugins.formats import handler_for, plugin_extensions
from portolan_cli.preparation import (
    _MEDIA_TYPE_MAP as _MEDIA_TYPE_MAP,
)
from portolan_cli.preparation import (
    _ROLE_MAP as _ROLE_MAP,
)

# Some names are used by the finalize code here, others re-exported for external.py and
# tests. The edge is one-way: preparation imports nothing from add.
from portolan_cli.preparation import (
    IGNORED_FILES as IGNORED_FILES,
)
from portolan_cli.preparation import (
    ROLE_SOURCE,
    _cleanup_orphaned_output,
    ignored_by,
    is_rewrite_temp,
)
from portolan_cli.preparation import (
    PreparedItem as PreparedItem,
)
from portolan_cli.preparation import (
    _convert_and_extract_metadata as _convert_and_extract_metadata,
)
from portolan_cli.preparation import (
    _create_and_save_item as _create_and_save_item,
)
from portolan_cli.preparation import (
    _derive_item_id_and_asset_level as _derive_item_id_and_asset_level,
)
from portolan_cli.preparation import (
    _get_asset_role as _get_asset_role,
)
from portolan_cli.preparation import (
    _get_media_type as _get_media_type,
)
from portolan_cli.preparation import (
    _pre_validate_geometry as _pre_validate_geometry,
)
from portolan_cli.preparation import (
    _scan_item_assets as _scan_item_assets,
)
from portolan_cli.preparation import (
    _warn_if_source_newer as _warn_if_source_newer,
)
from portolan_cli.preparation import (
    convert_raster as convert_raster,
)
from portolan_cli.preparation import (
    convert_tabular as convert_tabular,
)
from portolan_cli.preparation import (
    convert_vector as convert_vector,
)
from portolan_cli.preparation import (
    prepare_item as prepare_item,
)
from portolan_cli.remove import remove_files  # noqa: F401 - re-exported
from portolan_cli.report import emit
from portolan_cli.stac import (
    MergeStrategy,
    create_collection,
    update_catalog_provenance,
)
from portolan_cli.stac_enrich import enrich_collection_file, enrich_item_file
from portolan_cli.stac_parquet import PARQUET_FILENAME
from portolan_cli.viz.pmtiles_links import PMTILES_SUFFIX
from portolan_cli.viz.style import enrich_cog_assets
from portolan_core.config import get_ignored_files, load_merged_metadata
from portolan_core.constants import (
    GEOSPATIAL_EXTENSIONS,
    TABULAR_EXTENSIONS,
)
from portolan_core.errors import (
    AssetKeyCollisionError,
    CRSMismatchError,
    CRSTransformError,
    InvalidBboxError,
    MissingLicenseError,
    NoGeometryError,
    PartitionCollisionError,
)
from portolan_core.format_families import VECTOR
from portolan_core.json_io import write_json_atomic
from portolan_core.licensing import LICENSE_REL, license_gap, resolve_license
from portolan_core.query import (  # noqa: F401 - reexport
    ItemInfo,
    get_item_info,
    is_current,
    list_items,
)
from portolan_core.trace import span, submit
from portolan_core.versions import Asset, versions_asset_key

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractContextManager
    from datetime import datetime

    from portolan_cli.extract.common.progress import MessageCallback
    from portolan_cli.stac_parquet import MirrorReport
    from portolan_cli.viz.pmtiles import PMTilesOutcome, PMTilesReport
    from portolan_core.run_state import RunState


logger = logging.getLogger(__name__)

# Error patterns of a parquet file without geometry (no 'geo' key): tabular, tracked as auxiliary.
_PARQUET_NO_GEOMETRY_PATTERNS: tuple[str, ...] = (
    "missing bounding box",
    "no valid geometry",
)


def _is_parquet_no_geometry_error(err: ValueError) -> bool:
    """Check if a ValueError indicates a parquet file lacks geometry."""
    err_msg = str(err).lower()
    return any(pattern in err_msg for pattern in _PARQUET_NO_GEOMETRY_PATTERNS)


def _batch_sibling_names(sources: list[Path]) -> frozenset[str]:
    """Base filenames of a batch's source files plus their converted outputs."""
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
    """Information about a failed add operation."""

    path: Path
    error: str


def _maybe_partition_large_file(
    prepared: PreparedItem,
    catalog_root: Path,
    item_datetime: datetime | None,
    skip_partitioning: bool = False,
    state: RunState | None = None,
) -> list[PreparedItem]:
    """Partition a large GeoParquet file if it exceeds the size threshold."""
    from portolan_cli.partitioning import (
        build_glob_pattern,
        get_partition_metadata,
        partition_columns_of,
        strategy_of_column,
    )
    from portolan_cli.plugins.formats import handler_named

    if skip_partitioning:
        return [prepared]

    # The format handler owns the size partition rule.
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

    # Collection-level glob asset: one URL for DuckDB/PyArrow/GDAL to read all partitions.
    glob_pattern = build_glob_pattern(strategy, partition_columns=columns)
    glob_asset = pystac.Asset(
        href=glob_pattern,
        media_type="application/vnd.apache.parquet",
        roles=["data"],
        title="Partitioned GeoParquet",
        description=f"Glob pattern for {len(partition_files)} spatial partitions",
        # partition:glob will be populated on push with the remote URL
    )

    # Partition metadata for the STAC partition extension.
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
    """Add files to a Portolan catalog."""
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
    """Build the collection of standalone tabular data in memory."""
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

    # Human-readable title; description defaults to it.
    tabular_title = humanize_slug(collection_id)
    collection = create_collection(
        collection_id=collection_id,
        description=tabular_title,
        title=tabular_title,
        bbox=final_bbox,
    )

    # A tabular-only add never reaches finalize_items, so apply the metadata.yaml title,
    # license, providers, and provenance here.
    apply_human_metadata(collection, collection_dir, catalog_root, state)
    return collection


def _sweep_catalog(catalog_root: Path, state: RunState) -> None:
    """Run the catalog-wide conformance sweeps after a new tabular collection."""
    from portolan_cli.catalog import (
        apply_catalog_human_titles,
        ensure_link_titles,
        ensure_schema_uris,
    )
    from portolan_cli.readme import ensure_readmes
    from portolan_core.agents_md import ensure_agents_md_tree

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
    """Add all geospatial files in a directory to a collection."""
    files = list(iter_geospatial_files(path, recursive=recursive))

    # Skip cross-item siblings in collection-level asset scans so per-file work stays O(n).
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


# Note: SIDECAR_PATTERNS imported from portolan_core.constants


def _copy_non_geo_to_item_dir(
    file_path: Path,
    item_dir: Path,
) -> Path:
    """Copy a non-geospatial file to an item directory as a companion asset."""
    dest_path = item_dir / file_path.name
    if dest_path.exists() and dest_path.resolve() == file_path.resolve():
        # Already in place
        return dest_path
    shutil.copy2(file_path, dest_path)
    return dest_path


def _ensure_nested_catalogs(
    collection_id: str, catalog_root: Path, setup_collections: set[str]
) -> None:
    """Ensure intermediate catalogs exist for nested collection IDs."""
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
    """Return why ``ignored_files`` excludes ``file_path`` from the add, or None."""
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
    """Check a tabular item companion against its owner's record as well."""
    if is_current(file_path, collection_dir / "versions.json", state=state):
        return True
    if state is None or file_path.suffix.lower() not in TABULAR_EXTENSIONS:
        return False
    for owner in file_path.parent.parents:
        if owner.resolve() == catalog_root.resolve():
            break
        if not (owner / "versions.json").exists():
            continue
        record = state.tracked_assets(owner).get(versions_asset_key(file_path, owner))
        return record is not None and state.unchanged(file_path, record)
    return False


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
    """Collect and filter files for add operation (Phase 1)."""
    processed_paths: set[Path] = set()
    files_to_process: list[tuple[Path, str]] = []
    reasons = skip_reasons if skip_reasons is not None else {}

    def skip(file_path: Path, reason: str) -> None:
        skipped.append(file_path)
        reasons[file_path] = reason

    geo_extensions = GEOSPATIAL_EXTENSIONS | plugin_extensions()
    ignored_patterns = get_ignored_files(catalog_root, state=state)

    for path in paths:
        files = iter_files_with_sidecars(path) if path.is_dir() else [path, *get_sidecars(path)]

        for listed_path in files:
            # Resolve symlinks to track the real file
            file_path = listed_path.resolve() if listed_path.is_symlink() else listed_path

            if file_path in processed_paths:
                continue
            processed_paths.add(file_path)

            # Skip non-geospatial files; a plugin format handler adds its extensions.
            if file_path.suffix.lower() not in geo_extensions:
                continue

            # `ignored_files` in config.yaml excludes a file, with the basename rule of `portolan list`.
            ignore_reason = _ignore_reason(file_path, catalog_root, ignored_patterns, state)
            if ignore_reason is not None:
                skip(file_path, ignore_reason)
                continue

            # Never re-ingest the item mirror: the spec reserves items.parquet in the collection root
            # (PORTO-FMT-040).
            if file_path.name == PARQUET_FILENAME:
                skip(file_path, SKIP_ITEM_MIRROR)
                continue

            # Never ingest the hidden scratch file of the in-place GeoParquet rewrite; a killed run
            # leaves one behind.
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

            # Skip unchanged files unless force=True.
            if not force:
                collection_dir = catalog_root / Path(*coll_id.split("/"))
                # The run state reads each versions.json once.
                if _unchanged_for_add(file_path, collection_dir, catalog_root, state):
                    skip(file_path, SKIP_UNCHANGED)
                    continue

            # A tabular file can be an item's companion, so phase 1's inferred collection can be an item
            # directory. Phases 2 and 3 set up the catalogs once they know where the file goes.
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
    """Skip each PMTiles that renders no GeoParquet."""

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
    """Immutable per-batch inputs threaded through the phase-2 prep helpers."""

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
    """Accumulated output of phase 2 (per-item preparation)."""

    prepared_items: list[PreparedItem] = field(default_factory=list)
    failures: list[AddFailure] = field(default_factory=list)
    skips: dict[Path, str] = field(default_factory=dict)
    deferred_non_geo: list[tuple[Path, Path, str]] = field(default_factory=list)
    #: Source directory to (item document, collection id, item id).
    source_to_item_dir: dict[Path, tuple[Path, str, str]] = field(default_factory=dict)
    source_to_collection_dir: dict[Path, tuple[Path, str]] = field(default_factory=dict)


#: The errors on which prepare_item removes a new output that has no bbox.
_NO_BBOX_ERRORS = (NoGeometryError, CRSTransformError, CRSMismatchError, InvalidBboxError)


def _prepare_multilayer_file(
    file_path: Path,
    coll_id: str,
    opts: _PrepareOptions,
) -> tuple[list[PreparedItem], list[AddFailure], dict[Path, str]]:
    """Convert + prepare every layer of a multi-layer file."""
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
                # Apply partitioning to each layer.
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
                # prepare_item cannot tell a new layer file from an old one, so a layer file this run
                # wrote goes here with no STAC record. The cleanup logs an OSError so other layers go on.
                if isinstance(err, _NO_BBOX_ERRORS) and result.output not in existing:
                    _cleanup_orphaned_output(result.output, file_path)
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
    """Prepare one source file inside an ``add.file`` span."""
    with span("add.file", file=file_path.name, collection=coll_id):
        return _prepare_file(file_path, coll_id, opts)


def _prepare_file(
    file_path: Path,
    coll_id: str,
    opts: _PrepareOptions,
) -> _FileOutcome:
    """Prepare one source file for finalization (phase 2, parallelizable)."""
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
        # Partitioning returns several PreparedItems; otherwise [prepared].
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
    """Fold one file's prepared items into the running phase-2 accumulator."""
    source_dir = file_path.parent
    collection_dir = catalog_root / Path(*coll_id.split("/"))
    for prepared in prepared_list:
        result.prepared_items.append(prepared)
        if prepared.is_collection_level_asset:
            # Collection-level: map the source to the collection dir.
            result.source_to_collection_dir[source_dir] = (collection_dir, coll_id)
        # The document location is independent of --item-id.
        elif prepared.item_json_path is not None:
            result.source_to_item_dir[source_dir] = (
                prepared.item_json_path,
                coll_id,
                prepared.item_id,
            )


def _phase_process(
    files_to_process: list[tuple[Path, str]],
    opts: _PrepareOptions,
    *,
    workers: int,
    on_progress: Callable[[Path], None] | None,
) -> _ProcessResult:
    """Phase 2: prepare every collected file, sequentially or across workers."""
    from portolan_cli.convert import _batch_has_output_inputs

    result = _ProcessResult()
    if workers > 1 and _batch_has_output_inputs(
        [path for path, _ in files_to_process], opts.catalog_root, opts.state
    ):
        workers = 1

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
    """Read the license and license-link state off a collection.json already on disk."""
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
    """Stop the add when a target collection would end up with no usable license."""
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


def add_files(  # noqa: PLR0913 - each add option is a keyword argument
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
    """Add files to a Portolan catalog."""
    from portolan_cli.version_ops import owned_run

    # Every footer read of the run goes through the run memo (formats.in_run).
    with owned_run(catalog_root, state) as run_state, in_run(run_state):
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
                state=run_state,
            )
            stage.set(files=len(files_to_process), skipped=len(skipped))
        if not files_to_process:
            return [], skipped, []

        # Phase 1.5: refuse the add before any GDAL work if a target collection would lack a license.
        with span("add.licenses"):
            _require_licenses(catalog_root, {coll_id for _, coll_id in files_to_process}, run_state)

        # Filenames of every batch item, so a collection-level asset scan skips siblings tracked
        # as their own items.
        opts = _PrepareOptions(
            catalog_root=catalog_root,
            item_id=item_id,
            item_datetime=item_datetime,
            force=force,
            reconvert=reconvert,
            skip_partitioning=skip_partitioning,
            batch_exclude_names=_batch_sibling_names([fp for fp, _ in files_to_process]),
            state=run_state,
            vector_settings=get_vector_settings(catalog_root, state=run_state),
        )

        # Phase 2: prepare each file (GDAL work), accumulating prepared items.
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
        plan = _plan_deferred(proc, catalog_root, failures, run_state)

        # Phase 2.6: finalize writes each collection once.
        with span("add.finalize", items=len(proc.prepared_items)):
            added: list[ItemInfo] = finalize_items(
                catalog_root,
                proc.prepared_items,
                merge_strategy,
                state=run_state,
                companions=plan.companions,
            )

        # Phase 3: write the deferred files that finalize did not write.
        touched_items: list[tuple[str, str, Path]] = []
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
                state=run_state,
            )

        # Phase 4: run the enrichment hooks on the documents phase 3 changed.
        with span("add.enrich", items=len(touched_items)):
            _enrich_deferred_documents(catalog_root, affected_collections, touched_items)

        # Phase 3 tracks a non-geospatial file next to a geo file as an asset and
        # lists it as skipped, because it is not converted.
        for path in skipped:
            reasons.setdefault(path, SKIP_COMPANION)
        return added, skipped, failures


@dataclass(frozen=True)
class AddOutcome:
    """The result of one ``portolan add`` run (:func:`run_add`)."""

    added: list[ItemInfo]
    skipped: list[tuple[Path, str]]
    failures: list[AddFailure]
    mirrors: MirrorReport
    pmtiles: PMTilesReport

    def to_dict(self) -> dict[str, Any]:
        """Return the envelope ``data`` of ``portolan add``."""
        return {
            "added": [
                {
                    "item_id": ds.item_id,
                    "collection_id": ds.collection_id,
                    "format_type": ds.format_type or "unknown",
                    "bbox": ds.bbox,
                }
                for ds in self.added
            ],
            "skipped": [{"path": str(p), "reason": reason} for p, reason in self.skipped],
            "failures": [{"path": str(f.path), "error": f.error} for f in self.failures],
        }


def _collections_to_refresh(
    added: list[ItemInfo], skips: list[tuple[Path, str]], catalog_root: Path
) -> tuple[set[str], set[str]]:
    """Warn about each file that stayed out of the catalog, and find the collections to refresh."""
    from portolan_cli.remove import _resolve_collection_dir

    versioned = {a.collection_id for a in added if a.collection_id}
    affected = set(versioned)
    for p, reason in skips:
        if reason not in SKIPS_THAT_TRACK:
            emit("warn", f"Skipping {p.name}: {reason}")
            continue
        # A nested collection id spans several path segments.
        collection_dir = _resolve_collection_dir(p, catalog_root)
        if collection_dir is not None:
            affected.add(collection_dir.relative_to(catalog_root.resolve()).as_posix())
    return affected, versioned


def run_add(  # noqa: PLR0913 - each add option is a keyword argument
    paths: list[Path],
    catalog_root: Path,
    *,
    item_id: str | None = None,
    item_datetime: datetime | None = None,
    workers: int = 1,
    generate_parquet: bool = False,
    generate_pmtiles: bool = False,
    force_pmtiles: bool = False,
    generate_thumbnails: bool | None = None,
    force_thumbnails: bool = False,
    force: bool = False,
    reconvert: bool = False,
    merge_strategy: str = "smart",
    skip_partitioning: bool = False,
    on_progress: Callable[[Path], None] | None = None,
    adding: AbstractContextManager[object] | None = None,
    on_message: MessageCallback | None = None,
    on_step: Callable[[MirrorReport | PMTilesOutcome], None] | None = None,
    state: RunState | None = None,
) -> AddOutcome:
    """Run the whole ``portolan add`` workflow: add the files, then the side-steps."""
    from portolan_cli.collection_thumbnail import ensure_collection_thumbnails
    from portolan_cli.report import reporting
    from portolan_cli.stac import MergeStrategy
    from portolan_cli.stac_parquet import generate_parquet_mirrors
    from portolan_cli.version_ops import commit_run
    from portolan_cli.viz import pmtiles
    from portolan_core import trace
    from portolan_core.run_state import RunState

    if state is None:
        state = RunState.open(catalog_root)
    with reporting(on_message):
        skip_reasons: dict[Path, str] = {}
        with adding or contextlib.nullcontext():
            added, skipped, failures = add_files(
                paths=paths,
                catalog_root=catalog_root,
                collection_id=None,
                item_id=item_id,
                item_datetime=item_datetime,
                on_progress=on_progress,
                workers=workers,
                force=force,
                reconvert=reconvert,
                skip_partitioning=skip_partitioning,
                merge_strategy=MergeStrategy(merge_strategy),
                skip_reasons=skip_reasons,
                state=state,
            )
        # add_files gives every skip a reason. A path without one reads as unchanged.
        skips = [(p, skip_reasons.get(p, SKIP_UNCHANGED)) for p in skipped]
        affected, versioned = _collections_to_refresh(added, skips, catalog_root)

        try:
            with trace.span("add.mirrors"):
                mirrors = generate_parquet_mirrors(
                    catalog_root,
                    affected,
                    generate_parquet=generate_parquet,
                    versioned_collections=versioned,
                    state=state,
                )
            if on_step is not None:
                on_step(mirrors)
            with trace.span("add.pmtiles"):
                tiles = pmtiles.generate_or_suggest_pmtiles(
                    catalog_root,
                    affected,
                    generate_pmtiles=generate_pmtiles,
                    force=force_pmtiles,
                    state=state,
                    on_outcome=on_step,
                )
            if tiles.failure is None:
                ensure_collection_thumbnails(
                    catalog_root,
                    affected,
                    generate=generate_thumbnails,
                    force=force_thumbnails,
                    versioned_collections=versioned,
                    state=state,
                )
        finally:
            commit_run(state)
    return AddOutcome(added, skips, failures, mirrors, tiles)


def _enrich_deferred_documents(
    catalog_root: Path,
    affected_collections: set[Path],
    touched_items: list[tuple[str, str, Path]],
) -> None:
    """Run the enrichment hooks on the items and collections phase 3 changed."""
    for coll_id, item_id, item_path in dict.fromkeys(touched_items):
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
    """Read a tabular collection's area-of-interest bbox from collection.json."""
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
    """Map each companion directory that holds an unchanged, tracked geo file."""
    from portolan_core.stac_links import owned_item_hrefs

    root = catalog_root.resolve()
    item_paths: dict[Path, dict[Path, Path]] = {}
    for _file, source_dir, _coll_id in deferred_non_geo:
        if source_dir in source_to_item_dir or source_dir in source_to_collection_dir:
            continue
        for collection_dir in (source_dir, *source_dir.parents):
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
                if collection_dir not in item_paths:
                    item_paths[collection_dir] = {
                        path.parent.resolve(): path
                        for _href, path in owned_item_hrefs(collection_dir / "collection.json")
                    }
                item_path = item_paths[collection_dir].get(source_dir.resolve())
                if item_path is None or not item_path.is_file():
                    continue
                item_data = json.loads(item_path.read_text(encoding="utf-8"))
                source_to_item_dir[source_dir] = (item_path, coll_id, item_data["id"])
            break


@dataclass
class _DeferredPlan:
    """Where each deferred file without geometry goes (phase 3)."""

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
    """Decide where each deferred file goes, before any collection is written."""
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
            # Record the failure and continue.
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
    """Plan a file with no geo file beside it as standalone tabular data."""
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
        # Convert CSV/TSV/XLSX to Parquet via gpio and track both files. The plan takes the
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
    touched_items: list[tuple[str, str, Path]],
    skip_reasons: dict[Path, str],
    state: RunState,
) -> set[Path]:
    """Write the deferred files that ``finalize_items`` did not write (phase 3)."""
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
        # A tabular collection takes its providers from metadata.yaml, so it can turn the catalog
        # into a mirror; the root then needs its sync time (PTL-PRO-003).
        update_catalog_provenance(catalog_root)

    staged = state.staged()
    item_ids: dict[str, dict[Path, str]] = {}
    for owner, item_id, item_path in touched_items:
        # Key on the directory of the item file, so a --item-id item matches too.
        item_ids.setdefault(owner, {})[item_path.parent] = item_id
    for directory in sorted(affected):
        coll_id = directory.relative_to(catalog_root).as_posix()
        change = staged.get(coll_id)
        if coll_id not in finalized and change is not None:
            stage_deferred_post_add(state, coll_id, change.assets, item_ids.get(coll_id, {}))

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
    """Write the companions of one collection in one ``collection.json`` write."""
    collection_dir = catalog_root / Path(*coll_id.split("/"))
    collection_json_path = collection_dir / "collection.json"
    if collection_json_path.exists():
        data = json.loads(collection_json_path.read_text(encoding="utf-8"))
        # metadata.yaml applies here too, so `add --force` carries an edited title, license, or
        # providers into STAC.
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
    touched_items: list[tuple[str, str, Path]] | None,
    state: RunState,
) -> None:
    """Copy a non-geo file into an item directory and add it to the item."""
    item_path, coll_id, item_id = item
    item_dir = item_path.parent
    dest_path = _copy_non_geo_to_item_dir(file_path, item_dir)
    logger.info(
        "Tracking %s as non-geospatial %s asset (no conversion): %s",
        file_path,
        file_path.suffix.upper().lstrip("."),
        dest_path.name,
    )
    try:
        _update_item_with_asset(
            catalog_root=catalog_root,
            collection_id=coll_id,
            item_id=item_id,
            state=state,
            item_json_path=item_path,
        )
    except AssetKeyCollisionError:
        if dest_path.resolve() != file_path.resolve():
            dest_path.unlink(missing_ok=True)
        raise
    if touched_items is not None:
        touched_items.append((coll_id, item_id, item_path))


def _update_item_with_asset(
    catalog_root: Path,
    collection_id: str,
    item_id: str,
    state: RunState,
    *,
    item_json_path: Path | None = None,
) -> None:
    """Update a STAC item to include a new asset file."""
    collection_dir = catalog_root / collection_id
    item_json_path = item_json_path or collection_dir / item_id / f"{item_id}.json"
    item_dir = item_json_path.parent

    if not item_json_path.exists():
        logger.warning("Item JSON not found: %s", item_json_path)
        return

    # Load existing item
    with Path(item_json_path).open(encoding="utf-8") as f:
        item_data = json.load(f)

    # Find the primary data file from existing assets first; prefer the "data" asset so a
    # just-copied tabular parquet is not reselected.
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
        state=state,
    )

    # Enrich COG assets with render extension properties.
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
    tracked = state.tracked_assets(collection_dir)
    refreshed: dict[str, Asset] = {}
    for path, checksum, size in asset_files.values():
        key = versions_asset_key(path, collection_dir)
        href = f"{collection_id}/{key}"
        # A directory rescan refreshes bytes, not conversion ownership or the freshness baseline;
        # losing source_path makes claim_output refuse the next reconversion.
        previous = tracked.get(key, Asset(sha256=checksum, size_bytes=size, href=href))
        refreshed[key] = replace(
            previous, sha256=checksum, size_bytes=size, href=href, mtime=path.stat().st_mtime
        )
    state.stage(collection_id, minor=True, assets=refreshed)
