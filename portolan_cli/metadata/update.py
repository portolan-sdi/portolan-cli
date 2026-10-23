"""Metadata update functions for STAC items, collections, and versions.

This module provides functions to update existing metadata when files change:
- update_item_metadata(): Re-extract metadata and update existing STAC item
- create_missing_item(): Create new STAC item for file without metadata
- update_versions_tracking(): Update source_mtime in versions.json

Part of Phase 2c: Update Functions (check-metadata-handling feature).
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path, PurePath
from typing import Any

import rasterio

from portolan_cli.formats import RASTER, RASTER_EXTENSIONS
from portolan_cli.json_io import write_json_atomic
from portolan_cli.metadata.cog import extract_cog_metadata
from portolan_cli.metadata.detection import versions_asset_lookup_keys
from portolan_cli.models.item import ItemModel
from portolan_cli.versions import (
    Asset,
    Version,
    VersionsFile,
    read_versions,
    versions_asset_key,
    write_versions,
)

logger = logging.getLogger(__name__)

#: The band name ``extract_cog_metadata`` generates. A band called anything else
#: was named by a human, so the band refresh below keeps that name rather than
#: writing the placeholder back over it.
_GENERATED_BAND_NAME = re.compile(r"^band_\d+$")


def update_item_metadata(item_path: Path, file_path: Path) -> ItemModel:
    """Re-extract metadata from file and update existing STAC item.

    Refreshes only the fields this metadata pass owns, the item's bbox,
    geometry, datetime, the data asset's href, and, for a raster, the data
    asset's ``bands`` array. Everything else on disk is preserved. That
    deliberately includes the item's ``stac_extensions``, every non-``data``
    asset such as the ``thumbnail`` that ``portolan add`` registers for COGs
    (#657), a non-raster asset's ``bands`` (#659), and the data asset's media
    type, which the refresh backfills only when it is missing.

    A raster's ``bands`` array is the exception, because it describes the shape
    of the file and formats.md makes the file authoritative for band statistics.
    Re-shape a 4-band COG into a 1-band COG and a preserved array documents
    bands that no longer exist (#737), on a command whose job is freshness. So
    the refresh re-reads the array from the bytes with the extractors
    ``portolan add`` uses. A failed or empty extraction keeps what is on disk,
    because a refresh that cannot read the file must not destroy the record
    (#659).

    The update edits the raw item JSON in place rather than round-tripping
    through :class:`ItemModel`. The model only carries href/type/roles/title for
    assets and drops ``stac_extensions`` entirely, so serializing through it
    would silently destroy those fields (#659).

    Args:
        item_path: Path to existing item JSON file.
        file_path: Path to the data file (GeoParquet or COG).

    Returns:
        Updated ItemModel (a lossy view, the on-disk JSON is the full record).

    Raises:
        FileNotFoundError: If item_path or file_path doesn't exist.
    """
    # ``item`` imports the ``metadata`` package, which imports this module.
    from portolan_cli.item import _extract_geometry_from_file, _get_media_type

    # Validate both files exist
    if not file_path.exists():
        raise FileNotFoundError(f"Data file not found: {file_path}")
    if not item_path.exists():
        raise FileNotFoundError(f"Item file not found: {item_path}")

    # Load the raw item as the source of truth so nothing outside the refresh's
    # scope is dropped.
    item_data: dict[str, Any] = json.loads(item_path.read_text(encoding="utf-8"))

    # Extract fresh metadata from file
    bbox, geometry = _extract_geometry_from_file(file_path)
    item_data["bbox"] = bbox
    item_data["geometry"] = geometry

    # `datetime` is the acquisition time of the observation, not the mtime of
    # the file. The refresh cannot read it off the bytes, so it keeps whatever
    # the item already carries. `add` writes `null` when the caller passes no
    # `--datetime`, and stamping the time of the run over that null published a
    # fabricated acquisition date (#709). An item with no `datetime` key at all
    # gets the same `null` `add` would have written.
    properties = item_data.setdefault("properties", {})
    properties.setdefault("datetime", None)

    # Refresh the data asset's href/type in place, leaving any human-authored
    # title and all other assets untouched.
    assets: dict[str, Any] = item_data.setdefault("assets", {})
    data_key = _find_data_asset_key(assets)
    data_asset = assets.setdefault(data_key, {})
    # Keep the href's existing spelling when it already names this file. `add`
    # writes `./data.tif`, and rewriting that to `data.tif` changed the item on
    # every run without changing where it points (#709).
    if not _href_names(data_asset.get("href"), file_path):
        data_asset["href"] = file_path.name
    # Keep the media type `add` registered. `add` derives it from the extension
    # registry, which gives a COG the `profile=cloud-optimized` suffix that
    # PORTO-CORE-026 makes a MUST; the local extension map here does not, so
    # overwriting would downgrade a valid COG asset to plain GeoTIFF. Only
    # backfill when the field is absent or blank.
    if not data_asset.get("type"):
        data_asset["type"] = _get_media_type(file_path)
    data_asset.setdefault("roles", ["data"])
    _refresh_raster_bands(data_asset, file_path)

    # Write the full record back to disk. Atomic, so a kill mid-write cannot
    # leave a truncated item.json where a valid one used to be (issue #687).
    write_json_atomic(item_path, item_data)

    return ItemModel.from_dict(item_data)


def _refresh_raster_bands(data_asset: dict[str, Any], file_path: Path) -> None:
    """Re-extract a raster data asset's ``bands`` array in place (#737).

    A non-raster asset is untouched, so a GeoParquet asset's array still survives
    the refresh verbatim (#659). So does a raster's array when the extraction
    fails or reads no bands, because a refresh that cannot see the file has
    nothing better to write than what is already recorded.

    When the band count is unchanged the fresh values are merged over the
    existing band, keeping the descriptive fields extraction cannot derive. When
    the count changes the file has been re-shaped, so a label written for one of
    four bands cannot be assumed to describe the one band that is left, and the
    fresh array replaces the old one outright.
    """
    if file_path.suffix.lower() not in RASTER_EXTENSIONS:
        return

    fresh = _extract_raster_bands(file_path)
    if not fresh:
        logger.warning(
            "Could not re-extract bands from '%s'; keeping the recorded array",
            file_path.name,
        )
        return

    existing = data_asset.get("bands")
    if isinstance(existing, list) and len(existing) == len(fresh):
        fresh = [
            _merge_band(new, old) if isinstance(old, dict) else new
            for new, old in zip(fresh, existing, strict=True)
        ]

    data_asset["bands"] = fresh


def _extract_raster_bands(file_path: Path) -> list[dict[str, Any]]:
    """The ``bands`` array ``portolan add`` would write for this raster.

    Orchestrates the extractors the add path uses, ``extract_cog_metadata`` for
    the per-band shape and ``preparation``'s statistics helpers for the
    ``statistics`` sub-object, so a refreshed array and a freshly added one are
    built by the same code and carry the same STAC field names. Returns an empty
    list when the raster cannot be read, which the caller treats as "keep what is
    on disk".

    Reads with ``GDAL_PAM_ENABLED=NO`` so a ``.aux.xml`` sidecar cannot answer
    for the file. formats.md is explicit that statistics MUST be embedded in the
    file and that a PAM sidecar does not satisfy that, and GDAL never invalidates
    a sidecar when the raster underneath is replaced. `add` leaves one next to
    every raster it computes statistics for, so a refresh that trusted it would
    copy the old raster's numbers onto the new bands. It is also how rashid reads
    a COG for PTL-DAT-009.
    """
    # Imported inside the function on purpose: `preparation` imports the
    # `metadata` package this module belongs to, so a module-level import would
    # close an import cycle.
    from portolan_cli.preparation import (
        _add_statistics_to_properties,
        _extract_statistics_best_effort,
    )

    with rasterio.Env(GDAL_PAM_ENABLED="NO"):
        try:
            stac_properties = extract_cog_metadata(file_path).to_stac_properties()
        except Exception:
            logger.warning("Raster metadata extraction failed for '%s'", file_path, exc_info=True)
            return []

        # Statistics are a configured enrichment, so they need the catalog whose
        # settings gate them. A raster outside any catalog gets the band shape only.
        catalog_root, collection_dir = _statistics_scope(file_path)
        if catalog_root is not None:
            band_stats, parquet_stats = _extract_statistics_best_effort(
                file_path, RASTER, catalog_root, collection_path=collection_dir
            )
            # `_extract_statistics_best_effort` already applied the
            # `statistics.enabled` gate, returning nothing when the setting is
            # off, so the flag here is always True.
            _add_statistics_to_properties(
                stac_properties, RASTER, band_stats, parquet_stats, stats_enabled=True
            )

    bands = stac_properties.get("bands")
    if not isinstance(bands, list):
        return []
    return [band for band in bands if isinstance(band, dict)]


def _statistics_scope(file_path: Path) -> tuple[Path | None, Path | None]:
    """The (catalog root, collection dir) a statistics setting resolves against.

    The refresh is handed one file, so the scope `add` receives as arguments has
    to be recovered from the path. The catalog root comes from the usual sentinel
    walk, the collection from the nearest ancestor holding a ``collection.json``.
    Either may be None, and `get_setting` then falls back a level.
    """
    from portolan_cli.catalog import find_catalog_root

    catalog_root = find_catalog_root(file_path.parent)
    if catalog_root is None:
        return None, None

    for parent in file_path.parents:
        if (parent / "collection.json").exists():
            return catalog_root, parent
        if parent == catalog_root:
            break
    return catalog_root, None


def _merge_band(fresh: dict[str, Any], existing: dict[str, Any]) -> dict[str, Any]:
    """Fresh band values over the recorded ones, keeping what pixels cannot say.

    The split follows what is derivable. ``data_type``, ``nodata`` and
    ``statistics`` come from the file, which formats.md makes the authority for
    them. A ``name`` or ``description`` a human wrote is descriptive, not
    derivable, so it survives. A name is kept unless it is the generated
    ``band_N`` placeholder, and any other recorded key the extractor does not
    produce carries over. That also means a value extraction failed to read,
    including a ``nodata`` that came from ``metadata.yaml`` defaults the refresh
    never sees, is preserved rather than dropped.
    """
    merged = {**existing, **fresh}

    name = existing.get("name")
    if isinstance(name, str) and name and not _GENERATED_BAND_NAME.match(name):
        merged["name"] = name

    return merged


def _tracked_asset_key(
    assets: dict[str, Asset],
    file_path: Path,
    collection_dir: Path,
) -> str | None:
    """The versions.json key that tracks ``file_path``, or None.

    Mirrors :func:`~portolan_cli.metadata.detection.find_versions_asset` so the
    reader and the writer agree on which entry belongs to a file. The
    collection-relative key is what ``add`` writes, the bare file name covers an
    older versions.json, and the ``href`` sweep covers a nested layout. A
    collection-level file with the same name owns the bare key, so an item-level
    asset skips the bare name and never binds to a foreign baseline (issue #709).
    """
    relative = versions_asset_key(file_path, collection_dir)
    for key in versions_asset_lookup_keys(file_path, collection_dir):
        if key in assets:
            return key
    for key, asset in assets.items():
        if asset.href and PurePath(asset.href).as_posix().endswith(relative):
            return key
    return None


def _href_names(href: Any, file_path: Path) -> bool:
    """Whether ``href`` already points at ``file_path``, whatever its spelling.

    ``./data.tif`` and ``data.tif`` name the same file. Only a scheme-qualified
    or absolute href names something else.
    """
    if not isinstance(href, str) or not href:
        return False
    resolved = (file_path.parent / href).resolve()
    return resolved == file_path.resolve()


def _find_data_asset_key(assets: dict[str, Any]) -> str:
    """Return the key of the item's data asset, defaulting to ``"data"``.

    Prefers an asset whose ``roles`` include ``"data"``, then a literal
    ``"data"`` key, so the href/type refresh lands on the real data asset
    without disturbing sibling assets (thumbnail, metadata, ...).
    """
    for key, asset in assets.items():
        roles = asset.get("roles") if isinstance(asset, dict) else None
        if isinstance(roles, list) and "data" in roles:
            return key
    return "data"


def create_missing_item(file_path: Path, collection_path: Path) -> Path:
    """Create new STAC item for a file without metadata.

    The item comes from ``preparation.prepare_item``, the code ``add`` uses, so
    a recreated item equals the one ``add`` wrote: the same id, assets, bands,
    ``proj:*`` and ``raster:*`` fields, and datetime (issue #1046). A file
    directly in the collection directory keeps its stem as the item id, so
    ``check --fix`` still writes ``{item_dir}/{stem}.json`` for it.

    Args:
        file_path: Path to the data file (GeoParquet or COG).
        collection_path: Path to the parent collection directory.

    Returns:
        Path to the created item JSON file.

    Raises:
        FileNotFoundError: If file_path doesn't exist or collection.json not found.
    """
    from portolan_cli.catalog import find_catalog_root
    from portolan_cli.finalization import record_prepared_items
    from portolan_cli.hookspecs import EnrichContext
    from portolan_cli.preparation import prepare_item
    from portolan_cli.stac_enrich import enrich_item_file

    if not file_path.exists():
        raise FileNotFoundError(f"Data file not found: {file_path}")

    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        raise FileNotFoundError(f"collection.json not found: {collection_json_path}")

    # A nested collection id holds a slash, which the model rejects, so read the raw id.
    collection_id = json.loads(collection_json_path.read_text(encoding="utf-8"))["id"]
    catalog_root = find_catalog_root(collection_path) or collection_path.parent
    collection_path_id = collection_path.resolve().relative_to(catalog_root.resolve()).as_posix()
    in_collection_dir = file_path.parent.resolve() == collection_path.resolve()
    prepared = prepare_item(
        path=file_path,
        catalog_root=catalog_root,
        collection_id=collection_path_id,
        item_id=file_path.stem if in_collection_dir else None,
    )
    item_path = prepared.item_json_path
    if item_path is None:
        raise FileNotFoundError(f"add writes no item for {file_path}; run portolan add")

    # finalize_items sets the collection field when it saves the collection.
    item_data = json.loads(item_path.read_text(encoding="utf-8"))
    item_data["collection"] = collection_id
    write_json_atomic(item_path, item_data)
    # The item enrichment hook runs on the created item, as on an added one (#947).
    enrich_item_file(
        item_path,
        EnrichContext(
            catalog_root=catalog_root,
            collection_id=collection_path_id,
            document_path=item_path,
            item_id=prepared.item_id,
        ),
    )
    # The snapshot records every file the item declares, as add does (#1046).
    record_prepared_items(catalog_root, collection_path, [prepared])
    return item_path


def update_versions_tracking(file_path: Path, versions_path: Path) -> None:
    """Update source_mtime in versions.json for a file.

    Reads versions.json, finds the asset entry for the file,
    updates the source_mtime to the current file modification time,
    and writes the updated versions.json.

    Args:
        file_path: Path to the data file.
        versions_path: Path to the versions.json file.

    Raises:
        FileNotFoundError: If file_path or versions_path doesn't exist.
        KeyError: If asset not found in versions.json.
    """
    # Validate file exists
    if not file_path.exists():
        raise FileNotFoundError(f"Data file not found: {file_path}")

    # Read versions.json
    versions_file = read_versions(versions_path)

    # Get current version
    if not versions_file.versions:
        raise KeyError("No versions in versions.json")

    current_version = versions_file.versions[-1]

    # Find the asset. `add` keys an item-level asset `{item_id}/{filename}`, so
    # the bare file name misses it, raises KeyError, and leaves the baseline
    # unwritten. `_fix_single_file` discarded that error, so the item stayed
    # STALE run after run (#709).
    collection_dir = versions_path.parent
    asset_name = _tracked_asset_key(current_version.assets, file_path, collection_dir)
    if asset_name is None:
        raise KeyError(
            f"Asset not found in versions.json: {versions_asset_key(file_path, collection_dir)}"
        )

    # Get current mtime from file
    current_mtime = file_path.stat().st_mtime

    # Refresh both mtime fields to the current file so the entry stays
    # internally consistent — carrying the current `source_mtime` next to a
    # stale `mtime` would leave the two fields describing different on-disk
    # states. Preserve the freshness heuristics (feature_count,
    # schema_fingerprint) so refreshing mtime does not wipe them (#512).
    old_asset = current_version.assets[asset_name]
    updated_asset = Asset(
        sha256=old_asset.sha256,
        size_bytes=old_asset.size_bytes,
        href=old_asset.href,
        source_path=old_asset.source_path,
        source_mtime=current_mtime,
        mtime=current_mtime,
        feature_count=old_asset.feature_count,
        schema_fingerprint=old_asset.schema_fingerprint,
    )

    # Create updated assets dict
    updated_assets = dict(current_version.assets)
    updated_assets[asset_name] = updated_asset

    # Create updated version
    updated_version = Version(
        version=current_version.version,
        created=current_version.created,
        breaking=current_version.breaking,
        assets=updated_assets,
        changes=current_version.changes,
    )

    # Create updated versions list
    updated_versions = list(versions_file.versions[:-1])
    updated_versions.append(updated_version)

    # Create updated versions file
    updated_versions_file = VersionsFile(
        spec_version=versions_file.spec_version,
        current_version=versions_file.current_version,
        versions=updated_versions,
    )

    # Write updated versions.json
    write_versions(versions_path, updated_versions_file)
