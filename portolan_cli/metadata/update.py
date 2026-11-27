"""Metadata update functions for STAC items, collections, and versions."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import replace
from pathlib import Path, PurePath
from typing import TYPE_CHECKING, Any

from portolan_cli.formats import RASTER, RASTER_EXTENSIONS
from portolan_core.json_io import write_json_atomic
from portolan_core.models.item import ItemModel
from portolan_core.versions import (
    Asset,
    read_versions,
    versions_asset_key,
    write_versions,
)

if TYPE_CHECKING:
    from portolan_cli.preparation import PreparedItem
    from portolan_core.config import ConfigSource
    from portolan_core.run_state import RunState

logger = logging.getLogger(__name__)

#: The band name ``extract_cog_metadata`` generates. A band called anything else
#: was named by a human, so the band refresh below keeps that name rather than
#: writing the placeholder back over it.
_GENERATED_BAND_NAME = re.compile(r"^band_\d+$")


def update_item_metadata(
    item_path: Path, file_path: Path, *, state: ConfigSource | None = None
) -> ItemModel:
    """Re-extract metadata from file and update existing STAC item."""
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

    # `datetime` is the acquisition time, not the file mtime; keep the item's value, or null.
    properties = item_data.setdefault("properties", {})
    properties.setdefault("datetime", None)

    # Refresh the data asset's href/type in place, leaving any human-authored
    # title and all other assets untouched.
    assets: dict[str, Any] = item_data.setdefault("assets", {})
    data_key = _find_data_asset_key(assets)
    data_asset = assets.setdefault(data_key, {})
    # Keep the href spelling when it already names this file.
    if not _href_names(data_asset.get("href"), file_path):
        data_asset["href"] = file_path.name
    # Keep the registered media type; the local map lacks the COG profile suffix (PORTO-CORE-026).
    if not data_asset.get("type"):
        data_asset["type"] = _get_media_type(file_path)
    data_asset.setdefault("roles", ["data"])
    _refresh_raster_bands(data_asset, file_path, state)

    # Atomic write, so a kill mid-write cannot leave a truncated item.json.
    write_json_atomic(item_path, item_data)

    return ItemModel.from_dict(item_data)


def _refresh_raster_bands(
    data_asset: dict[str, Any], file_path: Path, state: ConfigSource | None
) -> None:
    """Refresh raster band metadata on the asset."""
    if file_path.suffix.lower() not in RASTER_EXTENSIONS:
        return

    fresh = _extract_raster_bands(file_path, state)
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


def _extract_raster_bands(file_path: Path, state: ConfigSource | None) -> list[dict[str, Any]]:
    """The ``bands`` array ``portolan add`` would write for this raster."""
    # Imported here: `preparation` imports this package (cycle), and `portolan check` must not load rasterio early.
    import rasterio

    from portolan_cli.metadata.cog import extract_cog_metadata
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
                file_path, RASTER, catalog_root, collection_path=collection_dir, state=state
            )
            # The statistics.enabled gate already ran, so the flag is always True.
            _add_statistics_to_properties(
                stac_properties, RASTER, band_stats, parquet_stats, stats_enabled=True
            )

    bands = stac_properties.get("bands")
    if not isinstance(bands, list):
        return []
    return [band for band in bands if isinstance(band, dict)]


def _statistics_scope(file_path: Path) -> tuple[Path | None, Path | None]:
    """The (catalog root, collection dir) a statistics setting resolves against."""
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
    """Fresh band values over the recorded ones, keeping what pixels cannot say."""
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
    """The versions.json key that tracks ``file_path``, or None."""
    # Imported here: `detection` loads rasterio, which `portolan check` must not load early.
    from portolan_cli.metadata.detection import versions_asset_lookup_keys

    relative = versions_asset_key(file_path, collection_dir)
    for key in versions_asset_lookup_keys(file_path, collection_dir):
        if key in assets:
            return key
    for key, asset in assets.items():
        if asset.href and PurePath(asset.href).as_posix().endswith(relative):
            return key
    return None


def _href_names(href: Any, file_path: Path) -> bool:
    """Whether ``href`` already points at ``file_path``, whatever its spelling."""
    if not isinstance(href, str) or not href:
        return False
    resolved = (file_path.parent / href).resolve()
    return resolved == file_path.resolve()


def _find_data_asset_key(assets: dict[str, Any]) -> str:
    """Return the key of the item's data asset, defaulting to ``"data"``."""
    for key, asset in assets.items():
        roles = asset.get("roles") if isinstance(asset, dict) else None
        if isinstance(roles, list) and "data" in roles:
            return key
    return "data"


def prepare_missing_item(
    file_path: Path, collection_path: Path, *, state: RunState | None = None
) -> tuple[Path, PreparedItem]:
    """Prepare a new STAC item for a file without metadata, as ``add`` does."""
    from portolan_cli.catalog import find_catalog_root
    from portolan_cli.preparation import prepare_item

    if not file_path.exists():
        raise FileNotFoundError(f"Data file not found: {file_path}")

    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        raise FileNotFoundError(f"collection.json not found: {collection_json_path}")

    catalog_root = find_catalog_root(collection_path) or collection_path.parent
    collection_path_id = collection_path.resolve().relative_to(catalog_root.resolve()).as_posix()
    in_collection_dir = file_path.parent.resolve() == collection_path.resolve()
    prepared = prepare_item(
        path=file_path,
        catalog_root=catalog_root,
        collection_id=collection_path_id,
        item_id=file_path.stem if in_collection_dir else None,
        state=state,
    )
    if prepared.item_json_path is None:
        raise FileNotFoundError(f"add writes no item for {file_path}; run portolan add")
    return catalog_root, prepared


def update_versions_tracking(
    file_path: Path, versions_path: Path, *, state: RunState | None = None
) -> None:
    """Refresh the file's checksum, size, and mtimes together in versions.json."""
    # Validate file exists
    if not file_path.exists():
        raise FileNotFoundError(f"Data file not found: {file_path}")

    from portolan_core.run_state import RunState

    run = state if state is not None else RunState.open(versions_path.parent.parent)

    # Read versions.json
    versions_file = read_versions(versions_path)

    # Get current version
    if not versions_file.versions:
        raise KeyError("No versions in versions.json")

    current_version = versions_file.versions[-1]

    # `add` keys an item-level asset `{item_id}/{filename}`, not the bare file name.
    collection_dir = versions_path.parent
    asset_name = _tracked_asset_key(current_version.assets, file_path, collection_dir)
    if asset_name is None:
        raise KeyError(
            f"Asset not found in versions.json: {versions_asset_key(file_path, collection_dir)}"
        )

    # A refreshed mtime certifies these bytes. Keeping the old hash or size
    # would make add/pull's stat fast path trust a false content record.
    checksum = run.checksum(file_path)
    current_stat = file_path.stat()
    current_mtime = current_stat.st_mtime

    # Refresh both mtime fields together and keep feature_count and schema_fingerprint.
    old_asset = current_version.assets[asset_name]
    updated_asset = replace(
        old_asset,
        sha256=checksum,
        size_bytes=current_stat.st_size,
        source_mtime=current_mtime,
        mtime=current_mtime,
    )

    # Create updated assets dict
    updated_assets = dict(current_version.assets)
    updated_assets[asset_name] = updated_asset

    # Create updated version
    updated_version = replace(current_version, assets=updated_assets)

    # Create updated versions list
    updated_versions = list(versions_file.versions[:-1])
    updated_versions.append(updated_version)

    # Create updated versions file
    updated_versions_file = replace(versions_file, versions=updated_versions)

    # Write updated versions.json
    write_versions(versions_path, updated_versions_file)
    run.invalidate(versions_path)
