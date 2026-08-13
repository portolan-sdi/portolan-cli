"""Collection-level thumbnail assets (Issue #683).

rashid's ``PTL-VIZ-001`` (spec ``PORTO-CORE-067`` / ``PORTO-FMT-033``) requires
every geospatial collection to carry an asset with the ``thumbnail`` role. Until
this module existed, ``portolan init`` followed by ``portolan add`` produced a
catalog that failed its own ``portolan check``: vector rendering ran only inside
the PMTiles side-step, which is gated on ``--pmtiles``, and rasters got an
item-level sidecar that no collection asset ever pointed at.

This is the orchestrator, not a renderer. It decides *whether* a collection
needs a thumbnail and *where* one comes from, then delegates the drawing to
``viz.thumbnail``. It lives at the package root rather than under ``viz/``
because it reads STAC metadata and writes versions.json, and ``viz`` is a leaf
that may not reach the sync layer (see the ``viz-is-a-leaf`` import contract).

The ladder, cheapest rung first:

1. Opt-out, via ``thumbnails.enabled`` or ``--no-thumbnails``.
2. A ``thumbnail``-role asset is already registered.
3. The collection is not geospatial, so the rule does not apply.
4. A conventionally named image is on disk; adopt it.
5. A raster item already carries a sidecar; point at it.
6. Render from the collection's GeoParquet.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path, PurePath
from typing import Any

from portolan_cli import extension_registry as _reg
from portolan_cli.formats import is_geoparquet
from portolan_cli.json_io import write_json_atomic
from portolan_cli.output import detail, warn
from portolan_cli.sync.checksums import compute_checksum, multihash_sha256
from portolan_cli.versions import track_generated_assets
from portolan_cli.viz.thumbnail import generate_vector_thumbnail, get_thumbnail_config

logger = logging.getLogger(__name__)

#: The canonical asset key. STAC consumers find a thumbnail by role, but a
#: well-known key keeps collection.json readable and matches the item-level
#: convention in ``preparation._ROLE_KEYS``.
THUMBNAIL_ASSET_KEY = "thumbnail"

#: Extensions that may serve as a thumbnail, per rashid's ``_THUMBNAIL_TYPES``.
#: Sourced from the extension registry so media types stay single-sourced.
_MEDIA_TYPE_MAP: dict[str, str] = _reg.field_map("media_type")
_THUMBNAIL_SUFFIXES = (".png", ".jpg", ".jpeg", ".webp")

#: Filenames we are willing to adopt. Deliberately a short list of conventions
#: rather than "any image": a legend or a logo dropped in a collection directory
#: must never be promoted to the collection's thumbnail.
_ADOPTABLE_STEMS = ("thumbnail", "preview")

#: Media types that make a collection geospatial on sight. A ``.parquet`` needs
#: a schema read instead, because a tabular Parquet carries no geometry and
#: ``PTL-VIZ-001`` skips it.
_SPATIAL_SUFFIXES = (".pmtiles", ".fgb", ".gpkg", ".shp", ".gdb", ".tif", ".tiff", ".copc.laz")


def _load_collection(collection_path: Path) -> dict[str, Any] | None:
    """Read collection.json, or None when it is absent or unreadable."""
    collection_json = collection_path / "collection.json"
    if not collection_json.exists():
        return None
    try:
        data: dict[str, Any] = json.loads(collection_json.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.debug("Cannot read %s: %s", collection_json, exc)
        return None
    return data


def _media_type_for(path: Path) -> str:
    return _MEDIA_TYPE_MAP.get(path.suffix.lower(), "application/octet-stream")


def register_collection_thumbnail(
    collection_path: Path,
    thumbnail_path: Path,
    *,
    title: str | None = None,
) -> None:
    """Register ``thumbnail_path`` as the collection's thumbnail asset.

    The single writer of this asset. Emits ``file:size`` and ``file:checksum``
    so the result does not trip ``PTL-AST-003``; the checksum is a hex multihash,
    not a bare digest (Issue #654).

    Args:
        collection_path: Path to the collection directory.
        thumbnail_path: Path to the image, inside the collection directory or one
            of its item directories.
        title: Optional human-facing title.
    """
    collection_json = collection_path / "collection.json"
    data = _load_collection(collection_path)
    if data is None:
        return

    try:
        href = PurePath(thumbnail_path.relative_to(collection_path)).as_posix()
    except ValueError:
        logger.debug("%s is outside %s; not registering", thumbnail_path, collection_path)
        return

    asset: dict[str, Any] = {
        "href": f"./{href}",
        "type": _media_type_for(thumbnail_path),
        "roles": ["thumbnail"],
    }
    if title:
        asset["title"] = title
    try:
        asset["file:size"] = thumbnail_path.stat().st_size
        asset["file:checksum"] = multihash_sha256(compute_checksum(thumbnail_path))
    except (OSError, ValueError) as exc:
        # Size and checksum are a PTL-AST-003 warning, not an error. Registering
        # the asset without them beats registering nothing.
        logger.debug("Cannot stat or checksum %s: %s", thumbnail_path, exc)

    data.setdefault("assets", {})[THUMBNAIL_ASSET_KEY] = asset
    write_json_atomic(collection_json, data)


def _has_thumbnail_asset(data: dict[str, Any]) -> bool:
    return any("thumbnail" in asset.get("roles", []) for asset in data.get("assets", {}).values())


def _claimed_hrefs(data: dict[str, Any]) -> set[str]:
    """Every href already spoken for by an asset, normalized to a bare name."""
    claimed: set[str] = set()
    for asset in data.get("assets", {}).values():
        href = asset.get("href", "")
        if href:
            claimed.add(PurePath(href.removeprefix("./")).as_posix())
    return claimed


def _is_adoptable(path: Path) -> bool:
    """True for the filenames the CLI, a human, or the MapLibre skill produce."""
    if path.suffix.lower() not in _THUMBNAIL_SUFFIXES:
        return False
    stem = path.stem.lower()
    # `data.thumb.jpg` — the convention `thumbnail_path_for` writes.
    if stem.endswith(".thumb"):
        return True
    return stem in _ADOPTABLE_STEMS


def _find_adoptable_image(collection_path: Path, data: dict[str, Any]) -> Path | None:
    claimed = _claimed_hrefs(data)
    candidates = sorted(
        p
        for p in collection_path.iterdir()
        if p.is_file() and _is_adoptable(p) and p.name not in claimed
    )
    return candidates[0] if candidates else None


def _item_paths(collection_path: Path, data: dict[str, Any]) -> list[Path]:
    """Resolve the collection's item links to paths on disk."""
    items: list[Path] = []
    for link in data.get("links", []):
        if link.get("rel") != "item":
            continue
        href = link.get("href", "")
        if not href:
            continue
        candidate = (collection_path / href.removeprefix("./")).resolve()
        if candidate.exists():
            items.append(candidate)
    return items


def _find_item_thumbnail(collection_path: Path, data: dict[str, Any]) -> Path | None:
    """The sidecar `add` already writes beside a converted COG (Issue #657)."""
    for item_json in _item_paths(collection_path, data):
        try:
            item: dict[str, Any] = json.loads(item_json.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for asset in item.get("assets", {}).values():
            if "thumbnail" not in asset.get("roles", []):
                continue
            href = str(asset.get("href", "")).removeprefix("./")
            candidate = item_json.parent / href
            if candidate.exists():
                return candidate
    return None


def _collection_data_assets(collection_path: Path, data: dict[str, Any]) -> list[Path]:
    paths: list[Path] = []
    for asset in data.get("assets", {}).values():
        roles = asset.get("roles", [])
        if roles and "data" not in roles:
            continue
        href = asset.get("href", "").removeprefix("./")
        if not href:
            continue
        candidate = collection_path / href
        if candidate.exists():
            paths.append(candidate)
    return paths


def _find_geoparquet(collection_path: Path, data: dict[str, Any]) -> Path | None:
    """The GeoParquet to draw, preferring a collection-level asset over an item."""
    for path in _collection_data_assets(collection_path, data):
        if path.suffix.lower() == ".parquet" and is_geoparquet(path):
            return path
    for item_json in _item_paths(collection_path, data):
        try:
            item: dict[str, Any] = json.loads(item_json.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for asset in item.get("assets", {}).values():
            href = str(asset.get("href", "")).removeprefix("./")
            candidate = item_json.parent / href
            if candidate.suffix.lower() == ".parquet" and candidate.exists():
                if is_geoparquet(candidate):
                    return candidate
    return None


def _is_geospatial(collection_path: Path, data: dict[str, Any]) -> bool:
    """Mirror rashid's positive-signal test, cheaply and from disk.

    A tabular collection — Parquet with no ``geo`` schema metadata — is not
    geospatial, and ``PTL-VIZ-001`` skips it. Derive that status, never flag it.
    """
    for path in _collection_data_assets(collection_path, data):
        suffix = path.suffix.lower()
        if suffix == ".parquet":
            if is_geoparquet(path):
                return True
        elif suffix in _SPATIAL_SUFFIXES:
            return True
    for item_json in _item_paths(collection_path, data):
        try:
            item: dict[str, Any] = json.loads(item_json.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if item.get("geometry") is not None:
            return True
    return False


def _resolve_for_collection(
    collection_path: Path, catalog_root: Path, data: dict[str, Any]
) -> tuple[Path, bool] | None:
    """Return (thumbnail path, was_rendered), or None when there is nothing to do."""
    adopted = _find_adoptable_image(collection_path, data)
    if adopted is not None:
        return adopted, False

    item_thumb = _find_item_thumbnail(collection_path, data)
    if item_thumb is not None:
        return item_thumb, False

    geoparquet = _find_geoparquet(collection_path, data)
    if geoparquet is None:
        return None

    from portolan_cli.viz.pmtiles import _discover_style_for_thumbnail

    rendered = generate_vector_thumbnail(
        pmtiles_path=None,
        geoparquet_path=geoparquet,
        config=get_thumbnail_config(catalog_root),
        style_path=_discover_style_for_thumbnail(collection_path),
    )
    if rendered is None:
        return None
    return rendered, True


def ensure_collection_thumbnail(
    collection_path: Path,
    catalog_root: Path,
    *,
    verbose: bool = False,
) -> Path | None:
    """Give one collection a thumbnail asset, if it needs and can have one.

    Never raises: a thumbnail is worth a warning, never a failed ``add``.

    Args:
        collection_path: Path to the collection directory.
        catalog_root: Path to the catalog root.
        verbose: Emit a line per collection touched.

    Returns:
        The registered thumbnail path, or None when nothing was registered.
    """
    data = _load_collection(collection_path)
    if data is None:
        return None
    if _has_thumbnail_asset(data):
        return None
    if not _is_geospatial(collection_path, data):
        return None

    try:
        resolved = _resolve_for_collection(collection_path, catalog_root, data)
    except Exception as exc:
        warn(f"Thumbnail generation failed for {collection_path.name}: {exc}")
        return None
    if resolved is None:
        return None

    thumbnail, was_rendered = resolved
    register_collection_thumbnail(collection_path, thumbnail)

    if was_rendered:
        # Only track what we wrote. An adopted file is either already tracked or
        # is the user's to manage.
        try:
            track_generated_assets(
                collection_path,
                [thumbnail],
                catalog_root,
                message=f"Generated thumbnail: {thumbnail.name}",
                only_if_missing=True,
            )
        except Exception as exc:
            warn(f"Failed to track {thumbnail.name} in versions.json: {exc}")

    if verbose:
        verb = "Rendered" if was_rendered else "Registered existing"
        detail(f"{verb} thumbnail for {collection_path.name}: {thumbnail.name}")
    return thumbnail


def ensure_collection_thumbnails(
    catalog_root: Path,
    affected_collections: set[str],
    *,
    generate: bool | None = None,
    verbose: bool = False,
) -> None:
    """Ensure every affected collection carries a thumbnail asset.

    Runs after the PMTiles side-step, so a higher-fidelity thumbnail rendered
    from tiles is adopted rather than replaced.

    Args:
        catalog_root: Path to the catalog root.
        affected_collections: Collection IDs touched by the command.
        generate: ``--thumbnails/--no-thumbnails``. None defers to the
            ``thumbnails.enabled`` catalog setting.
        verbose: Emit a line per collection touched.
    """
    if not affected_collections:
        return
    if generate is False:
        return
    if generate is None and not get_thumbnail_config(catalog_root).enabled:
        return

    for collection_id in sorted(affected_collections):
        collection_path = catalog_root / collection_id
        if not (collection_path / "collection.json").exists():
            continue
        ensure_collection_thumbnail(collection_path, catalog_root, verbose=verbose)


__all__ = [
    "THUMBNAIL_ASSET_KEY",
    "ensure_collection_thumbnail",
    "ensure_collection_thumbnails",
    "register_collection_thumbnail",
]
