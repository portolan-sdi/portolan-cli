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
from portolan_cli.constants import ROLE_THUMBNAIL
from portolan_cli.formats import is_geoparquet
from portolan_cli.json_io import write_json_atomic
from portolan_cli.output import detail, warn
from portolan_cli.stac_parquet import sync_file_extension
from portolan_cli.sync.checksums import compute_checksum, multihash_sha256
from portolan_cli.versions import track_generated_assets
from portolan_cli.viz.thumbnail import (
    generate_vector_thumbnail,
    get_thumbnail_config,
    is_generated_thumbnail,
)

logger = logging.getLogger(__name__)

#: The canonical asset key. STAC consumers find a thumbnail by role, but a
#: well-known key keeps collection.json readable and matches the item-level
#: convention in ``preparation._ROLE_KEYS``.
THUMBNAIL_ASSET_KEY = ROLE_THUMBNAIL

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
) -> bool:
    """Register ``thumbnail_path`` as the collection's thumbnail asset.

    The single writer of this asset. Emits ``file:size`` and ``file:checksum``
    so the result does not trip ``PTL-AST-003``; the checksum is a hex multihash,
    not a bare digest (Issue #654).

    Args:
        collection_path: Path to the collection directory.
        thumbnail_path: Path to the image, inside the collection directory or one
            of its item directories.
        title: Optional human-facing title.

    Returns:
        True when collection.json was rewritten, False when nothing was written.
    """
    collection_json = collection_path / "collection.json"
    data = _load_collection(collection_path)
    if data is None:
        return False

    # Resolve BOTH sides. `_item_paths` resolves its candidates, so comparing one
    # resolved path against an unresolved collection directory raised ValueError
    # for every catalog reached through a symlink — macOS `/var` -> `/private/var`
    # among them — and silently dropped the raster branch. Resolving both keeps
    # the containment check that rejects an href escaping the collection.
    try:
        href = PurePath(thumbnail_path.resolve().relative_to(collection_path.resolve())).as_posix()
    except (OSError, ValueError):
        logger.debug("%s is outside %s; not registering", thumbnail_path, collection_path)
        return False

    asset: dict[str, Any] = {
        "href": f"./{href}",
        "type": _media_type_for(thumbnail_path),
        "roles": [ROLE_THUMBNAIL],
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

    assets = data.setdefault("assets", {})
    assets[THUMBNAIL_ASSET_KEY] = asset
    # Declaring the file extension is a conditional MUST once an asset carries
    # `file:` fields. The other two writers that sync it — stac.py's
    # `declare_file_extension` and stac_parquet's mirror path — both run
    # before this side-step, so a collection whose only `file:`-bearing asset
    # is the thumbnail would otherwise ship the fields undeclared (Issue #654).
    sync_file_extension(data, assets, collection_json)
    write_json_atomic(collection_json, data)
    return True


def _has_thumbnail_asset(data: dict[str, Any]) -> bool:
    return any(
        ROLE_THUMBNAIL in asset.get("roles", []) for asset in data.get("assets", {}).values()
    )


def _claimed_hrefs(data: dict[str, Any]) -> set[str]:
    """Every href spoken for by a non-thumbnail asset, collection-relative.

    A thumbnail-role asset is excluded on purpose. It points at the very file
    this module is deciding about, so counting it as claimed would make a forced
    refresh refuse to re-adopt the image the user chose and render over it.
    """
    claimed: set[str] = set()
    for asset in data.get("assets", {}).values():
        if ROLE_THUMBNAIL in asset.get("roles", []):
            continue
        href = asset.get("href", "")
        if href:
            claimed.add(PurePath(href.removeprefix("./")).as_posix())
    return claimed


def _is_generated(path: Path) -> bool:
    """True for the ``{stem}.thumb.{ext}`` name ``thumbnail_path_for`` writes."""
    return is_generated_thumbnail(path.name)


def _is_adoptable(path: Path) -> bool:
    """True for the filenames the CLI, a human, or the MapLibre skill produce."""
    if path.suffix.lower() not in _THUMBNAIL_SUFFIXES:
        return False
    if _is_generated(path):
        return True
    return path.stem.lower() in _ADOPTABLE_STEMS


def _find_adoptable_image(
    collection_path: Path, data: dict[str, Any], *, skip_generated: bool = False
) -> Path | None:
    """The conventionally named image to adopt, if the collection holds one.

    Args:
        collection_path: Path to the collection directory.
        data: Parsed collection.json.
        skip_generated: Ignore our own ``*.thumb.*`` output. A forced refresh
            passes True so it re-renders instead of re-adopting the stale render
            it wrote last time. A human's ``thumbnail.png`` still wins, because
            a force must not overwrite a picture somebody chose.
    """
    claimed = _claimed_hrefs(data)
    candidates = sorted(
        p
        for p in collection_path.iterdir()
        if p.is_file()
        and _is_adoptable(p)
        and not (skip_generated and _is_generated(p))
        and PurePath(p.relative_to(collection_path)).as_posix() not in claimed
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
            if ROLE_THUMBNAIL not in asset.get("roles", []):
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


def _find_source(
    collection_path: Path, data: dict[str, Any], *, force: bool = False
) -> tuple[Path, str] | None:
    """Pick the thumbnail source without drawing anything.

    Split out of ``_resolve_for_collection`` so ``check --fix --dry-run`` can
    answer "would this collection get a thumbnail?" using the same gates the
    real run uses. Before the split, dry-run skipped straight to a "would
    generate" line and promised work the real run then declined to do.

    Returns:
        (path, kind) where kind is ``adopt``, ``item``, or ``render``, or None
        when the collection offers no source at all.
    """
    adopted = _find_adoptable_image(collection_path, data, skip_generated=force)
    if adopted is not None:
        return adopted, "adopt"

    item_thumb = _find_item_thumbnail(collection_path, data)
    if item_thumb is not None:
        return item_thumb, "item"

    geoparquet = _find_geoparquet(collection_path, data)
    if geoparquet is None:
        return None
    return geoparquet, "render"


def _resolve_for_collection(
    collection_path: Path, catalog_root: Path, data: dict[str, Any], *, force: bool = False
) -> tuple[Path, bool] | None:
    """Return (thumbnail path, was_rendered), or None when there is nothing to do."""
    source = _find_source(collection_path, data, force=force)
    if source is None:
        return None
    path, kind = source
    if kind != "render":
        return path, False

    from portolan_cli.viz.pmtiles import _discover_style_for_thumbnail

    rendered = generate_vector_thumbnail(
        pmtiles_path=None,
        geoparquet_path=path,
        config=get_thumbnail_config(catalog_root),
        style_path=_discover_style_for_thumbnail(collection_path),
    )
    if rendered is None:
        return None
    return rendered, True


def _thumbnails_wanted(catalog_root: Path, generate: bool | None) -> bool:
    """Resolve ``--thumbnails/--no-thumbnails`` against the catalog setting."""
    if generate is not None:
        return generate
    return get_thumbnail_config(catalog_root).enabled


def would_generate_thumbnail(
    collection_path: Path, catalog_root: Path, *, generate: bool | None = None
) -> bool:
    """True when :func:`ensure_collection_thumbnail` would register something.

    Runs every gate except the render itself, so a dry run and a real run agree.
    """
    if not _thumbnails_wanted(catalog_root, generate):
        return False
    data = _load_collection(collection_path)
    if data is None or _has_thumbnail_asset(data):
        return False
    if not _is_geospatial(collection_path, data):
        return False
    return _find_source(collection_path, data) is not None


def ensure_collection_thumbnail(
    collection_path: Path,
    catalog_root: Path,
    *,
    generate: bool | None = None,
    force: bool = False,
    amend_latest: bool = False,
    verbose: bool = False,
) -> Path | None:
    """Give one collection a thumbnail asset, if it needs and can have one.

    Never raises: a thumbnail is worth a warning, never a failed ``add``.

    Args:
        collection_path: Path to the collection directory.
        catalog_root: Path to the catalog root.
        generate: ``--thumbnails/--no-thumbnails``. None defers to the
            ``thumbnails.enabled`` catalog setting. Checked here, not only in
            the plural wrapper, so ``check --fix`` cannot generate a thumbnail
            for a catalog that set ``enabled: false``.
        force: Re-resolve and re-render even when a thumbnail asset is already
            registered. Refreshes ``file:size`` and ``file:checksum`` too.
        amend_latest: Fold the thumbnail into the version the caller just wrote
            instead of bumping. ``add`` sets this for collections it versioned
            this run, so one add stays one version.
        verbose: Emit a line per collection touched.

    Returns:
        The registered thumbnail path, or None when nothing was registered.
    """
    if not _thumbnails_wanted(catalog_root, generate):
        return None
    data = _load_collection(collection_path)
    if data is None:
        return None
    if _has_thumbnail_asset(data) and not force:
        return None
    if not _is_geospatial(collection_path, data):
        return None

    try:
        resolved = _resolve_for_collection(collection_path, catalog_root, data, force=force)
    except Exception as exc:
        warn(f"Thumbnail generation failed for {collection_path.name}: {exc}")
        return None
    if resolved is None:
        return None

    thumbnail, was_rendered = resolved
    if not register_collection_thumbnail(collection_path, thumbnail):
        return None

    # Track every thumbnail we register, not only the ones we drew. An adopted
    # image is a STAC asset the moment we point at it, and versions.json is the
    # source of truth that push, drift, and integrity all read (ADR-0005). A
    # forced refresh rewrites the record, so `only_if_missing` goes off.
    try:
        track_generated_assets(
            collection_path,
            [thumbnail],
            catalog_root,
            message=f"{'Generated' if was_rendered else 'Registered'} thumbnail: {thumbnail.name}",
            only_if_missing=not force,
            amend_latest=amend_latest,
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
    force: bool = False,
    versioned_collections: set[str] | None = None,
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
        force: ``--force-thumbnails``. Redraw even when one is registered.
        versioned_collections: Collections whose versions.json this ``add`` just
            wrote. Their thumbnail is folded into that snapshot rather than
            bumping a second version. A collection that was skipped is absent,
            so its backfilled thumbnail gets its own version instead of editing
            a snapshot that may already be published.
        verbose: Emit a line per collection touched.
    """
    if not affected_collections:
        return
    # Resolve the flag against config once, then hand the answer down, so the
    # config read does not repeat per collection.
    if not _thumbnails_wanted(catalog_root, generate):
        return

    for collection_id in sorted(affected_collections):
        collection_path = catalog_root / collection_id
        if not (collection_path / "collection.json").exists():
            continue
        ensure_collection_thumbnail(
            collection_path,
            catalog_root,
            generate=True,
            force=force,
            amend_latest=collection_id in (versioned_collections or set()),
            verbose=verbose,
        )


__all__ = [
    "THUMBNAIL_ASSET_KEY",
    "ensure_collection_thumbnail",
    "ensure_collection_thumbnails",
    "register_collection_thumbnail",
    "would_generate_thumbnail",
]
