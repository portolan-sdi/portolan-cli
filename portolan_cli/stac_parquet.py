"""STAC GeoParquet item mirrors for efficient collection queries.

This module generates the ``items.parquet`` item mirror, enabling efficient
spatial/temporal queries without N HTTP requests (issue #319, reshaped by
issue #654 to match the ratified spec).

Per PORTO-FMT-040..043:
- The mirror SHOULD be published for every item-bearing collection; the spec
  applies no item-count threshold, so generation defaults on and gates only
  on ``parquet.enabled`` and a non-zero item count.
- Registration is a single collection-level asset carrying media type
  ``application/vnd.apache.parquet`` and the role ``collection-mirror``.
  That registration is the whole requirement; no ``rel: "items"`` link is
  written, and a legacy link is removed on refresh.
- ``items.parquet`` is tracked in versions.json so push detects changes.

Usage:
    from portolan_cli.stac_parquet import (
        generate_items_parquet,
        register_mirror_asset,
    )

    parquet_path = generate_items_parquet(collection_path)
    register_mirror_asset(collection_path)
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from rashid.catalog import is_absolute_href

from portolan_cli.json_io import write_json_atomic
from portolan_cli.output import info, warn
from portolan_cli.sync.checksums import file_fields

# Constants
PARQUET_FILENAME = "items.parquet"
PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"


def _resolve_href(base_dir: Path, href: str) -> Path:
    """Resolve a STAC link href against the directory holding the linking object."""
    if href.startswith("./"):
        return base_dir / href[2:]
    if href.startswith("../"):
        return (base_dir / href).resolve()
    return base_dir / href


def owned_item_hrefs(node_json_path: Path) -> list[tuple[str, Path]]:
    """Every (href, path) pair for the items the object at ``node_json_path`` owns.

    A catalog may sit below a collection to organize its items (core.md:168-170),
    so ownership follows ``rel="child"`` links down into catalogs rather than
    stopping at the collection's own ``rel="item"`` links. Descent stops at a
    child collection, whose items belong to that collection instead.

    The href is carried alongside the resolved path because it is what the
    operator wrote and therefore what a stale-link error should name.
    """
    if not node_json_path.exists():
        return []

    data = json.loads(node_json_path.read_text(encoding="utf-8"))
    base_dir = node_json_path.parent
    owned: list[tuple[str, Path]] = []

    for link in data.get("links", []):
        href = link.get("href", "")
        if not isinstance(href, str) or not href:
            continue
        rel = link.get("rel")
        if rel == "item":
            owned.append((href, _resolve_href(base_dir, href)))
        elif rel == "child":
            child_path = _resolve_href(base_dir, href)
            if child_path.name == "catalog.json":
                owned.extend(owned_item_hrefs(child_path))

    return owned


def count_items(collection_path: Path) -> int:
    """Count items in a collection from collection.json links.

    Args:
        collection_path: Path to collection directory containing collection.json.

    Returns:
        Number of items (links with rel="item").

    Raises:
        FileNotFoundError: If collection.json doesn't exist.
    """
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        raise FileNotFoundError(f"collection.json not found in {collection_path}")

    return len(owned_item_hrefs(collection_json_path))


def has_mirror_asset(collection_path: Path) -> bool:
    """Check if collection.json registers the item mirror.

    The canonical registration is the collection-level asset; a legacy
    ``rel="items"`` link written by an older CLI also counts so a legacy
    catalog is recognized as mirrored before its next refresh.

    Args:
        collection_path: Path to collection directory.

    Returns:
        True if the mirror asset (or a legacy link) exists.
    """
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        return False

    data = json.loads(collection_json_path.read_text(encoding="utf-8"))

    # Check asset (both old key "items_parquet" and new key "geoparquet-items")
    assets = data.get("assets", {})
    has_asset = (
        "geoparquet-items" in assets
        or "items_parquet" in assets
        or any(asset.get("href") == f"./{PARQUET_FILENAME}" for asset in assets.values())
    )
    if has_asset:
        return True

    # Legacy link-only registration
    links = data.get("links", [])
    return any(
        link.get("type") == PARQUET_MEDIA_TYPE and link.get("rel") == "items" for link in links
    )


def _load_item_dicts(collection_path: Path) -> list[dict[str, Any]]:
    """Load all STAC item dictionaries from a collection.

    Args:
        collection_path: Path to collection directory.

    Returns:
        List of STAC item dictionaries.

    Raises:
        ValueError: If no items found.
    """
    collection_json_path = collection_path / "collection.json"
    owned = owned_item_hrefs(collection_json_path)

    if not owned:
        raise ValueError(f"No items found in collection at {collection_path}")

    items = []
    missing_hrefs = []

    for href, item_path in owned:
        if item_path.exists():
            item_data = json.loads(item_path.read_text(encoding="utf-8"))
            items.append(item_data)
        else:
            missing_hrefs.append(href)

    # Fail fast on stale links - items.parquet must be in sync with collection.json
    if missing_hrefs:
        missing_list = ", ".join(missing_hrefs[:5])
        if len(missing_hrefs) > 5:
            missing_list += f" ... and {len(missing_hrefs) - 5} more"
        raise ValueError(
            f"collection.json at {collection_path} has stale item links. "
            f"Missing items: {missing_list}"
        )

    if not items:
        raise ValueError(f"No items found in collection at {collection_path}")

    return items


def generate_items_parquet(collection_path: Path) -> Path:
    """Generate items.parquet from STAC items in a collection.

    Uses stac-geoparquet to convert all STAC items to GeoParquet format,
    enabling efficient spatial/temporal queries.

    Args:
        collection_path: Path to collection directory containing collection.json
            and item subdirectories.

    Returns:
        Path to generated items.parquet file.

    Raises:
        ValueError: If no items found in collection.
        ImportError: If stac-geoparquet not installed.
    """
    try:
        import stac_geoparquet.arrow
    except ImportError as e:
        raise ImportError(
            "stac-geoparquet is required for items.parquet generation. "
            "Install with: pip install stac-geoparquet"
        ) from e

    # Load all item dictionaries
    items = _load_item_dicts(collection_path)

    # Convert to Arrow using stac-geoparquet
    # parse_stac_items_to_arrow returns a RecordBatchReader
    record_batch_reader = stac_geoparquet.arrow.parse_stac_items_to_arrow(items)
    table = record_batch_reader.read_all()

    # Write to parquet with GeoParquet metadata
    output_path = collection_path / PARQUET_FILENAME
    stac_geoparquet.arrow.to_parquet(table, output_path)

    return output_path


_FILE_FIELDS = ("file:size", "file:checksum")


def _strip_file_fields(asset: dict[str, Any]) -> bool:
    """Drop both file fields, reporting whether either was there to drop.

    Tests membership rather than the popped value, so a field written as JSON
    ``null`` still counts as a change and the caller still writes the file.
    """
    present = [name for name in _FILE_FIELDS if name in asset]
    for name in present:
        del asset[name]
    return bool(present)


def _stamp_file_fields(asset: dict[str, Any], base_dir: Path) -> bool:
    """Refresh ``file:size``/``file:checksum`` from the bytes the asset points at.

    PORTO-CORE-028 makes the fields a SHOULD, so their absence is only a
    PTL-AST-003 warning. PORTO-CORE-030 makes a *published* value a claim about
    the bytes the href resolves to, and rashid's data pass reports a mismatch as
    an error. A stale value is therefore worse than no value, which is why a
    vanished file strips the fields instead of leaving them behind.

    Only a relative href that resolves to nothing is proof the claim is false.
    Everything else is unverifiable rather than wrong, so it is left untouched:
    an absolute or remote href points at bytes this process cannot read, and a
    directory (a FileGDB asset) is measured by ``compute_dir_checksum`` elsewhere.
    Deleting a value on that evidence would destroy metadata the operator
    supplied. ``_local_asset_path`` in ``validation.fixers`` skips the same hrefs
    for the same reason.

    Reads each asset's own href rather than assuming ``items.parquet``: the caller
    matches by asset key as well as by href, so an asset keyed ``geoparquet-items``
    may legitimately point somewhere else, and must not inherit another file's digest.

    Args:
        asset: STAC asset dict, mutated in place.
        base_dir: Directory holding the collection.json that owns the asset.

    Returns:
        Whether the asset changed, so the caller writes collection.json only when
        there is something to write.
    """
    href = asset.get("href")
    if not isinstance(href, str) or not href or is_absolute_href(href):
        return False

    path = _resolve_href(base_dir, href)
    if not path.exists():
        return _strip_file_fields(asset)
    if not path.is_file():
        return False

    fields = file_fields(path)
    if all(asset.get(name) == value for name, value in fields.items()):
        return False
    asset.update(fields)
    return True


def sync_file_extension(data: dict[str, Any], assets: dict[str, Any]) -> bool:
    """Declare the STAC file extension while an asset uses it, withdraw it after.

    ``update_collection_file_statistics`` does this for the add and finalize paths,
    but it needs a ``pystac.Collection`` and this module works on raw JSON on
    purpose (pystac leaks absolute paths, see known-issues/pystac-absolute-paths.md).

    Public because ``collection_thumbnail.register_collection_thumbnail`` needs the
    same logic: it writes ``file:size`` and ``file:checksum`` on the thumbnail after
    both other writers have run, so a collection whose only ``file:``-bearing asset
    is the thumbnail carried the fields without the declaration (Issue #654).

    Withdrawal matters because :func:`_stamp_file_fields` strips the fields when
    the mirror disappears, which can leave the collection declaring an extension
    nothing uses. It reads ``portolan:asset_count`` first: that tally covers item
    assets too, and ``update_collection_file_statistics`` redeclares from the same
    wider scope, so removing the URI while items still carry ``file:`` fields would
    make the two writers fight over collection.json on every run.

    Args:
        data: Parsed collection.json, mutated in place.
        assets: The collection's asset dicts.

    Returns:
        Whether the extension list changed.
    """
    from portolan_cli.stac import EXTENSION_URLS

    extensions = data.get("stac_extensions")
    if extensions is not None and not isinstance(extensions, list):
        # Malformed. Schema validation reports it; do not crash on .append here.
        return False

    url = EXTENSION_URLS["file"]
    in_use = any(
        isinstance(asset, dict) and any(key.startswith("file:") for key in asset)
        for asset in assets.values()
    )

    if in_use:
        if extensions is None:
            extensions = []
            data["stac_extensions"] = extensions
        if url in extensions:
            return False
        extensions.append(url)
        return True

    if extensions is None or url not in extensions:
        return False
    count = data.get("portolan:asset_count")
    if isinstance(count, int) and not isinstance(count, bool) and count > 0:
        return False
    extensions.remove(url)
    return True


def register_mirror_asset(collection_path: Path) -> None:
    """Register the item mirror as a collection-level asset in collection.json.

    The asset carries media type ``application/vnd.apache.parquet`` and the
    role ``collection-mirror`` (PORTO-FMT-041, rashid PTL-MIR-002). That
    single registration is the whole requirement; the spec asks for no
    companion link, so a legacy ``rel="items"`` link written by an older CLI
    is removed, and the undefined community role ``stac-items`` is dropped
    from a legacy asset (issue #654).

    Idempotent - won't duplicate if already present.

    Args:
        collection_path: Path to collection directory.

    Raises:
        FileNotFoundError: If collection.json doesn't exist.
    """
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        raise FileNotFoundError(f"collection.json not found in {collection_path}")

    data = json.loads(collection_json_path.read_text(encoding="utf-8"))
    modified = False

    # --- Remove the legacy rel="items" link ---
    links = data.get("links", [])
    kept_links = [
        link
        for link in links
        if not (link.get("type") == PARQUET_MEDIA_TYPE and link.get("rel") == "items")
    ]
    if len(kept_links) != len(links):
        data["links"] = kept_links
        modified = True

    # --- Register the collection-level asset ---
    # Key follows the community convention; the roles are spec-normative.
    assets = data.get("assets", {})
    asset_key = "geoparquet-items"

    # Match by key or by href, then upgrade every match to the spec roles: a
    # catalog written before "collection-mirror" existed has the asset but
    # only the community role, and PTL-MIR-002 flags it.
    matching = [
        asset
        for key, asset in assets.items()
        if key == asset_key or asset.get("href") == f"./{PARQUET_FILENAME}"
    ]
    if matching:
        for asset in matching:
            roles = asset.setdefault("roles", [])
            if "collection-mirror" not in roles:
                roles.append("collection-mirror")
                modified = True
            if "stac-items" in roles:
                roles.remove("stac-items")
                modified = True
            # Refresh, do not skip: generate_items_parquet overwrote the bytes
            # moments ago, so an asset carried over from an earlier run describes
            # a file that no longer exists in that form.
            if _stamp_file_fields(asset, collection_path):
                modified = True
    else:
        asset = {
            "href": f"./{PARQUET_FILENAME}",
            "type": PARQUET_MEDIA_TYPE,
            "title": "STAC items as GeoParquet",
            "roles": ["collection-mirror"],
        }
        _stamp_file_fields(asset, collection_path)
        assets[asset_key] = asset
        data["assets"] = assets
        modified = True

    if sync_file_extension(data, assets):
        modified = True

    # Write back only if changes were made
    if modified:
        write_json_atomic(collection_json_path, data)


def remove_mirror_from_collection(collection_path: Path) -> bool:
    """Remove the item mirror registration from collection.json.

    Removes the collection-level asset and any legacy ``rel="items"`` link
    an older CLI may have written.

    Args:
        collection_path: Path to collection directory.

    Returns:
        True if link or asset was removed, False if neither existed.
    """
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        return False

    data = json.loads(collection_json_path.read_text(encoding="utf-8"))
    modified = False

    # Remove link
    links = data.get("links", [])
    original_link_count = len(links)
    links = [
        link
        for link in links
        if not (link.get("type") == PARQUET_MEDIA_TYPE and link.get("rel") == "items")
    ]
    if len(links) != original_link_count:
        data["links"] = links
        modified = True

    # Remove asset (by key or by href)
    # Check both old key (items_parquet) and new key (geoparquet-items)
    assets = data.get("assets", {})
    for key_to_remove in ("geoparquet-items", "items_parquet"):
        if key_to_remove in assets:
            del assets[key_to_remove]
            data["assets"] = assets
            modified = True
            break
    else:
        # Check by href in case it was added with a different key
        for key, asset in list(assets.items()):
            if asset.get("href") == f"./{PARQUET_FILENAME}":
                del assets[key]
                data["assets"] = assets
                modified = True
                break

    if modified:
        write_json_atomic(collection_json_path, data)

    return modified


def track_parquet_in_versions(
    collection_path: Path,
    catalog_root: Path | None = None,
    *,
    amend_latest: bool = False,
) -> None:
    """Track items.parquet in versions.json so push detects changes.

    Delegates to :func:`portolan_cli.versions.track_generated_assets`, the
    shared writer for derived assets (PMTiles, thumbnails, this mirror), so
    every side-step records versions the same way.

    A mirror whose bytes are already tracked in the latest snapshot is a
    no-op: regenerating identical content records no new user intent, so it
    must not bump or amend anything (the #683 double-versioning family).

    Args:
        collection_path: Path to collection directory.
        catalog_root: Catalog root for catalog-root-relative hrefs. Defaults
            to the collection's parent, which is correct for top-level
            collections.
        amend_latest: Fold the mirror into the version the caller just wrote
            (the add flow) instead of creating a new one. Leave False when
            the latest version may already be published.

    Raises:
        FileNotFoundError: If items.parquet doesn't exist.
    """
    from portolan_cli.versions import read_versions, track_generated_assets

    parquet_path = collection_path / PARQUET_FILENAME
    versions_path = collection_path / "versions.json"

    if not parquet_path.exists():
        raise FileNotFoundError(f"items.parquet not found at {parquet_path}")

    if versions_path.exists():
        versions_file = read_versions(versions_path)
        if versions_file.versions:
            tracked = versions_file.versions[-1].assets.get(PARQUET_FILENAME)
            if tracked is not None:
                sha256 = hashlib.sha256(parquet_path.read_bytes()).hexdigest()
                if tracked.sha256 == sha256:
                    return

    track_generated_assets(
        collection_path,
        [parquet_path],
        catalog_root if catalog_root is not None else collection_path.parent,
        message="Generated items.parquet for STAC GeoParquet queries",
        amend_latest=amend_latest,
    )


def generate_parquet_mirrors(
    catalog_root: Path,
    affected_collections: set[str],
    *,
    generate_parquet: bool,
    verbose: bool,
    versioned_collections: set[str] | None = None,
) -> None:
    """Generate the item mirror for each affected item-bearing collection.

    PORTO-FMT-040 says the mirror SHOULD be published, and the spec applies
    no item-count threshold, so generation defaults on. For each affected
    collection the only gates are:

    - ``parquet.enabled`` (hierarchical lookup, default true) as the opt-out;
      the explicit ``--stac-geoparquet`` flag overrides it.
    - A non-zero item count: a collection with only collection-level assets
      has no items to mirror.

    Generation always runs regardless of output mode so the JSON envelope
    reflects the final state. An explicitly-requested generation that fails
    re-raises; a default-generation failure only warns.

    Args:
        catalog_root: Catalog root directory.
        affected_collections: Collection IDs modified by the add command.
        generate_parquet: Whether ``--stac-geoparquet`` was passed.
        verbose: Whether to emit per-collection success detail.
        versioned_collections: Collections the add command wrote a version
            for in this run. Their mirror folds into that snapshot instead
            of bumping a second version (one add is one version, #683).
    """
    if not affected_collections:
        return

    from portolan_cli.config import coerce_bool, get_setting

    for coll_id in affected_collections:
        coll_path = catalog_root / coll_id
        if not (coll_path / "collection.json").exists():
            continue

        # Opt-out setting per-collection with hierarchical lookup
        parquet_enabled = coerce_bool(
            get_setting(
                "parquet.enabled",
                catalog_path=catalog_root,
                collection=coll_id,
                collection_path=coll_path,
            ),
            default=True,
        )

        if not (generate_parquet or parquet_enabled):
            continue

        item_count = count_items(coll_path)
        if item_count == 0:
            continue

        try:
            generate_items_parquet(coll_path)
            register_mirror_asset(coll_path)
            track_parquet_in_versions(
                coll_path,
                catalog_root,
                amend_latest=coll_id in (versioned_collections or set()),
            )
            if verbose:
                info(f"Generated items.parquet for '{coll_id}'")
        except Exception as e:
            # Explicit --stac-geoparquet should fail the command
            if generate_parquet:
                raise
            # Default-generation failures just warn
            warn(f"Failed to generate parquet for '{coll_id}': {e}")
