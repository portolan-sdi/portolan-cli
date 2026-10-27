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
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any

from rashid.catalog import is_absolute_href

from portolan_cli.constants import ROLE_COLLECTION_MIRROR
from portolan_cli.json_io import write_json_atomic
from portolan_cli.stac_links import owned_item_hrefs, resolve_href
from portolan_cli.sync.checksums import file_fields, file_fields_from
from portolan_cli.trace import count

if TYPE_CHECKING:
    from collections.abc import Set as AbstractSet
    from pathlib import Path

    from portolan_cli.run_state import RunState

# Constants
PARQUET_FILENAME = "items.parquet"
PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"


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


def _load_item_dicts(collection_path: Path, owned: list[tuple[str, Path]]) -> list[dict[str, Any]]:
    """Load all STAC item dictionaries from a collection.

    Args:
        collection_path: Path to collection directory.
        owned: The ``(href, path)`` pairs of the items the collection owns.

    Returns:
        List of STAC item dictionaries.

    Raises:
        ValueError: If no items found.
    """
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


#: The run-state memo kind that carries the fingerprint of the item files from
#: the build of ``items.parquet`` to its versions.json record.
_SOURCE_FINGERPRINT = "items.parquet.source_fingerprint"


def _items_fingerprint(owned: list[tuple[str, Path]]) -> str | None:
    """Hash the href, size, and mtime of each owned item file, in link order.

    Returns None when an item file is missing, so the build runs and reports it.
    """
    digest = hashlib.sha256()
    for href, item_path in owned:
        try:
            st = item_path.stat()
        except FileNotFoundError:
            return None
        digest.update(f"{href}\0{st.st_size}\0{st.st_mtime_ns}\n".encode())
    return digest.hexdigest()


def _mirror_is_current(run: RunState, collection_path: Path, fingerprint: str) -> bool:
    """Whether the latest version records this mirror, built from these item files.

    The record must hold the same item fingerprint, and the file must still hold
    the recorded bytes. A file that another writer replaced fails the second test.
    """
    from portolan_cli.versions import versions_asset_key

    output_path = collection_path / PARQUET_FILENAME
    record = run.tracked_assets(collection_path).get(
        versions_asset_key(output_path, collection_path)
    )
    if record is None or record.source_fingerprint != fingerprint:
        return False
    return run.unchanged(output_path, record)


def generate_items_parquet(collection_path: Path, *, state: RunState | None = None) -> Path:
    """Generate items.parquet from STAC items in a collection.

    Uses stac-geoparquet to convert all STAC items to GeoParquet format,
    enabling efficient spatial/temporal queries.

    The versions.json record of ``items.parquet`` holds a fingerprint of the
    owned item files: href, size, and mtime, in link order. When the record
    holds the fingerprint of the files now, and the file holds the recorded
    bytes, the mirror is current. Then this returns with no item read and no
    write (issue #995). A collection with no record always builds.

    The fingerprint goes into the memo of the run.
    :func:`track_parquet_in_versions` reads it from there and records it. The
    Parquet file holds only the stac-geoparquet metadata. A footer key would
    need a private stac-geoparquet helper, because ``to_parquet`` replaces all
    schema metadata.

    Args:
        collection_path: Path to collection directory containing collection.json
            and item subdirectories.
        state: The run state. Pass the state that the call to
            :func:`track_parquet_in_versions` uses, or the record gets no
            fingerprint and the next run builds again.

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
    from portolan_cli.run_state import RunState

    run = state if state is not None else RunState.open(collection_path.parent)
    output_path = collection_path / PARQUET_FILENAME
    owned = owned_item_hrefs(collection_path / "collection.json")
    fingerprint = _items_fingerprint(owned)
    if fingerprint is None or not _mirror_is_current(run, collection_path, fingerprint):
        items = _load_item_dicts(collection_path, owned)
        # parse_stac_items_to_arrow returns a RecordBatchReader
        table = stac_geoparquet.arrow.parse_stac_items_to_arrow(items).read_all()
        stac_geoparquet.arrow.to_parquet(table, output_path)
        count("items_parquet.writes")
        run.invalidate(output_path)
    run.memo(_SOURCE_FINGERPRINT, output_path, lambda _path: fingerprint)
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


def stamp_file_fields(
    asset: dict[str, Any], base_dir: Path, *, state: RunState | None = None
) -> bool:
    """Refresh ``file:size``/``file:checksum`` from the bytes the asset points at.

    Public because ``viz.style`` stamps style assets the same way. The rule is one
    canonical home for the claim, so both writers share this function rather than
    each hashing on its own terms.

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
        state: The run state. Its memo hashes each file once per run.

    Returns:
        Whether the asset changed, so the caller writes collection.json only when
        there is something to write.
    """
    href = asset.get("href")
    if not isinstance(href, str) or not href or is_absolute_href(href):
        return False

    path = resolve_href(base_dir, href)
    if not path.exists():
        return _strip_file_fields(asset)
    if not path.is_file():
        return False

    if state is None:
        fields = file_fields(path)
    else:
        fields = file_fields_from(state.checksum(path), path.stat().st_size)
    if all(asset.get(name) == value for name, value in fields.items()):
        return False
    asset.update(fields)
    return True


def _declares_file_fields(assets: Any) -> bool:
    """Whether any asset in an assets mapping carries a ``file:`` field."""
    if not isinstance(assets, dict):
        return False
    return any(
        isinstance(asset, dict) and any(key.startswith("file:") for key in asset)
        for asset in assets.values()
    )


def _items_declare_file_fields(collection_json_path: Path) -> bool:
    """Whether any item the collection owns carries a ``file:`` field on an asset.

    Read from the items themselves rather than from a count beside them: the
    ``portolan:asset_count`` tally this used to consult is gone (issue #654),
    and a tally can be stale in a way the items cannot.

    The caller checks the collection's own assets first, so this walk only runs
    for a collection that has already lost every ``file:`` field of its own —
    the rare case, not the routine one.
    """
    for _href, item_path in owned_item_hrefs(collection_json_path):
        try:
            item = json.loads(item_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(item, dict) and _declares_file_fields(item.get("assets")):
            return True
    return False


def sync_file_extension(
    data: dict[str, Any], assets: dict[str, Any], collection_json_path: Path
) -> bool:
    """Declare the STAC file extension while an asset uses it, withdraw it after.

    Public alongside :func:`stamp_file_fields`, and called by ``viz.style`` for
    the same reason: an asset that carries ``file:`` fields obliges the
    collection to declare the extension, whichever writer stamped it.

    ``stac.declare_file_extension`` does this for the add and finalize paths, but
    it needs a ``pystac.Collection`` and this module works on raw JSON on purpose
    (pystac ``to_dict()`` can write absolute local paths into links).

    ``collection_thumbnail.register_collection_thumbnail`` needs the same logic
    too: it writes ``file:size`` and ``file:checksum`` on the thumbnail after
    both other writers have run, so a collection whose only ``file:``-bearing
    asset is the thumbnail carried the fields without the declaration (#654).

    Withdrawal matters because :func:`stamp_file_fields` strips the fields when
    the mirror disappears, which can leave the collection declaring an extension
    nothing uses. Both writers scope the question the same way — the collection's
    own assets *and* its items' — so neither withdraws a URI the other is about
    to redeclare, and they stop trading edits on collection.json.

    Args:
        data: Parsed collection.json, mutated in place.
        assets: The collection's asset dicts.
        collection_json_path: Path to the collection.json ``data`` came from,
            used to reach the items it owns.

    Returns:
        Whether the extension list changed.
    """
    from portolan_cli.stac import EXTENSION_URLS

    extensions = data.get("stac_extensions")
    if extensions is not None and not isinstance(extensions, list):
        # Malformed. Schema validation reports it; do not crash on .append here.
        return False

    url = EXTENSION_URLS["file"]
    in_use = _declares_file_fields(assets) or _items_declare_file_fields(collection_json_path)

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
    extensions.remove(url)
    return True


def declare_extensions_in_use(document: dict[str, Any]) -> bool:
    """Declare the file and table extensions whose fields one STAC document uses.

    Issue #1037: items carried ``file:size``, ``file:checksum`` and
    ``table:column_statistics`` while only their collection declared the URIs.
    A reader validates each document against the extensions it declares, so
    every document declares its own. Only the document's own fields count: its
    top level, its ``properties``, and its assets. The call never withdraws a
    URI. :func:`sync_file_extension` owns the withdrawal on a collection.

    Args:
        document: A parsed item or collection, mutated in place.

    Returns:
        Whether ``stac_extensions`` changed.
    """
    from portolan_cli.stac import EXTENSION_URLS

    holders: list[Any] = [document, document.get("properties")]
    assets = document.get("assets")
    if isinstance(assets, dict):
        holders.extend(assets.values())
    prefixes = {
        f"{name}:"
        for holder in holders
        if isinstance(holder, dict)
        for key in holder
        for name in ("file", "table")
        if key.startswith(f"{name}:")
    }
    extensions = document.get("stac_extensions")
    if not prefixes or (extensions is not None and not isinstance(extensions, list)):
        return False
    if extensions is None:
        extensions = document["stac_extensions"] = []
    missing = [EXTENSION_URLS[p[:-1]] for p in sorted(prefixes)]
    missing = [url for url in missing if url not in extensions]
    extensions.extend(missing)
    return bool(missing)


def _is_selected(asset: dict[str, Any], base_dir: Path, only: AbstractSet[Path] | None) -> bool:
    """Whether ``asset`` points at one of the resolved ``only``. All are when it is None."""
    if only is None:
        return True
    href = asset.get("href")
    if not isinstance(href, str) or not href or is_absolute_href(href):
        return False
    return resolve_href(base_dir, href).resolve() in only


def restamp_collection_assets(
    collection_dir: Path, *, only: AbstractSet[Path] | None = None
) -> bool:
    """Stamp ``file:size``/``file:checksum`` on the local assets of a collection.

    The one pass for a writer that changed files under a collection, such as
    ``readme`` (issue #930). It walks collection.json and every item the
    collection owns, stamps each asset with :func:`stamp_file_fields`, and
    declares the extensions each document uses. A document is written only
    when it changed.

    Args:
        collection_dir: Directory that holds collection.json.
        only: Stamp only the assets whose href resolves to one of these files.
            A stamp hashes every byte, so a writer that changed one file names
            it. None stamps every asset.

    Returns:
        Whether any document changed.
    """
    collection_json = collection_dir / "collection.json"
    if not collection_json.is_file():
        return False
    documents = [collection_json, *(path for _href, path in owned_item_hrefs(collection_json))]
    if only is not None:
        only = {path.resolve() for path in only}
    changed_any = False
    for path in documents:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        assets = data.get("assets") if isinstance(data, dict) else None
        if not isinstance(assets, dict):
            continue
        selected = [
            asset
            for asset in assets.values()
            if isinstance(asset, dict) and _is_selected(asset, path.parent, only)
        ]
        if not selected:
            # A document that names no selected file is not this writer's.
            continue
        # A list, not a generator: any() would stop at the first stamp.
        stamped = [stamp_file_fields(asset, path.parent) for asset in selected]
        changed = any(stamped)
        if path == collection_json:
            changed = sync_file_extension(data, assets, collection_json) or changed
        changed = declare_extensions_in_use(data) or changed
        if changed:
            write_json_atomic(path, data)
            changed_any = True
    return changed_any


def register_mirror_asset(collection_path: Path, *, state: RunState | None = None) -> None:
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
        state: The run state. Its memo hashes ``items.parquet`` once per run.

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

    # Match by key or by href, then set every match to the spec role. A catalog
    # written before "collection-mirror" existed has only the community role
    # "stac-items", which PTL-MIR-002 flags. An older asset scan also gave the
    # mirror the "data" role, which made it a tile source.
    matching = [
        asset
        for key, asset in assets.items()
        if key == asset_key or asset.get("href") == f"./{PARQUET_FILENAME}"
    ]
    if matching:
        for asset in matching:
            if asset.get("roles") != [ROLE_COLLECTION_MIRROR]:
                asset["roles"] = [ROLE_COLLECTION_MIRROR]
                modified = True
            # Refresh, do not skip: generate_items_parquet overwrote the bytes
            # moments ago, so an asset carried over from an earlier run describes
            # a file that no longer exists in that form.
            if stamp_file_fields(asset, collection_path, state=state):
                modified = True
    else:
        asset = {
            "href": f"./{PARQUET_FILENAME}",
            "type": PARQUET_MEDIA_TYPE,
            "title": "STAC items as GeoParquet",
            "roles": [ROLE_COLLECTION_MIRROR],
        }
        stamp_file_fields(asset, collection_path, state=state)
        assets[asset_key] = asset
        data["assets"] = assets
        modified = True

    if sync_file_extension(data, assets, collection_json_path):
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


_MIRROR_MESSAGE = "Generated items.parquet for STAC GeoParquet queries"


def track_parquet_in_versions(
    collection_path: Path,
    catalog_root: Path | None = None,
    *,
    amend_latest: bool = False,
    state: RunState | None = None,
) -> None:
    """Stage items.parquet in versions.json so push detects changes.

    The record goes into the run state, so the run commits one version for the
    collection. The checksum comes from the memo of the run, so
    ``items.parquet`` is hashed once per run (issue #995). On a plugin backend,
    the record goes to the local ``versions.json`` at once, as before #995. The
    plugin never gets the mirror, because it reads each Parquet asset as data.

    A mirror whose bytes are already tracked in the latest snapshot is a
    no-op: regenerating identical content records no new user intent, so it
    must not bump anything (the #683 double-versioning family). With
    ``amend_latest``, the record of the same bytes takes the new mtime and item
    fingerprint, because the version is the one this command writes.

    The item fingerprint comes from the memo of the run, where
    :func:`generate_items_parquet` puts it. A run that did not build the
    mirror records no fingerprint.

    Args:
        collection_path: Path to collection directory.
        catalog_root: Catalog root for catalog-root-relative hrefs. Defaults
            to the collection's parent, which is correct for top-level
            collections.
        amend_latest: Fold the mirror into the version the caller just wrote
            (the add flow) instead of creating a new one. Leave False when
            the latest version may already be published.
        state: The run state. Without one, the call opens its own and
            commits it.

    Raises:
        FileNotFoundError: If items.parquet doesn't exist.
    """
    from portolan_cli.version_ops import owned_run
    from portolan_cli.versions import generated_asset_record, track_generated_assets

    root = catalog_root if catalog_root is not None else collection_path.parent
    parquet_path = collection_path / PARQUET_FILENAME
    if not parquet_path.exists():
        raise FileNotFoundError(f"items.parquet not found at {parquet_path}")

    with owned_run(root, state) as run:
        digest = run.checksum(parquet_path)
        key, record = generated_asset_record(parquet_path, root, collection_path, digest)
        fingerprint: str | None = run.memo(_SOURCE_FINGERPRINT, parquet_path, lambda _path: None)
        record = replace(record, source_fingerprint=fingerprint)
        tracked = run.tracked_assets(collection_path).get(key)
        # For the same bytes, only a version that this command writes takes the
        # new mtime and fingerprint. A published version stays as it is.
        same_bytes = tracked is not None and tracked.sha256 == digest
        if same_bytes and (not amend_latest or tracked == record):
            return
        if (run.setting("backend") or "file") != "file":
            # A plugin backend reads each .parquet asset as table data, so the
            # mirror never goes to it. The local versions.json records it now.
            track_generated_assets(
                collection_path,
                [parquet_path],
                root,
                message=_MIRROR_MESSAGE,
                amend_latest=amend_latest,
                state=run,
            )
            return
        run.stage(
            collection_path.resolve().relative_to(root.resolve()).as_posix(),
            assets={key: record},
            message=None if amend_latest else _MIRROR_MESSAGE,
            amend=amend_latest,
        )


@dataclass
class MirrorReport:
    """The outcome of one ``generate_parquet_mirrors`` run.

    The function returns this report and writes no terminal output. The caller
    reports it, so a library caller does not load Click (issue #944).

    Attributes:
        generated: Collection IDs that received a new ``items.parquet``.
        failed: Collection IDs whose default generation failed, with the error.
    """

    generated: list[str] = field(default_factory=list)
    failed: dict[str, Exception] = field(default_factory=dict)


def generate_parquet_mirrors(
    catalog_root: Path,
    affected_collections: set[str],
    *,
    generate_parquet: bool,
    versioned_collections: set[str] | None = None,
    state: RunState | None = None,
) -> MirrorReport:
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
    re-raises. A default-generation failure goes into the report.

    Args:
        catalog_root: Catalog root directory.
        affected_collections: Collection IDs modified by the add command.
        generate_parquet: Whether ``--stac-geoparquet`` was passed.
        versioned_collections: Collections the add command wrote a version
            for in this run. Their mirror folds into that snapshot instead
            of bumping a second version (one add is one version, #683).
        state: The run state. Without one, the call opens its own and
            commits it when every collection is done.

    Returns:
        The collections that got a mirror and the ones that failed.
    """
    report = MirrorReport()
    if not affected_collections:
        return report

    from portolan_cli.config import coerce_bool
    from portolan_cli.version_ops import owned_run

    with owned_run(catalog_root, state) as run:
        for coll_id in affected_collections:
            coll_path = catalog_root / coll_id
            if not (coll_path / "collection.json").exists():
                continue

            # Opt-out setting per-collection with hierarchical lookup
            enabled = run.setting("parquet.enabled", collection_dir=coll_path)
            if not (generate_parquet or coerce_bool(enabled, default=True)):
                continue
            if count_items(coll_path) == 0:
                continue

            try:
                generate_items_parquet(coll_path, state=run)
                register_mirror_asset(coll_path, state=run)
                track_parquet_in_versions(
                    coll_path,
                    catalog_root,
                    amend_latest=coll_id in (versioned_collections or set()),
                    state=run,
                )
                report.generated.append(coll_id)
            except Exception as e:
                # Explicit --stac-geoparquet should fail the command
                if generate_parquet:
                    raise
                # The caller warns about a default-generation failure
                report.failed[coll_id] = e

    return report
