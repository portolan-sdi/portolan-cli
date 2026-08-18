"""Date a catalog by the markers an older Portolan left in it.

Three generations are detectable, and none is something rashid can report. A
rule fires on a spec requirement an object fails; these are properties the spec
has since removed, so no requirement names them and no rule can. `check` still
owes the operator a sentence about each, which is what this module writes.

The first marker is a catalog published before the versioned profile schema URI
existed, when Portolan described itself through ``portolan:``-prefixed custom
fields instead. Those catalogs are not conformant and never were (decision 1 in
the rashid migration: acceptable pre-1.0). Saying so in one sentence beats
leaving the operator to infer it from a PTL-CNF-001 on every object.

The second is the ``portolan:styles`` manifest, written up to 1.0.0b0 and
removed from the spec in favor of the ``default`` asset role (issue #739).

The third is the rest of the ``portolan:`` fields Portolan emitted through
1.0.0b0, dropped when the spec confirmed it defines none (issue #654). That one
is deliberately blind to the schema URI: the catalogs carrying those fields are
exactly the ones that also declare the URI, so gating on it would report
nothing.

The detectors are independent, and a catalog can trip any, all, or none.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from portolan_cli.constants import (
    LEGACY_STYLE_MANIFEST_FIELD,
    PORTOLAN_SCHEMA_URI,
    REMOVED_PORTOLAN_FIELDS,
)

#: Custom fields whose presence dates a catalog. Every ``portolan:`` field
#: Portolan ever wrote qualifies: the spec defines none of them, so a catalog
#: carrying any was generated before the version that stopped writing it.
LEGACY_FIELDS = REMOVED_PORTOLAN_FIELDS

#: What :func:`detect_removed_fields` looks for. The style manifest is excluded
#: because :func:`detect_style_manifest` reports it with a remedy of its own.
_REMOVED_FIELDS_TO_REPORT = tuple(
    field for field in REMOVED_PORTOLAN_FIELDS if field != LEGACY_STYLE_MANIFEST_FIELD
)

_MESSAGE = (
    "This catalog predates the Portolan profile schema: it declares no schema URI and still "
    "carries portolan: custom fields. Every object will fail PTL-CNF-001 until it is "
    "regenerated. Run `portolan check --fix` to stamp the schema URI and scaffold what the "
    "current generator emits."
)

_STYLE_MANIFEST_MESSAGE = (
    f"A collection still carries {LEGACY_STYLE_MANIFEST_FIELD}, which the spec removed. Clients "
    "read a collection's styles off the 'style' asset role and its default off the 'default' "
    "role, so the manifest is a stale second copy nothing reads. Run `portolan check --fix` to "
    "delete it, or re-run `portolan add --pmtiles` on the affected collections to rewrite their "
    "style assets."
)

_REMOVED_FIELDS_MESSAGE = (
    "This catalog carries portolan: fields the spec does not define. The prefix is reserved and "
    "empty: the profile schema URI dates the catalog, asset roles name what an asset is, and a "
    "consumer sums file:size itself, so nothing reads them. Run `portolan check --fix` to strip "
    "them."
)

#: Depth of the collection scan. Collections sit one level below the root
#: (nests catalogs, never collections), and one legacy marker is
#: enough — this does not walk the whole tree to count them.
_SCAN_GLOB = "*/collection.json"


def _load(path: Path) -> dict[str, Any] | None:
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return doc if isinstance(doc, dict) else None


def _has_schema_uri(doc: dict[str, Any]) -> bool:
    extensions = doc.get("stac_extensions")
    return isinstance(extensions, list) and PORTOLAN_SCHEMA_URI in extensions


def _has_legacy_field(doc: dict[str, Any]) -> bool:
    return any(field in doc for field in LEGACY_FIELDS)


def detect_legacy_generation(root: Path) -> str | None:
    """Return an explanatory note when ``root`` is a pre-schema-URI catalog.

    A catalog is legacy when its root catalog.json declares no Portolan schema
    URI *and* a ``portolan:`` custom field survives on the root or on one of its
    collections. Both halves are required: a catalog that already carries the
    schema URI is current whatever else it holds, and a plain STAC catalog
    Portolan never touched is not ours to date.

    Args:
        root: Catalog root directory.

    Returns:
        A message naming the remedy, or None when the catalog is current,
        missing, or unreadable.
    """
    catalog = _load(root / "catalog.json")
    if catalog is None or _has_schema_uri(catalog):
        return None

    if _has_legacy_field(catalog):
        return _MESSAGE

    for collection_path in sorted(root.glob(_SCAN_GLOB)):
        collection = _load(collection_path)
        if collection is not None and _has_legacy_field(collection):
            return _MESSAGE

    return None


def detect_style_manifest(root: Path) -> str | None:
    """Return an explanatory note when a collection still lists styles by hand.

    Walks the whole tree rather than the direct children :data:`_SCAN_GLOB`
    matches, because a collection nests under intermediate catalogs at any
    depth and a manifest hiding three levels down is the one nobody finds by
    hand. The message does not name the collection, so the walk returns on the
    first hit; a current catalog is the case that pays for the full pass, once
    per `check` run against a tree rashid reads in full anyway.

    Args:
        root: Catalog root directory.

    Returns:
        A message naming the remedy, or None when no collection carries the
        removed property.
    """
    for collection_path in root.rglob("collection.json"):
        collection = _load(collection_path)
        if collection is not None and LEGACY_STYLE_MANIFEST_FIELD in collection:
            return _STYLE_MANIFEST_MESSAGE
    return None


def _carries_removed_field(doc: dict[str, Any]) -> bool:
    """Whether a STAC object holds a removed field anywhere it could have landed.

    Three homes, because that is where they were written: the object itself
    (the aggregates, the manifests), an item's ``properties`` (the provisional
    marker), and an asset (the glob and the managed flag).
    """
    if any(field in doc for field in _REMOVED_FIELDS_TO_REPORT):
        return True
    properties = doc.get("properties")
    if isinstance(properties, dict) and any(
        field in properties for field in _REMOVED_FIELDS_TO_REPORT
    ):
        return True
    assets = doc.get("assets")
    if not isinstance(assets, dict):
        return False
    return any(
        isinstance(asset, dict) and any(field in asset for field in _REMOVED_FIELDS_TO_REPORT)
        for asset in assets.values()
    )


def _item_paths(collection_path: Path) -> list[Path]:
    """The items a collection owns, or nothing when it cannot be read.

    Items are found through the collection's ``rel="item"`` links, because
    Portolan names an item file after the item (``scene-001/scene-001.json``)
    and no filename pattern finds them all. An unreadable collection is
    somebody else's finding, so it yields no items rather than raising.
    """
    from portolan_cli.stac_parquet import owned_item_hrefs

    try:
        return [path for _href, path in owned_item_hrefs(collection_path)]
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return []


def _items_carry_removed_field(collection_path: Path) -> bool:
    """Whether an item the collection owns carries a removed field."""
    for item_path in _item_paths(collection_path):
        item = _load(item_path)
        if item is not None and _carries_removed_field(item):
            return True
    return False


def detect_removed_fields(root: Path) -> str | None:
    """Return an explanatory note when any object still carries a removed field.

    Walks the root catalog, then every collection, then the items each one owns,
    returning on the first hit: the message names no object, so counting them
    buys nothing. Items come last because reading them costs the most and
    ``portolan:datetime_provisional`` is the only field that lives there.

    Args:
        root: Catalog root directory.

    Returns:
        A message naming the remedy, or None when no object carries one.
    """
    catalog = _load(root / "catalog.json")
    if catalog is not None and _carries_removed_field(catalog):
        return _REMOVED_FIELDS_MESSAGE

    collection_paths = sorted(root.rglob("collection.json"))
    for collection_path in collection_paths:
        collection = _load(collection_path)
        if collection is not None and _carries_removed_field(collection):
            return _REMOVED_FIELDS_MESSAGE

    for collection_path in collection_paths:
        if _items_carry_removed_field(collection_path):
            return _REMOVED_FIELDS_MESSAGE

    return None


def detect_legacy_notes(root: Path) -> str | None:
    """Every legacy marker `check` found in ``root``, as one note.

    The detectors are independent, so a catalog can trip several. They are joined
    into the single note the report carries rather than given a channel each:
    the operator reads one warning block, and the ``legacy_note`` key in the
    JSON payload keeps the shape agents already parse.

    Args:
        root: Catalog root directory.

    Returns:
        The notes separated by a blank line, or None when the catalog is
        current.
    """
    detectors = (detect_legacy_generation, detect_style_manifest, detect_removed_fields)
    notes = [note for note in (detect(root) for detect in detectors) if note]
    return "\n\n".join(notes) if notes else None
