"""Date a catalog by the markers an older Portolan left in it.

Two generations are detectable, and neither is something rashid can report. A
rule fires on a spec requirement an object fails; these are properties the spec
has since removed, so no requirement names them and no rule can. `check` still
owes the operator a sentence about each, which is what this module writes.

The first marker is a catalog published before the versioned profile schema URI
existed, when Portolan described itself through ``portolan:``-prefixed custom
fields instead. Those catalogs are not conformant and never were (decision 1 in
the rashid migration: acceptable pre-1.0). Saying so in one sentence beats
leaving the operator to infer it from a PTL-CNF-001 on every object.

The second is the ``portolan:styles`` manifest, written up to 1.0.0b0 and
removed from the spec in favor of the ``default`` asset role (issue #739). A
catalog carrying it is otherwise current, so the two detectors are independent
and a catalog can trip either, both, or neither.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from portolan_cli.constants import LEGACY_STYLE_MANIFEST_FIELD, PORTOLAN_SCHEMA_URI

#: Custom fields only pre-#654 Portolan emitted. Their presence dates the
#: catalog; ``stac.py`` stopped writing them when the profile schema landed.
LEGACY_FIELDS = ("portolan:version", "portolan:geospatial")

_MESSAGE = (
    "This catalog predates the Portolan profile schema: it declares no schema URI and still "
    "carries portolan: custom fields. Every object will fail PTL-CNF-001 until it is "
    "regenerated. Run `portolan check --fix` to stamp the schema URI and scaffold what the "
    "current generator emits."
)

_STYLE_MANIFEST_MESSAGE = (
    f"A collection still carries {LEGACY_STYLE_MANIFEST_FIELD}, which the spec removed. Clients "
    "read a collection's styles off the 'style' asset role and its default off the 'default' "
    "role, so the manifest is a stale second copy nothing reads. Re-run `portolan add --pmtiles` "
    "on the affected collections to rewrite their style assets, or delete the property."
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


def detect_legacy_notes(root: Path) -> str | None:
    """Every legacy marker `check` found in ``root``, as one note.

    The detectors are independent, so a catalog can trip both. They are joined
    into the single note the report carries rather than given a channel each:
    the operator reads one warning block, and the ``legacy_note`` key in the
    JSON payload keeps the shape agents already parse.

    Args:
        root: Catalog root directory.

    Returns:
        The notes separated by a blank line, or None when the catalog is
        current.
    """
    notes = [note for note in (detect_legacy_generation(root), detect_style_manifest(root)) if note]
    return "\n\n".join(notes) if notes else None
