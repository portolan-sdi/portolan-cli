"""Detect catalogs generated before Portolan emitted the profile schema URI.

Portolan published catalogs for months without declaring the versioned Portolan
schema URI, describing itself instead through ``portolan:``-prefixed custom
fields. Those catalogs are not conformant and never were (decision 1 in the
rashid migration: acceptable pre-1.0). What matters is that `check` says so in
one sentence, instead of leaving the user to infer it from a PTL-CNF-001 on
every object.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from portolan_cli.constants import PORTOLAN_SCHEMA_URI

#: Custom fields only pre-#654 Portolan emitted. Their presence dates the
#: catalog; ``stac.py`` stopped writing them when the profile schema landed.
LEGACY_FIELDS = ("portolan:version", "portolan:geospatial")

_MESSAGE = (
    "This catalog predates the Portolan profile schema: it declares no schema URI and still "
    "carries portolan: custom fields. Every object will fail PTL-CNF-001 until it is "
    "regenerated. Run `portolan check --fix` to stamp the schema URI and scaffold what the "
    "current generator emits."
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
