"""The one walker for a local STAC catalog tree.

Every command that asks "which collections does this catalog hold?" or "which
items does this collection own?" calls this module. Before issue #944, nine
functions answered those questions with different strategies. ``list``,
``status``, and ``check`` then disagreed about the collections of one catalog.

Two walks live here:

- The containment walk finds every ``catalog.json`` and ``collection.json``
  below the root and skips dot-directories. ``catalog_collections`` uses it. It
  applies the rule of rashid's ``CatalogGraph``, which ``check`` uses, so a
  collection without a ``child`` link still counts. ``check`` reports that
  missing link as PTL-LNK-002.
- The link walk follows the ``rel`` links of one STAC object.
  ``owned_item_hrefs`` uses it to find the items of a collection.

The module imports only the standard library. A read module can call it
without the CLI or the geospatial stack.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator
    from pathlib import Path


def resolve_href(base_dir: Path, href: str) -> Path:
    """Resolve a STAC link href against the directory holding the linking object."""
    if href.startswith("./"):
        return base_dir / href[2:]
    if href.startswith("../"):
        return (base_dir / href).resolve()
    return base_dir / href


def iter_links(
    data: dict[str, Any],
    base_dir: Path,
    rels: Iterable[str],
) -> Iterator[tuple[dict[str, Any], str, Path]]:
    """Yield ``(link, href, path)`` for each link of ``data`` with a rel in ``rels``.

    A link without a non-empty string href is skipped. The link dict is the
    one inside ``data``, so a caller can change it in place.

    Args:
        data: A parsed STAC object.
        base_dir: Directory holding the file ``data`` came from.
        rels: Link relations to yield.
    """
    wanted = frozenset(rels)
    links = data.get("links")
    if not isinstance(links, list):
        return
    for link in links:
        if not isinstance(link, dict) or link.get("rel") not in wanted:
            continue
        href = link.get("href")
        if not isinstance(href, str) or not href:
            continue
        yield link, href, resolve_href(base_dir, href)


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
    owned: list[tuple[str, Path]] = []

    for link, href, path in iter_links(data, node_json_path.parent, ("item", "child")):
        if link["rel"] == "item":
            owned.append((href, path))
        elif path.name == "catalog.json":
            owned.extend(owned_item_hrefs(path))

    return owned


def visible_stac_files(catalog_root: Path) -> list[Path]:
    """Every ``catalog.json``/``collection.json`` in the *visible* catalog tree.

    Dot-directories (``.portolan/``, ``.git/``, editor scratch dirs) hold caches
    and backups, not published STAC objects; a sweep that descends into them
    rewrites files no publisher asked about. Shared by every catalog-wide sweep
    so they all walk exactly the same set.

    Args:
        catalog_root: Root directory of the catalog.

    Returns:
        Sorted catalog paths first, then sorted collection paths.
    """
    found: list[Path] = []
    for pattern in ("catalog.json", "collection.json"):
        for path in sorted(catalog_root.rglob(pattern)):
            rel_parts = path.parent.relative_to(catalog_root).parts
            if any(part.startswith(".") for part in rel_parts):
                continue
            found.append(path)
    return found


def iter_stac_objects(catalog_root: Path) -> Iterator[tuple[Path, dict[str, Any]]]:
    """Yield ``(path, data)`` for each visible catalog and collection that parses.

    A file that cannot be read, is not JSON, or holds no JSON object is skipped.
    ``check`` reports such a file. A catalog-wide sweep does not.

    Args:
        catalog_root: Root directory of the catalog.
    """
    for path in visible_stac_files(catalog_root):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(data, dict):
            yield path, data


def catalog_collections(catalog_root: Path) -> list[str]:
    """The ID of every collection in the catalog, as a path below the root.

    A collection is a directory below the root that holds ``collection.json``.
    A nested collection has an ID like ``climate/hittekaart``. A link from the
    parent is not necessary. This is the set ``check`` reports, so ``list`` and
    ``status`` report the same collections as ``check`` (issue #944).

    Args:
        catalog_root: Root directory of the catalog.

    Returns:
        Sorted collection IDs with forward slashes on every platform.
    """
    ids = {
        path.parent.relative_to(catalog_root).as_posix()
        for path in visible_stac_files(catalog_root)
        if path.name == "collection.json" and path.parent != catalog_root
    }
    return sorted(ids)
