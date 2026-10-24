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
  ``published_files`` uses it to find the files that push publishes and
  clone downloads, so the two commands agree on one set.

The module imports only the standard library. A read module can call it
without the CLI or the geospatial stack.
"""

from __future__ import annotations

import json
import posixpath
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator
    from pathlib import Path


#: Link order for a written catalog, collection, or item: the order the links
#: fixer rebuilds (``validation.fixers._rebuild_links``). Other rels follow in
#: their own order. One order for every writer makes the links fixer a no-op on
#: a tree that add wrote, and a repaired item byte-identical to an added one.
_LINK_RANK = {"root": 0, "parent": 1, "collection": 2, "child": 3, "item": 3}


def in_link_order(links: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """``links`` in the order the links fixer writes. The sort is stable."""
    return sorted(links, key=lambda link: _LINK_RANK.get(str(link.get("rel")), len(_LINK_RANK)))


# The links that walk the catalog tree. Every other relative link of a STAC
# object names a file beside it, such as a style or a logo.
TREE_RELS = frozenset({"child", "item", "self", "root", "parent"})
# A catalog or a collection carries these files with or without a link.
SIDE_FILES = ("README.md", "AGENTS.md")
# The GDAL persistent auxiliary metadata (PAM) sidecar, such as x.tif.aux.xml.
# GDAL writes it beside a raster when it computes statistics.
GDAL_PAM_SUFFIX = ".aux.xml"


def published_files(document: dict[str, Any], document_rel: str) -> tuple[set[str], set[str]]:
    """Return the files that one STAC object publishes, and its item documents.

    The files are the targets of its asset hrefs and of its relative links
    outside the tree, the README.md and AGENTS.md beside it, and the GDAL PAM
    sidecar of each GeoTIFF it names. An absolute or a remote href names no
    file. Push uploads these files, and clone downloads them.

    Args:
        document: A parsed STAC object.
        document_rel: The catalog-relative POSIX path of the object.

    Returns:
        The catalog-relative POSIX paths of the files, and of the items that
        its ``item`` links name.
    """
    base = posixpath.dirname(document_rel)
    links = [link for link in document.get("links", []) if isinstance(link, dict)]
    named = [asset.get("href") for asset in document.get("assets", {}).values()]
    named += [link.get("href") for link in links if link.get("rel") not in TREE_RELS]
    items = [link.get("href") for link in links if link.get("rel") == "item"]

    def resolve(hrefs: list[Any]) -> set[str]:
        local = [h for h in hrefs if isinstance(h, str) and h and "://" not in h]
        return {posixpath.normpath(posixpath.join(base, h)) for h in local if h[0] != "/"}

    files = resolve(named) | {posixpath.join(base, name) for name in SIDE_FILES}
    files |= {f + GDAL_PAM_SUFFIX for f in files if f.lower().endswith((".tif", ".tiff"))}
    return files, resolve(items)


def published_tree(catalog_root: Path, documents: Iterable[str]) -> set[str]:
    """Return the files that local STAC objects, and the items below them, publish.

    Args:
        catalog_root: The catalog root directory.
        documents: Catalog-relative POSIX paths of catalogs or collections.

    Returns:
        The catalog-relative POSIX paths from ``published_files``, and the
        paths of the documents that the walk read.
    """
    pending = list(documents)
    files: set[str] = set()
    read: set[str] = set()
    while pending:
        rel = pending.pop()
        path = catalog_root / rel
        if rel in read or not path.is_file():
            continue
        read.add(rel)
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if not isinstance(document, dict):
            continue
        named, items = published_files(document, rel)
        files |= named
        pending += sorted(items)
    return files | read


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
    return _owned_item_hrefs(node_json_path, set())


def _owned_item_hrefs(node_json_path: Path, active: set[Path]) -> list[tuple[str, Path]]:
    """Walk ``owned_item_hrefs`` with ``active``, the catalogs in the current branch.

    A ``child`` link back to an active catalog is a cycle and is skipped. A
    catalog leaves ``active`` when its walk ends, so two branches that reach the
    same catalog both include its items.
    """
    if not node_json_path.exists():
        return []

    data = json.loads(node_json_path.read_text(encoding="utf-8"))
    owned: list[tuple[str, Path]] = []
    key = node_json_path.resolve()
    active.add(key)

    for link, href, path in iter_links(data, node_json_path.parent, ("item", "child")):
        if link["rel"] == "item":
            owned.append((href, path))
        elif path.name == "catalog.json" and path.resolve() not in active:
            owned.extend(_owned_item_hrefs(path, active))

    active.discard(key)
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
    return [
        *_visible_files(catalog_root, "catalog.json"),
        *_visible_files(catalog_root, "collection.json"),
    ]


def _visible_files(catalog_root: Path, name: str) -> list[Path]:
    """Every file called ``name`` below ``catalog_root``, outside dot-directories, sorted."""
    found: list[Path] = []
    for path in sorted(catalog_root.rglob(name), key=lambda p: p.as_posix()):
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


def versioned_collections(catalog_root: Path) -> list[str]:
    """The ID of every directory below the root that holds ``versions.json``.

    ``push`` and ``pull`` find collections by this rule. A directory with
    ``versions.json`` but no ``collection.json`` is not in ``catalog_collections``.
    ``push`` still uploads it, so ``status`` must report it.

    Args:
        catalog_root: Root directory of the catalog.

    Returns:
        Sorted collection IDs with forward slashes on every platform.
    """
    ids = {
        path.parent.relative_to(catalog_root).as_posix()
        for path in _visible_files(catalog_root, "versions.json")
        if path.parent != catalog_root
    }
    return sorted(ids)
