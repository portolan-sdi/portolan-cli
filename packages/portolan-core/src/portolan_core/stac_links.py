"""The one walker for a local STAC catalog tree."""

from __future__ import annotations

import json
import posixpath
from pathlib import PureWindowsPath
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote

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


def _published_path(base: str, href: object) -> str | None:
    """Return the catalog-relative path that an href names, or None for no file of the catalog.

    The href decodes before the checks, so a percent escape cannot hide a path that escapes.
    """
    if not isinstance(href, str) or not href or "://" in href:
        return None
    decoded = unquote(href)
    if (
        "://" in decoded
        or decoded.startswith("/")
        or "\\" in decoded
        or "\x00" in decoded
        or PureWindowsPath(decoded).drive
    ):
        return None
    path = posixpath.normpath(posixpath.join(base, decoded))
    return None if path == ".." or path.startswith("../") else path


def published_files(document: dict[str, Any], document_rel: str) -> tuple[set[str], set[str]]:
    """Return the files that one STAC object publishes, and its item documents."""
    base = posixpath.dirname(document_rel)
    links = [link for link in document.get("links", []) if isinstance(link, dict)]
    named = [asset.get("href") for asset in document.get("assets", {}).values()]
    named += [link.get("href") for link in links if link.get("rel") not in TREE_RELS]
    items = [link.get("href") for link in links if link.get("rel") == "item"]

    def resolve(hrefs: list[Any]) -> set[str]:
        paths = (_published_path(base, href) for href in hrefs)
        return {path for path in paths if path is not None}

    files = resolve(named) | {posixpath.join(base, name) for name in SIDE_FILES}
    files |= {f + GDAL_PAM_SUFFIX for f in files if f.lower().endswith((".tif", ".tiff"))}
    return files, resolve(items)


def published_tree(catalog_root: Path, documents: Iterable[str]) -> set[str]:
    """Return the files that local STAC objects, and the items below them, publish."""
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
    from rashid_core.api import resolve_local_href

    # The document name is a placeholder. Only its directory, base_dir, matters.
    local = resolve_local_href("_", href)
    if local is not None:
        return base_dir / local
    if "://" not in href and not href.startswith("/"):
        href = unquote(href)
    if href.startswith("./"):
        return base_dir / href[2:]
    if href.startswith("../"):
        return (base_dir / href).resolve()
    return base_dir / href


def contained_link_target(catalog_root: Path, base_dir: Path, href: str) -> Path | None:
    """Return the file that a link href names, or None when it lies outside the catalog.

    Symlinks resolve before the check, so no href or symlink leads out of the catalog.
    """
    if "://" in href:
        return None
    target = resolve_href(base_dir, href).resolve()
    return target if catalog_root.resolve() in target.parents else None


# The link walkers read only the documents that the links name. They do not
# use rashid's CatalogGraph.load, because it parses every JSON file in the tree.
def iter_links(
    data: dict[str, Any],
    base_dir: Path,
    rels: Iterable[str],
) -> Iterator[tuple[dict[str, Any], str, Path]]:
    """Yield ``(link, href, path)`` for each link of ``data`` with a rel in ``rels``."""
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
    """Every (href, path) pair for the items the object at ``node_json_path`` owns."""
    return _owned_item_hrefs(node_json_path, set())


def _owned_item_hrefs(node_json_path: Path, active: set[Path]) -> list[tuple[str, Path]]:
    """Walk ``owned_item_hrefs`` with ``active``, the catalogs in the current branch."""
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
    """Every ``catalog.json``/``collection.json`` in the *visible* catalog tree."""
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
    """Yield ``(path, data)`` for each visible catalog and collection that parses."""
    for path in visible_stac_files(catalog_root):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(data, dict):
            yield path, data


def catalog_collections(catalog_root: Path) -> list[str]:
    """The ID of every collection in the catalog, as a path below the root."""
    ids = {
        path.parent.relative_to(catalog_root).as_posix()
        for path in visible_stac_files(catalog_root)
        if path.name == "collection.json" and path.parent != catalog_root
    }
    return sorted(ids)


def versioned_collections(catalog_root: Path) -> list[str]:
    """The ID of every directory below the root that holds ``versions.json``."""
    ids = {
        path.parent.relative_to(catalog_root).as_posix()
        for path in _visible_files(catalog_root, "versions.json")
        if path.parent != catalog_root
    }
    return sorted(ids)
